using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using Dapper;
using Resonance;
using Resonance.Models;
using Resonance.Repo.InternalModels;
using Newtonsoft.Json;
using MySql.Data.MySqlClient;

namespace Resonance.Repo.Database
{
    //2627: Duplicate key
    //1205: Deadlock victim

    public class MySqlEventingRepo : DbEventingRepo, IEventingRepo
    {
        /// <summary>
        /// Creates a new MsSqlEventingRepo.
        /// </summary>
        /// <param name="conn">IDbConnection to use. If not yet opened, it will be opened here.</param>
        public MySqlEventingRepo(MySqlConnection conn)
            : base(conn)
        {
        }

        public override int UpdateLastConsumedSubscriptionEvent(SubscriptionEvent subscriptionEvent)
        {
            var query = "INSERT INTO LastConsumedSubscriptionEvent (SubscriptionId, FunctionalKey, PublicationDateUtc)" +
                " VALUES(@subscriptionId, @functionalKey, @publicationDateUtc)" +
                " ON DUPLICATE KEY UPDATE PublicationDateUtc = @publicationDateUtc";

            return TranExecute(query, new Dictionary<string, object>
            {
                { "@subscriptionId", subscriptionEvent.SubscriptionId },
                { "@functionalKey", subscriptionEvent.FunctionalKey },
                { "@publicationDateUtc", subscriptionEvent.PublicationDateUtc },
            });
        }

        protected override IEnumerable<ConsumableEvent> ConsumeNextForSubscription(Subscription subscription, int visibilityTimeout, int maxCount)
        {
            // TODO: Optimize when not ordered: use old code

            // 1. Lock and get ids
            var lockedIds = LockNextSubscriptionEvents(subscription.Id, subscription.Ordered, visibilityTimeout, maxCount);

            // 2. Get se details
            var ces = new List<ConsumableEvent>();
            foreach (var sId in lockedIds)
            {
                ces.Add(GetConsumableEvent(sId));
            }

            return ces;
        }

        /// <summary>
        /// Gets a consumable event for a specific subscriptioneventId
        /// </summary>
        /// <param name="sId">SubscriptionEventId</param>
        /// <returns></returns>
        private ConsumableEvent GetConsumableEvent(Int64 sId)
        {
            var query = $"select se.Id, se.DeliveryKey, se.FunctionalKey, se.InvisibleUntilUtc, se.PayloadId" + // Get the minimal amount of data
                " from SubscriptionEvent se where se.Id = @sId";
            var ce = TranQuery<ConsumableEvent>(query, new { sId = sId }).SingleOrDefault();
            if (ce != null && ce.PayloadId.HasValue)
            {
                ce.Payload = GetPayload(ce.PayloadId.Value);
                ce.PayloadId = null; // No reason to keep it
            }
            return ce;
        }

        private IEnumerable<Int64> LockNextSubscriptionEvents(string subscriptionId, bool ordered, int visibilityTimeout, int maxCount)
        {
            var maxCountToUse = ordered ? 1 : maxCount; // Fix to one, when using functional ordering

            var lockedIds = new List<Int64>();

            if (!ordered)
            {
                for (int i = 0; i < maxCountToUse; i++)
                {
                    var deliveryKey = Guid.NewGuid().ToString();
                    var invisibleUntilUtc = DateTime.UtcNow.AddSeconds(visibilityTimeout);

                    var query = "set @updatedSeId := 0;" +
                        " update SubscriptionEvent" +
                        " set InvisibleUntilUtc = @invisibleUntilUtc," +
                        "   DeliveryCount = DeliveryCount + 1," +
                        "   DeliveryKey = @deliveryKey," +
                        "   DeliveryDateUtc = @utcNow," +
                        "   Id = (select @updatedSeId := Id)" +
                        " where Id in (" +
                        "   select Id from (" +
                        "     select      se.Id" +
                        "     from        SubscriptionEvent se" +
                        "     join        Subscription s on s.Id = se.SubscriptionId" +
                        "     where       se.SubscriptionId = @subscriptionId" +
                        "       and       (se.DeliveryDelayedUntilUtc IS NULL OR se.DeliveryDelayedUntilUtc < @utcNow)" +
                        "       and       (se.ExpirationDateUtc IS NULL OR se.ExpirationDateUtc > @utcNow)" +
                        "       and       (se.InvisibleUntilUtc IS NULL OR se.InvisibleUntilUtc < @utcNow)" +
                        "       and       (s.MaxDeliveries = 0 OR s.MaxDeliveries > se.DeliveryCount)" +
                        "     order by    se.Priority DESC, se.PublicationDateUtc ASC" +
                        "     limit 1) tmp" +
                        ");" +
                        "select @updatedSeId;";

                    var sId = TranQuery<Int64?>(query,
                        new Dictionary<string, object>
                        {
                        { "@subscriptionId", subscriptionId.ToDbKey() },
                        { "@utcNow", DateTime.UtcNow },
                        { "@deliveryKey", deliveryKey },
                        { "@invisibleUntilUtc", invisibleUntilUtc },
                        }).SingleOrDefault();
                    if (sId.HasValue && sId.Value > 0) // MySql returns the 0 used to initialize this variable with.
                        lockedIds.Add(sId.Value);
                    else
                        break; // Break out of for-loop
                }
            }
            else
            {

                for (int i = 0; i < maxCountToUse; i++)
                {
                    var deliveryKey = Guid.NewGuid().ToString();
                    var invisibleUntilUtc = DateTime.UtcNow.AddSeconds(visibilityTimeout);

                    var query = "set @updatedSeId := 0;" +
                        " update SubscriptionEvent" +
                        " set InvisibleUntilUtc = @invisibleUntilUtc," +
                        "   DeliveryCount = DeliveryCount + 1," +
                        "   DeliveryKey = @deliveryKey," +
                        "   DeliveryDateUtc = @utcNow," +
                        "   Id = (select @updatedSeId := Id)" +
                        " where Id in (" +
                        "   select Id from (" +
                        "     select      se.Id" +
                        "     from        SubscriptionEvent se" +
                        "     join        Subscription s on s.Id = se.SubscriptionId" +
                        "     left join   SubscriptionEvent seInv" +
                        "       on        seInv.SubscriptionId = se.SubscriptionId" +
                        "       and       seInv.FunctionalKey = se.FunctionalKey" +
                        "       and       seInv.Id != se.Id" +
                        "       and       seInv.InvisibleUntilUtc IS NOT NULL" +
                        "       and       seInv.InvisibleUntilUtc > @utcNow" +
                        "     left join   LastConsumedSubscriptionEvent lc" + // Functional ordering
                        "       on        lc.SubscriptionId = se.SubscriptionId" +
                        "       and       lc.FunctionalKey = se.FunctionalKey" +
                        "     where       se.SubscriptionId = @subscriptionId" +
                        "       and       (se.DeliveryDelayedUntilUtc IS NULL OR se.DeliveryDelayedUntilUtc < @utcNow)" +
                        "       and       (se.ExpirationDateUtc IS NULL OR se.ExpirationDateUtc > @utcNow)" +
                        "       and       (se.InvisibleUntilUtc IS NULL OR se.InvisibleUntilUtc < @utcNow)" +
                        "       and       (s.MaxDeliveries = 0 OR s.MaxDeliveries > se.DeliveryCount)" +
                        "       and       seInv.Id IS NULL" + // Geen in behandeling nu
                        "       and       (lc.SubscriptionId IS NULL OR (lc.PublicationDateUtc <= se.PublicationDateUtc))" + // <=, because publicationdateutc may be same :-s
                        "     order by    se.Priority DESC, se.PublicationDateUtc ASC" +
                        "     limit 1) tmp" +
                        ");" +
                        "select @updatedSeId;";

                    var sId = TranQuery<Int64?>(query,
                        new Dictionary<string, object>
                        {
                        { "@subscriptionId", subscriptionId.ToDbKey() },
                        { "@utcNow", DateTime.UtcNow },
                        { "@deliveryKey", deliveryKey },
                        { "@invisibleUntilUtc", invisibleUntilUtc },
                        }).SingleOrDefault();
                    if (sId.HasValue && sId.Value > 0) // MySql returns the 0 used to initialize this variable with.
                        lockedIds.Add(sId.Value);
                    else
                        break; // Break out of for-loop
                }
            }
            return lockedIds;
        }
    }
}
