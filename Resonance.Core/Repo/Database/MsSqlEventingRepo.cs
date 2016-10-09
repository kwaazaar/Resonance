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
using System.Data.Common;

namespace Resonance.Repo.Database
{
    //2627: Duplicate key
    //1205: Deadlock victim

    public class MsSqlEventingRepo : DbEventingRepo, IEventingRepo
    {
        /// <summary>
        /// ConnectionStringBuilder for easy access to properties of the connectionstring used.
        /// </summary>
        private readonly SqlConnectionStringBuilder _connStringBuilder;

        /// <summary>
        /// Creates a new MsSqlEventingRepo.
        /// </summary>
        /// <param name="conn">IDbConnection to use.</param>
        public MsSqlEventingRepo(SqlConnection conn)
            : base(conn)
        {
            _connStringBuilder = new SqlConnectionStringBuilder(conn.ConnectionString);
        }

        protected override bool CanRetry(DbException dbEx, int attempts)
        {
            var sqlEx = dbEx as SqlException;
            if (sqlEx != null && sqlEx.Number == 1205 && attempts < 3) // After 3 attempts give up deadlocks
                return true;
            else
                return base.CanRetry(dbEx, attempts);
        }

        public override bool ParallelQueriesSupport { get { return _connStringBuilder.MultipleActiveResultSets; } } // 'MARS' must be enabled in the connectionstring

        public override string GetLastAutoIncrementValue
        {
            get { return "SCOPE_IDENTITY()"; }
        }

        public override async Task<int> UpdateLastConsumedSubscriptionEvent(SubscriptionEvent subscriptionEvent)
        {
            var query = "MERGE LastConsumedSubscriptionEvent AS target" +
                            " USING(SELECT @subscriptionId, @functionalKey) as source(SubscriptionId, FunctionalKey)" +
                            " ON(target.SubscriptionId = source.SubscriptionId AND target.FunctionalKey = source.FunctionalKey)" +
                            " WHEN MATCHED THEN UPDATE SET PublicationDateUtc = @publicationDateUtc" +
                            " WHEN NOT MATCHED THEN INSERT(SubscriptionId, FunctionalKey, PublicationDateUtc) VALUES(source.SubscriptionId, source.FunctionalKey, @publicationDateUtc);";

            return await TranExecuteAsync(query, new Dictionary<string, object>
            {
                { "@subscriptionId", subscriptionEvent.SubscriptionId },
                { "@functionalKey", subscriptionEvent.FunctionalKey },
                { "@publicationDateUtc", subscriptionEvent.PublicationDateUtc },
            }).ConfigureAwait(false);
        }

        protected override async Task<IEnumerable<ConsumableEvent>> ConsumeNextForSubscription(Subscription subscription, int visibilityTimeout, int maxCount)
        {
            int maxCountToUse = maxCount;

            var ces = new List<ConsumableEvent>();

            if (!subscription.Ordered)
            {
                var invisibleUntilUtc = DateTime.UtcNow.AddSeconds(visibilityTimeout);

                var query = "DECLARE @l_PBEIds TABLE(ID bigint)\n"
                    + ";WITH DE AS ("
                    + $" select TOP {maxCountToUse} se.Id, se.DeliveryKey, se.InvisibleUntilUtc, se.DeliveryCount, se.DeliveryDateUtc" // Top 1!!!
                    + " from SubscriptionEvent se"
                    + " join Subscription s on s.Id = se.SubscriptionId" // Needed for MaxRetries
                    + " where se.SubscriptionId = @subscriptionId"
                    + " and (se.DeliveryDelayedUntilUtc IS NULL OR se.DeliveryDelayedUntilUtc < @utcNow)" // Must be allowed to be delivered
                    + " and (se.ExpirationDateUtc IS NULL OR se.ExpirationDateUtc > @utcNow)" // Must not yet have expired
                    + " and (se.InvisibleUntilUtc IS NULL OR se.InvisibleUntilUtc < @utcNow)" // Must not be 'locked'/made invisible by other consumer
                    + " and (s.MaxDeliveries = 0 OR s.MaxDeliveries > se.DeliveryCount)" // Must not have reached max. allowed delivery attempts
                    + " order by se.Priority DESC, se.PublicationDateUtc ASC" // Warning: prio can mess everything up!
                    + ") UPDATE DE"
                    + " SET InvisibleUntilUtc = @invisibleUntilUtc,"
                    + "    DeliveryCount = DE.DeliveryCount + 1,"
                    + "    DeliveryKey = NEWID()," // Let SQL server generate the deliverykey
                    + "    DeliveryDateUtc = @utcNow"
                    + " OUTPUT inserted.Id INTO @l_PBEIds"
                    + " select se.Id, se.DeliveryKey, se.FunctionalKey, se.InvisibleUntilUtc, se.PayloadId"
                    + " from SubscriptionEvent se"
                    + " join @l_PBEIds pbe on pbe.Id = se.Id";

                ces.AddRange(await TranQueryAsync<ConsumableEvent>(query,
                        new Dictionary<string, object>
                        {
                            { "@subscriptionId", subscription.Id.Value },
                            { "@utcNow", DateTime.UtcNow },
                            { "@invisibleUntilUtc", invisibleUntilUtc },
                        }).ConfigureAwait(false));
            }
            else // Functional ordering
            {
                for (int i = 0; i < maxCountToUse; i++) // Ordered altijd per 1 raadplegen
                {
                    var deliveryKey = Guid.NewGuid().ToString();
                    var invisibleUntilUtc = DateTime.UtcNow.AddSeconds(visibilityTimeout);

                    var query = "DECLARE @l_PBEIds TABLE(ID bigint)\n"
                        + ";WITH DE AS ("
                        + " select TOP 1 se.Id, se.DeliveryKey, se.InvisibleUntilUtc, se.DeliveryCount, se.DeliveryDateUtc" // Top 1!!!
                        + " from SubscriptionEvent se"
                        + " join Subscription s on s.Id = se.SubscriptionId" // Needed for MaxRetries
                        + " left join SubscriptionEvent seInv" // Controle of funckey al in behandeling (invisible)
                        + "   on seInv.SubscriptionId = se.SubscriptionId"
                        + "   and seInv.FunctionalKey = se.FunctionalKey"
                        + "   and seInv.Id != se.Id"
                        + "   and seInv.InvisibleUntilUtc IS NOT NULL"
                        + "   and seInv.InvisibleUntilUtc > @utcNow"
                        + " left join LastConsumedSubscriptionEvent lc" // For functional ordering
                        + "   on lc.SubscriptionId = se.SubscriptionId and lc.FunctionalKey = se.FunctionalKey"
                        + " where se.SubscriptionId = @subscriptionId"
                        + " and (se.DeliveryDelayedUntilUtc IS NULL OR se.DeliveryDelayedUntilUtc < @utcNow)" // Must be allowed to be delivered
                        + " and (se.ExpirationDateUtc IS NULL OR se.ExpirationDateUtc > @utcNow)" // Must not yet have expired
                        + " and (se.InvisibleUntilUtc IS NULL OR se.InvisibleUntilUtc < @utcNow)" // Must not be 'locked'/made invisible by other consumer
                        + " and (s.MaxDeliveries = 0 OR s.MaxDeliveries > se.DeliveryCount)" // Must not have reached max. allowed delivery attempts
                        + "	and	seInv.Id IS NULL" // Geen in behandeling nu
                        + " and	(lc.SubscriptionId IS NULL OR (lc.PublicationDateUtc < se.PublicationDateUtc))" // Newer than last published (TODO: PRIORITY!!)
                        + " order by se.Priority DESC, se.PublicationDateUtc ASC" // Warning: prio can mess everything up!
                        + ") UPDATE DE"
                        + " SET InvisibleUntilUtc = @invisibleUntilUtc,"
                        + "    DeliveryCount = DE.DeliveryCount + 1,"
                        + "    DeliveryKey = @deliveryKey,"
                        + "    DeliveryDateUtc = @utcNow"
                        + " OUTPUT inserted.Id INTO @l_PBEIds"
                        + " select se.Id, se.DeliveryKey, se.FunctionalKey, se.InvisibleUntilUtc, se.PayloadId"
                        + " from SubscriptionEvent se"
                        + " join @l_PBEIds pbe on pbe.Id = se.Id";

                    var cesInLoop = await TranQueryAsync<ConsumableEvent>(query,
                                        new Dictionary<string, object>
                                        {
                                            { "@subscriptionId", subscription.Id.Value },
                                            { "@utcNow", DateTime.UtcNow },
                                            { "@deliveryKey", deliveryKey },
                                            { "@invisibleUntilUtc", invisibleUntilUtc },
                                        }).ConfigureAwait(false);
                    if (cesInLoop.Count() > 0)
                        ces.AddRange(cesInLoop);
                    else
                        break; // Nothing found anymore, escape from the for-loop
                }
            }

            foreach (var ce in ces)
            {
                if (ce.PayloadId.HasValue)
                {
                    ce.Payload = await GetPayloadAsync(ce.PayloadId.Value).ConfigureAwait(false);
                    ce.PayloadId = null; // No reason to keep it
                }
            }

            return ces;
        }
    }
}
