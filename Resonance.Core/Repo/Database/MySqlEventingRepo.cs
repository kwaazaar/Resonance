using MySql.Data.MySqlClient;
using Resonance.Models;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;

namespace Resonance.Repo.Database
{
    //2627: Duplicate key
    //1205: Deadlock victim

    public class MySqlEventingRepo : DbEventingRepo, IEventingRepo
    {
        private readonly int _maxRetriesOnDeadlock;

        /// <summary>
        /// Creates a new MsSqlEventingRepo.
        /// Defaults to a maximum of 1 retries on a deadlock
        /// </summary>
        /// <param name="conn"></param>
        /// <param name="commandTimeout">Commandtimeout to use</param>
        public MySqlEventingRepo(MySqlConnection conn, TimeSpan commandTimeout)
            : this(conn, commandTimeout, 1)
        {
        }

        /// <summary>
        /// Creates a new MsSqlEventingRepo.
        /// </summary>
        /// <param name="conn">IDbConnection to use.</param>
        /// <param name="commandTimeout">Commandtimeout to use</param>
        /// <param name="maxRetriesOnDeadlock">Maximum number of times to retry DB-calls when deadlocks occur.</param>
        public MySqlEventingRepo(MySqlConnection conn, TimeSpan commandTimeout, int maxRetriesOnDeadlock)
            : base(conn, commandTimeout)
        {
            if (maxRetriesOnDeadlock < 0) throw new ArgumentOutOfRangeException("maxRetriesOnDeadlock");

            // Check if AllowUserVariables is enabled
            var connStringBuilder = new MySqlConnectionStringBuilder(conn.ConnectionString);
            if (!connStringBuilder.AllowUserVariables)
            {
                // Modify the connectionstring of the passed connection
                connStringBuilder.AllowUserVariables = true;
                if (_conn.State != ConnectionState.Closed)
                    _conn.Close();
                _conn.ConnectionString = connStringBuilder.ConnectionString;
            }

            _maxRetriesOnDeadlock = maxRetriesOnDeadlock;
        }

        public override string GetLastAutoIncrementValue
        {
            get { return "LAST_INSERT_ID()"; }
        }

        protected override bool CanRetry(DbException dbEx, int attempts)
        {
            var mysqlEx = dbEx as MySqlException;
            if (mysqlEx != null && mysqlEx.Number == 1213 && attempts < (_maxRetriesOnDeadlock + 1)) // After x attempts give up deadlocks
                return true;
            else
                return base.CanRetry(dbEx, attempts);
        }

        public override async Task<int> UpdateLastConsumedSubscriptionEvent(SubscriptionEvent subscriptionEvent)
        {
            var query = "INSERT INTO LastConsumedSubscriptionEvent (SubscriptionId, FunctionalKey, PublicationDateUtc)" +
                " VALUES(@subscriptionId, @functionalKey, @publicationDateUtc)" +
                " ON DUPLICATE KEY UPDATE PublicationDateUtc = @publicationDateUtc";

            return await TranExecuteAsync(query, new Dictionary<string, object>
            {
                { "@subscriptionId", subscriptionEvent.SubscriptionId },
                { "@functionalKey", subscriptionEvent.FunctionalKey },
                { "@publicationDateUtc", subscriptionEvent.PublicationDateUtc },
            }).ConfigureAwait(false);
        }

        protected override async Task<IEnumerable<ConsumableEvent>> ConsumeNextForSubscription(Subscription subscription, int visibilityTimeout, int maxCount)
        {
            // 1. Lock and get ids
            var lockedIds = await LockNextSubscriptionEventsAsync(subscription.Id.Value, subscription.Ordered, visibilityTimeout, maxCount).ConfigureAwait(false);

            // 2. Get se details
            var ces = new List<ConsumableEvent>();
            foreach (var sId in lockedIds)
            {
                ces.Add(await GetConsumableEvent(sId).ConfigureAwait(false));
            }

            return ces;
        }

        /// <summary>
        /// Gets a consumable event for a specific subscriptioneventId
        /// </summary>
        /// <param name="sId">SubscriptionEventId</param>
        /// <returns></returns>
        private async Task<ConsumableEvent> GetConsumableEvent(Int64 sId)
        {
            var query = $"select se.Id, se.DeliveryKey, se.EventName, se.FunctionalKey, se.InvisibleUntilUtc, se.PayloadId" + // Get the minimal amount of data
                " from SubscriptionEvent se where se.Id = @sId";
            var ces = await TranQueryAsync<ConsumableEvent>(query, new { sId = sId }).ConfigureAwait(false);
            var ce = ces.SingleOrDefault();
            if (ce != null && ce.PayloadId.HasValue)
            {
                ce.Payload = await GetPayloadAsync(ce.PayloadId.Value).ConfigureAwait(false);
                ce.PayloadId = null; // No reason to keep it
            }
            return ce;
        }

        private async Task<IEnumerable<Int64>> LockNextSubscriptionEventsAsync(Int64 subscriptionId, bool ordered, int visibilityTimeout, int maxCount)
        {
            var maxCountToUse = maxCount; // Warning: large batches with functional ordering may not work

            var lockedIds = new List<Int64>();

            for (int i = 0; i < maxCountToUse; i++)
            {
                try
                {
                    var sId = await TryLockNextSubscriptionEventAsync(subscriptionId, visibilityTimeout, ordered);
                    if (sId.HasValue)
                        lockedIds.Add(sId.Value);
                    else
                        break; // Break out of for-loop, since no more events are found
                }
                catch (RepoException repoEx)
                {
                    if ((repoEx.Error == RepoError.TooBusy) && (maxCountToUse > 1)) // When retrieving a batch, the already locked items must be returned instead of throwing an exception
                        break; // Break out of for-loop, since repo is too busy
                    else
                        throw;
                }
            }

            return lockedIds;
        }

        private async Task<Int64?> TryLockNextSubscriptionEventAsync(Int64 subscriptionId, int visibilityTimeout, bool ordered)
        {
            int attempt = 0;
            bool canRetry = false;

            do
            {
                attempt++;
                canRetry = false;

                var deliveryKey = Guid.NewGuid().ToString();
                var invisibleUntilUtc = DateTime.UtcNow.AddSeconds(visibilityTimeout);

                try
                {
                    if (!ordered)
                    {
                        // Locking details for MySql: http://dev.mysql.com/doc/refman/5.7/en/innodb-locking-reads.html
                        var query = "set @updatedSeId := 0;" +
                            " update SubscriptionEvent se" +
                            " join(" +
                            "   select seInner.Id, seInner.DeliveryKey" +
                            "   from SubscriptionEvent seInner" +
                            "   join Subscription s on s.Id = seInner.SubscriptionId" +
                            "   where seInner.SubscriptionId = @subscriptionId" +
                            "   and seInner.DeliveryDelayedUntilUtc < @utcNow" +
                            "   and seInner.ExpirationDateUtc > @utcNow" +
                            "   and seInner.InvisibleUntilUtc < @utcNow" +
                            "   and(s.MaxDeliveries = 0 OR s.MaxDeliveries > seInner.DeliveryCount)" +
                            "   order by seInner.Priority ASC, seInner.PublicationDateUtc ASC" +
                            "   limit 1) tmp on tmp.Id = se.Id and tmp.DeliveryKey = se.DeliveryKey" + // FOR UPDATE removed due to locking issues
                            " set se.InvisibleUntilUtc = @invisibleUntilUtc," +
                            " se.DeliveryCount = DeliveryCount + 1," +
                            " se.DeliveryKey = @deliveryKey," +
                            " se.DeliveryDateUtc = @utcNow," +
                            " se.Id = (select @updatedSeId:= se.Id)" +
                            " WHERE se.Id = tmp.Id AND se.DeliveryKey = tmp.DeliveryKey" + // Where is required, because FOR UPDATE is not used, thus no locks where aquired
                            ";select @updatedSeId;";

                        var sIds = await TranQueryAsync<Int64?>(query,
                            new Dictionary<string, object>
                            {
                        { "@subscriptionId", subscriptionId },
                        { "@utcNow", DateTime.UtcNow },
                        { "@deliveryKey", deliveryKey },
                        { "@invisibleUntilUtc", invisibleUntilUtc },
                            }).ConfigureAwait(false);

                        var sId = sIds.FirstOrDefault();
                        // sId may be null/0, event though there are more events. See comment below.
                        return (sId.HasValue && (sId.Value > 0)) ? sId.Value : default(Int64?);
                    }
                    else
                    {
                        var query = "set @updatedSeId := 0;" +
                            " update SubscriptionEvent seOuter" +
                            " join (" +
                                " select      se.Id, se.DeliveryKey" +
                                " from        subscriptionevent se" +
                                " join subscription s on s.Id = se.SubscriptionId" +
                                " left join	LastConsumedSubscriptionEvent lc" +
                                "     on		lc.SubscriptionId = se.SubscriptionId" +
                                "     and		lc.FunctionalKey = se.FunctionalKey" +
                                " left join	SubscriptionEvent seInv" +
                                "     on		seInv.SubscriptionId = se.SubscriptionId" +
                                "     and		seInv.FunctionalKey = se.FunctionalKey" +
                                "     and		seInv.InvisibleUntilUtc > @utcNow" +
                                "     and		seInv.Id != se.Id" +
                                " where		se.SubscriptionId = @subscriptionId" +
                                "     and		seInv.Id IS NULL" +
                                "     and		se.DeliveryDelayedUntilUtc < @utcNow" +
                                "     and		se.ExpirationDateUtc > @utcNow" +
                                "     and		se.InvisibleUntilUtc < @utcNow" +
                                "     and		(	s.MaxDeliveries = 0" +
                                "             OR	se.DeliveryCount < s.MaxDeliveries)" +
                                "     and		(	(se.PublicationDateUtc >= lc.PublicationDateUtc)" +
                                "              OR	lc.SubscriptionId IS NULL)" +
                                " order by 	se.Priority ASC, se.PublicationDateUtc ASC" + // Warning: prio must be stored inverted!
                                " limit 1" + // FOR UPDATE" + // FOR UPDATE locks all matching records, not restricted by the LIMIT 1
                            " ) tmp on tmp.Id = seOuter.Id and tmp.DeliveryKey = seOuter.DeliveryKey" +
                            " set seOuter.InvisibleUntilUtc = @invisibleUntilUtc," +
                            "   seOuter.DeliveryCount = seOuter.DeliveryCount + 1," +
                            "   seOuter.DeliveryKey = @deliveryKey," +
                            "   seOuter.DeliveryDateUtc = @utcNow," +
                            "   seOuter.Id = (select @updatedSeId := seOuter.Id)" +
                            " WHERE seOuter.Id = tmp.Id AND seOuter.DeliveryKey = tmp.DeliveryKey" + // WHERE-clause is needed when FOR UPDATE is not used
                            ";select @updatedSeId;";

                        var sIds = await TranQueryAsync<Int64?>(query,
                            new Dictionary<string, object>
                            {
                        { "@subscriptionId", subscriptionId },
                        { "@utcNow", DateTime.UtcNow },
                        { "@deliveryKey", deliveryKey },
                        { "@invisibleUntilUtc", invisibleUntilUtc },
                            }).ConfigureAwait(false);

                        var sId = sIds.FirstOrDefault();

                        // The update-statement (above) can fail (0 rows affected and @updatedSeId not set/still 0) when the found record was modified before it could be updated.
                        // (we no longer actively lock (FOR UPDATE) because of its performance impact on large tables (>200K records)
                        // This may occur under heavy load. We also cannot retry, because we cannot determine if this is the case: they may actually be no more events to process.
                        // When using the EventConsumptionWorker, the result will be that it backs off (minBackoffDelayMs), which may usually relief the process shortly.
                        return (sId.HasValue && (sId.Value > 0)) ? sId.Value : default(Int64?);
                    }
                }
                catch (DbException dbEx)
                {
                    canRetry = CanRetry(dbEx, attempt);
                    if (!canRetry)
                    {
                        if (attempt > 1) // Retries only occur when repo is busy and since retries happened, this is the cause
                            throw new RepoException($"Repo-action failed after {attempt} attempts", dbEx, RepoError.TooBusy);
                        else
                            throw new RepoException(dbEx);
                    }
                }
            } while (canRetry);

            return null; // Still no result after retrying, then just return null (give up).
        }

        public async Task PerformHouseKeepingTasksAsync()
        {
            await HouseKeeping_MaxDeliveriesReachedSubscriptionEvents().ConfigureAwait(false);
            await HouseKeeping_ExpiredSubscriptionEvents().ConfigureAwait(false);
            await HouseKeeping_OvertakenSubscriptionEvents().ConfigureAwait(false);
        }

        private async Task<int> HouseKeeping_ExpiredSubscriptionEvents()
        {
            await BeginTransactionAsync().ConfigureAwait(false);

            try
            {
                var reasonNr = ((int)ReasonType.Expired).ToString(CultureInfo.InvariantCulture);
                var query = "INSERT IGNORE INTO FailedSubscriptionEvent"
                    + " ( Id, SubscriptionId"
                    + " , EventName, PublicationDateUtc, FunctionalKey, Priority, PayloadId, DeliveryDateUtc, DeliveryCount"
                    + " , FailedDateUtc, Reason, ReasonOther)"
                    + " SELECT Id, SubscriptionId"
                    + " , EventName, PublicationDateUtc, FunctionalKey, Priority, PayloadId, DeliveryDateUtc, DeliveryCount"
                    + $" , @utcNow, {reasonNr}, null"
                    + " FROM SubscriptionEvent"
                    + " WHERE (SubscriptionEvent.ExpirationDateUtc IS NOT NULL AND SubscriptionEvent.ExpirationDateUtc < @utcNow AND SubscriptionEvent.InvisibleUntilUtc < @utcNow);"
                    + " DELETE se"
                    + " FROM SubscriptionEvent se"
                    + " JOIN FailedSubscriptionEvent fse ON fse.Id = se.Id;";
                var rowsAffected = await TranExecuteAsync(query, new { utcNow = DateTime.UtcNow }).ConfigureAwait(false);
                await CommitTransactionAsync().ConfigureAwait(false);

                return rowsAffected;
            }
            catch (Exception)
            {
                await RollbackTransactionAsync().ConfigureAwait(false);
                throw;
            }
        }

        private async Task<int> HouseKeeping_MaxDeliveriesReachedSubscriptionEvents()
        {
            await BeginTransactionAsync().ConfigureAwait(false);

            try
            {
                var reasonNr = ((int)ReasonType.MaxRetriesReached).ToString(CultureInfo.InvariantCulture);
                var query = "INSERT IGNORE INTO FailedSubscriptionEvent"
                    + " (Id, SubscriptionId"
                    + " , EventName, PublicationDateUtc, FunctionalKey, Priority, PayloadId, DeliveryDateUtc, DeliveryCount"
                    + " , FailedDateUtc, Reason, ReasonOther)"
                    + " SELECT se.Id, se.SubscriptionId"
                    + " , se.EventName, se.PublicationDateUtc, se.FunctionalKey, se.Priority, se.PayloadId, se.DeliveryDateUtc, se.DeliveryCount"
                    + $" , @utcNow, {reasonNr}, null"
                    + " FROM SubscriptionEvent se"
                    + " JOIN Subscription s ON s.Id = se.SubscriptionId"
                    + " WHERE (s.MaxDeliveries > 0 AND se.DeliveryCount >= s.MaxDeliveries AND se.InvisibleUntilUtc < @utcNow);"
                    + " DELETE se"
                    + " FROM SubscriptionEvent se"
                    + " JOIN FailedSubscriptionEvent fse ON fse.Id = se.Id;";
                var rowsAffected = await TranExecuteAsync(query, new { utcNow = DateTime.UtcNow }).ConfigureAwait(false);
                await CommitTransactionAsync().ConfigureAwait(false);

                return rowsAffected;
            }
            catch (Exception)
            {
                await RollbackTransactionAsync().ConfigureAwait(false);
                throw;
            }
        }

        private async Task<int> HouseKeeping_OvertakenSubscriptionEvents()
        {
            await BeginTransactionAsync().ConfigureAwait(false);

            try
            {
                var reasonNr = ((int)ReasonType.Overtaken).ToString(CultureInfo.InvariantCulture);
                var query = "INSERT IGNORE INTO FailedSubscriptionEvent"
                    + " (Id, SubscriptionId"
                    + " , EventName, PublicationDateUtc, FunctionalKey, Priority, PayloadId, DeliveryDateUtc, DeliveryCount"
                    + " , FailedDateUtc, Reason, ReasonOther)"
                    + " SELECT se.Id, se.SubscriptionId"
                    + " , se.EventName, se.PublicationDateUtc, se.FunctionalKey, se.Priority, se.PayloadId, se.DeliveryDateUtc, se.DeliveryCount"
                    + $" , @utcNow, {reasonNr}, null"
                    + " FROM SubscriptionEvent se"
                    + " JOIN LastConsumedSubscriptionEvent lc ON lc.SubscriptionId = se.SubscriptionId AND lc.FunctionalKey = se.FunctionalKey"
                    + " WHERE se.PublicationDateUtc < lc.PublicationDateUtc;"
                    + " DELETE se"
                    + " FROM SubscriptionEvent se"
                    + " JOIN FailedSubscriptionEvent fse ON fse.Id = se.Id;";
                var rowsAffected = await TranExecuteAsync(query, new { utcNow = DateTime.UtcNow }).ConfigureAwait(false);
                await CommitTransactionAsync().ConfigureAwait(false);

                return rowsAffected;
            }
            catch (Exception)
            {
                await RollbackTransactionAsync().ConfigureAwait(false);
                throw;
            }
        }
    }
}
