using Resonance.Models;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;

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
        /// <param name="commandTimeout">Commandtimeout to use</param>
        public MsSqlEventingRepo(SqlConnection conn, TimeSpan commandTimeout)
            : base(conn, commandTimeout)
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

        protected override bool ParallelQueriesSupport { get { return _connStringBuilder.MultipleActiveResultSets; } } // 'MARS' must be enabled in the connectionstring

        public override string GetLastAutoIncrementValue
        {
            get { return "SCOPE_IDENTITY()"; }
        }

        public override async Task<DateTime> GetNowUtcAsync()
        {
            var query = "select SYSUTCDATETIME();";
            var dateTimes = await TranQueryAsync<DateTime>(query).ConfigureAwait(false);
            return dateTimes.First();
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

            int attempt = 0;
            bool canRetry = false;

            var ces = new List<ConsumableEvent>();

            if (!subscription.Ordered)
            {
                do
                {
                    attempt++;
                    canRetry = false;

                    await BeginTransactionAsync();
                    var utcNow = await GetNowUtcAsync().ConfigureAwait(false);

                    try
                    {
                        var invisibleUntilUtc = utcNow.AddSeconds(visibilityTimeout);

                        var query = "DECLARE @l_PBEIds TABLE(ID bigint)\n"
                            + ";WITH DE AS ("
                            + $" select TOP {maxCount} se.Id, se.DeliveryKey, se.InvisibleUntilUtc, se.DeliveryCount, se.DeliveryDateUtc" // Top 1!!!
                            + " from SubscriptionEvent se"
                            + " join Subscription s on s.Id = se.SubscriptionId" // Needed for MaxRetries
                            + " where se.SubscriptionId = @subscriptionId"
                            + " and se.DeliveryDelayedUntilUtc < @utcNow" // Must be allowed to be delivered
                            + " and se.ExpirationDateUtc > @utcNow" // Must not yet have expired
                            + " and se.InvisibleUntilUtc < @utcNow" // Must not be 'locked'/made invisible by other consumer
                            + " and (s.MaxDeliveries = 0 OR s.MaxDeliveries > se.DeliveryCount)" // Must not have reached max. allowed delivery attempts
                            + " order by se.Priority ASC, se.PublicationDateUtc ASC" // Warning: prio can mess everything up!
                            + ") UPDATE DE"
                            + " SET InvisibleUntilUtc = @invisibleUntilUtc,"
                            + "    DeliveryCount = DE.DeliveryCount + 1,"
                            + "    DeliveryKey = NEWID()," // Let SQL server generate the deliverykey
                            + "    DeliveryDateUtc = @utcNow"
                            + " OUTPUT inserted.Id INTO @l_PBEIds"
                            + " select se.Id, se.DeliveryKey, se.EventName, se.FunctionalKey, se.InvisibleUntilUtc, se.PayloadId"
                            + " from SubscriptionEvent se"
                            + " join @l_PBEIds pbe on pbe.Id = se.Id";

                        ces.AddRange(await TranQueryAsync<ConsumableEvent>(query,
                                new Dictionary<string, object>
                                {
                                        { "@subscriptionId", subscription.Id.Value },
                                        { "@utcNow", utcNow },
                                        { "@invisibleUntilUtc", invisibleUntilUtc },
                                }).ConfigureAwait(false));

                        await CommitTransactionAsync();
                    }
                    catch (DbException dbEx)
                    {
                        await RollbackTransactionAsync();
                        canRetry = CanRetry(dbEx, attempt);

                        if (!canRetry)
                        {
                            if (attempt > 1) // Retries only occur when repo is busy and since retries happened, this is the cause
                                throw new RepoException($"Repo-action failed after {attempt} attempts", dbEx, RepoError.TooBusy);
                            else
                                throw new RepoException(dbEx);
                        }
                    }
                    catch (Exception)
                    {
                        await RollbackTransactionAsync();
                        throw;
                    }
                } while (canRetry);
            }
            else // Functional ordering
            {
                for (int i = 0; i < maxCount; i++) // Ordered altijd per 1 raadplegen
                {
                    attempt = 0; // Reset on every iteration
                    var noMoreEvents = false; // Indicates if not more events are available

                    do
                    {
                        attempt++;
                        canRetry = false;

                        await BeginTransactionAsync(); // Transaction is required, because tests show that under heavy load reads are not repeatable (which is weird since CTE's should be atomic)
                        var utcNow = await GetNowUtcAsync().ConfigureAwait(false);

                        try
                        {
                            var deliveryKey = Guid.NewGuid().ToString();
                            var invisibleUntilUtc = utcNow.AddSeconds(visibilityTimeout);

                            var query = "DECLARE @l_PBEIds TABLE(ID bigint)\n"
                                + ";WITH DE AS ("
                                + " select TOP 1 se.Id, se.DeliveryKey, se.InvisibleUntilUtc, se.DeliveryCount, se.DeliveryDateUtc" // Top 1!!!
                                + " from SubscriptionEvent se"
                                + " join Subscription s on s.Id = se.SubscriptionId" // Needed for MaxRetries
                                + " left join SubscriptionEvent seInv" // Controle of funckey al in behandeling (invisible)
                                + "   on seInv.SubscriptionId = se.SubscriptionId"
                                + "   and seInv.FunctionalKey = se.FunctionalKey"
                                + "   and seInv.Id != se.Id"
                                + "   and seInv.InvisibleUntilUtc > @utcNow"
                                + " left join LastConsumedSubscriptionEvent lc" // For functional ordering
                                + "   on lc.SubscriptionId = se.SubscriptionId and lc.FunctionalKey = se.FunctionalKey"
                                + " where se.SubscriptionId = @subscriptionId"
                                + " and se.DeliveryDelayedUntilUtc < @utcNow" // Must be allowed to be delivered
                                + " and se.ExpirationDateUtc > @utcNow" // Must not yet have expired
                                + " and se.InvisibleUntilUtc < @utcNow" // Must not be 'locked'/made invisible by other consumer
                                + " and (s.MaxDeliveries = 0 OR s.MaxDeliveries > se.DeliveryCount)" // Must not have reached max. allowed delivery attempts
                                + "	and	seInv.Id IS NULL" // Geen in behandeling nu
                                + " and	(lc.SubscriptionId IS NULL OR (lc.PublicationDateUtc <= se.PublicationDateUtc))" // Newer than last published
                                + " order by se.Priority ASC, se.PublicationDateUtc ASC" // Warning: prio can mess everything up!
                                + ") UPDATE DE"
                                + " SET InvisibleUntilUtc = @invisibleUntilUtc,"
                                + "    DeliveryCount = DE.DeliveryCount + 1,"
                                + "    DeliveryKey = @deliveryKey,"
                                + "    DeliveryDateUtc = @utcNow"
                                + " OUTPUT inserted.Id INTO @l_PBEIds"
                                + " select se.Id, se.DeliveryKey, se.EventName, se.FunctionalKey, se.InvisibleUntilUtc, se.PayloadId"
                                + " from SubscriptionEvent se"
                                + " join @l_PBEIds pbe on pbe.Id = se.Id";

                            var cesInLoop = await TranQueryAsync<ConsumableEvent>(query,
                                                new Dictionary<string, object>
                                                {
                                                    { "@subscriptionId", subscription.Id.Value },
                                                    { "@utcNow", utcNow },
                                                    { "@deliveryKey", deliveryKey },
                                                    { "@invisibleUntilUtc", invisibleUntilUtc },
                                                }).ConfigureAwait(false);

                            await CommitTransactionAsync();

                            if (cesInLoop.Count() > 0)
                                ces.AddRange(cesInLoop);
                            else
                                noMoreEvents = true; // Nothing found anymore
                        }
                        catch (DbException dbEx)
                        {
                            await RollbackTransactionAsync();
                            canRetry = CanRetry(dbEx, attempt);

                            if (!canRetry)
                            {
                                if (maxCount == 1)
                                {
                                    if (attempt > 1) // Retries only occur when repo is busy and since retries happened, this is the cause
                                        throw new RepoException($"Repo-action failed after {attempt} attempts", dbEx, RepoError.TooBusy);
                                    else
                                        throw new RepoException(dbEx);
                                }
                                else
                                    break; // When retrieving a batch, the already locked items must be returned instead of throwing an exception
                            }
                        }
                        catch (Exception)
                        {
                            await RollbackTransactionAsync();
                            throw;
                        }
                    } while (canRetry); // retries

                    if (noMoreEvents)
                        break; // No use to keep trying, so escape the for-loop
                }
            }

            // Now that items are locked, the actual payload itself can be added (if any)
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

        public async Task PerformHouseKeepingTasksAsync()
        {
            await HouseKeeping_MaxDeliveriesReachedSubscriptionEvents().ConfigureAwait(false);
            await HouseKeeping_ExpiredSubscriptionEvents().ConfigureAwait(false);
            await HouseKeeping_OvertakenSubscriptionEvents().ConfigureAwait(false);
        }

        private async Task<int> HouseKeeping_ExpiredSubscriptionEvents()
        {
            var reasonNr = ((int)ReasonType.Expired).ToString(CultureInfo.InvariantCulture);
            var query = "DELETE	SubscriptionEvent"
                + " OUTPUT deleted.Id, deleted.SubscriptionId"
                + " , deleted.EventName, deleted.PublicationDateUtc, deleted.FunctionalKey, deleted.Priority, deleted.PayloadId, deleted.DeliveryDateUtc, deleted.DeliveryCount"
                + $", @utcNow, {reasonNr}, null"
                + " INTO FailedSubscriptionEvent"
                + " (Id, SubscriptionId"
                + " , EventName, PublicationDateUtc, FunctionalKey, Priority, PayloadId, DeliveryDateUtc, DeliveryCount"
                + " , FailedDateUtc, Reason, ReasonOther)"
                + " WHERE (SubscriptionEvent.ExpirationDateUtc IS NOT NULL AND SubscriptionEvent.ExpirationDateUtc < @utcNow AND SubscriptionEvent.InvisibleUntilUtc < @utcNow)";
            var utcNow = await GetNowUtcAsync().ConfigureAwait(false);
            var rowsAffected = await TranExecuteAsync(query, new { utcNow = utcNow }).ConfigureAwait(false);

            return rowsAffected;
        }

        private async Task<int> HouseKeeping_MaxDeliveriesReachedSubscriptionEvents()
        {
            var reasonNr = ((int)ReasonType.MaxRetriesReached).ToString(CultureInfo.InvariantCulture);
            var query = "DELETE	se"
                + " OUTPUT deleted.Id, deleted.SubscriptionId"
                + "	, deleted.EventName, deleted.PublicationDateUtc, deleted.FunctionalKey, deleted.Priority, deleted.PayloadId, deleted.DeliveryDateUtc, deleted.DeliveryCount"
                + $", @utcNow, {reasonNr}, null"
                + " INTO FailedSubscriptionEvent"
                + " (Id, SubscriptionId"
                + " , EventName, PublicationDateUtc, FunctionalKey, Priority, PayloadId, DeliveryDateUtc, DeliveryCount"
                + " , FailedDateUtc, Reason, ReasonOther)"
                + " FROM     SubscriptionEvent se"
                + " JOIN    Subscription s ON s.Id = se.SubscriptionId"
                + " WHERE (s.MaxDeliveries > 0 AND se.DeliveryCount >= s.MaxDeliveries AND se.InvisibleUntilUtc < @utcNow)";
            var utcNow = await GetNowUtcAsync().ConfigureAwait(false);
            var rowsAffected = await TranExecuteAsync(query, new { utcNow = utcNow }).ConfigureAwait(false);

            return rowsAffected;
        }


        private async Task<int> HouseKeeping_OvertakenSubscriptionEvents()
        {
            var reasonNr = ((int)ReasonType.Overtaken).ToString(CultureInfo.InvariantCulture);
            var query = "DELETE	se"
                + " OUTPUT deleted.Id, deleted.SubscriptionId"
                + "	, deleted.EventName, deleted.PublicationDateUtc, deleted.FunctionalKey, deleted.Priority, deleted.PayloadId, deleted.DeliveryDateUtc, deleted.DeliveryCount"
                + $", @utcNow, {reasonNr}, null"
                + " INTO FailedSubscriptionEvent"
                + " (Id, SubscriptionId"
                + " , EventName, PublicationDateUtc, FunctionalKey, Priority, PayloadId, DeliveryDateUtc, DeliveryCount"
                + " , FailedDateUtc, Reason, ReasonOther)"
                + " FROM     SubscriptionEvent se"
                + " JOIN LastConsumedSubscriptionEvent lc ON  lc.SubscriptionId = se.SubscriptionId AND lc.FunctionalKey = se.FunctionalKey"
                + " WHERE se.PublicationDateUtc < lc.PublicationDateUtc";
            var utcNow = await GetNowUtcAsync().ConfigureAwait(false);
            var rowsAffected = await TranExecuteAsync(query, new { utcNow = utcNow }).ConfigureAwait(false);

            return rowsAffected;
        }

    }
}
