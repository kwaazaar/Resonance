using Dapper;
using Newtonsoft.Json;
using Resonance.Models;
using Resonance.Repo.InternalModels;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace Resonance.Repo.Database
{
    public abstract class DbEventingRepo : BaseEventingRepo, IDisposable
    {
        static DbEventingRepo()
        {
            // Add custom type handler to treat all DateTimes as UTC
            SqlMapper.AddTypeHandler(new DateTimeUtcTypeHandler());
        }

        #region Inner types
        public enum TranState : byte
        {
            Unchanged = 0,
            Committed = 1,
            Rollbacked = 2,
        }
        
        #endregion

        protected readonly IDbConnection _conn;
        protected IDbTransaction _runningTransaction;
        protected int _tranCount = 0;
        protected object _tranLock = new object();
        protected TranState _tranState = TranState.Unchanged;

        /// <summary>
        /// Creates a new DbEventingRepo.
        /// </summary>
        /// <param name="conn">IDbConnection to use. If not yet opened, it will be opened here.</param>
        public DbEventingRepo(IDbConnection conn, TimeSpan commandTimeout)
        {
            _conn = conn;
            CommandTimeout = commandTimeout;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// IDispose implementation
        /// </summary>
        /// <param name="disposing"></param>
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (_conn != null)
                {
                    if (_conn.State != ConnectionState.Closed)
                    {
                        try
                        {
                            _conn.Close();
                        }
                        catch (Exception) { } // Swallow, can't help it anyway
                    }
                    _conn.Dispose();
                }
            }
        }

        #region Connection Management
        /// <summary>
        /// Ensures the provided connection is available and open.
        /// </summary>
        /// <returns></returns>
        protected virtual async Task EnsureConnectionReady()
        {
            if (_conn == null)
                throw new InvalidOperationException("No connection available");

            var dbConn = _conn as DbConnection; // If _conn appears to be a (derived) DbConnection, we cast it, since it provides some async methods
            if (_conn.State == ConnectionState.Broken)
            {
                // When broken, just close and reopen again, might do the job
                _conn.Close();
                if (dbConn != null)
                    await dbConn.OpenAsync().ConfigureAwait(false);
                else
                    _conn.Open();
            }
            else if (_conn.State == ConnectionState.Closed)
            {
                if (dbConn != null)
                    await dbConn.OpenAsync().ConfigureAwait(false);
                else
                    _conn.Open();
            }
        }

        /// <summary>
        /// Indicates whether the repo supports running parallel queries on a single connection. Default=false.
        /// </summary>
        protected override bool ParallelQueriesSupport { get { return false; } }

        /// <summary>
        /// CommandTimeout to use. Set by constructor, but can be modified.
        /// </summary>
        public virtual TimeSpan CommandTimeout { get; set; }

        #endregion

        #region Transactions
        /// <summary>
        /// Starts a new transaction.
        /// NB: Transactions can be nested.
        /// </summary>
        protected async override Task BeginTransactionAsync(IsolationLevel isolationLevel = IsolationLevel.RepeatableRead) // RepeatableReads prevents deadlocks and, in case of MsSql, skipping/overtaking of events
        {
            await EnsureConnectionReady().ConfigureAwait(false);
            lock (_tranLock)
            {
                if (_runningTransaction == null)
                {
                    _runningTransaction = _conn.BeginTransaction(isolationLevel);
                    _tranState = TranState.Unchanged;
                }
                _tranCount++;
            }
        }

        /// <summary>
        /// Rolls back the the transaction and disposes it.
        /// Make sure there are no parallel threads/tasks still using the transaction!
        /// </summary>
        protected override async Task RollbackTransactionAsync()
        {
            if (_runningTransaction == null) // Check before waiting for lock to prevent unnessecary locks
                throw new ArgumentException($"No running transaction found");

            await EnsureConnectionReady().ConfigureAwait(false);
            lock (_tranLock)
            {
                if (_runningTransaction == null)
                    throw new ArgumentException($"No running transaction found");

                if (_tranState != TranState.Rollbacked) // Nothing to do if already rolled back
                {
                    if (_runningTransaction.Connection != null) // Would be weird, since it's only cleared after a rollback or commit (on SqlServer)
                        _runningTransaction.Rollback(); // Rollback immediately

                    _tranState = TranState.Rollbacked;
                }

                _tranCount--;
                if (_tranCount == 0)
                {
                    _runningTransaction.Dispose();
                    _runningTransaction = null;
                }
            }
        }

        /// <summary>
        /// Commits the transaction and disposes it.
        /// Make sure there are no parallel threads/tasks still using the transaction!
        /// </summary>
        protected override async Task CommitTransactionAsync()
        {
            if (_runningTransaction == null) // Check before waiting for lock to prevent unnessecary locks
                throw new ArgumentException($"No running transaction found");

            await EnsureConnectionReady().ConfigureAwait(false);
            lock (_tranLock)
            {
                if (_runningTransaction == null)
                    throw new ArgumentException($"No running transaction found");

                if (_tranState == TranState.Rollbacked)
                    throw new InvalidOperationException("Transaction has already been rolled back");

                _tranState = TranState.Committed;

                _tranCount--;
                if (_tranCount == 0)
                {
                    // We got till the highest level, so no perform the actual action on the transaction
                    _runningTransaction.Commit();
                    _runningTransaction.Dispose();
                    _runningTransaction = null;
                }
            }
        }

        /// <summary>
        /// Transacted execution; if a transaction was started, the execute will take place on/in it
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="param"></param>
        /// <param name="commandTimeout"></param>
        /// <returns></returns>
        protected async virtual Task<int> TranExecuteAsync(string sql, object param = null, int? commandTimeout = null)
        {
            await EnsureConnectionReady().ConfigureAwait(false);
            var query = sql.ToLowerInvariant(); // MySql requires lower-case table names. This way the queries do not need to change, which is more readable.
            return await _conn.ExecuteAsync(query,
                param: param,
                transaction: _runningTransaction,
                commandTimeout: commandTimeout.GetValueOrDefault((int)CommandTimeout.TotalSeconds)).ConfigureAwait(false);
        }

        /// <summary>
        /// Transacted query; if a transaction was started, the query will take place on/in it
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <param name="param"></param>
        /// <param name="commandTimeout"></param>
        /// <returns></returns>
        protected async virtual Task<IEnumerable<T>> TranQueryAsync<T>(string sql, object param = null, int? commandTimeout = null)
        {
            await EnsureConnectionReady().ConfigureAwait(false);
            var query = sql.ToLowerInvariant(); // MySql requires lower-case table names. This way the queries do not need to change, which is more readable.
            return await _conn.QueryAsync<T>(query,
                param: param,
                transaction: _runningTransaction,
                commandTimeout: commandTimeout.GetValueOrDefault((int)CommandTimeout.TotalSeconds)).ConfigureAwait(false);
        }
        #endregion

        #region Repo-specific DB-implementation

        /// <summary>
        /// Depending on the type of DbException, the specific repo can determine if a retry of the DB-call is usefull.
        /// </summary>
        /// <param name="dbEx">The DbException</param>
        /// <param name="attempts">The nr of attempts tried so far</param>
        /// <returns></returns>
        protected virtual bool CanRetry(DbException dbEx, int attempts)
        {
            return false;
        }

        /// <summary>
        /// Function signature for getting the latest autoincrement value
        /// </summary>
        public abstract string GetLastAutoIncrementValue { get; }
        #endregion

        #region Topic and Subscription Management
        public async Task<Subscription> AddOrUpdateSubscriptionAsync(Subscription subscription)
        {
            Subscription existingSubscription = (subscription.Id.HasValue)
                            ? existingSubscription = await GetSubscriptionAsync(subscription.Id.Value).ConfigureAwait(false)
                            : null;

            if (existingSubscription != null) // update
            {
                await BeginTransactionAsync().ConfigureAwait(false);
                try
                {
                    var parameters = new Dictionary<string, object>
                    {
                        { "@id", subscription.Id },
                        { "@name", subscription.Name },
                        { "@deliveryDelay", subscription.DeliveryDelay },
                        { "@maxDeliveries", subscription.MaxDeliveries },
                        { "@ordered", subscription.Ordered },
                        { "@timeToLive", subscription.TimeToLive },
                        { "@logConsumed", subscription.LogConsumed },
                        { "@logFailed", subscription.LogFailed }
                    };
                    await TranExecuteAsync("update Subscription set Name = @name, DeliveryDelay = @deliveryDelay, MaxDeliveries = @maxDeliveries, Ordered = @ordered, TimeToLive = @timeToLive, LogFailed = @logFailed, LogConsumed = @logConsumed where Id = @id", parameters).ConfigureAwait(false);

                    // Update TopicSubscriptions (by removing them and rebuilding them again)
                    await RemoveTopicSubscriptions(subscription.Id.Value).ConfigureAwait(false);

                    if (subscription.TopicSubscriptions != null && subscription.TopicSubscriptions.Count > 0)
                        await AddTopicSubscriptions(subscription.Id.Value, subscription.TopicSubscriptions).ConfigureAwait(false);

                    await CommitTransactionAsync().ConfigureAwait(false);
                }
                catch (Exception)
                {
                    await RollbackTransactionAsync().ConfigureAwait(false);
                    throw;
                }

                return await GetSubscriptionAsync(subscription.Id.Value).ConfigureAwait(false);
            }
            else
            {
                Int64 subscriptionId = Int64.MinValue;

                await BeginTransactionAsync().ConfigureAwait(false);
                try
                {
                    var parameters = new Dictionary<string, object>
                        {
                            { "@name", subscription.Name },
                            { "@deliveryDelay", subscription.DeliveryDelay },
                            { "@maxDeliveries", subscription.MaxDeliveries },
                            { "@ordered", subscription.Ordered },
                            { "@timeToLive", subscription.TimeToLive },
                            { "@logConsumed", subscription.LogConsumed },
                            { "@logFailed", subscription.LogFailed }
                        };
                    var subscriptionIds = await TranQueryAsync<Int64>("insert into Subscription (Name, DeliveryDelay, MaxDeliveries, Ordered, TimeToLive, LogConsumed, LogFailed) values (@name, @deliveryDelay, @maxDeliveries, @ordered, @timeToLive, @logConsumed, @logFailed)"
                        + $";select {GetLastAutoIncrementValue} as 'Id'",
                        parameters).ConfigureAwait(false);
                    subscriptionId = subscriptionIds.Single();
                    await AddTopicSubscriptions(subscriptionId, subscription.TopicSubscriptions).ConfigureAwait(false);
                    await CommitTransactionAsync().ConfigureAwait(false);
                }
                catch (Exception)
                {
                    await RollbackTransactionAsync().ConfigureAwait(false);
                    throw;
                }

                return await GetSubscriptionAsync(subscriptionId).ConfigureAwait(false);
            }
        }

        private async Task AddTopicSubscriptions(Int64 subscriptionId, List<TopicSubscription> topicSubscriptions)
        {
            await BeginTransactionAsync().ConfigureAwait(false);
            try
            {
                foreach (var topicSubscription in topicSubscriptions)
                {
                    var parameters = new Dictionary<string, object>
                        {
                            { "@topicId", topicSubscription.TopicId },
                            { "@subscriptionId", subscriptionId },
                            { "@enabled", topicSubscription.Enabled },
                            { "@filtered", topicSubscription.Filtered },
                        };
                    var topicSubscriptionIds = await TranQueryAsync<Int64>("insert into TopicSubscription (TopicId, SubscriptionId, Enabled, Filtered) values (@topicId, @subscriptionId, @enabled, @filtered)"
                        + $";select {GetLastAutoIncrementValue} as 'Id'",
                        parameters).ConfigureAwait(false);
                    var topicSubscriptionId = topicSubscriptionIds.Single();

                    if (topicSubscription.Filters != null)
                    {
                        foreach (var filter in topicSubscription.Filters)
                        {
                            parameters = new Dictionary<string, object>
                            {
                                { "@topicSubscriptionId", topicSubscriptionId },
                                { "@header", filter.Header },
                                { "@matchExpression", filter.MatchExpression },
                                { "@notmatch", filter.NotMatch },
                            };
                            await TranExecuteAsync("insert into TopicSubscriptionFilter (TopicSubscriptionId, Header, MatchExpression, NotMatch) values (@topicSubscriptionId, @header, @matchExpression, @notmatch)", parameters).ConfigureAwait(false);
                        }
                    }
                }
                await CommitTransactionAsync().ConfigureAwait(false);
            }
            catch (Exception)
            {
                await RollbackTransactionAsync().ConfigureAwait(false);
                throw;
            }
        }

        private async Task RemoveTopicSubscriptions(Int64 subscriptionId)
        {
            await BeginTransactionAsync().ConfigureAwait(false);
            try
            {
                var parameters = new Dictionary<string, object> { { "@subscriptionId", subscriptionId } };

                // Delete topicsubscriptionfilters
                var query = "delete tsf from TopicSubscriptionFilter tsf" +
                    " join TopicSubscription ts on ts.Id = tsf.TopicSubscriptionId" +
                    " where ts.SubscriptionId = @subscriptionId";
                await TranExecuteAsync(query, parameters).ConfigureAwait(false);

                // Delete topicsubscriptions
                query = "delete ts from TopicSubscription ts" +
                    " where ts.SubscriptionId = @subscriptionId";
                await TranExecuteAsync(query, parameters).ConfigureAwait(false);

                await CommitTransactionAsync().ConfigureAwait(false);
            }
            catch (Exception)
            {
                await RollbackTransactionAsync().ConfigureAwait(false);
                throw;
            }
        }

        public async Task<Topic> AddOrUpdateTopicAsync(Topic topic)
        {
            Topic existingTopic = (topic.Id.HasValue)
                ? existingTopic = await GetTopicAsync(topic.Id.Value).ConfigureAwait(false)
                : null;

            if (existingTopic != null) // update
            {
                var parameters = new Dictionary<string, object>
                {
                    { "@id", topic.Id.Value },
                    { "@name", topic.Name },
                    { "@notes", topic.Notes },
                    { "@log", topic.Log }
                };
                await TranExecuteAsync("update Topic set Name = @name, Notes = @notes, Log = @log where Id = @id", parameters).ConfigureAwait(false);
                return await GetTopicAsync(topic.Id.Value).ConfigureAwait(false);
            }
            else
            {
                var parameters = new Dictionary<string, object>
                {
                    { "@name", topic.Name },
                    { "@notes", topic.Notes },
                    { "@log", topic.Log }
                };
                var topicIds = await TranQueryAsync<Int64>("insert into Topic (Name, Notes, Log) values (@name, @notes, @log)"
                    + $";select {GetLastAutoIncrementValue} as 'Id'",
                    parameters).ConfigureAwait(false);
                var topicId = topicIds.Single();
                return await GetTopicAsync(topicId).ConfigureAwait(false);
            }
        }

        public async Task DeleteSubscriptionAsync(Int64 id)
        {
            await BeginTransactionAsync().ConfigureAwait(false);
            try
            {
                //TranExecute("delete from SubscriptionEvent where SubscriptionId = @subscriptionId",
                //    new Dictionary<string, object>
                //    {
                //        { "@subscriptionId", id.ToDbKey() },
                //    });
                await RemoveTopicSubscriptions(id).ConfigureAwait(false);
                await TranExecuteAsync("delete from Subscription where Id = @id",
                    new Dictionary<string, object>
                    {
                        { "@id", id },
                    }).ConfigureAwait(false);
                await CommitTransactionAsync().ConfigureAwait(false);
            }
            catch (Exception)
            {
                await RollbackTransactionAsync().ConfigureAwait(false);
                throw;
            }
        }

        public async Task DeleteTopicAsync(Int64 id, bool inclSubscriptions)
        {
            await BeginTransactionAsync().ConfigureAwait(false);
            try
            {
                if (inclSubscriptions)
                {
                    // Delete all topicsubscriptions in subscriptions for the specified topic
                    var subscriptions = await GetSubscriptionsAsync(topicId: id).ConfigureAwait(false);
                    foreach (var subscription in subscriptions)
                    {
                        var parameters = new Dictionary<string, object>
                        {
                            { "@subscriptionId", subscription.Id },
                            { "@topicId", id },
                        };

                        // Delete topicsubscriptionfilters
                        var query = "delete tsf from TopicSubscriptionFilter tsf" +
                            " join TopicSubscription ts on ts.Id = tsf.TopicSubscriptionId" +
                            " where ts.SubscriptionId = @subscriptionId and ts.TopicId = @topicId";
                        await TranExecuteAsync(query, parameters).ConfigureAwait(false);

                        // Delete topicsubscriptions
                        query = "delete ts from TopicSubscription ts" +
                            " where ts.SubscriptionId = @subscriptionId and ts.TopicId = @topicId";
                        await TranExecuteAsync(query, parameters).ConfigureAwait(false);
                    }
                }

                await TranExecuteAsync("delete from Topic where Id = @id", // No check on rowcount, if it aint there, it's fine
                    new Dictionary<string, object>
                    {
                        { "@id", id },
                    }).ConfigureAwait(false);

                await CommitTransactionAsync().ConfigureAwait(false);
            }
            catch (Exception)
            {
                await RollbackTransactionAsync().ConfigureAwait(false);
                throw;
            }
        }

        /// <summary>
        /// Returns all subscriptions, or optionally only the subscriptions that subscribe to (at least) the specified topic.
        /// </summary>
        /// <param name="topicId"></param>
        /// <returns></returns>
        public async Task<IEnumerable<Subscription>> GetSubscriptionsAsync(Int64? topicId = null)
        {
            var parameters = new Dictionary<string, object>
                {
                    { "@topicId", topicId },
                };

            var query = "select s.Id from Subscription s";
            if (topicId.HasValue)
            {
                query = "select s.Id, ts.TopicId from Subscription s"
                    + " join TopicSubscription ts on ts.SubscriptionId = s.Id and ts.TopicId = @topicId"
                    + " group by s.Id, ts.TopicId having count(*) > 0"; // Matching at least once
            }

            var matchedSubscriptions = await TranQueryAsync<Identifier>(query, parameters).ConfigureAwait(false);
            var subs = new List<Subscription>();
            foreach (var id in matchedSubscriptions.Select(ms => ms.Id))
            {
                subs.Add(await GetSubscriptionAsync(id).ConfigureAwait(false)); // Todo: run in parallel
            }

            return subs;
        }

        public async Task<Subscription> GetSubscriptionAsync(Int64 id)
        {
            var subscriptions = await TranQueryAsync<Subscription>("select * from Subscription where id = @id",
                new Dictionary<string, object> { { "@id", id } }).ConfigureAwait(false);
            var subscription = subscriptions.SingleOrDefault();
            if (subscription == null)
                return null;

            // Add topic-subscriptions
            var topicSubscriptions = await TranQueryAsync<TopicSubscription>("select * from TopicSubscription where SubscriptionId = @id",
                new Dictionary<string, object> { { "@id", id } }).ConfigureAwait(false);
            foreach (var topicSubscription in topicSubscriptions)
            {
                var topicSubscriptionFilters = await TranQueryAsync<TopicSubscriptionFilter>("select * from TopicSubscriptionFilter where TopicSubscriptionId = @topicSubscriptionId",
                    new Dictionary<string, object> { { "@topicSubscriptionId", topicSubscription.Id } }).ConfigureAwait(false);
                topicSubscription.Filters = topicSubscriptionFilters.ToList();
            }
            subscription.TopicSubscriptions = topicSubscriptions.ToList();

            return subscription;
        }

        public async Task<Subscription> GetSubscriptionByNameAsync(string name)
        {
            var subscriptions = await TranQueryAsync<Subscription>("select * from Subscription where name = @name",
                new Dictionary<string, object> { { "@name", name } }).ConfigureAwait(false);
            var subscription = subscriptions.SingleOrDefault();
            if (subscription == null)
                return null;

            // Add topic-subscriptions
            var topicSubscriptions = await TranQueryAsync<TopicSubscription>("select * from TopicSubscription where SubscriptionId = @id",
                new Dictionary<string, object> { { "@id", subscription.Id } }).ConfigureAwait(false);
            foreach (var topicSubscription in topicSubscriptions)
            {
                var topicSubscriptionFilters = await TranQueryAsync<TopicSubscriptionFilter>("select * from TopicSubscriptionFilter where TopicSubscriptionId = @topicSubscriptionId",
                    new Dictionary<string, object> { { "@topicSubscriptionId", topicSubscription.Id } }).ConfigureAwait(false);
                topicSubscription.Filters = topicSubscriptionFilters.ToList();
            }
            subscription.TopicSubscriptions = topicSubscriptions.ToList();

            return subscription;
        }

        public async Task<IEnumerable<SubscriptionSummary>> GetSubscriptionStatisticsAsync(DateTime periodStartUtc, DateTime periodEndUtc)
        {
            var parameters = new Dictionary<string, object>
                {
                    { "@start", periodStartUtc },
                    { "@end", periodEndUtc },
                };
            var query = "select s.Id"
                + "	,(select count(*) from SubscriptionEvent se where se.SubscriptionId = s.Id and se.PublicationDateUtc >= @start and se.PublicationDateUtc <= @end) as 'Open'"
                + " ,(select count(*) from ConsumedSubscriptionEvent cse where cse.SubscriptionId = s.Id and cse.PublicationDateUtc >= @start and cse.DeliveryDateUtc <= @end) as 'Consumed'"
                + " ,(select count(*) from FailedSubscriptionEvent fse where fse.SubscriptionId = s.Id and fse.PublicationDateUtc >= @start and fse.FailedDateUtc <= @end and fse.Reason = 0) as 'FailedUnknown'"
                + " ,(select count(*) from FailedSubscriptionEvent fse where fse.SubscriptionId = s.Id and fse.PublicationDateUtc >= @start and fse.FailedDateUtc <= @end and fse.Reason = 1) as 'FailedExpired'"
                + "	,(select count(*) from FailedSubscriptionEvent fse where fse.SubscriptionId = s.Id and fse.PublicationDateUtc >= @start and fse.FailedDateUtc <= @end and fse.Reason = 2) as 'FailedMaxDeliveriesReached'"
                + "	,(select count(*) from FailedSubscriptionEvent fse where fse.SubscriptionId = s.Id and fse.PublicationDateUtc >= @start and fse.FailedDateUtc <= @end and fse.Reason = 3) as 'FailedOvertaken'"
                + "	,(select count(*) from FailedSubscriptionEvent fse where fse.SubscriptionId = s.Id and fse.PublicationDateUtc >= @start and fse.FailedDateUtc <= @end and fse.Reason = 4) as 'FailedOther'"
                + " from Subscription s"
                + " group by s.Id";
            var stats = await TranQueryAsync<SubscriptionSummary>(query, parameters).ConfigureAwait(false);

            // Get subscriptions and enrich the statistics
            var subscriptions = await GetSubscriptionsAsync().ConfigureAwait(false);
            stats.ToList().ForEach(stat => stat.Subscription = subscriptions.Single(s => s.Id == stat.Id));

            return stats;
        }

        public async Task<Topic> GetTopicAsync(Int64 id)
        {
            var parameters = new Dictionary<string, object>
                {
                    { "@id", id },
                };

            var topics = await TranQueryAsync<Topic>("select * from Topic where id = @id", parameters).ConfigureAwait(false);
            return topics.SingleOrDefault();
        }

        public async Task<Topic> GetTopicByNameAsync(string name)
        {
            var parameters = new Dictionary<string, object>
                {
                    { "@name", name },
                };

            var topics = await TranQueryAsync<Topic>("select * from Topic where name = @name", parameters).ConfigureAwait(false);
            return topics.SingleOrDefault();
        }

        public async Task<IEnumerable<Topic>> GetTopicsAsync(string partOfName = null)
        {
            if (partOfName != null)
            {
                var parameters = new Dictionary<string, object>
                {
                    { "@partOfName", $"%{partOfName}%"},
                };
                return await TranQueryAsync<Topic>("select * from Topic where Name like @partOfName", parameters).ConfigureAwait(false);
            }
            else
                return await TranQueryAsync<Topic>("select * from Topic").ConfigureAwait(false);
        }
        #endregion

        #region Event publication
        public async Task<Int64> StorePayloadAsync(string payload)
        {
            var parameters = new Dictionary<string, object>
                {
                    { "@payload", payload },
                };
            var ids = await TranQueryAsync<Int64>("insert into EventPayload (Payload) values (@payload)" +
                $";select {GetLastAutoIncrementValue} as 'Id'",
                parameters).ConfigureAwait(false);
            return ids.SingleOrDefault();
        }

        public async Task<string> GetPayloadAsync(Int64 id)
        {
            var payloads = await TranQueryAsync<string>("select Payload from EventPayload where Id = @id",
                new Dictionary<string, object>
                {
                    { "@id", id },
                }).ConfigureAwait(false);
            return payloads.SingleOrDefault();
        }

        public async Task<int> DeletePayloadAsync(Int64 id)
        {
            return await TranExecuteAsync("delete EventPayload where Id = @id",
                new Dictionary<string, object> { { "@id", id } }).ConfigureAwait(false);
        }

        protected async override Task<Int64> AddTopicEventAsync(TopicEvent topicEvent)
        {
            if (topicEvent == null) throw new ArgumentNullException("topicEvent");
            if (topicEvent.Id.HasValue) throw new ArgumentException("topicEvent.Id cannot have a value", "topicEvent");

            var headers = topicEvent.Headers != null ? JsonConvert.SerializeObject(topicEvent.Headers) : null; // Not async, should be fast enough, so hardly blocks the thread
            var parameters = new Dictionary<string, object>
                {
                    { "@topicId", topicEvent.TopicId },
                    { "@eventName", topicEvent.EventName },
                    { "@functionalKey", topicEvent.FunctionalKey },
                    { "@publicationDateUtc", topicEvent.PublicationDateUtc },
                    { "@expirationDateUtc", topicEvent.ExpirationDateUtc },
                    { "@headers", headers },
                    { "@priority", topicEvent.Priority },
                    { "@payloadId", topicEvent.PayloadId },
                };
            var ids = await TranQueryAsync<Int64>("insert into TopicEvent (TopicId, EventName, FunctionalKey, PublicationDateUtc, ExpirationDateUtc, Headers, Priority, PayloadId) values (@topicId, @eventName, @functionalKey, @publicationDateUtc, @expirationDateUtc, @headers, @priority, @payloadId)" +
                $";select {GetLastAutoIncrementValue} as 'Id'",
                parameters).ConfigureAwait(false);

            return ids.Single();
        }

        protected async override Task<Int64> AddSubscriptionEventAsync(SubscriptionEvent subscriptionEvent)
        {
            if (subscriptionEvent == null) throw new ArgumentNullException("subscriptionEvent");
            if (subscriptionEvent.Id.HasValue) throw new ArgumentException("subscriptionEvent.Id cannot have a value", "subscriptionEvent");

            var parameters = new Dictionary<string, object>
                {
                    { "@subscriptionId", subscriptionEvent.SubscriptionId },
                    { "@topicEventId", subscriptionEvent.TopicEventId },
                    { "@eventName", subscriptionEvent.EventName },
                    { "@publicationDateUtc", subscriptionEvent.PublicationDateUtc },
                    { "@functionalKey", subscriptionEvent.FunctionalKey },
                    { "@priority", subscriptionEvent.Priority },
                    { "@payloadId", subscriptionEvent.PayloadId },
                    { "@expirationDateUtc", subscriptionEvent.ExpirationDateUtc },
                    { "@deliveryDelayedUntilUtc", subscriptionEvent.DeliveryDelayedUntilUtc },
                    { "@deliveryCount", subscriptionEvent.DeliveryCount },
                    { "@deliveryDateUtc", subscriptionEvent.DeliveryDateUtc },
                    { "@deliveryKey", subscriptionEvent.DeliveryKey },
                    { "@invisibleUntilUtc", subscriptionEvent.InvisibleUntilUtc },
                };
            var ids = await TranQueryAsync<Int64>("insert into SubscriptionEvent (SubscriptionId, TopicEventId, EventName, PublicationDateUtc, FunctionalKey, Priority, PayloadId, ExpirationDateUtc, DeliveryDelayedUntilUtc, DeliveryCount, DeliveryDateUtc, DeliveryKey, InvisibleUntilUtc)"
                + " values (@subscriptionId, @topicEventId, @eventName, @publicationDateUtc, @functionalKey, @priority, @payloadId, @expirationDateUtc, @deliveryDelayedUntilUtc, @deliveryCount, @deliveryDateUtc, @deliveryKey, @invisibleUntilUtc)"
                + $";select {GetLastAutoIncrementValue} as 'Id'",
                parameters).ConfigureAwait(false);

            return ids.Single();
        }

        private async Task<SubscriptionEvent> GetSubscriptionEvent(Int64 id)
        {
            var subs = await TranQueryAsync<SubscriptionEvent>("select se.*, s.Ordered from SubscriptionEvent se" + // Ordered flag included for efficiency
                " join Subscription s on s.Id = se.SubscriptionId" +
                " where se.Id = @id",
                new Dictionary<string, object>
                {
                    { "@id", id },
                }).ConfigureAwait(false);
            return subs.SingleOrDefault();
        }

        private async Task<int> AddConsumedSubscriptionEvent(SubscriptionEvent subscriptionEvent)
        {
            var utcNow = await GetNowUtcAsync().ConfigureAwait(false);
            return await TranExecuteAsync("insert into ConsumedSubscriptionEvent (Id, SubscriptionId, EventName, PublicationDateUtc, FunctionalKey, Priority, PayloadId, DeliveryDateUtc, DeliveryCount, ConsumedDateUtc)" +
                " values (@id, @subscriptionId, @eventName, @publicationDateUtc, @functionalKey, @priority, @payloadId, @deliveryDateUtc, @deliveryCount, @consumedDateUtc)",
                new Dictionary<string, object>
                {
                { "@id", subscriptionEvent.Id },
                { "@subscriptionId", subscriptionEvent.SubscriptionId },
                { "@eventName", subscriptionEvent.EventName },
                { "@publicationDateUtc", subscriptionEvent.PublicationDateUtc },
                { "@functionalKey", subscriptionEvent.FunctionalKey },
                { "@priority", subscriptionEvent.Priority },
                { "@payloadId", subscriptionEvent.PayloadId },
                { "@deliveryDateUtc", subscriptionEvent.DeliveryDateUtc },
                { "@deliveryCount", subscriptionEvent.DeliveryCount },
                { "@consumedDateUtc", utcNow },
                }).ConfigureAwait(false);
        }

        public abstract Task<int> UpdateLastConsumedSubscriptionEvent(SubscriptionEvent subscriptionEvent);

        private async Task<int> AddFailedSubscriptionEvent(SubscriptionEvent subscriptionEvent, Reason reason)
        {
            var utcNow = await GetNowUtcAsync().ConfigureAwait(false);
            return await TranExecuteAsync("insert into FailedSubscriptionEvent (Id, SubscriptionId, EventName, PublicationDateUtc, FunctionalKey, Priority, PayloadId, DeliveryDateUtc, DeliveryCount, FailedDateUtc, Reason, ReasonOther)" +
                " values (@id, @subscriptionId, @eventName, @publicationDateUtc, @functionalKey, @priority, @payloadId, @deliveryDateUtc, @deliveryCount, @failedDateUtc, @reason, @reasonOther)",
                new Dictionary<string, object>
                {
                    { "@id", subscriptionEvent.Id },
                    { "@subscriptionId", subscriptionEvent.SubscriptionId },
                    { "@eventName", subscriptionEvent.EventName },
                    { "@publicationDateUtc", subscriptionEvent.PublicationDateUtc },
                    { "@functionalKey", subscriptionEvent.FunctionalKey },
                    { "@priority", subscriptionEvent.Priority },
                    { "@payloadId", subscriptionEvent.PayloadId },
                    { "@deliveryDateUtc", subscriptionEvent.DeliveryDateUtc },
                    { "@deliveryCount", subscriptionEvent.DeliveryCount },
                    { "@failedDateUtc", utcNow },
                    { "@reason", (int)reason.Type },
                    { "@reasonOther", reason.ReasonText },
                }).ConfigureAwait(false);
        }
        #endregion

        #region Event consumption

        protected abstract Task<IEnumerable<ConsumableEvent>> ConsumeNextForSubscription(Subscription subscription, int visibilityTimeout, int maxCount);

        public async Task<IEnumerable<ConsumableEvent>> ConsumeNextAsync(string subscriptionName, int visibilityTimeout, int maxCount)
        {
            var subscription = await GetSubscriptionByNameAsync(subscriptionName).ConfigureAwait(false);
            if (subscription == null) throw new ArgumentException($"No subscription with this name exists: {subscriptionName}");

            return await ConsumeNextForSubscription(subscription, visibilityTimeout, maxCount).ConfigureAwait(false);
        }

        public virtual async Task MarkConsumedAsync(IEnumerable<ConsumableEventId> consumableEventsIds, bool transactional = true)
        {
            var multiple = consumableEventsIds.Count() > 1;

            if (multiple && transactional)
                await BeginTransactionAsync().ConfigureAwait(false);

            try
            {
                if (this.ParallelQueriesSupport)
                {
                    // How smart is the usage of AsParallel here, in relation to deadlocks?
                    consumableEventsIds.AsParallel().ForAll((ceId) =>
                    {
                        Task.Run(async () => // Threadpool task to wait for async parts in inner task (ExecuteWork)
                        {
                            try
                            {
                                await MarkConsumedAsync(ceId.Id, ceId.DeliveryKey, multiple && transactional).ConfigureAwait(false);
                            }
                            catch (Exception)
                            {
                                if (transactional)
                                    throw;
                                // Otherwise ignore (event will be retried and we continue to mark the other events in the set as consumed)
                            }
                        }).GetAwaiter().GetResult(); // Need to block, because ForAll does not
                    });
                }
                else
                {
                    foreach (var ceId in consumableEventsIds)
                    {
                        try
                        {
                            await MarkConsumedAsync(ceId.Id, ceId.DeliveryKey, multiple).ConfigureAwait(false);
                        }
                        catch (Exception)
                        {
                            if (transactional)
                                throw;
                            // Otherwise ignore (event will be retried and we continue to mark the other events in the set as consumed)
                        }
                    }
                }

                if (multiple && transactional)
                    await CommitTransactionAsync().ConfigureAwait(false);
            }
            catch (Exception)
            {
                if (multiple && transactional)
                    await RollbackTransactionAsync().ConfigureAwait(false);

                throw;
            }
        }

        public virtual async Task MarkConsumedAsync(Int64 id, string deliveryKey)
        {
            await MarkConsumedAsync(id, deliveryKey, false).ConfigureAwait(false);
        }

        public virtual async Task MarkConsumedAsync(Int64 id, string deliveryKey, bool hasSurroundingTran)
        {
            var se = await GetSubscriptionEvent(id).ConfigureAwait(false);
            if (se == null) throw new ArgumentException($"No subscription-event found with id {id}. Maybe it has already been consumed (by another). Using a higher visibility timeout may help.");

            var utcNow = await GetNowUtcAsync().ConfigureAwait(false);
            if ((!se.DeliveryKey.Equals(deliveryKey, StringComparison.OrdinalIgnoreCase)) // Locked by another
                || (se.InvisibleUntilUtc < utcNow)) // expired
                throw new ArgumentException($"Subscription-event with id {id} has expired and/or it has already been locked again.");

            var sub = await GetSubscriptionAsync(se.SubscriptionId).ConfigureAwait(false);

            int attempt = 0;
            bool success = false;
            bool allowRetry = false;
            do
            {
                attempt++;
                // Reinit these on very loop:
                success = false;
                allowRetry = false;

                await BeginTransactionAsync().ConfigureAwait(false);
                try
                {
                    // 1. Remove from SubscriptionEvent
                    int rowsUpdated = await TranExecuteAsync("delete from SubscriptionEvent where Id = @id and DeliveryKey = @deliveryKey",
                        new Dictionary<string, object>
                        {
                        { "@id", se.Id },
                        { "@deliveryKey", se.DeliveryKey }, // Make sure we delete the one we just inspected (in race conditions it may have been locked again)
                        }).ConfigureAwait(false);

                    if (rowsUpdated == 0)
                        throw new ArgumentException($"Subscription-event with id {id} has expired while attempting to mark it complete. Maybe use higher a visibility timeout?");

                    // 2. Insert into ConsumedEvent
                    if (sub.LogConsumed)
                    {
                        rowsUpdated = await AddConsumedSubscriptionEvent(se).ConfigureAwait(false);
                        if (rowsUpdated == 0)
                            throw new InvalidOperationException($"Failed to add ConsumedSubscriptionEvent for SubscriptionEvent with id {id}.");
                    }

                    // 3. Upsert LastConsumedSubscriptionEvent (only for ordered subscription)
                    if (se.Ordered && se.FunctionalKey != null) // Only makes sense with a functional key
                    {
                        rowsUpdated = await UpdateLastConsumedSubscriptionEvent(se).ConfigureAwait(false);
                        if (rowsUpdated > 2) // On MySql an upsert (on duplicate key...) will report 2 rows hit (by design)
                            throw new InvalidOperationException($"Failed to upsert LastConsumedSubscriptionEvent for SubscriptionEvent with id {id}.");
                        else if (rowsUpdated == 0)
                            System.Diagnostics.Debug.WriteLine($"Warning: combination of SubscriptionId ({se.SubscriptionId}), FunctionalKey ({se.FunctionalKey}) and PublicationDateUtc ({se.PublicationDateUtc}) is found to be not unique for SubscriptionEvent with Id {se.Id}. Functional ordering cannot be guaranteed.");
                    }

                    await CommitTransactionAsync().ConfigureAwait(false);
                    success = true;
                }
                catch (DbException dbEx)
                {
                    await RollbackTransactionAsync().ConfigureAwait(false);

                    allowRetry = !hasSurroundingTran // It's impossible to retry because of the surrounding transaction; it cannot complete anymore because of the above rollback
                        && CanRetry(dbEx, attempt);

                    if (!allowRetry)
                    {
                        if (attempt > 1)
                            throw new RepoException($"Repo-action failed after {attempt} attempts", dbEx, RepoError.TooBusy);
                        else
                            throw new RepoException(dbEx);
                    }
                    else
                        await Task.Delay(TimeSpan.FromMilliseconds(50)).ConfigureAwait(false); // Wait shortly before retrying to let the repo take some breath
                }
                catch (Exception)
                {
                    await RollbackTransactionAsync().ConfigureAwait(false);
                    throw;
                }
            } while (!success && allowRetry);
        }

        public virtual async Task MarkFailedAsync(Int64 id, string deliveryKey, Reason reason)
        {
            await MarkFailedAsync(id, deliveryKey, reason, false).ConfigureAwait(false);
        }

        public virtual async Task MarkFailedAsync(Int64 id, string deliveryKey, Reason reason, bool hasSurroundingTran)
        {
            var se = await GetSubscriptionEvent(id).ConfigureAwait(false);
            if (se == null) throw new ArgumentException($"No subscription-event found with id {id}.");

            var utcNow = await GetNowUtcAsync().ConfigureAwait(false);
            if ((!se.DeliveryKey.Equals(deliveryKey, StringComparison.OrdinalIgnoreCase)) // Locked by another
                || (se.InvisibleUntilUtc < utcNow)) // expired
                throw new ArgumentException($"Subscription-event with id {id} has expired and/or it has already been locked again.");

            var sub = await GetSubscriptionAsync(se.SubscriptionId).ConfigureAwait(false);

            await BeginTransactionAsync().ConfigureAwait(false);
            try
            {
                // 1. Remove from SubscriptionEvent
                int rowsUpdated = await TranExecuteAsync("delete from SubscriptionEvent where Id = @id and DeliveryKey = @deliveryKey",
                    new Dictionary<string, object>
                    {
                        { "@id", se.Id },
                        { "@deliveryKey", se.DeliveryKey }, // Make sure we delete the one we just inspected (in race conditions it may have been locked again)
                    }).ConfigureAwait(false);

                if (rowsUpdated == 0)
                    throw new ArgumentException($"Subscription-event with id {id} has expired while attempting to mark it complete. Maybe use higher a visibility timeout?");

                // 2. Insert into FailedEvent
                if (sub.LogFailed)
                {
                    rowsUpdated = await AddFailedSubscriptionEvent(se, reason).ConfigureAwait(false);
                    if (rowsUpdated == 0)
                        throw new InvalidOperationException($"Failed to add FailedSubscriptionEvent for SubscriptionEvent with id {id} and reason {reason}.");
                }

                await CommitTransactionAsync().ConfigureAwait(false);
            }
            catch (Exception)
            {
                await RollbackTransactionAsync().ConfigureAwait(false);
                throw;
            }
        }
        #endregion
    }
}
