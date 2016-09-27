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
    public abstract class DbEventingRepo : IDisposable
    {
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
        public DbEventingRepo(IDbConnection conn)
        {
            _conn = conn;
            if (_conn.State == ConnectionState.Closed)
                _conn.Open();
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
                    if (_conn.State == ConnectionState.Open)
                        _conn.Close();
                    _conn.Dispose();
                }
            }
        }


        #region Transactions
        /// <summary>
        /// Starts a new transaction.
        /// NB: Transactions can be nested.
        /// </summary>
        public virtual void BeginTransaction()
        {
            lock (_tranLock)
            {
                if (_runningTransaction == null)
                {
                    _runningTransaction = _conn.BeginTransaction(IsolationLevel.ReadCommitted);
                    _tranState = TranState.Unchanged;
                }
                _tranCount++;
            }
        }

        /// <summary>
        /// Rolls back the the transaction and disposes it.
        /// Make sure there are no parallel threads/tasks still using the transaction!
        /// </summary>
        public virtual void RollbackTransaction()
        {
            if (_runningTransaction == null) // Check before waiting for lock to prevent unnessecary locks
                throw new ArgumentException($"No running transaction found");

            lock (_tranLock)
            {
                if (_runningTransaction == null)
                    throw new ArgumentException($"No running transaction found");

                if (_tranState != TranState.Rollbacked) // Nothing to do if already rolled back
                {
                    if (_runningTransaction.Connection != null) // Would be weird, since it's only cleared after a rollback or commit
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
        public virtual void CommitTransaction()
        {
            if (_runningTransaction == null) // Check before waiting for lock to prevent unnessecary locks
                throw new ArgumentException($"No running transaction found");

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
        protected virtual int TranExecute(string sql, object param = null, int? commandTimeout = null)
        {
            return _conn.Execute(sql, param: param, transaction: _runningTransaction, commandTimeout: commandTimeout);
        }

        /// <summary>
        /// Transacted query; if a transaction was started, the query will take place on/in it
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <param name="param"></param>
        /// <param name="commandTimeout"></param>
        /// <returns></returns>
        protected virtual IEnumerable<T> TranQuery<T>(string sql, object param = null, int? commandTimeout = null)
        {
            return _conn.Query<T>(sql, param: param, transaction: _runningTransaction, commandTimeout: commandTimeout);
        }
        #endregion

        #region Repo-specific DB-implementation

        /// <summary>
        /// Depending on the type of DbException, the specific repo can determine if a retry of the DB-call is usefull.
        /// </summary>
        /// <param name="dbEx">The DbException</param>
        /// <param name="attempts">The nr of attempts tried so far</param>
        /// <returns></returns>
        public virtual bool CanRetry(DbException dbEx, int attempts)
        {
            return false;
        }

        /// <summary>
        /// Function signature for getting the latest autoincrement value
        /// </summary>
        public abstract string GetLastAutoIncrementValue { get; }
        #endregion

        #region Topic and Subscription Management
        public Subscription AddOrUpdateSubscription(Subscription subscription)
        {
            Subscription existingSubscription = (subscription.Id.HasValue)
                            ? existingSubscription = GetSubscription(subscription.Id.Value)
                            : null;

            if (existingSubscription != null) // update
            {
                BeginTransaction();
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
                    };
                    TranExecute("update Subscription set Name = @name, DeliveryDelay = @deliveryDelay, MaxDeliveries = @maxDeliveries, Ordered = @ordered, TimeToLive = @timeToLive where Id = @id", parameters);

                    // Update TopicSubscriptions (by removing them and rebuilding them again)
                    RemoveTopicSubscriptions(subscription.Id.Value);
                    AddTopicSubscriptions(subscription.Id.Value, subscription.TopicSubscriptions);

                    CommitTransaction();
                }
                catch (Exception)
                {
                    RollbackTransaction();
                    throw;
                }

                return GetSubscription(subscription.Id.Value);
            }
            else
            {
                Int64 subscriptionId = Int64.MinValue;

                BeginTransaction();
                try
                {
                    var parameters = new Dictionary<string, object>
                        {
                            { "@name", subscription.Name },
                            { "@deliveryDelay", subscription.DeliveryDelay },
                            { "@maxDeliveries", subscription.MaxDeliveries },
                            { "@ordered", subscription.Ordered },
                            { "@timeToLive", subscription.TimeToLive },
                        };
                    subscriptionId = TranQuery<Int64>("insert into Subscription (Name, DeliveryDelay, MaxDeliveries, Ordered, TimeToLive) values (@name, @deliveryDelay, @maxDeliveries, @ordered, @timeToLive)"
                        + $";select {GetLastAutoIncrementValue} as 'Id'",
                        parameters).Single();
                    AddTopicSubscriptions(subscriptionId, subscription.TopicSubscriptions);
                    CommitTransaction();
                }
                catch (Exception)
                {
                    RollbackTransaction();
                    throw;
                }

                return GetSubscription(subscriptionId);
            }
        }

        private void AddTopicSubscriptions(Int64 subscriptionId, List<TopicSubscription> topicSubscriptions)
        {
            BeginTransaction();
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
                    var topicSubscriptionId = TranQuery<Int64>("insert into TopicSubscription (TopicId, SubscriptionId, Enabled, Filtered) values (@topicId, @subscriptionId, @enabled, @filtered)"
                        + $";select {GetLastAutoIncrementValue} as 'Id'",
                        parameters).Single();

                    if (topicSubscription.Filters != null)
                    {
                        foreach (var filter in topicSubscription.Filters)
                        {
                            parameters = new Dictionary<string, object>
                            {
                                { "@topicSubscriptionId", topicSubscriptionId },
                                { "@header", filter.Header },
                                { "@matchExpression", filter.MatchExpression },
                            };
                            TranExecute("insert into TopicSubscriptionFilter (TopicSubscriptionId, Header, MatchExpression) values (@topicSubscriptionId, @header, @matchExpression)", parameters);
                        }
                    }
                }
                CommitTransaction();
            }
            catch (Exception)
            {
                RollbackTransaction();
                throw;
            }
        }

        private void RemoveTopicSubscriptions(Int64 subscriptionId)
        {
            BeginTransaction();
            try
            {
                var parameters = new Dictionary<string, object> { { "@subscriptionId", subscriptionId } };

                // Delete topicsubscriptionfilters
                var query = "delete tsf from TopicSubscriptionFilter tsf" +
                    " join TopicSubscription ts on ts.Id = tsf.TopicSubscriptionId" +
                    " where ts.SubscriptionId = @subscriptionId";
                TranExecute(query, parameters);

                // Delete topicsubscriptions
                query = "delete ts from TopicSubscription ts" +
                    " where ts.SubscriptionId = @subscriptionId";
                TranExecute(query, parameters);

                CommitTransaction();
            }
            catch (Exception)
            {
                RollbackTransaction();
                throw;
            }
        }

        public Topic AddOrUpdateTopic(Topic topic)
        {
            Topic existingTopic = (topic.Id.HasValue)
                ? existingTopic = GetTopic(topic.Id.Value)
                : null;

            if (existingTopic != null) // update
            {
                var parameters = new Dictionary<string, object>
                {
                    { "@id", topic.Id.Value },
                    { "@name", topic.Name },
                    { "@notes", topic.Notes },
                };
                TranExecute("update Topic set Name = @name, Notes = @notes where Id = @id", parameters);
                return GetTopic(topic.Id.Value);
            }
            else
            {
                var parameters = new Dictionary<string, object>
                {
                    { "@name", topic.Name },
                    { "@notes", topic.Notes },
                };
                var topicId = TranQuery<Int64>("insert into Topic (Name, Notes) values (@name, @notes)"
                    + $";select {GetLastAutoIncrementValue} as 'Id'",
                    parameters).Single();
                return GetTopic(topicId);
            }
        }

        public void DeleteSubscription(Int64 id)
        {
            BeginTransaction();
            try
            {
                //TranExecute("delete from SubscriptionEvent where SubscriptionId = @subscriptionId",
                //    new Dictionary<string, object>
                //    {
                //        { "@subscriptionId", id.ToDbKey() },
                //    });
                RemoveTopicSubscriptions(id);
                TranExecute("delete from Subscription where Id = @id",
                    new Dictionary<string, object>
                    {
                        { "@id", id },
                    });
                CommitTransaction();
            }
            catch (Exception)
            {
                RollbackTransaction();
                throw;
            }
        }

        public void DeleteTopic(Int64 id, bool inclSubscriptions)
        {
            BeginTransaction();
            try
            {
                if (inclSubscriptions)
                {
                    // Delete all topicsubscriptions in subscriptions for the specified topic
                    var subscriptions = GetSubscriptions(topicId: id);
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
                        TranExecute(query, parameters);

                        // Delete topicsubscriptions
                        query = "delete ts from TopicSubscription ts" +
                            " where ts.SubscriptionId = @subscriptionId and ts.TopicId = @topicId";
                        TranExecute(query, parameters);
                    }
                }

                TranExecute("delete from Topic where Id = @id", // No check on rowcount, if it aint there, it's fine
                    new Dictionary<string, object>
                    {
                        { "@id", id },
                    });

                CommitTransaction();
            }
            catch (Exception)
            {
                RollbackTransaction();
                throw;
            }
        }

        /// <summary>
        /// Returns all subscriptions, or optionally only the subscriptions that subscribe to (at least) the specified topic.
        /// </summary>
        /// <param name="topicId"></param>
        /// <returns></returns>
        public IEnumerable<Subscription> GetSubscriptions(Int64? topicId = null)
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

            var matchedSubscriptions = TranQuery<Identifier>(query, parameters);
            var subs = new List<Subscription>();
            foreach (var id in matchedSubscriptions.Select(ms => ms.Id))
            {
                subs.Add(GetSubscription(id));
            }

            return subs;
        }

        public Subscription GetSubscription(Int64 id)
        {
            var subscription = TranQuery<Subscription>("select * from Subscription where id = @id",
                new Dictionary<string, object> { { "@id", id } })
                .SingleOrDefault();
            if (subscription == null)
                return null;

            // Add topic-subscriptions
            var topicSubscriptions = TranQuery<TopicSubscription>("select * from TopicSubscription where SubscriptionId = @id",
                new Dictionary<string, object> { { "@id", id } }).ToList();
            foreach (var topicSubscription in topicSubscriptions)
            {
                var topicSubscriptionFilters = TranQuery<TopicSubscriptionFilter>("select * from TopicSubscriptionFilter where TopicSubscriptionId = @topicSubscriptionId",
                    new Dictionary<string, object> { { "@topicSubscriptionId", topicSubscription.Id } }).ToList();
                topicSubscription.Filters = topicSubscriptionFilters;
            }
            subscription.TopicSubscriptions = topicSubscriptions;

            return subscription;
        }

        public Subscription GetSubscriptionByName(string name)
        {
            var subscription = TranQuery<Subscription>("select * from Subscription where name = @name",
                new Dictionary<string, object> { { "@name", name } }).SingleOrDefault();
            if (subscription == null)
                return null;

            // Add topic-subscriptions
            var topicSubscriptions = TranQuery<TopicSubscription>("select * from TopicSubscription where SubscriptionId = @id",
                new Dictionary<string, object> { { "@id", subscription.Id } }).ToList();
            foreach (var topicSubscription in topicSubscriptions)
            {
                var topicSubscriptionFilters = TranQuery<TopicSubscriptionFilter>("select * from TopicSubscriptionFilter where TopicSubscriptionId = @topicSubscriptionId",
                    new Dictionary<string, object> { { "@topicSubscriptionId", topicSubscription.Id } }).ToList();
                topicSubscription.Filters = topicSubscriptionFilters;
            }
            subscription.TopicSubscriptions = topicSubscriptions;

            return subscription;
        }

        public Topic GetTopic(Int64 id)
        {
            var parameters = new Dictionary<string, object>
                {
                    { "@id", id },
                };

            return TranQuery<Topic>("select * from Topic where id = @id", parameters)
                .SingleOrDefault();
        }

        public Topic GetTopicByName(string name)
        {
            var parameters = new Dictionary<string, object>
                {
                    { "@name", name },
                };

            return TranQuery<Topic>("select * from Topic where name = @name", parameters)
                .SingleOrDefault();
        }

        public IEnumerable<Topic> GetTopics(string partOfName = null)
        {
            if (partOfName != null)
            {
                var parameters = new Dictionary<string, object>
                {
                    { "@partOfName", $"%{partOfName}%"},
                };
                return TranQuery<Topic>("select * from Topic where Name like @partOfName", parameters);
            }
            else
                return TranQuery<Topic>("select * from Topic");
        }
        #endregion

        #region Event publication
        public Int64 StorePayload(string payload)
        {
            var parameters = new Dictionary<string, object>
                {
                    { "@payload", payload },
                };
            var id = TranQuery<Int64>("insert into EventPayload (Payload) values (@payload)" +
                $";select {GetLastAutoIncrementValue} as 'Id'",
                parameters).SingleOrDefault();
            return id;
        }

        public string GetPayload(Int64 id)
        {
            return TranQuery<string>("select Payload from EventPayload where Id = @id",
                new Dictionary<string, object>
                {
                    { "@id", id },
                })
                .SingleOrDefault();
        }

        public int DeletePayload(Int64 id)
        {
            return TranExecute("delete EventPayload where Id = @id",
                new Dictionary<string, object> { { "@id", id } });
        }

        public Int64 AddTopicEvent(TopicEvent topicEvent)
        {
            if (topicEvent == null) throw new ArgumentNullException("topicEvent");
            if (topicEvent.Id.HasValue) throw new ArgumentException("topicEvent.Id cannot have a value", "topicEvent");

            var headers = topicEvent.Headers != null ? JsonConvert.SerializeObject(topicEvent.Headers) : null; // Just serialization. Not used anymore (filtering uses the original dictionary).
            var parameters = new Dictionary<string, object>
                {
                    { "@topicId", topicEvent.TopicId },
                    { "@functionalKey", topicEvent.FunctionalKey },
                    { "@publicationDateUtc", topicEvent.PublicationDateUtc },
                    { "@expirationDateUtc", topicEvent.ExpirationDateUtc },
                    { "@headers", headers },
                    { "@priority", topicEvent.Priority },
                    { "@payloadId", topicEvent.PayloadId },
                };
            var id = TranQuery<Int64>("insert into TopicEvent (TopicId, FunctionalKey, PublicationDateUtc, ExpirationDateUtc, Headers, Priority, PayloadId) values (@topicId, @functionalKey, @publicationDateUtc, @expirationDateUtc, @headers, @priority, @payloadId)" +
                $";select {GetLastAutoIncrementValue} as 'Id'",
                parameters).Single();

            return id;
        }

        public Int64 AddSubscriptionEvent(SubscriptionEvent subscriptionEvent)
        {
            if (subscriptionEvent == null) throw new ArgumentNullException("subscriptionEvent");
            if (subscriptionEvent.Id.HasValue) throw new ArgumentException("subscriptionEvent.Id cannot have a value", "subscriptionEvent");

            var parameters = new Dictionary<string, object>
                {
                    { "@subscriptionId", subscriptionEvent.SubscriptionId },
                    { "@topicEventId", subscriptionEvent.TopicEventId },
                    { "@publicationDateUtc", subscriptionEvent.PublicationDateUtc },
                    { "@functionalKey", subscriptionEvent.FunctionalKey },
                    { "@priority", subscriptionEvent.Priority },
                    { "@payloadId", subscriptionEvent.PayloadId },
                    { "@expirationDateUtc", subscriptionEvent.ExpirationDateUtc },
                    { "@deliveryDelayedUntilUtc", subscriptionEvent.DeliveryDelayedUntilUtc },
                    { "@deliveryCount", default(int) },
                    { "@deliveryDateUtc", default(DateTime?) },
                    { "@deliveryKey", default(string) },
                    { "@invisibleUntilUtc", default(DateTime?) },
                };
            var id = TranQuery<Int64>("insert into SubscriptionEvent (SubscriptionId, TopicEventId, PublicationDateUtc, FunctionalKey, Priority, PayloadId, ExpirationDateUtc, DeliveryDelayedUntilUtc, DeliveryCount, DeliveryDateUtc, DeliveryKey, InvisibleUntilUtc)"
                + " values (@subscriptionId, @topicEventId, @publicationDateUtc, @functionalKey, @priority, @payloadId, @expirationDateUtc, @deliveryDelayedUntilUtc, @deliveryCount, @deliveryDateUtc, @deliveryKey, @invisibleUntilUtc)"
                + $";select {GetLastAutoIncrementValue} as 'Id'",
                parameters).Single();

            return id;
        }

        private SubscriptionEvent GetSubscriptionEvent(Int64 id)
        {
            return TranQuery<SubscriptionEvent>("select se.*, s.Ordered from SubscriptionEvent se" + // Ordered flag included for efficiency
                " join Subscription s on s.Id = se.SubscriptionId" +
                " where se.Id = @id",
                new Dictionary<string, object>
                {
                    { "@id", id },
                })
                .SingleOrDefault();
        }

        private int AddConsumedSubscriptionEvent(SubscriptionEvent subscriptionEvent)
        {
            return TranExecute("insert into ConsumedSubscriptionEvent (Id, SubscriptionId, PublicationDateUtc, FunctionalKey, Priority, PayloadId, DeliveryDateUtc, ConsumedDateUtc)" +
                " values (@id, @subscriptionId, @publicationDateUtc, @functionalKey, @priority, @payloadId, @deliveryDateUtc, @consumedDateUtc)",
                new Dictionary<string, object>
                {
                { "@id", subscriptionEvent.Id },
                { "@subscriptionId", subscriptionEvent.SubscriptionId },
                { "@publicationDateUtc", subscriptionEvent.PublicationDateUtc },
                { "@functionalKey", subscriptionEvent.FunctionalKey },
                { "@priority", subscriptionEvent.Priority },
                { "@payloadId", subscriptionEvent.PayloadId },
                { "@deliveryDateUtc", subscriptionEvent.DeliveryDateUtc },
                { "@consumedDateUtc", DateTime.UtcNow },
                });
        }

        public abstract int UpdateLastConsumedSubscriptionEvent(SubscriptionEvent subscriptionEvent);

        private int AddFailedSubscriptionEvent(SubscriptionEvent subscriptionEvent, Reason reason)
        {
            return TranExecute("insert into FailedSubscriptionEvent (Id, SubscriptionId, PublicationDateUtc, FunctionalKey, Priority, PayloadId, DeliveryDateUtc, FailedDateUtc, Reason, ReasonOther)" +
                " values (@id, @subscriptionId, @publicationDateUtc, @functionalKey, @priority, @payloadId, @deliveryDateUtc, @failedDateUtc, @reason, @reasonOther)",
                new Dictionary<string, object>
                {
                    { "@id", subscriptionEvent.Id },
                    { "@subscriptionId", subscriptionEvent.SubscriptionId },
                    { "@publicationDateUtc", subscriptionEvent.PublicationDateUtc },
                    { "@functionalKey", subscriptionEvent.FunctionalKey },
                    { "@priority", subscriptionEvent.Priority },
                    { "@payloadId", subscriptionEvent.PayloadId },
                    { "@deliveryDateUtc", subscriptionEvent.DeliveryDateUtc },
                    { "@failedDateUtc", DateTime.UtcNow },
                    { "@reason", (int)reason.Type },
                    { "@reasonOther", reason.ReasonText },
                });
        }
        #endregion

        #region Event consumption

        protected abstract IEnumerable<ConsumableEvent> ConsumeNextForSubscription(Subscription subscription, int visibilityTimeout, int maxCount);

        public IEnumerable<ConsumableEvent> ConsumeNext(string subscriptionName, int visibilityTimeout, int maxCount)
        {
            var subscription = GetSubscriptionByName(subscriptionName);
            if (subscription == null) throw new ArgumentException($"No subscription with this name exists: {subscriptionName}");

            return ConsumeNextForSubscription(subscription, visibilityTimeout, maxCount);
        }

        public virtual void MarkConsumed(Int64 id, string deliveryKey)
        {
            var se = GetSubscriptionEvent(id);
            if (se == null) throw new ArgumentException($"No subscription-event found with id {id}. Maybe it has already been consumed (by another). Using a higher visibility timeout may help.");

            if (!se.DeliveryKey.Equals(deliveryKey, StringComparison.OrdinalIgnoreCase) // Mismatch is only ok... (we DID consume it)
               && se.InvisibleUntilUtc > DateTime.UtcNow) // ... if not currently locked
                throw new ArgumentException($"Subscription-event with id {id} had expired and it has already been locked again.");

            int attempts = 0;
            bool success = false;
            bool allowRetry = false;
            do
            {
                attempts++;
                // Reinit these on very loop:
                success = false;
                allowRetry = false;

                BeginTransaction();
                try
                {
                    // 1. Remove from SubscriptionEvent
                    int rowsUpdated = TranExecute("delete from SubscriptionEvent where Id = @id and DeliveryKey = @deliveryKey",
                        new Dictionary<string, object>
                        {
                        { "@id", se.Id },
                        { "@deliveryKey", se.DeliveryKey }, // Make sure we delete the one we just inspected (in race conditions it may have been locked again)
                        });

                    if (rowsUpdated == 0)
                        throw new ArgumentException($"Subscription-event with id {id} has expired while attempting to mark it complete. Maybe use higher a visibility timeout?");

                    // 2. Insert into ConsumedEvent
                    rowsUpdated = AddConsumedSubscriptionEvent(se);
                    if (rowsUpdated == 0)
                        throw new InvalidOperationException($"Failed to add ConsumedSubscriptionEvent for SubscriptionEvent with id {id}.");

                    // 3. Upsert LastConsumedSubscriptionEvent (only for ordered subscription)
                    if (se.Ordered && se.FunctionalKey != null) // Only makes sense with a functional key
                    {
                        rowsUpdated = UpdateLastConsumedSubscriptionEvent(se);
                        if (rowsUpdated > 2) // On MySql an upsert (on duplicate key...) will report 2 rows hit (by design)
                            throw new InvalidOperationException($"Failed to upsert LastConsumedSubscriptionEvent for SubscriptionEvent with id {id}.");
                        else if (rowsUpdated == 0)
                            System.Diagnostics.Debug.WriteLine($"Warning: combination of SubscriptionId ({se.SubscriptionId}), FunctionalKey ({se.FunctionalKey}) and PublicationDateUtc ({se.PublicationDateUtc}) is found to be not unique for SubscriptionEvent with Id {se.Id}. Functional ordering cannot be guaranteed.");
                    }

                    CommitTransaction();
                    success = true;
                }
                catch (DbException dbEx)
                {
                    RollbackTransaction();
                    allowRetry = CanRetry(dbEx, attempts);
                    if (!allowRetry)
                        throw;
                }
                catch (Exception)
                {
                    RollbackTransaction();
                    throw;
                }
            } while (!success && allowRetry);
        }

        public virtual void MarkFailed(Int64 id, string deliveryKey, Reason reason)
        {
            var se = GetSubscriptionEvent(id);
            if (se == null) throw new ArgumentException($"No subscription-event found with id {id}.");

            if (!se.DeliveryKey.Equals(deliveryKey, StringComparison.OrdinalIgnoreCase) // Mismatch is only ok... (we DID consume it)
               && se.InvisibleUntilUtc > DateTime.UtcNow) // ... If not currently locked
                throw new ArgumentException($"Subscription-event with id {id} had expired and it has already been locked again.");

            BeginTransaction();
            try
            {
                // 1. Remove from SubscriptionEvent
                int rowsUpdated = TranExecute("delete from SubscriptionEvent where Id = @id and DeliveryKey = @deliveryKey",
                    new Dictionary<string, object>
                    {
                        { "@id", se.Id },
                        { "@deliveryKey", se.DeliveryKey }, // Make sure we delete the one we just inspected (in race conditions it may have been locked again)
                    });

                if (rowsUpdated == 0)
                    throw new ArgumentException($"Subscription-event with id {id} has expired while attempting to mark it complete. Maybe use higher a visibility timeout?");

                // 2. Insert into ConsumedEvent
                rowsUpdated = AddFailedSubscriptionEvent(se, reason);
                if (rowsUpdated == 0)
                    throw new InvalidOperationException($"Failed to add FailedSubscriptionEvent for SubscriptionEvent with id {id} and reason {reason}.");

                CommitTransaction();
            }
            catch (Exception)
            {
                RollbackTransaction();
                throw;
            }
        }
        #endregion

    }
}
