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

namespace Resonance.Repo.Database
{
    //2627: Duplicate key
    //1205: Deadlock victim

    public class MsSqlEventingRepo : IEventingRepo, IDisposable
    {
        protected readonly IDbConnection _conn;
        protected IDbTransaction _runningTransaction;
        protected int _tranCount = 0;
        protected object _tranLock = new object();

        /// <summary>
        /// Creates a new MsSqlEventingRepo.
        /// </summary>
        /// <param name="conn">IDbConnection to use. If not yet opened, it will be opened here.</param>
        public MsSqlEventingRepo(IDbConnection conn)
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
        public void BeginTransaction()
        {
            lock (_tranLock)
            {
                if (_runningTransaction == null)
                {
                    _runningTransaction = _conn.BeginTransaction(IsolationLevel.ReadCommitted);
                }
                _tranCount++;
            }
        }

        /// <summary>
        /// Rolls back the the transaction and disposes it.
        /// Make sure there are no parallel threads/tasks still using the transaction!
        /// </summary>
        public void RollbackTransaction()
        {
            if (_runningTransaction == null) // Check before waiting for lock to prevent unnessecary locks
                throw new ArgumentException($"No running transaction found");

            lock (_tranLock)
            {
                if (_runningTransaction == null)
                    throw new ArgumentException($"No running transaction found");

                if (_runningTransaction.Connection != null) // Has not yet been committed/rollbacked
                    _runningTransaction.Rollback();

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
        public void CommitTransaction()
        {
            if (_runningTransaction == null) // Check before waiting for lock to prevent unnessecary locks
                throw new ArgumentException($"No running transaction found");

            lock (_tranLock)
            {
                if (_runningTransaction == null)
                    throw new ArgumentException($"No running transaction found");

                if (_runningTransaction.Connection != null) // Has not yet been committed/rollbacked
                    _runningTransaction.Commit();

                _tranCount--;
                if (_tranCount == 0)
                {
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
        protected int TranExecute(string sql, object param = null, int? commandTimeout = null)
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
        protected IEnumerable<T> TranQuery<T>(string sql, object param = null, int? commandTimeout = null)
        {
            return _conn.Query<T>(sql, param: param, transaction: _runningTransaction, commandTimeout: commandTimeout);
        }
        #endregion

        #region Topic and Subscription Management
        public Subscription AddOrUpdateSubscription(Subscription subscription)
        {
            Subscription existingSubscription = (subscription.Id != null)
                            ? existingSubscription = GetSubscription(subscription.Id)
                            : null;

            if (existingSubscription != null) // update
            {
                BeginTransaction();
                try
                {
                    var parameters = new Dictionary<string, object>
                    {
                        { "@id", subscription.Id.ToDbKey() },
                        { "@name", subscription.Name },
                        { "@deliveryDelay", subscription.DeliveryDelay },
                        { "@maxDeliveries", subscription.MaxDeliveries },
                        { "@ordered", subscription.Ordered },
                        { "@timeToLive", subscription.TimeToLive },
                    };
                    TranExecute("update Subscription set Name = @name, DeliveryDelay = @deliveryDelay, MaxDeliveries = @maxDeliveries, Ordered = @ordered, TimeToLive = @timeToLive where Id = @id", parameters);

                    // Update TopicSubscriptions (by removing them and rebuilding them again)
                    RemoveTopicSubscriptions(subscription.Id);
                    AddTopicSubscriptions(subscription.Id, subscription.TopicSubscriptions);

                    CommitTransaction();
                }
                catch (Exception)
                {
                    RollbackTransaction();
                    throw;
                }

                return GetSubscription(subscription.Id);
            }
            else
            {
                var subscriptionId = subscription.Id != null ? subscription.Id : Guid.NewGuid().ToString();

                BeginTransaction();
                try
                {
                    var parameters = new Dictionary<string, object>
                        {
                            { "@id", subscriptionId.ToDbKey() },
                            { "@name", subscription.Name },
                            { "@deliveryDelay", subscription.DeliveryDelay },
                            { "@maxDeliveries", subscription.MaxDeliveries },
                            { "@ordered", subscription.Ordered },
                            { "@timeToLive", subscription.TimeToLive },
                        };
                    TranExecute("insert into Subscription (Id, Name, DeliveryDelay, MaxDeliveries, Ordered, TimeToLive) values (@id, @name, @deliveryDelay, @maxDeliveries, @ordered, @timeToLive)", parameters);
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

        private void AddTopicSubscriptions(string subscriptionId, List<TopicSubscription> topicSubscriptions)
        {
            BeginTransaction();
            try
            {
                foreach (var topicSubscription in topicSubscriptions)
                {
                    var topicSubscriptionId = topicSubscription.Id != null ? topicSubscription.Id : Guid.NewGuid().ToString();
                    var parameters = new Dictionary<string, object>
                        {
                            { "@id", topicSubscriptionId.ToDbKey() },
                            { "@topicId", topicSubscription.TopicId.ToDbKey() },
                            { "@subscriptionId", subscriptionId.ToDbKey() },
                            { "@enabled", topicSubscription.Enabled },
                            { "@filtered", topicSubscription.Filtered },
                        };
                    TranExecute("insert into TopicSubscription (Id, TopicId, SubscriptionId, Enabled, Filtered) values (@id, @topicId, @subscriptionId, @enabled, @filtered)", parameters);

                    if (topicSubscription.Filters != null)
                    {
                        foreach (var filter in topicSubscription.Filters)
                        {
                            var topicSubscriptionFilterId = filter.Id != null ? filter.Id : Guid.NewGuid().ToString();
                            parameters = new Dictionary<string, object>
                        {
                            { "@id", topicSubscriptionFilterId.ToDbKey() },
                            { "@topicSubscriptionId", topicSubscriptionId.ToDbKey() },
                            { "@header", filter.Header },
                            { "@matchExpression", filter.MatchExpression },
                        };
                            TranExecute("insert into TopicSubscriptionFilter (Id, TopicSubscriptionId, Header, MatchExpression) values (@id, @topicSubscriptionId, @header, @matchExpression)", parameters);
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

        private void RemoveTopicSubscriptions(string subscriptionId)
        {
            BeginTransaction();
            try
            {
                var parameters = new Dictionary<string, object> { { "@subscriptionId", subscriptionId.ToDbKey() } };

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
            }
        }

        public Topic AddOrUpdateTopic(Topic topic)
        {
            Topic existingTopic = (topic.Id != null)
                ? existingTopic = GetTopic(topic.Id)
                : null;

            if (existingTopic != null) // update
            {
                var parameters = new Dictionary<string, object>
                {
                    { "@id", topic.Id.ToDbKey() },
                    { "@name", topic.Name },
                    { "@notes", topic.Notes },
                };
                TranExecute("update Topic set Name = @name, Notes = @notes where Id = @id", parameters);
                return GetTopic(topic.Id);
            }
            else
            {
                var topicId = topic.Id != null ? topic.Id : Guid.NewGuid().ToString();
                var parameters = new Dictionary<string, object>
                {
                    { "@id", topicId.ToDbKey() },
                    { "@name", topic.Name },
                    { "@notes", topic.Notes },
                };
                TranExecute("insert into Topic (Id, Name, Notes) values (@id, @name, @notes)", parameters);
                return GetTopic(topicId);
            }
        }

        public void DeleteSubscription(string id)
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
                        { "@id", id.ToDbKey() },
                    });
                CommitTransaction();
            }
            catch (Exception)
            {
                RollbackTransaction();
                throw;
            }
        }

        public void DeleteTopic(string id, bool inclSubscriptions)
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
                        { "@subscriptionId", subscription.Id.ToDbKey() },
                        { "@topicId", id.ToDbKey() },
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
                        { "@id", id.ToDbKey() },
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
        public IEnumerable<Subscription> GetSubscriptions(string topicId = null)
        {
            var parameters = new Dictionary<string, object>
                {
                    { "@topicId", topicId != null ? topicId.ToDbKey() : null },
                };

            var query = "select s.Id from Subscription s";
            if (topicId != null)
            {
                query = "select s.Id, ts.TopicId from Subscription s"
                    + " join TopicSubscription ts on ts.SubscriptionId = s.Id and ts.TopicId = @topicId"
                    + " group by s.Id, ts.TopicId having count(*) > 0"; // Matching at least once
            }

            var matchedSubscriptions = TranQuery<Identifier>(query, parameters);
            foreach (var id in matchedSubscriptions.Select(ms => ms.Id))
            {
                yield return GetSubscription(id);
            }
        }

        public Subscription GetSubscription(string id)
        {
            var subscription = TranQuery<Subscription>("select * from Subscription where id = @id",
                new Dictionary<string, object> { { "@id", id.ToDbKey() } })
                .SingleOrDefault();
            if (subscription == null)
                return null;

            // Add topic-subscriptions
            var topicSubscriptions = TranQuery<TopicSubscription>("select * from TopicSubscription where SubscriptionId = @id",
                new Dictionary<string, object> { { "@id", id.ToDbKey() } }).ToList();
            foreach (var topicSubscription in topicSubscriptions)
            {
                var topicSubscriptionFilters = TranQuery<TopicSubscriptionFilter>("select * from TopicSubscriptionFilter where TopicSubscriptionId = @topicSubscriptionId",
                    new Dictionary<string, object> { { "@topicSubscriptionId", topicSubscription.Id.ToDbKey() } }).ToList();
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
                new Dictionary<string, object> { { "@id", subscription.Id.ToDbKey() } }).ToList();
            foreach (var topicSubscription in topicSubscriptions)
            {
                var topicSubscriptionFilters = TranQuery<TopicSubscriptionFilter>("select * from TopicSubscriptionFilter where TopicSubscriptionId = @topicSubscriptionId",
                    new Dictionary<string, object> { { "@topicSubscriptionId", topicSubscription.Id.ToDbKey() } }).ToList();
                topicSubscription.Filters = topicSubscriptionFilters;
            }
            subscription.TopicSubscriptions = topicSubscriptions;

            return subscription;
        }

        public Topic GetTopic(string id)
        {
            var parameters = new Dictionary<string, object>
                {
                    { "@id", id.ToDbKey() },
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
        public string StorePayload(string payload)
        {
            var id = Guid.NewGuid().ToString();
            var parameters = new Dictionary<string, object>
                {
                    { "@id", id.ToDbKey() },
                    { "@payload", payload },
                };
            TranExecute("insert into EventPayload (Id, Payload) values (@id, @payload)", parameters);
            return id;
        }

        public string GetPayload(string id)
        {
            return TranQuery<string>("select Payload from EventPayload where Id = @id",
                new Dictionary<string, object>
                {
                    { "@id", id.ToDbKey() },
                })
                .SingleOrDefault();
        }

        public int DeletePayload(string id)
        {
            return TranExecute("delete EventPayload where Id = @id",
                new Dictionary<string, object> { { "@id", id.ToDbKey() } });
        }

        public string AddTopicEvent(TopicEvent topicEvent)
        {
            var id = topicEvent.Id != null ? topicEvent.Id : Guid.NewGuid().ToString();

            var headers = topicEvent.Headers != null ? JsonConvert.SerializeObject(topicEvent.Headers) : null; // Just serialization. Not used anymore (filtering uses the original dictionary).
            var parameters = new Dictionary<string, object>
                {
                    { "@id", id.ToDbKey() },
                    { "@topicId", topicEvent.TopicId.ToDbKey() },
                    { "@functionalKey", topicEvent.FunctionalKey },
                    { "@publicationDateUtc", topicEvent.PublicationDateUtc },
                    { "@expirationDateUtc", topicEvent.ExpirationDateUtc },
                    { "@headers", headers },
                    { "@priority", topicEvent.Priority },
                    { "@payloadId", topicEvent.PayloadId.ToDbKey() },
                };
            TranExecute("insert into TopicEvent (Id, TopicId, FunctionalKey, PublicationDateUtc, ExpirationDateUtc, Headers, Priority, PayloadId) values (@id, @topicId, @functionalKey, @publicationDateUtc, @expirationDateUtc, @headers, @priority, @payloadId)", parameters);
            return id;
        }

        public string AddSubscriptionEvent(SubscriptionEvent subscriptionEvent)
        {
            var id = subscriptionEvent.Id != null ? subscriptionEvent.Id : Guid.NewGuid().ToString();

            var parameters = new Dictionary<string, object>
                {
                    { "@id", id.ToDbKey() },
                    { "@subscriptionId", subscriptionEvent.SubscriptionId.ToDbKey() },
                    { "@topicEventId", subscriptionEvent.TopicEventId.ToDbKey() },
                    { "@publicationDateUtc", subscriptionEvent.PublicationDateUtc },
                    { "@functionalKey", subscriptionEvent.FunctionalKey },
                    { "@priority", subscriptionEvent.Priority },
                    { "@payloadId", subscriptionEvent.PayloadId.ToDbKey() },
                    { "@expirationDateUtc", subscriptionEvent.ExpirationDateUtc },
                    { "@deliveryDelayedUntilUtc", subscriptionEvent.DeliveryDelayedUntilUtc },
                    { "@deliveryCount", default(int) },
                    { "@deliveryDateUtc", default(DateTime?) },
                    { "@deliveryKey", default(string) },
                    { "@invisibleUntilUtc", default(DateTime?) },
                };
            TranExecute("insert into SubscriptionEvent (Id, SubscriptionId, TopicEventId, PublicationDateUtc, FunctionalKey, Priority, PayloadId, ExpirationDateUtc, DeliveryDelayedUntilUtc, DeliveryCount, DeliveryDateUtc, DeliveryKey, InvisibleUntilUtc)"
                + " values (@id, @subscriptionId, @topicEventId, @publicationDateUtc, @functionalKey, @priority, @payloadId, @expirationDateUtc, @deliveryDelayedUntilUtc, @deliveryCount, @deliveryDateUtc, @deliveryKey, @invisibleUntilUtc)", parameters);
            return id;
        }

        private SubscriptionEvent GetSubscriptionEvent(string id)
        {
            return TranQuery<SubscriptionEvent>("select * from SubscriptionEvent where Id = @id",
                new Dictionary<string, object>
                {
                    { "@id", id.ToDbKey() },
                })
                .SingleOrDefault();
        }

        private int AddConsumedSubscriptionEvent(SubscriptionEvent subscriptionEvent)
        {
            return TranExecute("insert into ConsumedSubscriptionEvent (Id, SubscriptionId, PublicationDateUtc, FunctionalKey, Priority, PayloadId, DeliveryDateUtc, ConsumedDateUtc)" +
                " values (@id, @subscriptionId, @publicationDateUtc, @functionalKey, @priority, @payloadId, @deliveryDateUtc, @consumedDateUtc)",
                new Dictionary<string, object>
                {
                { "@id", subscriptionEvent.Id.ToDbKey() },
                { "@subscriptionId", subscriptionEvent.SubscriptionId.ToDbKey() },
                { "@publicationDateUtc", subscriptionEvent.PublicationDateUtc },
                { "@functionalKey", subscriptionEvent.FunctionalKey },
                { "@priority", subscriptionEvent.Priority },
                { "@payloadId", subscriptionEvent.PayloadId.ToDbKey() },
                { "@deliveryDateUtc", subscriptionEvent.DeliveryDateUtc },
                { "@consumedDateUtc", DateTime.UtcNow },
                });
        }

        private int UpdateLastConsumedSubscriptionEvent(SubscriptionEvent subscriptionEvent)
        {
            var query = "MERGE LastConsumedSubscriptionEvent AS target" +
                " USING(SELECT @subscriptionId, @functionalKey) as source(SubscriptionId, FunctionalKey)" +
                " ON(target.SubscriptionId = source.SubscriptionId AND target.FunctionalKey = source.FunctionalKey)" +
                " WHEN MATCHED THEN UPDATE SET PublicationDateUtc = @publicationDateUtc" +
                " WHEN NOT MATCHED THEN INSERT(SubscriptionId, FunctionalKey, PublicationDateUtc) VALUES(source.SubscriptionId, source.FunctionalKey, @publicationDateUtc);";

            return TranExecute(query, new Dictionary<string, object>
            {
                { "@subscriptionId", subscriptionEvent.SubscriptionId },
                { "@functionalKey", subscriptionEvent.FunctionalKey },
                { "@publicationDateUtc", subscriptionEvent.PublicationDateUtc },
            });
        }

        private int AddFailedSubscriptionEvent(SubscriptionEvent subscriptionEvent, Reason reason)
        {
            return TranExecute("insert into FailedSubscriptionEvent (Id, SubscriptionId, PublicationDateUtc, FunctionalKey, Priority, PayloadId, DeliveryDateUtc, FailedDateUtc, Reason, ReasonOther)" +
                " values (@id, @subscriptionId, @publicationDateUtc, @functionalKey, @priority, @payloadId, @deliveryDateUtc, @failedDateUtc, @reason, @reasonOther)",
                new Dictionary<string, object>
                {
                    { "@id", subscriptionEvent.Id.ToDbKey() },
                    { "@subscriptionId", subscriptionEvent.SubscriptionId.ToDbKey() },
                    { "@publicationDateUtc", subscriptionEvent.PublicationDateUtc },
                    { "@functionalKey", subscriptionEvent.FunctionalKey },
                    { "@priority", subscriptionEvent.Priority },
                    { "@payloadId", subscriptionEvent.PayloadId.ToDbKey() },
                    { "@deliveryDateUtc", subscriptionEvent.DeliveryDateUtc },
                    { "@failedDateUtc", DateTime.UtcNow },
                    { "@reason", (int)reason.Type },
                    { "@reasonOther", reason.ReasonText },
                });
        }
        #endregion

        #region Event consumption

        public bool TryLockConsumableEvent(SubscriptionEventIdentifier sId, string deliveryKey, DateTime invisibleUntilUtc)
        {
            int rowsUpdated = TranExecute("update s" +
                " set s.DeliveryKey = @newDeliveryKey, s.DeliveryDateUtc = @deliveryDateUtc, s.InvisibleUntilUtc = @invisibleUntilUtc, s.DeliveryCount = s.DeliveryCount + 1" +
                " from SubscriptionEvent s" +
                " where s.Id = @id" +
                " and ( (s.DeliveryKey is NULL and @deliveryKey is null)" + // We use DeliveryKey for OCC, since it changes on every update anyway
                "     or s.DeliveryKey = @deliveryKey)",
                new Dictionary<string, object>
                {
                                    { "@id", sId.Id.ToDbKey() },
                                    { "@deliveryKey", sId.DeliveryKey },
                                    { "@deliveryDateUtc", DateTime.UtcNow },
                                    { "@newDeliveryKey", deliveryKey },
                                    { "@invisibleUntilUtc", invisibleUntilUtc },
                });

            return (rowsUpdated > 0);
        }

        public IEnumerable<SubscriptionEventIdentifier> FindConsumableEventsForSubscription(Subscription subscription, int maxCount)
        {
            int bufferSize = subscription.Ordered ? maxCount * 5 : maxCount; // When ordered delivery, the resultset must be filtered to make sure every functional key is unique

            string query = null;
            if (!subscription.Ordered)
            {
                query = $"select TOP {bufferSize} se.Id, se.DeliveryKey, se.FunctionalKey, se.PayloadId" // Get the minimal amount of data
                    + " from SubscriptionEvent se"
                    + " join Subscription s on s.Id = se.SubscriptionId" // Needed for MaxRetries
                    + " where se.SubscriptionId = @subscriptionId"
                    + " and (se.DeliveryDelayedUntilUtc IS NULL OR se.DeliveryDelayedUntilUtc < @utcNow)" // Must be allowed to be delivered
                    + " and (se.ExpirationDateUtc IS NULL OR se.ExpirationDateUtc > @utcNow)" // Must not yet have expired
                    + " and (se.InvisibleUntilUtc IS NULL OR se.InvisibleUntilUtc < @utcNow)" // Must not be 'locked'/made invisible by other consumer
                    + " and (s.MaxDeliveries = 0 OR s.MaxDeliveries > se.DeliveryCount)" // Must not have reached max. allowed delivery attempts
                    + " order by se.Priority DESC, se.PublicationDateUtc ASC"; // Highest prio first, oldest first
            }
            else
            {
                query = $"select TOP {bufferSize} se.Id, se.DeliveryKey, se.FunctionalKey, se.PayloadId" // Get the minimal amount of data
                    + " from SubscriptionEvent se"
                    + " join Subscription s on s.Id = se.SubscriptionId" // Needed for MaxRetries
                    + " left join LastConsumedSubscriptionEvent lc" // For functional ordering
                    + "   on lc.SubscriptionId = se.SubscriptionId and lc.FunctionalKey = se.FunctionalKey"
                    + " where se.SubscriptionId = @subscriptionId"
                    + " and (se.DeliveryDelayedUntilUtc IS NULL OR se.DeliveryDelayedUntilUtc < @utcNow)" // Must be allowed to be delivered
                    + " and (se.ExpirationDateUtc IS NULL OR se.ExpirationDateUtc > @utcNow)" // Must not yet have expired
                    + " and (se.InvisibleUntilUtc IS NULL OR se.InvisibleUntilUtc < @utcNow)" // Must not be 'locked'/made invisible by other consumer
                    + " and (s.MaxDeliveries = 0 OR s.MaxDeliveries > se.DeliveryCount)" // Must not have reached max. allowed delivery attempts
                    + " and	(lc.SubscriptionId IS NULL OR (lc.PublicationDateUtc < se.PublicationDateUtc))" // Newer than last published (TODO: PRIORITY!!)
                    + " order by se.Priority DESC, se.PublicationDateUtc ASC"; // Warning: prio can mess everything up!
            }

            // Get the list
            var sIds = TranQuery<SubscriptionEventIdentifier>(query, new Dictionary<string, object>
                {
                    { "@subscriptionId", subscription.Id.ToDbKey() },
                    { "@utcNow", DateTime.UtcNow },
                }).ToList();

            if (subscription.Ordered)
            {
                var functionalKeyGroups = sIds
                    .GroupBy(sId => sId.FunctionalKey.ToLowerInvariant())
                    .ToList();

                sIds = functionalKeyGroups.Select((g) =>
                    {
                        var first = g.First();
                        return new SubscriptionEventIdentifier
                        {
                            Id = first.Id,
                            DeliveryKey = first.DeliveryKey,
                            PayloadId = first.PayloadId,
                            FunctionalKey = first.FunctionalKey,
                        };
                    }).ToList();
            }

            return sIds;
        }

        public void MarkConsumed(string id, string deliveryKey)
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

                BeginTransaction();
                try
                {
                    // 1. Remove from SubscriptionEvent
                    int rowsUpdated = TranExecute("delete from SubscriptionEvent where Id = @id and DeliveryKey = @deliveryKey",
                        new Dictionary<string, object>
                        {
                        { "@id", se.Id.ToDbKey() },
                        { "@deliveryKey", se.DeliveryKey }, // Make sure we delete the one we just inspected (in race conditions it may have been locked again)
                        });

                    if (rowsUpdated == 0)
                        throw new ArgumentException($"Subscription-event with id {id} has expired while attempting to mark it complete. Maybe use higher a visibility timeout?");

                    // 2. Insert into ConsumedEvent
                    rowsUpdated = AddConsumedSubscriptionEvent(se);
                    if (rowsUpdated == 0)
                        throw new InvalidOperationException($"Failed to add ConsumedSubscriptionEvent for SubscriptionEvent with id {id}.");

                    // 3. Upsert LastConsumedSubscriptionEvent
                    if (se.FunctionalKey != null) // Only makes sense with a functional key
                    {
                        if (UpdateLastConsumedSubscriptionEvent(se) != 1) // Must hit exactly 1 row
                            throw new InvalidOperationException($"Failed to upsert LastConsumedSubscriptionEvent for SubscriptionEvent with id {id}.");
                    }

                    CommitTransaction();
                    success = true;
                }
                catch (SqlException sqlEx)
                {
                    RollbackTransaction();
                    if (sqlEx.Number == 1205)
                        allowRetry = true;
                }
                catch (Exception)
                {
                    RollbackTransaction();
                    throw;
                }
            } while (!success && allowRetry && attempts < 3); // Allow 3 attempts to solve SQL-specific issues, like deadlocks
        }

        public void MarkFailed(string id, string deliveryKey, Reason reason)
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
                        { "@id", se.Id.ToDbKey() },
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
