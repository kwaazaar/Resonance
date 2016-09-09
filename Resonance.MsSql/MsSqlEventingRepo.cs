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

namespace Resonance.Repo
{
    public class MsSqlEventingRepo : IEventingRepo
    {
        protected readonly IDbConnection _conn;
        protected readonly Stack<IDbTransaction> _runningTransactions = new Stack<IDbTransaction>();

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

        #region Transactions
        public void BeginTransaction()
        {
            var transaction = _conn.BeginTransaction(IsolationLevel.ReadCommitted);
            _runningTransactions.Push(transaction);
        }

        public void RollbackTransaction()
        {
            IDbTransaction transaction = _runningTransactions.Pop();
            if (transaction == null)
                throw new ArgumentException($"No running transaction found");

            transaction.Rollback();
            transaction.Dispose();
        }

        public void CommitTransaction()
        {
            IDbTransaction transaction = _runningTransactions.Pop();
            if (transaction == null)
                throw new ArgumentException($"No running transaction found");

            transaction.Commit();
            transaction.Dispose();
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
            IDbTransaction tran = null;
            if (_runningTransactions.Count > 0)
            {
                try
                {
                    tran = _runningTransactions.Peek();
                }
                catch (InvalidOperationException) { } // Don't care, probably empty
            }
            return _conn.Execute(sql, param: param, transaction: tran, commandTimeout: commandTimeout);
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
                if (!existingSubscription.TopicId.Equals(subscription.TopicId, StringComparison.OrdinalIgnoreCase))
                    throw new ArgumentException("TopicId cannot be updated on a subscription", "subscription");

                var parameters = new Dictionary<string, object>
                {
                    { "@id", subscription.Id.ToDbKey() },
                    { "@name", subscription.Name },
                    { "@enabled", subscription.Enabled },
                    { "@deliveryDelay", subscription.DeliveryDelay },
                    { "@maxDeliveries", subscription.MaxDeliveries },
                    { "@ordered", subscription.Ordered },
                    { "@timeToLive", subscription.TimeToLive },
                };
                TranExecute("update Subscription set Name = @name, Enabled = @enabled, DeliveryDelay = @deliveryDelay, MaxDeliveries = @maxDeliveries, Ordered = @ordered, TimeToLive = @timeToLive where Id = @id", parameters);
                return GetSubscription(subscription.Id);
            }
            else
            {
                var subscriptionId = subscription.Id != null ? subscription.Id : Guid.NewGuid().ToString();
                var parameters = new Dictionary<string, object>
                {
                    { "@id", subscriptionId.ToDbKey() },
                    { "@name", subscription.Name },
                    { "@enabled", subscription.Enabled },
                    { "@topicId", subscription.TopicId.ToDbKey() },
                    { "@deliveryDelay", subscription.DeliveryDelay },
                    { "@maxDeliveries", subscription.MaxDeliveries },
                    { "@ordered", subscription.Ordered },
                    { "@timeToLive", subscription.TimeToLive },
                };
                TranExecute("insert into Subscription (Id, Name, TopicId, Enabled, DeliveryDelay, MaxDeliveries, Ordered, TimeToLive) values (@id, @name, @topicId, @enabled, @deliveryDelay, @maxDeliveries, @ordered, @timeToLive)", parameters);
                return GetSubscription(subscriptionId);
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
            throw new NotImplementedException();
        }

        public void DeleteTopic(string id, bool inclSubscriptions)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<Subscription> GetSubscriptions(string partOfName = null, string topicId = null)
        {
            var parameters = new Dictionary<string, object>
                {
                    { "@topicId", topicId.ToDbKey() },
                    { "@partOfName", $"%{partOfName}%"},
                };
            var query = "select * from Subscription";

            var conditions = new List<string>();
            if (partOfName != null)
                conditions.Add("Name like @partOfName");
            if (topicId != null)
                conditions.Add("TopicId = @topicId");
            if (conditions.Count > 0)
                query += (" where " + String.Join(" and ", conditions));

            return _conn
                .Query<Subscription>(query, parameters);
        }

        public Subscription GetSubscription(string id)
        {
            var parameters = new Dictionary<string, object>
                {
                    { "@id", id.ToDbKey() },
                };

            return _conn
                .Query<Subscription>("select * from Subscription where id = @id", parameters)
                .SingleOrDefault();
        }

        public Subscription GetSubscriptionByName(string name)
        {
            var parameters = new Dictionary<string, object>
                {
                    { "@name", name },
                };

            return _conn
                .Query<Subscription>("select * from Subscription where name = @name", parameters)
                .SingleOrDefault();
        }

        public Topic GetTopic(string id)
        {
            var parameters = new Dictionary<string, object>
                {
                    { "@id", id.ToDbKey() },
                };

            return _conn
                .Query<Topic>("select * from Topic where id = @id", parameters)
                .SingleOrDefault();
        }

        public Topic GetTopicByName(string name)
        {
            var parameters = new Dictionary<string, object>
                {
                    { "@name", name },
                };

            return _conn
                .Query<Topic>("select * from Topic where name = @name", parameters)
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
                return _conn
                    .Query<Topic>("select * from Topic where Name like @partOfName", parameters);
            }
            else
                return _conn
                    .Query<Topic>("select * from Topic");
        }
        #endregion

        #region Statistics
        public IEnumerable<TopicStats> GetTopicStatistics(string id)
        {
            throw new NotImplementedException();
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
            return _conn.Query<string>("select Payload from EventPayload where Id = @id",
                new Dictionary<string, object>
                {
                    { "@id", id.ToDbKey() },
                })
                .SingleOrDefault();
        }

        public string AddTopicEvent(TopicEvent topicEvent)
        {
            var id = topicEvent.Id != null ? topicEvent.Id : Guid.NewGuid().ToString();

            var parameters = new Dictionary<string, object>
                {
                    { "@id", id.ToDbKey() },
                    { "@topicId", topicEvent.TopicId.ToDbKey() },
                    { "@functionalKey", topicEvent.FunctionalKey },
                    { "@publicationDateUtc", topicEvent.PublicationDateUtc },
                    { "@expirationDateUtc", topicEvent.ExpirationDateUtc },
                    { "@payloadId", topicEvent.PayloadId.ToDbKey() },
                };
            TranExecute("insert into TopicEvent (Id, TopicId, FunctionalKey, PublicationDateUtc, ExpirationDateUtc, PayloadId) values (@id, @topicId, @functionalKey, @publicationDateUtc, @expirationDateUtc, @payloadId)", parameters);
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
                    { "@payloadId", subscriptionEvent.PayloadId.ToDbKey() },
                    { "@expirationDateUtc", subscriptionEvent.ExpirationDateUtc },
                    { "@deliveryDelayedUntilUtc", subscriptionEvent.DeliveryDelayedUntilUtc },
                    { "@deliveryCount", default(int) },
                    { "@deliveryKey", default(string) },
                    { "@invisibleUntilUtc", default(DateTime?) },
                };
            TranExecute("insert into SubscriptionEvent (Id, SubscriptionId, TopicEventId, PublicationDateUtc, FunctionalKey, PayloadId, ExpirationDateUtc, DeliveryDelayedUntilUtc, DeliveryCount, DeliveryKey, InvisibleUntilUtc)"
                + " values (@id, @subscriptionId, @topicEventId, @publicationDateUtc, @functionalKey, @payloadId, @expirationDateUtc, @deliveryDelayedUntilUtc, @deliveryCount, @deliveryKey, @invisibleUntilUtc)", parameters);
            return id;
        }

        private SubscriptionEvent GetSubscriptionEvent(string id)
        {
            return _conn.Query<SubscriptionEvent>("select * from SubscriptionEvent where Id = @id",
                new Dictionary<string, object>
                {
                    { "@id", id.ToDbKey() },
                })
                .SingleOrDefault();
        }

        private int AddConsumedSubscriptionEvent(SubscriptionEvent subscriptionEvent)
        {
            return TranExecute("insert into ConsumedSubscriptionEvent (Id, SubscriptionId, PublishedDateUtc, FunctionalKey, PayloadId, ConsumedDateUtc)" +
                " values (@id, @subscriptionId, @publishedDateUtc, @functionalKey, @payloadId, @consumedDateUtc)",
                new Dictionary<string, object>
                {
                    { "@id", subscriptionEvent.Id.ToDbKey() },
                    { "@subscriptionId", subscriptionEvent.SubscriptionId.ToDbKey() },
                    { "@publishedDateUtc", subscriptionEvent.PublicationDateUtc },
                    { "@functionalKey", subscriptionEvent.FunctionalKey },
                    { "@payloadId", subscriptionEvent.PayloadId.ToDbKey() },
                    { "@consumedDateUtc", DateTime.UtcNow },
                });
        }

        private int AddFailedSubscriptionEvent(SubscriptionEvent subscriptionEvent, Reason reason)
        {
            return TranExecute("insert into FailedSubscriptionEvent (Id, SubscriptionId, PublishedDateUtc, FunctionalKey, PayloadId, FailedDateUtc, Reason, ReasonOther)" +
                " values (@id, @subscriptionId, @publishedDateUtc, @functionalKey, @payloadId, @failedDateUtc, @reason, @reasonOther)",
                new Dictionary<string, object>
                {
                    { "@id", subscriptionEvent.Id.ToDbKey() },
                    { "@subscriptionId", subscriptionEvent.SubscriptionId.ToDbKey() },
                    { "@publishedDateUtc", subscriptionEvent.PublicationDateUtc },
                    { "@functionalKey", subscriptionEvent.FunctionalKey },
                    { "@payloadId", subscriptionEvent.PayloadId.ToDbKey() },
                    { "@failedDateUtc", DateTime.UtcNow },
                    { "@reason", (int)reason.Type },
                    { "@reasonOther", reason.ReasonText },
                });
        }
        #endregion

        #region Event consumption

        public ConsumableEvent ConsumeNext(string subscriptionName, int? visibilityTimeout = default(int?))
        {
            var subscription = GetSubscriptionByName(subscriptionName);
            if (subscription == null) throw new ArgumentException($"No subscription with this name exists: {subscriptionName}");

            // Larger buffer failes for ordered delivery subscriptions (it will return more than one event per functional key and a second cannot yet be consumed while we're not sure about the first
            int bufferSize = subscription.Ordered ? 1 : 10;

            string query = $"select TOP {bufferSize} se.Id, se.DeliveryKey, se.FunctionalKey, se.PayloadId" // Get the minimal amount of data
                + " from SubscriptionEvent se"
                + " join Subscription s on s.Id = se.SubscriptionId" // Needed for MaxRetries
                + " where se.SubscriptionId = @subscriptionId"
                + " and (se.DeliveryDelayedUntilUtc IS NULL OR se.DeliveryDelayedUntilUtc < @utcNow)" // Must be allowed to be delivered
                + " and (se.ExpirationDateUtc IS NULL OR se.ExpirationDateUtc > @utcNow)" // Must not yet have expired
                + " and (se.InvisibleUntilUtc IS NULL OR se.InvisibleUntilUtc < @utcNow)" // Must not be 'locked'/made invisible by other consumer
                + " and (s.MaxDeliveries = 0 OR s.MaxDeliveries > se.DeliveryCount)" // Must not have reached max. allowed delivery attempts
                + " order by se.PublicationDateUtc DESC"; // Oldest first (fifo)

            var sIds = _conn.Query<SubscriptionEventIdentifier>(query, new Dictionary<string, object>
                {
                    { "@subscriptionId", subscription.Id.ToDbKey() },
                    { "@utcNow", DateTime.UtcNow },
                }).ToList();

            if (sIds.Count == 0)
                return null; // Nothing found

            string deliveryKey = Guid.NewGuid().ToString();

            // Loop through all found subscriptionevents until one of them could be 'locked'
            foreach (var sId in sIds)
            {
                var invisibleUntilUtc = DateTime.UtcNow.AddSeconds(visibilityTimeout.GetValueOrDefault(60)); // Recalc on every attempt in this loop, since every attempt make take considerable time

                // Attempt to lock it now
                int rowsUpdated = TranExecute("update SubscriptionEvent" +
                    " set DeliveryKey = @newDeliveryKey, InvisibleUntilUtc = @invisibleUntilUtc" +
                    " where Id = @id" +
                    " and ( (DeliveryKey is NULL and @deliveryKey is null)" + // We use DeliveryKey for OCC, since it changes on every update anyway
                    "     or DeliveryKey = @deliveryKey)",
                    new Dictionary<string,object>
                    {
                        { "@id", sId.Id.ToDbKey() },
                        { "@deliveryKey", sId.DeliveryKey },
                        { "@newDeliveryKey", deliveryKey },
                        { "@invisibleUntilUtc", invisibleUntilUtc },
                    });

                if (rowsUpdated > 0) // We got it! Now get the rest of the details
                {
                    var @event = new ConsumableEvent
                    {
                        Id = sId.Id,
                        DeliveryKey = deliveryKey, // Updated above
                        FunctionalKey = sId.FunctionalKey,
                        InvisibleUntilUtc = invisibleUntilUtc,
                    };
                    @event.Payload = GetPayload(sId.PayloadId);
                    return @event;
                }
            }

            return null;
        }

        public void MarkConsumed(string id, string deliveryKey)
        {
            var se = GetSubscriptionEvent(id);
            if (se == null) throw new ArgumentException($"No subscription-event found with id {id}. Maybe it has already been consumed (by another). Using a higher visibility timeout may help.");

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
                rowsUpdated = AddConsumedSubscriptionEvent(se);
                if (rowsUpdated == 0)
                    throw new InvalidOperationException($"Failed to add ConsumedSubscriptionEvent for SubscriptionEvent with id {id}.");

                CommitTransaction();
            }
            catch (Exception)
            {
                RollbackTransaction();
                throw;
            }
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
