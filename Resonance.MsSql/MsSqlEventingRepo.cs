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

namespace Resonance.Repo
{
    public class MsSqlEventingRepo : IEventingRepo
    {
        protected readonly IDbConnection _conn;

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
                    { "@id", subscription.Id },
                    { "@name", subscription.Name },
                    { "@enabled", subscription.Enabled },
                    { "@deliveryDelay", subscription.DeliveryDelay },
                    { "@maxDeliveries", subscription.MaxDeliveries },
                    { "@ordered", subscription.Ordered },
                    { "@timeToLive", subscription.TimeToLive },
                };
                _conn.Execute("update Subscription set Name = @name, Enabled = @enabled, DeliveryDelay = @deliveryDelay, MaxDeliveries = @maxDeliveries, Ordered = @ordered, TimeToLive = @timeToLive where Id = @id", parameters);
                return GetSubscription(subscription.Id);
            }
            else
            {
                var subscriptionId = subscription.Id != null ? subscription.Id : Guid.NewGuid().ToString();
                var parameters = new Dictionary<string, object>
                {
                    { "@id", subscriptionId },
                    { "@name", subscription.Name },
                    { "@enabled", subscription.Enabled },
                    { "@topicId", subscription.TopicId },
                    { "@deliveryDelay", subscription.DeliveryDelay },
                    { "@maxDeliveries", subscription.MaxDeliveries },
                    { "@ordered", subscription.Ordered },
                    { "@timeToLive", subscription.TimeToLive },
                };
                _conn.Execute("insert into Subscription (Id, Name, TopicId, Enabled, DeliveryDelay, MaxDeliveries, Ordered, TimeToLive) values (@id, @name, @topicId, @enabled, @deliveryDelay, @maxDeliveries, @ordered, @timeToLive)", parameters);
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
                    { "@id", topic.Id },
                    { "@name", topic.Name },
                    { "@notes", topic.Notes },
                };
                _conn.Execute("update Topic set Name = @name, Notes = @notes where Id = @id", parameters);
                return GetTopic(topic.Id);
            }
            else
            {
                var topicId = topic.Id != null ? topic.Id : Guid.NewGuid().ToString();
                var parameters = new Dictionary<string, object>
                {
                    { "@id", topicId },
                    { "@name", topic.Name },
                    { "@notes", topic.Notes },
                };
                _conn.Execute("insert into Topic (Id, Name, Notes) values (@id, @name, @notes)", parameters);
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
                    { "@topicId", topicId },
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
                    { "@id", id },
                };

            return _conn
                .Query<Subscription>("select * from Subscription where id = @id", parameters)
                .SingleOrDefault();
        }

        public Topic GetTopic(string id)
        {
            var parameters = new Dictionary<string, object>
                {
                    { "@id", id },
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

        public IEnumerable<TopicStats> GetTopicStatistics(string id)
        {
            throw new NotImplementedException();
        }

        public string StorePayload(string payload)
        {
            var id = Guid.NewGuid().ToString();
            var parameters = new Dictionary<string, object>
                {
                    { "@id", id },
                    { "@payload", payload },
                };
            _conn.Execute("insert into EventPayload (Id, Payload) values (@id, @payload)", parameters);
            return id;
        }

        public string AddTopicEvent(TopicEvent topicEvent)
        {
            var id = topicEvent.Id != null ? topicEvent.Id : Guid.NewGuid().ToString();

            var parameters = new Dictionary<string, object>
                {
                    { "@id", id },
                    { "@topicId", topicEvent.TopicId },
                    { "@functionalKey", topicEvent.FunctionalKey },
                    { "@publicationDateUtc", topicEvent.PublicationDateUtc },
                    { "@expirationDateUtc", topicEvent.ExpirationDateUtc },
                    { "@payloadId", topicEvent.PayloadId },
                };
            _conn.Execute("insert into TopicEvent (Id, TopicId, FunctionalKey, PublicationDateUtc, ExpirationDateUtc, PayloadId) values (@id, @topicId, @functionalKey, @publicationDateUtc, @expirationDateUtc, @payloadId)", parameters);
            return id;
        }
    }
}
