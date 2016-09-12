using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Resonance.Models;
using Newtonsoft.Json;
using Resonance.Repo;

namespace Resonance
{
    public class EventPublisher : IEventPublisher
    {
        private IEventingRepo _repo;

        public EventPublisher(IEventingRepo repo)
        {
            _repo = repo;
        }

        public Topic AddOrUpdateTopic(Topic topic)
        {
            return _repo.AddOrUpdateTopic(topic);
        }

        public void DeleteTopic(string id, bool inclSubscriptions)
        {
            _repo.DeleteTopic(id, inclSubscriptions);
        }

        public Topic GetTopic(string id)
        {
            return _repo.GetTopic(id);
        }

        public Topic GetTopicByName(string name)
        {
            return _repo.GetTopicByName(name);
        }

        public IEnumerable<Topic> GetTopics(string partOfName = null)
        {
            return _repo.GetTopics(partOfName)
                .ToList();
        }

        public TopicEvent Publish(string topicName, DateTime? publicationDateUtc = default(DateTime?), DateTime? expirationDateUtc = default(DateTime?), string functionalKey = null, Dictionary<string, string> headers = null, string payload = null)
        {
            var topic = _repo.GetTopicByName(topicName);
            if (topic == null)
                throw new ArgumentException($"Topic with name {topicName} not found", "topicName");

            // Store payload (outside transaction, no need to lock right now already)
            string payloadId = (payload != null) ? _repo.StorePayload(payload) : null;

            var subscriptions = _repo.GetSubscriptions(topicId: topic.Id).ToList();

            _repo.BeginTransaction();
            try
            {
                // Store topic event
                var newTopicEvent = new TopicEvent
                {
                    TopicId = topic.Id,
                    PublicationDateUtc = publicationDateUtc.GetValueOrDefault(DateTime.UtcNow),
                    FunctionalKey = functionalKey,
                    ExpirationDateUtc = expirationDateUtc,
                    Headers = headers,
                    PayloadId = payloadId,
                };
                string topicEventId = _repo.AddTopicEvent(newTopicEvent);
                newTopicEvent.Id = topicEventId;

                foreach (var subscription in subscriptions)
                {
                    // Create SubscriptionEvents for topicsubscriptions for the specified topic that are enabled
                    foreach (var topicSubscription in subscription.TopicSubscriptions
                        .Where((ts) => ts.TopicId.Equals(topic.Id, StringComparison.OrdinalIgnoreCase)) // Correct topic
                        .Where((ts) => ts.Enabled) // Enabled
                        .Where((ts) => !ts.Filtered || CheckFilters(ts.Filters, headers))) // Filters match
                    {
                        // By default a SubscriptionEvent takes expirationdate of TopicEvent
                        var subExpirationDateUtc = newTopicEvent.ExpirationDateUtc;

                        // If subscription has its own TTL, it will be applied, but may never exceed the TopicEvent expiration
                        if (subscription.TimeToLive.HasValue)
                        {
                            subExpirationDateUtc = newTopicEvent.PublicationDateUtc.Value.AddSeconds(subscription.TimeToLive.Value);
                            if (newTopicEvent.ExpirationDateUtc.HasValue && (newTopicEvent.ExpirationDateUtc.Value < subExpirationDateUtc))
                                subExpirationDateUtc = newTopicEvent.ExpirationDateUtc;
                        }

                        // Delivery can be initially delayed, but it cannot exceed the expirationdate (would be useless)
                        var deliveryDelayedUntilUtc = subscription.DeliveryDelay.HasValue ? newTopicEvent.PublicationDateUtc.Value.AddSeconds(subscription.DeliveryDelay.Value) : default(DateTime?);
                        //if (deliveryDelayedUntilUtc.HasValue && subExpirationDateUtc.HasValue
                        //    && deliveryDelayedUntilUtc.Value > subExpirationDateUtc.Value)
                        //    break; // Skip this one, it would have been expired immedi

                        var newSubscriptionEvent = new SubscriptionEvent
                        {
                            SubscriptionId = subscription.Id,
                            TopicEventId = newTopicEvent.Id,
                            PublicationDateUtc = newTopicEvent.PublicationDateUtc.Value,
                            FunctionalKey = newTopicEvent.FunctionalKey,
                            PayloadId = newTopicEvent.PayloadId,
                            Payload = null, // Only used when consuming
                            ExpirationDateUtc = subExpirationDateUtc,
                            DeliveryDelayedUntilUtc = deliveryDelayedUntilUtc,
                            DeliveryCount = 0,
                            DeliveryKey = null,
                            InvisibleUntilUtc = null,
                        };
                        _repo.AddSubscriptionEvent(newSubscriptionEvent);
                    }
                }

                _repo.CommitTransaction();

                // Return the topic event
                return newTopicEvent;
            }
            catch (Exception)
            {
                _repo.RollbackTransaction();

                if (payloadId != null)
                {
                    try
                    {
                        _repo.DeletePayload(payloadId);
                    }
                    catch (Exception) { } // Don't bother, not too much of a problem (just a little storage lost)
                }
                throw;
            }
        }

        public TopicEvent Publish<T>(string topicName, DateTime? publicationDateUtc = default(DateTime?), DateTime? expirationDateUtc = default(DateTime?), string functionalKey = null, Dictionary<string, string> headers = null, T payload = null) where T:class
        {
            string payloadAsString = null;
            if (payload != null)
                payloadAsString = JsonConvert.SerializeObject(payload); // No specific parameters: the consumer must understand the json as well

            return Publish(topicName, publicationDateUtc, expirationDateUtc, functionalKey, headers, payloadAsString);
        }

        private bool CheckFilters(List<TopicSubscriptionFilter> filters, Dictionary<string, string> headers)
        {
            if (headers == null)
                return false;
            if (filters == null)
                return false;
            if (filters.Count == 0)
                return false;

#warning TODO: Implement filter-logic
            return false;
        }
    }
}
