using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Resonance.Models;
using Newtonsoft.Json;

namespace Resonance
{
    public class EventPublisher : IEventPublisher
    {
        private IEventingRepo _repo;

        public EventPublisher(IEventingRepo repo)
        {
            _repo = repo;
        }

        public TopicEvent Publish(string topicName, DateTime? publicationDateUtc = default(DateTime?), DateTime? expirationDateUtc = default(DateTime?), string functionalKey = null, string payload = null)
        {
            var topic = _repo.GetTopicByName(topicName);
            if (topic == null)
                throw new ArgumentException($"Topic with name {topicName} not found", "topicName");

            // Store payload (outside transaction, no need to lock right now already)
            string payloadId = (payload != null) ? _repo.StorePayload(payload) : null;

            var subscriptions = _repo.GetSubscriptions(topicId: topic.Id);

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
                    PayloadId = payloadId,
                };
                string topicEventId = _repo.AddTopicEvent(newTopicEvent);
                newTopicEvent.Id = topicEventId;

                // Create SubscriptionEvents
                foreach (var subscription in subscriptions.Where((s) => s.Enabled))
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

                _repo.CommitTransaction();

                // Return the topic event
                return newTopicEvent;
            }
            catch (Exception)
            {
                _repo.RollbackTransaction();
                throw;
            }
        }

        public TopicEvent Publish<T>(string topicName, DateTime? publicationDateUtc = default(DateTime?), DateTime? expirationDateUtc = default(DateTime?), string functionalKey = null, T payload = null) where T:class
        {
            string payloadAsString = null;
            if (payload != null)
                payloadAsString = JsonConvert.SerializeObject(payload); // No specific parameters: the consumer must understand the json as well

            return Publish(topicName, publicationDateUtc, expirationDateUtc, functionalKey, payloadAsString);
        }
    }
}
