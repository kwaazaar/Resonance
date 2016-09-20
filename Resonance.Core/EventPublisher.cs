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
        private IEventingRepoFactory _repoFactory;

        public EventPublisher(IEventingRepoFactory repoFactory)
        {
            _repoFactory = repoFactory;
        }

        public Topic AddOrUpdateTopic(Topic topic)
        {
            using (var repo = _repoFactory.CreateRepo())
                return repo.AddOrUpdateTopic(topic);
        }

        public void DeleteTopic(string id, bool inclSubscriptions)
        {
            using (var repo = _repoFactory.CreateRepo())
                repo.DeleteTopic(id, inclSubscriptions);
        }

        public Topic GetTopic(string id)
        {
            using (var repo = _repoFactory.CreateRepo())
                return repo.GetTopic(id);
        }

        public Topic GetTopicByName(string name)
        {
            using (var repo = _repoFactory.CreateRepo())
                return repo.GetTopicByName(name);
        }

        public IEnumerable<Topic> GetTopics(string partOfName = null)
        {
            using (var repo = _repoFactory.CreateRepo())
                return repo.GetTopics(partOfName).ToList();
        }

        public TopicEvent Publish(string topicName, DateTime? publicationDateUtc = default(DateTime?), DateTime? expirationDateUtc = default(DateTime?), string functionalKey = null, Dictionary<string, string> headers = null, string payload = null)
        {
            using (var repo = _repoFactory.CreateRepo())
            {
                var topic = repo.GetTopicByName(topicName);
                if (topic == null)
                    throw new ArgumentException($"Topic with name {topicName} not found", "topicName");

                // Store payload (outside transaction, no need to lock right now already)
                string payloadId = (payload != null) ? repo.StorePayload(payload) : null;

                var subscriptions = repo.GetSubscriptions(topicId: topic.Id).ToList();

                repo.BeginTransaction();
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
                    string topicEventId = repo.AddTopicEvent(newTopicEvent);
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
                            repo.AddSubscriptionEvent(newSubscriptionEvent);
                        }
                    }

                    repo.CommitTransaction();

                    // Return the topic event
                    return newTopicEvent;
                }
                catch (Exception)
                {
                    repo.RollbackTransaction();

                    if (payloadId != null)
                    {
                        try
                        {
                            repo.DeletePayload(payloadId);
                        }
                        catch (Exception) { } // Don't bother, not too much of a problem (just a little storage lost)
                    }
                    throw;
                }
            }
        }

        public TopicEvent Publish<T>(string topicName, DateTime? publicationDateUtc = default(DateTime?), DateTime? expirationDateUtc = default(DateTime?), string functionalKey = null, Dictionary<string, string> headers = null, T payload = null) where T : class
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

            foreach (var filter in filters)
            {
                if (!CheckFilters(filter, headers))
                    return false;
            }

            return true;
        }

        private bool CheckFilters(TopicSubscriptionFilter filter, Dictionary<string, string> headers)
        {
            if (!headers.Any((h) => h.Key.Equals(filter.Header, StringComparison.OrdinalIgnoreCase)))
                return false;

            if (filter.MatchExpression == "*") return true;

            var headerValue = headers.First((h) => h.Key.Equals(filter.Header, StringComparison.OrdinalIgnoreCase)).Value;
            var endsWith = filter.MatchExpression.EndsWith("*");
            var startsWith = filter.MatchExpression.EndsWith("*");

            if (endsWith && startsWith)
            {
#warning TODO: fix endwith+startswith filter
                return ((filter.MatchExpression.Length >= 3)
                    && filter.MatchExpression.Substring(1).Substring(0, filter.MatchExpression.Length - 2).Equals(headerValue, StringComparison.OrdinalIgnoreCase));
            }
            else if (endsWith)
            {
                return ((filter.MatchExpression.Length >= 2)
                    && headerValue.StartsWith(filter.MatchExpression.Substring(0, filter.MatchExpression.Length - 1), StringComparison.OrdinalIgnoreCase));
            }
            else if (startsWith)
            {
                return ((filter.MatchExpression.Length >= 2)
                    && headerValue.EndsWith(filter.MatchExpression.Substring(1, filter.MatchExpression.Length - 1), StringComparison.OrdinalIgnoreCase));
            }
            else
            {
                return filter.MatchExpression.Equals(headerValue, StringComparison.OrdinalIgnoreCase);
            }
        }
    }
}