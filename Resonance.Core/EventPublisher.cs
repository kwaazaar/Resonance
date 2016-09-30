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

        public async Task<Topic> AddOrUpdateTopic(Topic topic)
        {
            using (var repo = _repoFactory.CreateRepo())
                return await repo.AddOrUpdateTopic(topic);
        }

        public async Task DeleteTopic(Int64 id, bool inclSubscriptions)
        {
            using (var repo = _repoFactory.CreateRepo())
                await repo.DeleteTopic(id, inclSubscriptions);
        }

        public async Task<Topic> GetTopic(Int64 id)
        {
            using (var repo = _repoFactory.CreateRepo())
                return await repo.GetTopic(id);
        }

        public async Task<Topic> GetTopicByName(string name)
        {
            using (var repo = _repoFactory.CreateRepo())
                return await repo.GetTopicByName(name);
        }

        public async Task<IEnumerable<Topic>> GetTopics(string partOfName = null)
        {
            using (var repo = _repoFactory.CreateRepo())
                return await repo.GetTopics(partOfName);
        }

        public async Task<TopicEvent> Publish(string topicName, DateTime? publicationDateUtc = default(DateTime?), DateTime? expirationDateUtc = default(DateTime?), string functionalKey = null, Dictionary<string, string> headers = null, string payload = null)
        {
            using (var repo = _repoFactory.CreateRepo())
            {
                var topic = await repo.GetTopicByName(topicName);
                if (topic == null)
                    throw new ArgumentException($"Topic with name {topicName} not found", "topicName");

                // Store payload (outside transaction, no need to lock right now already)
                var payloadId = (payload != null) ? await repo.StorePayload(payload) : default(Int64?);

                var subscriptions = await repo.GetSubscriptions(topicId: topic.Id);

                repo.BeginTransaction();
                try
                {
                    // Store topic event
                    var newTopicEvent = new TopicEvent
                    {
                        TopicId = topic.Id.Value,
                        PublicationDateUtc = publicationDateUtc.GetValueOrDefault(DateTime.UtcNow),
                        FunctionalKey = functionalKey,
                        ExpirationDateUtc = expirationDateUtc,
                        Headers = headers,
                        PayloadId = payloadId,
                    };
                    var topicEventId = await repo.AddTopicEvent(newTopicEvent);
                    newTopicEvent.Id = topicEventId;

                    foreach (var subscription in subscriptions)
                    {
                        // Create SubscriptionEvents for topicsubscriptions for the specified topic that are enabled
                        foreach (var topicSubscription in subscription.TopicSubscriptions
                            .Where((ts) => ts.TopicId == topic.Id.Value) // Correct topic
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
                                SubscriptionId = subscription.Id.Value,
                                TopicEventId = newTopicEvent.Id.Value,
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
                            await repo.AddSubscriptionEvent(newSubscriptionEvent);
                        }
                    }

                    repo.CommitTransaction();

                    // Return the topic event
                    return newTopicEvent;
                }
                catch (Exception)
                {
                    repo.RollbackTransaction();

                    if (payloadId.HasValue)
                    {
                        try
                        {
                            await repo.DeletePayload(payloadId.Value);
                        }
                        catch (Exception) { } // Don't bother, not too much of a problem (just a little storage lost)
                    }
                    throw;
                }
            }
        }

        public async Task<TopicEvent> Publish<T>(string topicName, DateTime? publicationDateUtc = default(DateTime?), DateTime? expirationDateUtc = default(DateTime?), string functionalKey = null, Dictionary<string, string> headers = null, T payload = null) where T : class
        {
            string payloadAsString = null;
            if (payload != null)
                payloadAsString = await Task.Factory.StartNew(() => JsonConvert.SerializeObject(payload)); // No specific parameters: the consumer must understand the json as well

            return await Publish(topicName, publicationDateUtc, expirationDateUtc, functionalKey, headers, payloadAsString);
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