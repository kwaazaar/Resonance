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

        #region Sync
        public Topic AddOrUpdateTopic(Topic topic)
        {
            return AddOrUpdateTopicAsync(topic).GetAwaiter().GetResult();
        }

        public void DeleteTopic(long id, bool inclSubscriptions)
        {
            DeleteTopicAsync(id, inclSubscriptions).GetAwaiter().GetResult();
        }

        public Topic GetTopic(long id)
        {
            return GetTopicAsync(id).GetAwaiter().GetResult();
        }

        public Topic GetTopicByName(string name)
        {
            return GetTopicByNameAsync(name).GetAwaiter().GetResult();
        }

        public IEnumerable<Topic> GetTopics(string partOfName = null)
        {
            return GetTopicsAsync(partOfName).GetAwaiter().GetResult();
        }

        public TopicEvent Publish(string topicName, string eventName = null, DateTime? publicationDateUtc = default(DateTime?), DateTime? expirationDateUtc = default(DateTime?), string functionalKey = null, int priority = 0, Dictionary<string, string> headers = null, string payload = null)
        {
            return PublishAsync(topicName, eventName, publicationDateUtc, expirationDateUtc, functionalKey, priority, headers, payload).GetAwaiter().GetResult();
        }

        public TopicEvent Publish<T>(string topicName, string eventName = null, DateTime? publicationDateUtc = default(DateTime?), DateTime? expirationDateUtc = default(DateTime?), string functionalKey = null, int priority = 0, Dictionary<string, string> headers = null, T payload = null) where T : class
        {
            return PublishAsync<T>(topicName, eventName, publicationDateUtc, expirationDateUtc, functionalKey, priority, headers, payload).GetAwaiter().GetResult();
        }
        #endregion

        #region Async
        public async Task<Topic> AddOrUpdateTopicAsync(Topic topic)
        {
            using (var repo = _repoFactory.CreateRepo())
                return await repo.AddOrUpdateTopicAsync(topic).ConfigureAwait(false);
        }

        public async Task DeleteTopicAsync(Int64 id, bool inclSubscriptions)
        {
            using (var repo = _repoFactory.CreateRepo())
                await repo.DeleteTopicAsync(id, inclSubscriptions).ConfigureAwait(false);
        }

        public async Task<Topic> GetTopicAsync(Int64 id)
        {
            using (var repo = _repoFactory.CreateRepo())
                return await repo.GetTopicAsync(id).ConfigureAwait(false);
        }

        public async Task<Topic> GetTopicByNameAsync(string name)
        {
            using (var repo = _repoFactory.CreateRepo())
                return await repo.GetTopicByNameAsync(name).ConfigureAwait(false);
        }

        public async Task<IEnumerable<Topic>> GetTopicsAsync(string partOfName = null)
        {
            using (var repo = _repoFactory.CreateRepo())
                return await repo.GetTopicsAsync(partOfName).ConfigureAwait(false);
        }

        public async Task<TopicEvent> PublishAsync(string topicName, string eventName = null, DateTime? publicationDateUtc = default(DateTime?), DateTime? expirationDateUtc = default(DateTime?), string functionalKey = null, int priority = 0, Dictionary<string, string> headers = null, string payload = null)
        {
            using (var repo = _repoFactory.CreateRepo())
            {
                var topic = await repo.GetTopicByNameAsync(topicName).ConfigureAwait(false);
                if (topic == null)
                    throw new ArgumentException($"Topic with name {topicName} not found", "topicName");

                // Store payload (outside transaction, no need to lock right now already)
                var payloadId = (payload != null) ? await repo.StorePayloadAsync(payload).ConfigureAwait(false) : default(Int64?);

                var subscriptions = await repo.GetSubscriptionsAsync(topicId: topic.Id).ConfigureAwait(false);

                var eventNameToUse = eventName;
                if (eventNameToUse == null && headers != null)
                {
                    var eventNameHeader = headers.FirstOrDefault(h => h.Key.Equals("EventName", StringComparison.OrdinalIgnoreCase));
                    if (eventNameHeader.Key != null)
                        eventNameToUse = eventNameHeader.Value;
                }

                // Store topic event
                var newTopicEvent = new TopicEvent
                {
                    TopicId = topic.Id.Value,
                    EventName = eventNameToUse,
                    PublicationDateUtc = publicationDateUtc.GetValueOrDefault(DateTime.UtcNow),
                    FunctionalKey = functionalKey,
                    Priority = priority,
                    ExpirationDateUtc = expirationDateUtc,
                    Headers = headers,
                    PayloadId = payloadId,
                };

                // Determine for which subscriptions the topic must be published
                var subscriptionsMatching = subscriptions
                    .Where((s) => s.TopicSubscriptions.Any((ts) => ((ts.TopicId == topic.Id.Value) && ts.Enabled && (!ts.Filtered || CheckFilters(ts.Filters, headers)))))
                    .Distinct(); // Nessecary when one subscription has more than once topicsubscription for the same topic (probably with different filters)

                try
                {
                    var topicEvent = await repo.PublishTopicEventAsync(newTopicEvent, subscriptionsMatching).ConfigureAwait(false);
                    return topicEvent;
                }
                catch (Exception)
                {
                    if (payloadId.HasValue)
                    {
                        try
                        {
                            await repo.DeletePayloadAsync(payloadId.Value).ConfigureAwait(false);
                        }
                        catch (Exception) { } // Don't bother, not too much of a problem (just a little storage lost)
                    }

                    throw;
                }
            }
        }

        public async Task<TopicEvent> PublishAsync<T>(string topicName, string eventName = null, DateTime? publicationDateUtc = default(DateTime?), DateTime? expirationDateUtc = default(DateTime?), string functionalKey = null, int priority = 0, Dictionary<string, string> headers = null, T payload = null) where T : class
        {
            string payloadAsString = null;
            if (payload != null)
                payloadAsString = JsonConvert.SerializeObject(payload); // No specific parameters: the consumer must understand the json as well

            return await PublishAsync(topicName, eventName, publicationDateUtc, expirationDateUtc, functionalKey, priority, headers, payloadAsString).ConfigureAwait(false);
        }
        #endregion

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
            if (!headers.Any((h) => h.Key.Equals(filter.Header, StringComparison.OrdinalIgnoreCase))) // Header must be provided, otherwise mismatch anyway (even with MatchExpression '*')
                return false;

            if (filter.MatchExpression == "*") return true;

            var headerValue = headers.First((h) => h.Key.Equals(filter.Header, StringComparison.OrdinalIgnoreCase)).Value;
            var endsWith = filter.MatchExpression.StartsWith("*");
            var startsWith = filter.MatchExpression.EndsWith("*");

            if (endsWith && startsWith)
            {
                return ((filter.MatchExpression.Length >= 3)
                    && headerValue.ToLowerInvariant().Contains(filter.MatchExpression.Substring(1).Substring(0, filter.MatchExpression.Length - 2).ToLowerInvariant()));
            }
            else if (endsWith)
            {
                return ((filter.MatchExpression.Length >= 2)
                    && headerValue.EndsWith(filter.MatchExpression.Substring(1, filter.MatchExpression.Length - 1), StringComparison.OrdinalIgnoreCase));
            }
            else if (startsWith)
            {
                return ((filter.MatchExpression.Length >= 2)
                    && headerValue.StartsWith(filter.MatchExpression.Substring(0, filter.MatchExpression.Length - 1), StringComparison.OrdinalIgnoreCase));
            }
            else
            {
                return filter.MatchExpression.Equals(headerValue, StringComparison.OrdinalIgnoreCase);
            }
        }
    }
}