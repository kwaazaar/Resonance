using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Resonance.Models;
using Newtonsoft.Json;
using Resonance.Repo;
using Microsoft.Extensions.Caching.Memory;

namespace Resonance
{
    public class EventPublisher : IEventPublisher, IEventPublisherAsync
    {
        private readonly IEventingRepoFactory _repoFactory;
        private readonly DateTimeProvider _dtProvider;
        private readonly TimeSpan _cacheDuration;

        protected static IMemoryCache topicCache = new MemoryCache(new MemoryCacheOptions());
        protected static IMemoryCache topicSubscriptionCache = new MemoryCache(new MemoryCacheOptions());

        public EventPublisher(IEventingRepoFactory repoFactory)
            : this(repoFactory, DateTimeProvider.Repository)
        {
        }

        public EventPublisher(IEventingRepoFactory repoFactory, DateTimeProvider dtProvider)
            : this(repoFactory, dtProvider, TimeSpan.FromSeconds(30))
        {
        }

        public EventPublisher(IEventingRepoFactory repoFactory, DateTimeProvider dtProvider, TimeSpan cacheDuration)
        {
            _repoFactory = repoFactory;
            _dtProvider = dtProvider;
            _cacheDuration = cacheDuration;
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

        public TopicEvent Publish(string topicName, string eventName = null, DateTime? publicationDateUtc = default(DateTime?), DateTime? deliveryDelayedUntilUtc = default(DateTime?), DateTime? expirationDateUtc = default(DateTime?), string functionalKey = null, int priority = 100, Dictionary<string, string> headers = null, string payload = null)
        {
            return PublishAsync(topicName, eventName, publicationDateUtc, deliveryDelayedUntilUtc, expirationDateUtc, functionalKey, priority, headers, payload).GetAwaiter().GetResult();
        }

        public TopicEvent Publish<T>(string topicName, string eventName = null, DateTime? publicationDateUtc = default(DateTime?), DateTime? deliveryDelayedUntilUtc = default(DateTime?), DateTime? expirationDateUtc = default(DateTime?), string functionalKey = null, int priority = 100, Dictionary<string, string> headers = null, T payload = null) where T : class
        {
            return PublishAsync<T>(topicName, eventName, publicationDateUtc, deliveryDelayedUntilUtc, expirationDateUtc, functionalKey, priority, headers, payload).GetAwaiter().GetResult();
        }

        public void PerformHouseKeepingTasks()
        {
            PerformHouseKeepingTasksAsync().GetAwaiter().GetResult();
        }
        #endregion

        #region Async
        public async Task<Topic> AddOrUpdateTopicAsync(Topic topic)
        {
            using (var repo = _repoFactory.CreateRepo())
            {
                var newTopic = await repo.AddOrUpdateTopicAsync(topic).ConfigureAwait(false);
                UpdateTopicCache(newTopic);
                return newTopic;
            }
        }

        public async Task DeleteTopicAsync(Int64 id, bool inclSubscriptions)
        {
            using (var repo = _repoFactory.CreateRepo())
            {
                var topic = await GetTopicAsync(id).ConfigureAwait(false);
                if (topic != null)
                {
                    await repo.DeleteTopicAsync(id, inclSubscriptions).ConfigureAwait(false);
                    UpdateTopicCache(topic, deleted: true);
                }
            }
        }

        public async Task<Topic> GetTopicAsync(Int64 id)
        {
            using (var repo = _repoFactory.CreateRepo())
                return await repo.GetTopicAsync(id).ConfigureAwait(false);
        }

        public Task<Topic> GetTopicByNameAsync(string name)
        {
            var topic = topicCache.GetOrCreateAsync<Topic>(name, async (t) =>
            {
                using (var repo = _repoFactory.CreateRepo())
                    return await repo.GetTopicByNameAsync(name).ConfigureAwait(false);
            });

            return topic;
        }

        public async Task<IEnumerable<Topic>> GetTopicsAsync(string partOfName = null)
        {
            using (var repo = _repoFactory.CreateRepo())
                return await repo.GetTopicsAsync(partOfName).ConfigureAwait(false);
        }

        private Task<List<Subscription>> GetTopicSubscriptionsAsync(Int64 topicId)
        {
            var subs = topicSubscriptionCache.GetOrCreateAsync<List<Subscription>>(topicId, async (t) =>
            {
                using (var repo = _repoFactory.CreateRepo())
                    return (await repo.GetSubscriptionsAsync(topicId: topicId).ConfigureAwait(false)).ToList();
            });

            return subs;
        }

        public async Task<TopicEvent> PublishAsync(string topicName, string eventName = null, DateTime? publicationDateUtc = default(DateTime?), DateTime? deliveryDelayedUntilUtc = default(DateTime?), DateTime? expirationDateUtc = default(DateTime?), string functionalKey = null, int priority = 100, Dictionary<string, string> headers = null, string payload = null)
        {
            using (var repo = _repoFactory.CreateRepo())
            {
                var topic = await GetTopicByNameAsync(topicName).ConfigureAwait(false);
                if (topic == null)
                    throw new ArgumentException($"Topic with name {topicName} not found", "topicName");

                // Store payload (outside transaction, no need to lock right now already)
                var payloadId = (payload != null) ? await repo.StorePayloadAsync(payload).ConfigureAwait(false) : default(Int64?);

                var subscriptions = await GetTopicSubscriptionsAsync(topic.Id.Value).ConfigureAwait(false);

                var eventNameToUse = eventName;
                if (eventNameToUse == null && headers != null)
                {
                    var eventNameHeader = headers.FirstOrDefault(h => h.Key.Equals("EventName", StringComparison.OrdinalIgnoreCase));
                    if (eventNameHeader.Key != null)
                        eventNameToUse = eventNameHeader.Value;
                }

                var utcNow = _dtProvider == DateTimeProvider.Repository ? await repo.GetNowUtcAsync().ConfigureAwait(false) : DateTime.UtcNow;

                // Store topic event
                var newTopicEvent = new TopicEvent
                {
                    TopicId = topic.Id.Value,
                    EventName = eventNameToUse,
                    PublicationDateUtc = publicationDateUtc.GetValueOrDefault(utcNow),
                    FunctionalKey = functionalKey ?? string.Empty,
                    Priority = priority,
                    ExpirationDateUtc = expirationDateUtc.GetValueOrDefault(BaseEventingRepo.MaxDateTime),
                    Headers = headers,
                    PayloadId = payloadId,
                };

                // Determine for which subscriptions the topic must be published
                var subscriptionsMatching = subscriptions
                    .Where((s) => s.TopicSubscriptions.Any((ts) => ((ts.TopicId == topic.Id.Value) && ts.Enabled && (!ts.Filtered || CheckFilters(ts.Filters, headers)))))
                    .Distinct(); // Nessecary when one subscription has more than once topicsubscription for the same topic (probably with different filters)

                try
                {
                    var topicEvent = await repo.PublishTopicEventAsync(newTopicEvent, topic.Log, subscriptionsMatching, deliveryDelayedUntilUtc).ConfigureAwait(false);
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

        public async Task<TopicEvent> PublishAsync<T>(string topicName, string eventName = null, DateTime? publicationDateUtc = default(DateTime?), DateTime? deliveryDelayedUntilUtc = default(DateTime?), DateTime? expirationDateUtc = default(DateTime?), string functionalKey = null, int priority = 100, Dictionary<string, string> headers = null, T payload = null) where T : class
        {
            string payloadAsString = null;
            if (payload != null)
                payloadAsString = JsonConvert.SerializeObject(payload); // No specific parameters: the consumer must understand the json as well

            return await PublishAsync(topicName, eventName, publicationDateUtc, deliveryDelayedUntilUtc, expirationDateUtc, functionalKey, priority, headers, payloadAsString).ConfigureAwait(false);
        }

        public async Task PerformHouseKeepingTasksAsync()
        {
            using (var repo = _repoFactory.CreateRepo())
                await repo.PerformHouseKeepingTasksAsync().ConfigureAwait(false);
        }
        #endregion

        private bool CheckFilters(List<TopicSubscriptionFilter> filters, Dictionary<string, string> headers)
        {
            if (filters == null)
                return false;
            if (filters.Count == 0)
                return false;
            if (headers == null)
                return filters.Any(f => f.NotMatch) ? true : false; // If any filter says it cannot match, then that filter automatically matches when no headers where supplied

            foreach (var filter in filters)
            {
                if (!CheckFilter(filter, headers))
                    return false;
            }

            return true;
        }

        private bool CheckFilter(TopicSubscriptionFilter filter, Dictionary<string, string> headers)
        {
            var notMask = !filter.NotMatch;

            if (!headers.Any((h) => h.Key.Equals(filter.Header, StringComparison.OrdinalIgnoreCase))) // Header must be provided, otherwise mismatch anyway (even with MatchExpression '*')
                return (false == notMask);

            if (filter.MatchExpression == "*") return (true && notMask);

            var headerValue = headers.First((h) => h.Key.Equals(filter.Header, StringComparison.OrdinalIgnoreCase)).Value;
            var endsWith = filter.MatchExpression.StartsWith("*");
            var startsWith = filter.MatchExpression.EndsWith("*");

            if (endsWith && startsWith)
            {
                return (((filter.MatchExpression.Length >= 3)
                    && headerValue.ToLowerInvariant().Contains(filter.MatchExpression.Substring(1).Substring(0, filter.MatchExpression.Length - 2).ToLowerInvariant()))
                    == notMask);
            }
            else if (endsWith)
            {
                return (((filter.MatchExpression.Length >= 2)
                    && headerValue.EndsWith(filter.MatchExpression.Substring(1, filter.MatchExpression.Length - 1), StringComparison.OrdinalIgnoreCase))
                    == notMask);
            }
            else if (startsWith)
            {
                return (((filter.MatchExpression.Length >= 2)
                    && headerValue.StartsWith(filter.MatchExpression.Substring(0, filter.MatchExpression.Length - 1), StringComparison.OrdinalIgnoreCase))
                    == notMask);
            }
            else
            {
                return (filter.MatchExpression.Equals(headerValue, StringComparison.OrdinalIgnoreCase) == notMask);
            }
        }


        private void UpdateTopicCache(Topic topic, bool deleted = false)
        {
            if (deleted)
            {
                topicCache.Remove(topic.Name);
            }
            else
            {
                topicCache.Set<Topic>(topic.Name, topic, _cacheDuration);
            }
            UpdateTopicSubscriptionsCache(topic.Id.Value, null); // Always invalidate subscriptions
        }

        private void UpdateTopicSubscriptionsCache(Int64 topicId, List<Subscription> subs)
        {
            if (subs == null || subs.Count == 0)
            {
                topicSubscriptionCache.Remove(topicId);
            }
            else
            {
                topicSubscriptionCache.Set<List<Subscription>>(topicId, subs, _cacheDuration);
            }
        }

    }
}