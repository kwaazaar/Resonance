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
    public class EventConsumer : IEventConsumer, IEventConsumerAsync
    {
        private readonly IEventingRepoFactory _repoFactory;
        private readonly TimeSpan _cacheDuration;
        private readonly SafeExecOptions _safeExecOptions;

        protected static IMemoryCache subscriptionCache = new MemoryCache(new MemoryCacheOptions());

        public EventConsumer(IEventingRepoFactory repoFactory)
            : this(repoFactory, TimeSpan.FromSeconds(30))
        {
        }

        public EventConsumer(IEventingRepoFactory repoFactory, TimeSpan cacheDuration)
            : this(repoFactory, cacheDuration, SafeExecOptions.Default)
        {
        }

        public EventConsumer(IEventingRepoFactory repoFactory, TimeSpan cacheDuration, SafeExecOptions safeExecOptions)
        {
            _repoFactory = repoFactory;
            _cacheDuration = cacheDuration;
            _safeExecOptions = safeExecOptions;
        }

        #region Sync

        public IEnumerable<ConsumableEvent> ConsumeNext(string subscriptionName, int visibilityTimeout = 120, int maxCount = 1)
        {
            return ConsumeNextAsync(subscriptionName, visibilityTimeout, maxCount).GetAwaiter().GetResult();
        }

        public IEnumerable<ConsumableEvent<T>> ConsumeNext<T>(string subscriptionName, int visibilityTimeout = 120, int maxCount = 1)
        {
            return ConsumeNextAsync<T>(subscriptionName, visibilityTimeout, maxCount).GetAwaiter().GetResult();
        }

        public void MarkConsumed(IEnumerable<ConsumableEventId> consumableEventsIds, bool transactional = true)
        {
            MarkConsumedAsync(consumableEventsIds, transactional).GetAwaiter().GetResult();
        }

        public void MarkConsumed(long id, string deliveryKey)
        {
            MarkConsumedAsync(id, deliveryKey).GetAwaiter().GetResult();
        }

        public void MarkFailed(long id, string deliveryKey, Reason reason)
        {
            MarkFailedAsync(id, deliveryKey, reason).GetAwaiter().GetResult();
        }

        public IEnumerable<Subscription> GetSubscriptions(long? topicId = default(long?))
        {
            return GetSubscriptionsAsync(topicId).GetAwaiter().GetResult();
        }

        public Subscription GetSubscription(long id)
        {
            return GetSubscriptionAsync(id).GetAwaiter().GetResult();
        }

        public Subscription GetSubscriptionByName(string name)
        {
            return GetSubscriptionByNameAsync(name).GetAwaiter().GetResult();
        }

        public Subscription AddOrUpdateSubscription(Subscription subscription)
        {
            return AddOrUpdateSubscriptionAsync(subscription).GetAwaiter().GetResult();
        }

        public void DeleteSubscription(long id)
        {
            DeleteSubscriptionAsync(id).GetAwaiter().GetResult();
        }

        public IEnumerable<SubscriptionSummary> GetSubscriptionStatistics(DateTime periodStartUtc, DateTime periodEndUtc)
        {
            return GetSubscriptionStatisticsAsync(periodStartUtc, periodEndUtc).GetAwaiter().GetResult();
        }

        public void PerformHouseKeepingTasks()
        {
            PerformHouseKeepingTasksAsync().GetAwaiter().GetResult();
        }
        #endregion

        #region Async
        public async Task<Subscription> AddOrUpdateSubscriptionAsync(Subscription subscription)
        {
            var sub = await _repoFactory.SafeExecAsync<Subscription>(r => r.AddOrUpdateSubscriptionAsync(subscription), _safeExecOptions);
            UpdateSubscriptionCache(sub);
            return sub;
        }

        public async Task DeleteSubscriptionAsync(Int64 id)
        {
            var sub = await GetSubscriptionAsync(id);
            if (sub != null)
            {
                await _repoFactory.SafeExecAsync(r => r.DeleteSubscriptionAsync(id), _safeExecOptions);
                UpdateSubscriptionCache(sub, deleted: true);
            }
        }

        public Task<Subscription> GetSubscriptionAsync(Int64 id)
        {
            return _repoFactory.SafeExecAsync<Subscription>(r => r.GetSubscriptionAsync(id), _safeExecOptions);
        }

        public Task<Subscription> GetSubscriptionByNameAsync(string name)
        {
            var sub = subscriptionCache.GetOrCreateAsync<Subscription>(name, async (s) =>
            {
                return await _repoFactory.SafeExecAsync(r => r.GetSubscriptionByNameAsync(name), _safeExecOptions);
            });

            return sub;
        }

        public Task<IEnumerable<Subscription>> GetSubscriptionsAsync(Int64? topicId = null)
        {
            return _repoFactory.SafeExecAsync(r => r.GetSubscriptionsAsync(topicId), _safeExecOptions);
        }

        public Task<IEnumerable<SubscriptionSummary>> GetSubscriptionStatisticsAsync(DateTime periodStartUtc, DateTime periodEndUtc)
        {
            return _repoFactory.SafeExecAsync(r => r.GetSubscriptionStatisticsAsync(periodStartUtc, periodEndUtc), _safeExecOptions);
        }

        public async Task<IEnumerable<ConsumableEvent>> ConsumeNextAsync(string subscriptionName, int visibilityTimeout = 120, int maxCount = 1)
        {
            var sub = await GetSubscriptionByNameAsync(subscriptionName).ConfigureAwait(false);
            if (sub == null) throw new ArgumentException($"No subscription with this name exists: {subscriptionName}");

            return await _repoFactory.SafeExecAsync(r => r.ConsumeNextAsync(sub, visibilityTimeout, maxCount), _safeExecOptions);
        }

        public async Task<IEnumerable<ConsumableEvent<T>>> ConsumeNextAsync<T>(string subscriptionName, int visibilityTimeout = 120, int maxCount = 1)
        {
            var ces = new List<ConsumableEvent<T>>();

            foreach (var ce in await ConsumeNextAsync(subscriptionName, visibilityTimeout, maxCount).ConfigureAwait(false))
            {
                // Deserialize the payload
                T payloadAsObject = ce.Payload != null ? JsonConvert.DeserializeObject<T>(ce.Payload) : default(T);

                ces.Add(new ConsumableEvent<T>
                {
                    Id = ce.Id,
                    FunctionalKey = ce.FunctionalKey,
                    DeliveryKey = ce.DeliveryKey,
                    InvisibleUntilUtc = ce.InvisibleUntilUtc,
                    Payload = payloadAsObject,
                });
            }

            return ces;
        }

        public Task MarkConsumedAsync(IEnumerable<ConsumableEventId> consumableEventsIds, bool transactional = true)
        {
            return _repoFactory.SafeExecAsync(r => r.MarkConsumedAsync(consumableEventsIds, transactional), _safeExecOptions);
        }

        public Task MarkConsumedAsync(Int64 id, string deliveryKey)
        {
            return _repoFactory.SafeExecAsync(r => r.MarkConsumedAsync(id, deliveryKey), _safeExecOptions);
        }

        public Task MarkFailedAsync(Int64 id, string deliveryKey, Reason reason)
        {
            return _repoFactory.SafeExecAsync(r => r.MarkFailedAsync(id, deliveryKey, reason), _safeExecOptions);
        }

        public Task PerformHouseKeepingTasksAsync()
        {
            return _repoFactory.SafeExecAsync(r => r.PerformHouseKeepingTasksAsync(), SafeExecOptions.NoRetries); // No retries desired here
        }
        #endregion

        private void UpdateSubscriptionCache(Subscription sub, bool deleted = false)
        {
            if (deleted)
            {
                subscriptionCache.Remove(sub.Name);
            }
            else if (_cacheDuration != TimeSpan.Zero)
            {
                subscriptionCache.Set<Subscription>(sub.Name, sub, _cacheDuration);
            }
        }
    }
}
