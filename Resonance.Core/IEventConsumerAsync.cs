using Resonance.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Resonance
{
    public interface IEventConsumerAsync
    {
        Task<IEnumerable<ConsumableEvent>> ConsumeNextAsync(string subscriptionName, int visibilityTimeout = 120, int maxCount = 1);
        Task<IEnumerable<ConsumableEvent<T>>> ConsumeNextAsync<T>(string subscriptionName, int visibilityTimeout = 120, int maxCount = 1);
        Task MarkConsumedAsync(IEnumerable<ConsumableEventId> consumableEventsIds, bool transactional = true);
        Task MarkConsumedAsync(Int64 id, string deliveryKey);
        Task MarkFailedAsync(Int64 id, string deliveryKey, Reason reason);

        Task<IEnumerable<Subscription>> GetSubscriptionsAsync(Int64? topicId = null);
        Task<Subscription> GetSubscriptionAsync(Int64 id);
        Task<Subscription> GetSubscriptionByNameAsync(string name);
        Task<Subscription> AddOrUpdateSubscriptionAsync(Subscription subscription);
        Task DeleteSubscriptionAsync(Int64 id);
        Task<IEnumerable<SubscriptionSummary>> GetSubscriptionStatisticsAsync(DateTime periodStartUtc, DateTime periodEndUtc);
        Task PerformHouseKeepingTasksAsync();
    }
}
