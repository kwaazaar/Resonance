using Resonance.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Resonance
{
    public interface IEventConsumer
    {
        #region Sync
        IEnumerable<ConsumableEvent> ConsumeNext(string subscriptionName, int visibilityTimeout = 120, int maxCount = 1);
        IEnumerable<ConsumableEvent<T>> ConsumeNext<T>(string subscriptionName, int visibilityTimeout = 120, int maxCount = 1);
        void MarkConsumed(Int64 id, string deliveryKey);
        void MarkFailed(Int64 id, string deliveryKey, Reason reason);
        IEnumerable<Subscription> GetSubscriptions(Int64? topicId = null);
        Subscription GetSubscription(Int64 id);
        Subscription GetSubscriptionByName(string name);
        Subscription AddOrUpdateSubscription(Subscription subscription);
        void DeleteSubscription(Int64 id);
        IEnumerable<SubscriptionSummary> GetSubscriptionStatistics(DateTime periodStartUtc, DateTime periodEndUtc);
        void PerformHouseKeepingTasks();
        #endregion

        #region Async
        Task<IEnumerable<ConsumableEvent>> ConsumeNextAsync(string subscriptionName, int visibilityTimeout = 120, int maxCount = 1);
        Task<IEnumerable<ConsumableEvent<T>>> ConsumeNextAsync<T>(string subscriptionName, int visibilityTimeout = 120, int maxCount = 1);
        Task MarkConsumedAsync(Int64 id, string deliveryKey);
        Task MarkFailedAsync(Int64 id, string deliveryKey, Reason reason);

        Task<IEnumerable<Subscription>> GetSubscriptionsAsync(Int64? topicId = null);
        Task<Subscription> GetSubscriptionAsync(Int64 id);
        Task<Subscription> GetSubscriptionByNameAsync(string name);
        Task<Subscription> AddOrUpdateSubscriptionAsync(Subscription subscription);
        Task DeleteSubscriptionAsync(Int64 id);
        Task<IEnumerable<SubscriptionSummary>> GetSubscriptionStatisticsAsync(DateTime periodStartUtc, DateTime periodEndUtc);
        Task PerformHouseKeepingTasksAsync();
        #endregion
    }
}
