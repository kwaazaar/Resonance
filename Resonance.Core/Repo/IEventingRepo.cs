﻿using Resonance.Repo.InternalModels;
using Resonance.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Resonance.Repo
{
    public interface IEventingRepo : IDisposable
    {
        // Topic & subscription management
        Task<IEnumerable<Topic>> GetTopicsAsync(string partOfName = null);
        Task<Topic> GetTopicAsync(Int64 id);
        Task<Topic> GetTopicByNameAsync(string name);
        Task<Topic> AddOrUpdateTopicAsync(Topic topic);
        Task DeleteTopicAsync(Int64 id, bool inclSubscriptions);
        Task<IEnumerable<Subscription>> GetSubscriptionsAsync(Int64? topicId = null);
        Task<Subscription> GetSubscriptionAsync(Int64 id);
        Task<Subscription> GetSubscriptionByNameAsync(string name);
        Task<Subscription> AddOrUpdateSubscriptionAsync(Subscription subscription);
        Task DeleteSubscriptionAsync(Int64 id);
        Task<IEnumerable<SubscriptionSummary>> GetSubscriptionStatisticsAsync(DateTime periodStartUtc, DateTime periodEndUtc);

        // Publication
        Task<DateTime> GetNowUtcAsync();
        Task<Int64> StorePayloadAsync(string payload);
        Task<string> GetPayloadAsync(Int64 id);
        Task<int> DeletePayloadAsync(Int64 id);
        Task<TopicEvent> PublishTopicEventAsync(TopicEvent newTopicEvent, bool logTopicEvent, IEnumerable<Subscription> subscriptionsMatching, DateTime? deliveryDelayedUntilUtc);

        // Consumption
        Task<IEnumerable<ConsumableEvent>> ConsumeNextAsync(string subscriptionName, int visibilityTimeout, int maxCount = 1);
        Task<IEnumerable<ConsumableEvent>> ConsumeNextAsync(Subscription subscription, int visibilityTimeout, int maxCount = 1);
        Task MarkConsumedAsync(IEnumerable<ConsumableEventId> consumableEventsIds, bool transactional = true);
        Task MarkConsumedAsync(Int64 id, string deliveryKey);
        Task MarkFailedAsync(Int64 id, string deliveryKey, Reason reason);

        // Housekeeping
        Task PerformHouseKeepingTasksAsync();
    }

}
