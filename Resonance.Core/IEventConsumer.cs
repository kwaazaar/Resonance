using Resonance.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Resonance
{
    public interface IEventConsumer
    {
        Task<IEnumerable<ConsumableEvent>> ConsumeNext(string subscriptionName, int visibilityTimeout = 120, int maxCount = 1);
        Task<IEnumerable<ConsumableEvent<T>>> ConsumeNext<T>(string subscriptionName, int visibilityTimeout = 120, int maxCount = 1);
        Task MarkConsumed(Int64 id, string deliveryKey);
        Task MarkFailed(Int64 id, string deliveryKey, Reason reason);

        Task<IEnumerable<Subscription>> GetSubscriptions(Int64? topicId = null);
        Task<Subscription> GetSubscription(Int64 id);
        Task<Subscription> GetSubscriptionByName(string name);
        Task<Subscription> AddOrUpdateSubscription(Subscription subscription);
        Task DeleteSubscription(Int64 id);
    }
}
