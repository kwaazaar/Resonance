using Resonance.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Resonance
{
    public interface IEventConsumer
    {
        IEnumerable<ConsumableEvent> ConsumeNext(string subscriptionName, int visibilityTimeout = 120, int maxCount = 1);
        IEnumerable<ConsumableEvent<T>> ConsumeNext<T>(string subscriptionName, int visibilityTimeout = 120, int maxCount = 1);
        void MarkConsumed(Int64 id, string deliveryKey);
        void MarkFailed(Int64 id, string deliveryKey, Reason reason);

        IEnumerable<Subscription> GetSubscriptions(Int64? topicId = null);
        Subscription GetSubscription(Int64 id);
        Subscription GetSubscriptionByName(string name);
        Subscription AddOrUpdateSubscription(Subscription subscription);
        void DeleteSubscription(Int64 id);
    }
}
