using Resonance.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Resonance
{
    public interface IEventConsumer
    {
        ConsumableEvent ConsumeNext(string subscriptionName, int? visibilityTimeout = null);
        ConsumableEvent<T> ConsumeNext<T>(string subscriptionName, int? visibilityTimeout = default(int?));
        void MarkConsumed(string id, string deliveryKey);
        void MarkFailed(string id, string deliveryKey, Reason reason);

        IEnumerable<Subscription> GetSubscriptions(string topicId = null);
        Subscription GetSubscription(string id);
        Subscription GetSubscriptionByName(string name);
        Subscription AddOrUpdateSubscription(Subscription subscription);
        void DeleteSubscription(string id);
    }
}
