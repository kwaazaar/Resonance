using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Resonance.Models;
using Newtonsoft.Json;

namespace Resonance
{
    public class EventConsumer : IEventConsumer
    {
        private IEventingRepo _repo;

        public EventConsumer(IEventingRepo repo)
        {
            _repo = repo;
        }

        public Subscription AddOrUpdateSubscription(Subscription subscription)
        {
            return _repo.AddOrUpdateSubscription(subscription);
        }

        public void DeleteSubscription(string id)
        {
            _repo.DeleteSubscription(id);
        }

        public Subscription GetSubscription(string id)
        {
            return _repo.GetSubscription(id);
        }

        public Subscription GetSubscriptionByName(string name)
        {
            return _repo.GetSubscriptionByName(name);
        }

        public IEnumerable<Subscription> GetSubscriptions(string topicId = null)
        {
            return _repo.GetSubscriptions(topicId)
                .ToList();
        }

        public ConsumableEvent ConsumeNext(string subscriptionName, int? visibilityTimeout = default(int?))
        {
            return _repo.ConsumeNext(subscriptionName, visibilityTimeout);
        }

        public ConsumableEvent<T> ConsumeNext<T>(string subscriptionName, int? visibilityTimeout = default(int?))
        {
            var ce = _repo.ConsumeNext(subscriptionName, visibilityTimeout);
            if (ce == null)
                return default(ConsumableEvent<T>);

            // Deserialize the payload
            T payloadAsObject = JsonConvert.DeserializeObject<T>(ce.Payload);

            return new ConsumableEvent<T>
            {
                Id = ce.Id,
                FunctionalKey = ce.FunctionalKey,
                DeliveryKey = ce.DeliveryKey,
                InvisibleUntilUtc = ce.InvisibleUntilUtc,
                Payload = payloadAsObject,
            };
        }

        public void MarkConsumed(string id, string deliveryKey)
        {
            _repo.MarkConsumed(id, deliveryKey);   
        }

        public void MarkFailed(string id, string deliveryKey, Reason reason)
        {
            _repo.MarkFailed(id, deliveryKey, reason);
        }
    }
}
