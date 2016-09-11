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
            var subscription = GetSubscriptionByName(subscriptionName);
            if (subscription == null) throw new ArgumentException($"No subscription with this name exists: {subscriptionName}");

            // Find possible subscriptionevents for consumption (repo decides how many it can buffer)
            var sIds = _repo.FindConsumableEventsForSubscription(subscription).ToList();
            if (sIds.Count == 0)
                return null; // Nothing found

            // Determine new deliverykey
            string deliveryKey = Guid.NewGuid().ToString();

            // Loop through all found subscriptionevents until one of them could be 'locked'
            foreach (var sId in sIds)
            {
                var invisibleUntilUtc = DateTime.UtcNow.AddSeconds(visibilityTimeout.GetValueOrDefault(60)); // Recalc on every attempt in this loop, since every attempt make take considerable time

                // Attempt to lock it now
                if (_repo.TryLockConsumableEvent(sId, deliveryKey, invisibleUntilUtc))
                {
                    // We got it! Now get the rest of the details
                    var @event = new ConsumableEvent
                    {
                        Id = sId.Id,
                        DeliveryKey = deliveryKey, // Updated above
                        FunctionalKey = sId.FunctionalKey,
                        InvisibleUntilUtc = invisibleUntilUtc,
                    };
                    @event.Payload = _repo.GetPayload(sId.PayloadId);
                    return @event;
                }
            }

            return null;
        }

        public ConsumableEvent<T> ConsumeNext<T>(string subscriptionName, int? visibilityTimeout = default(int?))
        {
            var ce = ConsumeNext(subscriptionName, visibilityTimeout);
            if (ce != null)
            {
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
            else
                return default(ConsumableEvent<T>);
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
