using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Resonance.Models;
using Newtonsoft.Json;
using Resonance.Repo;

namespace Resonance
{
    public class EventConsumer : IEventConsumer
    {
        private IEventingRepoFactory _repoFactory;

        public EventConsumer(IEventingRepoFactory repoFactory)
        {
            _repoFactory = repoFactory;
        }

        public Subscription AddOrUpdateSubscription(Subscription subscription)
        {
            using (var repo = _repoFactory.CreateRepo())
                return repo.AddOrUpdateSubscription(subscription);
        }

        public void DeleteSubscription(string id)
        {
            using (var repo = _repoFactory.CreateRepo())
                repo.DeleteSubscription(id);
        }

        public Subscription GetSubscription(string id)
        {
            using (var repo = _repoFactory.CreateRepo())
                return repo.GetSubscription(id);
        }

        public Subscription GetSubscriptionByName(string name)
        {
            using (var repo = _repoFactory.CreateRepo())
                return repo.GetSubscriptionByName(name);
        }

        public IEnumerable<Subscription> GetSubscriptions(string topicId = null)
        {
            using (var repo = _repoFactory.CreateRepo())
                return repo.GetSubscriptions(topicId).ToList();
        }

        public IEnumerable<ConsumableEvent> ConsumeNext(string subscriptionName, int? visibilityTimeout = default(int?), int maxCount = 1)
        {
            var subscription = GetSubscriptionByName(subscriptionName);
            if (subscription == null) throw new ArgumentException($"No subscription with this name exists: {subscriptionName}");

            var ces = new List<ConsumableEvent>();
            using (var repo = _repoFactory.CreateRepo())
            {
                // Find possible subscriptionevents for consumption (repo decides how many it can buffer)
                var sIds = repo.FindConsumableEventsForSubscription(subscription, maxCount).ToList();

                // Loop through all found subscriptionevents until one of them could be 'locked'
                foreach (var sId in sIds)
                {
                    string deliveryKey = Guid.NewGuid().ToString(); // Determine new deliverykey
                    var invisibleUntilUtc = DateTime.UtcNow.AddSeconds(visibilityTimeout.GetValueOrDefault(60)); // Recalc on every attempt in this loop, since every attempt make take considerable time

                    // Attempt to lock it now
                    if (repo.TryLockConsumableEvent(sId, deliveryKey, invisibleUntilUtc))
                    {
                        // We got it! Now get the rest of the details
                        var @event = new ConsumableEvent
                        {
                            Id = sId.Id,
                            DeliveryKey = deliveryKey, // Updated above
                            FunctionalKey = sId.FunctionalKey,
                            InvisibleUntilUtc = invisibleUntilUtc,
                        };
                        @event.Payload = repo.GetPayload(sId.PayloadId);
                        ces.Add(@event);
                    }
                }

                if (ces.Count < sIds.Count)
                    System.Diagnostics.Debug.WriteLine($"Managed to lock only {ces.Count} events out of {sIds.Count} found.");
            }
            return ces;
        }

        public IEnumerable<ConsumableEvent<T>> ConsumeNext<T>(string subscriptionName, int? visibilityTimeout = default(int?), int maxCount = 1)
        {
            foreach (var ce in ConsumeNext(subscriptionName, visibilityTimeout, maxCount))
            {
                // Deserialize the payload
                T payloadAsObject = JsonConvert.DeserializeObject<T>(ce.Payload);

                yield return new ConsumableEvent<T>
                {
                    Id = ce.Id,
                    FunctionalKey = ce.FunctionalKey,
                    DeliveryKey = ce.DeliveryKey,
                    InvisibleUntilUtc = ce.InvisibleUntilUtc,
                    Payload = payloadAsObject,
                };
            }
        }

        public void MarkConsumed(string id, string deliveryKey)
        {
            using (var repo = _repoFactory.CreateRepo())
                repo.MarkConsumed(id, deliveryKey);   
        }

        public void MarkFailed(string id, string deliveryKey, Reason reason)
        {
            using (var repo = _repoFactory.CreateRepo())
                repo.MarkFailed(id, deliveryKey, reason);
        }
    }
}
