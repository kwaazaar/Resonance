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

        public IEnumerable<ConsumableEvent> ConsumeNext(string subscriptionName, int visibilityTimeout = 120, int maxCount = 1)
        {
            using (var repo = _repoFactory.CreateRepo())
                return repo.ConsumeNext(subscriptionName, visibilityTimeout, maxCount);
        }

        public IEnumerable<ConsumableEvent<T>> ConsumeNext<T>(string subscriptionName, int visibilityTimeout = 120, int maxCount = 1)
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
