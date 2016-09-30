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

        public async Task<Subscription> AddOrUpdateSubscription(Subscription subscription)
        {
            using (var repo = _repoFactory.CreateRepo())
                return await repo.AddOrUpdateSubscription(subscription);
        }

        public async Task DeleteSubscription(Int64 id)
        {
            using (var repo = _repoFactory.CreateRepo())
                await repo.DeleteSubscription(id);
        }

        public async Task<Subscription> GetSubscription(Int64 id)
        {
            using (var repo = _repoFactory.CreateRepo())
                return await repo.GetSubscription(id);
        }

        public async Task<Subscription> GetSubscriptionByName(string name)
        {
            using (var repo = _repoFactory.CreateRepo())
                return await repo.GetSubscriptionByName(name);
        }

        public async Task<IEnumerable<Subscription>> GetSubscriptions(Int64? topicId = null)
        {
            using (var repo = _repoFactory.CreateRepo())
                return await repo.GetSubscriptions(topicId);
        }

        public async Task<IEnumerable<ConsumableEvent>> ConsumeNext(string subscriptionName, int visibilityTimeout = 120, int maxCount = 1)
        {
            using (var repo = _repoFactory.CreateRepo())
                return await repo.ConsumeNext(subscriptionName, visibilityTimeout, maxCount);
        }

        public async Task<IEnumerable<ConsumableEvent<T>>> ConsumeNext<T>(string subscriptionName, int visibilityTimeout = 120, int maxCount = 1)
        {
            var ces = new List<ConsumableEvent<T>>();

            foreach (var ce in await ConsumeNext(subscriptionName, visibilityTimeout, maxCount))
            {
                // Deserialize the payload
                T payloadAsObject = await Task.Factory.StartNew(() => JsonConvert.DeserializeObject<T>(ce.Payload));

                ces.Add(new ConsumableEvent<T>
                {
                    Id = ce.Id,
                    FunctionalKey = ce.FunctionalKey,
                    DeliveryKey = ce.DeliveryKey,
                    InvisibleUntilUtc = ce.InvisibleUntilUtc,
                    Payload = payloadAsObject,
                });
            }

            return ces;
        }

        public async Task MarkConsumed(Int64 id, string deliveryKey)
        {
            using (var repo = _repoFactory.CreateRepo())
                await repo.MarkConsumed(id, deliveryKey);   
        }

        public async Task MarkFailed(Int64 id, string deliveryKey, Reason reason)
        {
            using (var repo = _repoFactory.CreateRepo())
                await repo.MarkFailed(id, deliveryKey, reason);
        }
    }
}
