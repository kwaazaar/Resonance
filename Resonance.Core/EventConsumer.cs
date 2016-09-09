using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Resonance.Models;

namespace Resonance
{
    public class EventConsumer : IEventConsumer
    {
        private IEventingRepo _repo;

        public EventConsumer(IEventingRepo repo)
        {
            _repo = repo;
        }

        public SubscriptionEvent ConsumeNext(string subscriptionName, int? visibilityTimeout = default(int?))
        {
            return _repo.ConsumeNext(subscriptionName, visibilityTimeout);
        }

        public void MarkConsumed(string id, string deliveryKey)
        {
            
        }

        public void MarkFailed(string id, string deliveryKey, string reason)
        {
            
        }
    }
}
