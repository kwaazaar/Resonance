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
        void MarkConsumed(string id, string deliveryKey);
        void MarkFailed(string id, string deliveryKey, Reason reason);
    }
}
