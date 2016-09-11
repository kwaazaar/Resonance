using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Resonance.InternalModels
{
    public class SubscriptionEventIdentifier
    {
        public string Id { get; set; }
        public string DeliveryKey { get; set; }
        public string FunctionalKey { get; set; }
        public string PayloadId { get; set; }
    }
}
