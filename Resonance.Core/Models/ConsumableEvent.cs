using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Resonance.Models
{
    public class ConsumableEvent
    {
        public string Id { get; set; }
        public string FunctionalKey { get; set; }
        public string DeliveryKey { get; set; }
        public DateTime InvisibleUntilUtc { get; set; }
        public string Payload { get; set; }
    }
}
