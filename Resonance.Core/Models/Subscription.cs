using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Resonance.Models
{
    public class Subscription
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string TopicId { get; set; }
        public bool Enabled { get; set; }
        public bool Ordered { get; set; }
        public int? TimeToLive { get; set; }
        public int MaxDeliveries { get; set; }
        public int? DeliveryDelay { get; set; }
    }
}
