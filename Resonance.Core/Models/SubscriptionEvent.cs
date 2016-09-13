using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Resonance.Models
{
    public class SubscriptionEvent
    {
        public string Id { get; set; }
        public string SubscriptionId { get; set; }
        public string TopicEventId { get; set; }
        public DateTime PublicationDateUtc { get; set; }
        public string FunctionalKey { get; set; }
        public int Priority { get; set; }
        public string PayloadId { get; set; }
        public string Payload { get; set; }
        public DateTime? ExpirationDateUtc { get; set; }
        public DateTime? DeliveryDelayedUntilUtc { get; set; }
        public int DeliveryCount { get; set; }
        public DateTime? DeliveryDateUtc { get; set; }
        public string DeliveryKey { get; set; }
        public DateTime? InvisibleUntilUtc { get; set; }
    }
}
