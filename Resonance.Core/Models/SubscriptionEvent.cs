using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Resonance.Models
{
    public class SubscriptionEvent
    {
        public Int64? Id { get; set; }
        public Int64 SubscriptionId { get; set; }
        public bool Ordered { get; set; }
        /// <summary>
        /// Id of the topicevent (only available if the topicevent was actually logged)
        /// </summary>
        public Int64? TopicEventId { get; set; }
        public string EventName { get; set; }
        public DateTime PublicationDateUtc { get; set; }
        public string FunctionalKey { get; set; }
        public int Priority { get; set; }
        public Int64? PayloadId { get; set; }
        public string Payload { get; set; }
        public DateTime ExpirationDateUtc { get; set; }
        public DateTime DeliveryDelayedUntilUtc { get; set; }
        public int DeliveryCount { get; set; }
        public DateTime? DeliveryDateUtc { get; set; }
        public string DeliveryKey { get; set; }
        public DateTime InvisibleUntilUtc { get; set; }
    }
}
