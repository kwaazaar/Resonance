using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Resonance.Models
{
    public class Subscription
    {
        public Int64? Id { get; set; }
        public string Name { get; set; }
        public List<TopicSubscription> TopicSubscriptions { get; set; }
        public bool Ordered { get; set; }
        public int? TimeToLive { get; set; }
        public int MaxDeliveries { get; set; }
        public int? DeliveryDelay { get; set; }
    }

    public class TopicSubscription
    {
        public Int64? Id { get; set; }
        public Int64 TopicId { get; set; }
        public bool Enabled { get; set; }
        /// <summary>
        /// Indicates wheter this subscriptions is filtered, meaning not all messages of the topic will be propagated, but only the ones that match the specified filter.
        /// Make sure filters are specified, otherwise no messages will match.
        /// </summary>
        public bool Filtered { get; set; }
        /// <summary>
        /// Topic-filters. Make sure Filtered is set to true, otherwise the filters are ignored.
        /// </summary>
        public List<TopicSubscriptionFilter> Filters { get; set; }
    }

    public class TopicSubscriptionFilter
    {
        public Int64? Id { get; set; }
        /// <summary>
        /// The header to check for
        /// </summary>
        public string Header { get; set; }
        /// <summary>
        /// The expression to match the header on (case insensitive):
        /// - OrderPlaced will match only exactly that
        /// - * will match anything
        /// - OrderEvents.* will match OrderEvents.OrderPlaced, OrderEvents.OrderCancelled, etc.
        /// - *Cancelled will match OrderCancelled, PaymentCancelled, etc.
        /// </summary>
        public string MatchExpression { get; set; }
    }
}
