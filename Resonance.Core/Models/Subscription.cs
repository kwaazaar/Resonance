using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Resonance.Models
{
    public class Subscription
    {
        /// <summary>
        /// Id of the subscription. Will be automatically determined by the repo on add/insert.
        /// </summary>
        public Int64? Id { get; set; }
        
        /// <summary>
        /// Name of the subscription. Must be unique across the repo.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Topic subscriptions for this subscription.
        /// </summary>
        public List<TopicSubscription> TopicSubscriptions { get; set; } = new List<TopicSubscription>();
        
        /// <summary>
        /// Apply functional ordering (only enable this when required)
        /// </summary>
        public bool Ordered { get; set; }
        
        /// <summary>
        /// Time to live (seconds)
        /// </summary>
        public int? TimeToLive { get; set; }

        /// <summary>
        /// 0 = Unlimited
        /// </summary>
        public int MaxDeliveries { get; set; }

        /// <summary>
        /// Delivery delay (seconds)
        /// </summary>
        public int? DeliveryDelay { get; set; }

        /// <summary>
        /// When set to true (default), consumed events will be logged to the ConsumedSubscriptionEvent-table
        /// </summary>
        public bool LogConsumed { get; set; } = true;

        /// <summary>
        /// When set to true (default), failed events will be logged to the FailedSubscriptionEvent-table
        /// </summary>
        public bool LogFailed { get; set; } = true;
    }

    public class TopicSubscription
    {
        public Int64? Id { get; set; }
        public Int64 TopicId { get; set; }
        public bool Enabled { get; set; } = true;

        /// <summary>
        /// Indicates wheter this subscriptions is filtered, meaning not all messages of the topic will be propagated, but only the ones that match the specified filter.
        /// Make sure filters are specified, otherwise no messages will match.
        /// </summary>
        public bool Filtered { get; set; }

        /// <summary>
        /// Topic-filters. Make sure Filtered is set to true, otherwise the filters are ignored.
        /// </summary>
        public List<TopicSubscriptionFilter> Filters { get; set; } = new List<TopicSubscriptionFilter>();
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

        /// <summary>
        /// Indicates whether this filter must NOT match.
        /// When set to true, the filter-result is inverted.
        /// </summary>
        public bool NotMatch { get; set; }
    }
}
