using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Resonance.Models
{
    public class SubscriptionSummary
    {
        public Int64 Id { get; set; }

        public long Open { get; set; }
        public long Consumed { get; set; }
        public long FailedUnknown { get; set; }
        public long FailedExpired { get; set; }
        public long FailedMaxDeliveriesReached { get; set; }
        public long FailedOvertaken { get; set; }
        public long FailedOther { get; set; }
        public Subscription Subscription { get; set; }
    }
}
