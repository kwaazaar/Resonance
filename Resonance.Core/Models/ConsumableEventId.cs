using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Resonance.Models
{
    /// <summary>
    /// Represents a consumable events by its identifiers (id and optional deliverykey)
    /// </summary>
    public class ConsumableEventId
    {
        /// <summary>
        /// Id of the consumable event (subscriptionevent)
        /// </summary>
        public Int64 Id { get; set; }

        /// <summary>
        /// Optional: the current deliverykey of the consumable event (created on consumption)
        /// </summary>
        public string DeliveryKey { get; set; }
    }
}
