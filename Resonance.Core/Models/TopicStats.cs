using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Resonance.Models
{
    public class TopicStats
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public int NrOfSubscriptions { get; set; }
        public int DeliveredEvents { get; set; }
        public int UndeliveredEvents { get; set; }
        public int FailedEvents { get; set; }
    }
}
