using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Resonance.Models
{
    public class TopicEvent
    {
        public string Id { get; set; }
        public string TopicId { get; set; }
        public DateTime? PublicationDateUtc { get; set; }
        public DateTime? ExpirationDateUtc { get; set; }
        public string FunctionalKey { get; set; }
        public int Priority { get; set; }
        public string PayloadId { get; set; }
        public Dictionary<string,string> Headers { get; set; }
    }
}
