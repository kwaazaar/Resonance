using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Resonance.Models
{
    public class Topic
    {
        /// <summary>
        /// Id for the topic. Will be automatically determined bij the repo on add/insert.
        /// </summary>
        public Int64? Id { get; set; }

        /// <summary>
        /// Name of the topic. Must be unique across the repo.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Optional notes.
        /// </summary>
        public string Notes { get; set; }

        /// <summary>
        /// When set to true (default), published events will be logged to the TopicEvent-table
        /// </summary>
        public bool Log { get; set; } = true;
    }
}
