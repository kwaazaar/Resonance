using Resonance.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Resonance
{
    public interface IEventPublisher
    {
        TopicEvent Publish(string topicName, DateTime? publicationDateUtc = null, DateTime? expirationDateUtc = null, string functionalKey = null, string payload = null);
        TopicEvent Publish<T>(string topicName, DateTime? publicationDateUtc = null, DateTime? expirationDateUtc = null, string functionalKey = null, T payload = null) where T : class;
    }
}
