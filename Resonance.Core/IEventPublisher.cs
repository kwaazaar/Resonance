using Resonance.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Resonance
{
    public interface IEventPublisher
    {
        TopicEvent Publish(string topicName, string eventName = null, DateTime? publicationDateUtc = null, DateTime? deliveryDelayedUntilUtc = null, DateTime? expirationDateUtc = null, string functionalKey = null, int priority = 100, Dictionary<string, string> headers = null, string payload = null);
        TopicEvent Publish<T>(string topicName, string eventName = null, DateTime? publicationDateUtc = null, DateTime? deliveryDelayedUntilUtc = null, DateTime? expirationDateUtc = null, string functionalKey = null, int priority = 100, Dictionary<string, string> headers = null, T payload = null) where T : class;

        IEnumerable<Topic> GetTopics(string partOfName = null);
        Topic GetTopic(Int64 id);
        Topic GetTopicByName(string name);
        Topic AddOrUpdateTopic(Topic topic);
        void DeleteTopic(Int64 id, bool inclSubscriptions);
        void PerformHouseKeepingTasks();
    }
}
