using Resonance.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Resonance
{
    public interface IEventPublisher
    {
        Task<TopicEvent> Publish(string topicName, DateTime? publicationDateUtc = null, DateTime? expirationDateUtc = null, string functionalKey = null, Dictionary<string, string> headers = null, string payload = null);
        Task<TopicEvent> Publish<T>(string topicName, DateTime? publicationDateUtc = null, DateTime? expirationDateUtc = null, string functionalKey = null, Dictionary<string, string> headers=null, T payload = null) where T : class;

        Task<IEnumerable<Topic>> GetTopics(string partOfName = null);
        Task<Topic> GetTopic(Int64 id);
        Task<Topic> GetTopicByName(string name);
        Task<Topic> AddOrUpdateTopic(Topic topic);
        Task DeleteTopic(Int64 id, bool inclSubscriptions);
    }
}
