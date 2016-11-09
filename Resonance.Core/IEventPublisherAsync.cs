using Resonance.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Resonance
{
    public interface IEventPublisherAsync
    {
        Task<TopicEvent> PublishAsync(string topicName, string eventName = null, DateTime? publicationDateUtc = null, DateTime? expirationDateUtc = null, string functionalKey = null, int priority = 100, Dictionary<string, string> headers = null, string payload = null);
        Task<TopicEvent> PublishAsync<T>(string topicName, string eventName = null, DateTime? publicationDateUtc = null, DateTime? expirationDateUtc = null, string functionalKey = null, int priority = 100, Dictionary<string, string> headers = null, T payload = null) where T : class;

        Task<IEnumerable<Topic>> GetTopicsAsync(string partOfName = null);
        Task<Topic> GetTopicAsync(Int64 id);
        Task<Topic> GetTopicByNameAsync(string name);
        Task<Topic> AddOrUpdateTopicAsync(Topic topic);
        Task DeleteTopicAsync(Int64 id, bool inclSubscriptions);
        Task PerformHouseKeepingTasksAsync();
    }
}
