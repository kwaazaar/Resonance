using Resonance.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Resonance
{
    public interface IEventPublisher
    {
        #region Sync
        TopicEvent Publish(string topicName, string eventName = null, DateTime? publicationDateUtc = null, DateTime? expirationDateUtc = null, string functionalKey = null, int priority = 0, Dictionary<string, string> headers = null, string payload = null);
        TopicEvent Publish<T>(string topicName, string eventName = null, DateTime? publicationDateUtc = null, DateTime? expirationDateUtc = null, string functionalKey = null, int priority = 0, Dictionary<string, string> headers = null, T payload = null) where T : class;

        IEnumerable<Topic> GetTopics(string partOfName = null);
        Topic GetTopic(Int64 id);
        Topic GetTopicByName(string name);
        Topic AddOrUpdateTopic(Topic topic);
        void DeleteTopic(Int64 id, bool inclSubscriptions);
        void PerformHouseKeepingTasks();
        #endregion

        #region Async
        Task<TopicEvent> PublishAsync(string topicName, string eventName = null, DateTime? publicationDateUtc = null, DateTime? expirationDateUtc = null, string functionalKey = null, int priority = 0, Dictionary<string, string> headers = null, string payload = null);
        Task<TopicEvent> PublishAsync<T>(string topicName, string eventName = null, DateTime? publicationDateUtc = null, DateTime? expirationDateUtc = null, string functionalKey = null, int priority = 0, Dictionary<string, string> headers=null, T payload = null) where T : class;

        Task<IEnumerable<Topic>> GetTopicsAsync(string partOfName = null);
        Task<Topic> GetTopicAsync(Int64 id);
        Task<Topic> GetTopicByNameAsync(string name);
        Task<Topic> AddOrUpdateTopicAsync(Topic topic);
        Task DeleteTopicAsync(Int64 id, bool inclSubscriptions);
        Task PerformHouseKeepingTasksAsync();
        #endregion
    }
}
