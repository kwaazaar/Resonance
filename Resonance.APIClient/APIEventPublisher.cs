using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Resonance.Models;
using System.Net.Http;

namespace Resonance.APIClient
{
    public class APIEventPublisher : APIClientBase, IEventPublisherAsync
    {
        public APIEventPublisher(Uri resonanceApiBaseAddress)
            : this(resonanceApiBaseAddress, new HttpClientHandler(), TimeSpan.FromSeconds(30))
        {
        }

        public APIEventPublisher(Uri resonanceApiBaseAddress, HttpMessageHandler messageHandler, TimeSpan timeout)
            : base(resonanceApiBaseAddress, messageHandler, timeout)
        {
        }

        public Task<Topic> AddOrUpdateTopicAsync(Topic topic)
        {
            throw new NotImplementedException();
        }

        public Task DeleteTopicAsync(long id, bool inclSubscriptions)
        {
            throw new NotImplementedException();
        }

        public Task<Topic> GetTopicAsync(long id)
        {
            throw new NotImplementedException();
        }

        public Task<Topic> GetTopicByNameAsync(string name)
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<Topic>> GetTopicsAsync(string partOfName = null)
        {
            throw new NotImplementedException();
        }

        public Task PerformHouseKeepingTasksAsync()
        {
            throw new NotImplementedException();
        }

        public Task<TopicEvent> PublishAsync(string topicName, string eventName = null, DateTime? publicationDateUtc = default(DateTime?), DateTime? deliveryDelayedUntilUtc = null, DateTime? expirationDateUtc = default(DateTime?), string functionalKey = null, int priority = 100, Dictionary<string, string> headers = null, string payload = null)
        {
            throw new NotImplementedException();
        }

        public Task<TopicEvent> PublishAsync<T>(string topicName, string eventName = null, DateTime? publicationDateUtc = default(DateTime?), DateTime? deliveryDelayedUntilUtc = null, DateTime? expirationDateUtc = default(DateTime?), string functionalKey = null, int priority = 100, Dictionary<string, string> headers = null, T payload = null) where T : class
        {
            throw new NotImplementedException();
        }
    }
}
