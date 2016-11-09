using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Resonance.Models;
using System.Net.Http;

namespace Resonance.APIClient
{
    public class APIEventConsumer : APIClientBase, IEventConsumerAsync
    {
        public APIEventConsumer(Uri resonanceApiBaseAddress)
            : this(resonanceApiBaseAddress, new HttpClientHandler(), TimeSpan.FromSeconds(30))
        {
        }

        public APIEventConsumer(Uri resonanceApiBaseAddress, HttpMessageHandler messageHandler, TimeSpan timeout)
            : base(resonanceApiBaseAddress, messageHandler, timeout)
        {
        }

        public Task<Subscription> AddOrUpdateSubscriptionAsync(Subscription subscription)
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<ConsumableEvent>> ConsumeNextAsync(string subscriptionName, int visibilityTimeout = 120, int maxCount = 1)
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<ConsumableEvent<T>>> ConsumeNextAsync<T>(string subscriptionName, int visibilityTimeout = 120, int maxCount = 1)
        {
            throw new NotImplementedException();
        }

        public Task DeleteSubscriptionAsync(long id)
        {
            throw new NotImplementedException();
        }

        public Task<Subscription> GetSubscriptionAsync(long id)
        {
            throw new NotImplementedException();
        }

        public Task<Subscription> GetSubscriptionByNameAsync(string name)
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<Subscription>> GetSubscriptionsAsync(long? topicId = default(long?))
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<SubscriptionSummary>> GetSubscriptionStatisticsAsync(DateTime periodStartUtc, DateTime periodEndUtc)
        {
            throw new NotImplementedException();
        }

        public Task MarkConsumedAsync(long id, string deliveryKey)
        {
            throw new NotImplementedException();
        }

        public Task MarkFailedAsync(long id, string deliveryKey, Reason reason)
        {
            throw new NotImplementedException();
        }

        public Task PerformHouseKeepingTasksAsync()
        {
            throw new NotImplementedException();
        }
    }
}
