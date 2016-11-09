using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Resonance.Models;
using System.Net.Http;
using System.Net;

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

        public async Task<Subscription> AddOrUpdateSubscriptionAsync(Subscription subscription)
        {
            using (var httpClient = CreateHttpClient())
            {
                HttpResponseMessage response;

                if (subscription.Id.HasValue)
                {
                    var existingSub = await GetSubscriptionAsync(subscription.Id.Value).ConfigureAwait(false);
                    if (existingSub == null)
                        throw new ArgumentException($"Subscription with Id {subscription.Id.Value} not found");

                    response = await httpClient.PutAsync("subscriptions/" + WebUtility.UrlEncode(subscription.Name), subscription.ToStringContent()).ConfigureAwait(false);
                }
                else
                    response = await httpClient.PostAsync("subscriptions", subscription.ToStringContent()).ConfigureAwait(false);

                response.EnsureSuccessStatusCode();
                var responseContent = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                return responseContent.FromJson<Subscription>();
            }
        }

        public Task<IEnumerable<ConsumableEvent>> ConsumeNextAsync(string subscriptionName, int visibilityTimeout = 120, int maxCount = 1)
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<ConsumableEvent<T>>> ConsumeNextAsync<T>(string subscriptionName, int visibilityTimeout = 120, int maxCount = 1)
        {
            throw new NotImplementedException();
        }

        public async Task DeleteSubscriptionAsync(long id)
        {
            using (var httpClient = CreateHttpClient())
            {
                var existingSub = await GetSubscriptionAsync(id).ConfigureAwait(false);
                if (existingSub != null)
                {
                    var response = await httpClient.DeleteAsync("subscriptions/" + WebUtility.UrlEncode(existingSub.Name));
                    response.EnsureSuccessStatusCode();
                }
                else
                    throw new ArgumentException($"Subscription with Id {id} not found");
            }
        }

        public async Task<Subscription> GetSubscriptionAsync(long id)
        {
            using (var httpClient = CreateHttpClient())
            {
                // Cannot get by id, so get all subscriptions and then look it up
                var response = await httpClient.GetAsync("subscriptions").ConfigureAwait(false);
                response.EnsureSuccessStatusCode();

                var responseContent = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                var subscriptions = responseContent.FromJson<IEnumerable<Subscription>>().ToList();
                return subscriptions.FirstOrDefault(s => s.Id.GetValueOrDefault() == id);
            }
        }

        public async Task<Subscription> GetSubscriptionByNameAsync(string name)
        {
            using (var httpClient = CreateHttpClient())
            {
                // Cannot get by id, so get all subscriptions and then look it up
                var response = await httpClient.GetAsync("subscriptions/" + WebUtility.UrlEncode(name)).ConfigureAwait(false);
                if (response.StatusCode == HttpStatusCode.NotFound)
                    return null;

                response.EnsureSuccessStatusCode();

                var responseContent = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                return responseContent.FromJson<Subscription>();
            }
        }

        public async Task<IEnumerable<Subscription>> GetSubscriptionsAsync(long? topicId = default(long?))
        {
            using (var httpClient = CreateHttpClient())
            {
                var response = await httpClient.GetAsync("subscriptions").ConfigureAwait(false);
                response.EnsureSuccessStatusCode();

                var responseContent = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                return responseContent.FromJson<IEnumerable<Subscription>>().ToList();
            }
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
