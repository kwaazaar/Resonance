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

                    response = await httpClient.PutAsync("subscriptions/" + Uri.EscapeDataString(subscription.Name), subscription.ToStringContent()).ConfigureAwait(false);
                }
                else
                    response = await httpClient.PostAsync("subscriptions", subscription.ToStringContent()).ConfigureAwait(false);

                if (!response.IsSuccessStatusCode)
                    throw await HttpResponseException.Create(response);

                var responseContent = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                return responseContent.FromJson<Subscription>();
            }
        }

        public async Task<IEnumerable<ConsumableEvent>> ConsumeNextAsync(string subscriptionName, int visibilityTimeout = 120, int maxCount = 1)
        {
            if (String.IsNullOrWhiteSpace(subscriptionName)) throw new ArgumentNullException("subscriptionName");

            using (var httpClient = CreateHttpClient())
            {
                var response = await httpClient.GetAsync($"consume/{Uri.EscapeDataString(subscriptionName)}?visibilityTimeout={visibilityTimeout}&maxCount={maxCount}").ConfigureAwait(false);
                if (!response.IsSuccessStatusCode)
                {
                    if (response.StatusCode == HttpStatusCode.NotFound)
                        return new List<ConsumableEvent>(); // Empty list

                    throw await HttpResponseException.Create(response);
                }

                var responseContent = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                return responseContent.FromJson<IEnumerable<ConsumableEvent>>().ToList();
            }
        }

        public async Task<IEnumerable<ConsumableEvent<T>>> ConsumeNextAsync<T>(string subscriptionName, int visibilityTimeout = 120, int maxCount = 1)
        {
            var ces = new List<ConsumableEvent<T>>();

            foreach (var ce in await ConsumeNextAsync(subscriptionName, visibilityTimeout, maxCount).ConfigureAwait(false))
            {
                // Deserialize the payload
                T payloadAsObject = ce.Payload != null ? ce.Payload.FromJson<T>() : default(T);

                ces.Add(new ConsumableEvent<T>
                {
                    Id = ce.Id,
                    FunctionalKey = ce.FunctionalKey,
                    DeliveryKey = ce.DeliveryKey,
                    InvisibleUntilUtc = ce.InvisibleUntilUtc,
                    Payload = payloadAsObject,
                });
            }

            return ces;
        }

        public async Task DeleteSubscriptionAsync(long id)
        {
            using (var httpClient = CreateHttpClient())
            {
                var existingSub = await GetSubscriptionAsync(id).ConfigureAwait(false);
                if (existingSub != null)
                {
                    var response = await httpClient.DeleteAsync("subscriptions/" + Uri.EscapeDataString(existingSub.Name));
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
                if (!response.IsSuccessStatusCode)
                    throw await HttpResponseException.Create(response);

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
                var response = await httpClient.GetAsync("subscriptions/" + Uri.EscapeDataString(name)).ConfigureAwait(false);
                if (!response.IsSuccessStatusCode)
                {
                    if (response.StatusCode == HttpStatusCode.NotFound)
                        return null;
                    throw await HttpResponseException.Create(response);
                }

                var responseContent = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                return responseContent.FromJson<Subscription>();
            }
        }

        public async Task<IEnumerable<Subscription>> GetSubscriptionsAsync(long? topicId = default(long?))
        {
            using (var httpClient = CreateHttpClient())
            {
                var url = "subscriptions";
                if (topicId.HasValue)
                    url += $"?topicId={topicId.Value}";

                var response = await httpClient.GetAsync(url).ConfigureAwait(false);
                if (!response.IsSuccessStatusCode)
                    throw await HttpResponseException.Create(response);

                var responseContent = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                return responseContent.FromJson<IEnumerable<Subscription>>().ToList();
            }
        }

        public async Task<IEnumerable<SubscriptionSummary>> GetSubscriptionStatisticsAsync(DateTime periodStartUtc, DateTime periodEndUtc)
        {
            if (periodEndUtc < periodStartUtc)
                throw new ArgumentOutOfRangeException("periodEndUtc cannot be less than periodStartUtc");

            using (var httpClient = CreateHttpClient())
            {
                var response = await httpClient.GetAsync($"subscriptions/stats?periodStartUtc={Uri.EscapeDataString(periodStartUtc.ToString("o"))}&periodEndUtc={Uri.EscapeDataString(periodEndUtc.ToString("o"))}").ConfigureAwait(false);
                if (!response.IsSuccessStatusCode)
                    throw await HttpResponseException.Create(response);

                var responseContent = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                return responseContent.FromJson<IEnumerable<SubscriptionSummary>>().ToList();
            }
        }

        public async Task MarkConsumedAsync(long id, string deliveryKey)
        {
            if (id <= 0) throw new ArgumentOutOfRangeException("id");
            if (String.IsNullOrWhiteSpace(deliveryKey)) throw new ArgumentNullException("deliveryKey");

            using (var httpClient = CreateHttpClient())
            {
                var response = await httpClient.GetAsync($"mark/{id}/{Uri.EscapeDataString(deliveryKey)}/consumed").ConfigureAwait(false);
                if (!response.IsSuccessStatusCode)
                    throw await HttpResponseException.Create(response);
            }
        }

        public async Task MarkFailedAsync(long id, string deliveryKey, Reason reason)
        {
            if (id <= 0) throw new ArgumentOutOfRangeException("id");
            if (String.IsNullOrWhiteSpace(deliveryKey)) throw new ArgumentNullException("deliveryKey");

            using (var httpClient = CreateHttpClient())
            {
                // Use POST instead of GET, since 'reason' may be a large text
                var response = await httpClient.PostAsync($"mark/{id}/{Uri.EscapeDataString(deliveryKey)}/failed", new
                    {
                        reason = (reason.Type == ReasonType.Other) ? reason.ReasonText : $"{reason.Type}: {reason.ReasonText}"
                    }.ToStringContent()).ConfigureAwait(false);

                if (!response.IsSuccessStatusCode)
                    throw await HttpResponseException.Create(response);
            }
        }
    }
}
