using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Resonance.Models;
using System.Net.Http;
using System.Net;

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

        public async Task<Topic> AddOrUpdateTopicAsync(Topic topic)
        {
            using (var httpClient = CreateHttpClient())
            {
                HttpResponseMessage response;

                if (topic.Id.HasValue)
                {
                    var existingTopic = await GetTopicAsync(topic.Id.Value).ConfigureAwait(false);
                    if (existingTopic == null)
                        throw new ArgumentException($"Topic with Id {topic.Id.Value} not found");

                    response = await httpClient.PutAsync("topics/" + Uri.EscapeDataString(topic.Name), topic.ToStringContent()).ConfigureAwait(false);
                }
                else
                    response = await httpClient.PostAsync("topics", topic.ToStringContent()).ConfigureAwait(false);

                if (!response.IsSuccessStatusCode)
                    throw await HttpResponseException.Create(response);

                var responseContent = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                return responseContent.FromJson<Topic>();
            }
        }

        public async Task DeleteTopicAsync(long id, bool incltopics)
        {
            using (var httpClient = CreateHttpClient())
            {
                var existingTopic = await GetTopicAsync(id).ConfigureAwait(false);
                if (existingTopic != null)
                {
                    var response = await httpClient.DeleteAsync("topics/" + Uri.EscapeDataString(existingTopic.Name));
                    response.EnsureSuccessStatusCode();
                }
                else
                    throw new ArgumentException($"Topic with Id {id} not found");
            }
        }

        public async Task<Topic> GetTopicAsync(long id)
        {
            using (var httpClient = CreateHttpClient())
            {
                // Cannot get by id, so get all topics and then look it up
                var response = await httpClient.GetAsync("topics").ConfigureAwait(false);
                if (!response.IsSuccessStatusCode)
                    throw await HttpResponseException.Create(response);

                var responseContent = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                var topics = responseContent.FromJson<IEnumerable<Topic>>().ToList();
                return topics.FirstOrDefault(s => s.Id.GetValueOrDefault() == id);
            }
        }

        public async Task<Topic> GetTopicByNameAsync(string name)
        {
            using (var httpClient = CreateHttpClient())
            {
                // Cannot get by id, so get all topics and then look it up
                var response = await httpClient.GetAsync("topics/" + Uri.EscapeDataString(name)).ConfigureAwait(false);
                if (!response.IsSuccessStatusCode)
                {
                    if (response.StatusCode == HttpStatusCode.NotFound)
                        return null;
                    throw await HttpResponseException.Create(response);
                }

                var responseContent = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                return responseContent.FromJson<Topic>();
            }
        }

        public async Task<IEnumerable<Topic>> GetTopicsAsync(string partOfName = null)
        {
            using (var httpClient = CreateHttpClient())
            {
                var url = "topics";
                if (!String.IsNullOrWhiteSpace(partOfName))
                    url += $"?{Uri.EscapeDataString(partOfName)}";

                var response = await httpClient.GetAsync(url).ConfigureAwait(false);
                if (!response.IsSuccessStatusCode)
                {
                    if (response.StatusCode == HttpStatusCode.NotFound)
                        return null;
                    throw await HttpResponseException.Create(response);
                }

                var responseContent = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                return responseContent.FromJson<IEnumerable<Topic>>();
            }
        }

        public async Task<TopicEvent> PublishAsync(string topicName, string eventName = null, DateTime? publicationDateUtc = default(DateTime?), DateTime? deliveryDelayedUntilUtc = null, DateTime? expirationDateUtc = default(DateTime?), string functionalKey = null, int priority = 100, Dictionary<string, string> headers = null, string payload = null)
        {
            using (var httpClient = CreateHttpClient())
            {
                // Query path (route)
                var url = $"publish/{Uri.EscapeDataString(topicName)}";
                if (!String.IsNullOrWhiteSpace(functionalKey))
                    url += $"/{Uri.EscapeDataString(functionalKey)}";

                // Query arguments
                url += $"?priority={priority}"; // first arg, so rest can be appended with &
                if (!String.IsNullOrWhiteSpace(eventName))
                    url += $"&eventName={Uri.EscapeDataString(eventName)}";
                if (publicationDateUtc.HasValue)
                    url += $"&publicationDateUtc={Uri.EscapeDataString(publicationDateUtc.Value.ToString("o"))}";
                if (deliveryDelayedUntilUtc.HasValue)
                    url += $"&deliveryDelayedUntilUtc={Uri.EscapeDataString(deliveryDelayedUntilUtc.Value.ToString("o"))}";
                if (expirationDateUtc.HasValue)
                    url += $"&expirationDateUtc={Uri.EscapeDataString(expirationDateUtc.Value.ToString("o"))}";

                // Payload can be large, so it's put in the body
                var httpRequestMessage = new HttpRequestMessage(HttpMethod.Post, url) { Content = payload != null ? payload.ToStringContent() : null };

                // Add headers (if any) as HTTP-headers to the request
                if (headers != null)
                {
                    headers.AsEnumerable().ToList().ForEach((kvp) =>
                    {
                        if (httpRequestMessage.Headers.Contains(kvp.Key))
                            throw new ArgumentOutOfRangeException("headers", $"The header with key {kvp.Key} is already in use for HTTP-communication");

                        httpRequestMessage.Headers.Add(kvp.Key, kvp.Value);
                    });
                }

                var response = await httpClient.SendAsync(httpRequestMessage).ConfigureAwait(false);
                if (!response.IsSuccessStatusCode)
                    throw await HttpResponseException.Create(response);

                var responseContent = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                return responseContent.FromJson<TopicEvent>();
            }
        }

        public async Task<TopicEvent> PublishAsync<T>(string topicName, string eventName = null, DateTime? publicationDateUtc = default(DateTime?), DateTime? deliveryDelayedUntilUtc = null, DateTime? expirationDateUtc = default(DateTime?), string functionalKey = null, int priority = 100, Dictionary<string, string> headers = null, T payload = null) where T : class
        {
            return await PublishAsync(topicName,
                eventName: eventName,
                publicationDateUtc: publicationDateUtc,
                deliveryDelayedUntilUtc: deliveryDelayedUntilUtc,
                expirationDateUtc: expirationDateUtc,
                functionalKey: functionalKey,
                priority: priority,
                headers: headers,
                payload: payload != null ? payload.ToJson() : null);
        }
    }
}
