using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;

namespace Resonance.APIClient
{
    public abstract class APIClientBase
    {
        protected readonly Uri _resonanceApiBaseAddress;
        protected readonly HttpMessageHandler _messageHandler;
        protected readonly TimeSpan _timeout;

        public APIClientBase(Uri resonanceApiBaseAddress, HttpMessageHandler messageHandler, TimeSpan timeout)
        {
            _resonanceApiBaseAddress = resonanceApiBaseAddress;
            _messageHandler = messageHandler;
            _timeout = timeout;
        }

        protected async Task CheckConnectionAsync()
        {
            using (var httpClient = CreateHttpClient())
            {
                var response = await httpClient.GetAsync("topics").ConfigureAwait(false); // Try to retrieve all topics
                if (!response.IsSuccessStatusCode)
                {
                    throw new InvalidOperationException($"Failed to connect to Resonance API on {_resonanceApiBaseAddress}: Response: HTTP {(int)response.StatusCode}.");
                }
            }
        }

        protected HttpClient CreateHttpClient()
        {
            var httpClient = new HttpClient(_messageHandler, false)
            {
                BaseAddress = _resonanceApiBaseAddress,
                Timeout = _timeout,
            };
            return httpClient;
        }

        public virtual async Task PerformHouseKeepingTasksAsync()
        {
            using (var httpClient = CreateHttpClient())
            {
                var response = await httpClient.GetAsync($"maintenance/housekeeping").ConfigureAwait(false);
                if (!response.IsSuccessStatusCode)
                    throw await HttpResponseException.Create(response);
            }
        }

    }
}
