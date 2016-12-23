using System;
using System.Net.Http;
using System.Threading.Tasks;

namespace Resonance.APIClient
{
    public abstract class APIClientBase
    {
        protected readonly Uri _resonanceApiBaseAddress;
        protected readonly TimeSpan _housekeepingRequestTimeout;
        protected readonly static HttpClient _httpClient;

        static APIClientBase()
        {
            _httpClient = new HttpClient();
        }

        public APIClientBase(Uri resonanceApiBaseAddress, TimeSpan timeout, TimeSpan housekeepingRequestTimeout)
        {
            _resonanceApiBaseAddress = resonanceApiBaseAddress;

            // Overwrite HttpClient-settings if nessecary (will only happen once)
            if (_httpClient.BaseAddress == null || _httpClient.BaseAddress != resonanceApiBaseAddress)
                _httpClient.BaseAddress = resonanceApiBaseAddress;

            if (_httpClient.Timeout != timeout)
                _httpClient.Timeout = timeout;

            _housekeepingRequestTimeout = housekeepingRequestTimeout;
        }

        protected async Task CheckConnectionAsync()
        {
            var response = await _httpClient.GetAsync("topics").ConfigureAwait(false); // Try to retrieve all topics
            if (!response.IsSuccessStatusCode)
            {
                throw new InvalidOperationException($"Failed to connect to Resonance API on {_resonanceApiBaseAddress}: Response: HTTP {(int)response.StatusCode}.");
            }
        }

        public virtual async Task PerformHouseKeepingTasksAsync()
        {
            // Custom httpclient for housekeeping, since it needs a different timeout
            using (var httpClient = new HttpClient())
            {
                httpClient.BaseAddress = _resonanceApiBaseAddress;
                httpClient.Timeout = _housekeepingRequestTimeout;
                var response = await httpClient.GetAsync($"maintenance/housekeeping").ConfigureAwait(false);
                if (!response.IsSuccessStatusCode)
                    throw await HttpResponseException.Create(response);
            }
        }
    }
}
