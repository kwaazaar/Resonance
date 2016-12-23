using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;

namespace Resonance.APIClient
{
    /// <summary>
    /// Simplified list of statuscodes in regard to HttpStatusCodes
    /// </summary>
    public enum ResponseStatus : int
    {
        /// <summary>
        /// Not used
        /// </summary>
        Unspecified = 0,
        /// <summary>
        /// Request contents incorrect
        /// </summary>
        BadRequest = 400,
        /// <summary>
        /// Error/timeout occurred on server, try again
        /// </summary>
        ServerError = 500,
        /// <summary>
        /// Network error/timeout, try again
        /// </summary>
        ConnectionError = 600,
    }

    public class HttpResponseException : HttpRequestException
    {
        public HttpStatusCode HttpStatusCode { get; protected set; }
        public ResponseStatus ResponseStatus { get; protected set; }

        /// <summary>
        /// Dont use this constructor. Use the static Create-method instead!
        /// </summary>
        public HttpResponseException()
            : base()
        {
            this.HttpStatusCode = HttpStatusCode.InternalServerError;
            this.ResponseStatus = ResponseStatus.Unspecified;
        }

        /// <summary>
        /// Internal constructor
        /// </summary>
        /// <param name="message"></param>
        /// <param name="httpStatusCode"></param>
        /// <param name="responseStatus"></param>
        protected HttpResponseException(string message, HttpStatusCode httpStatusCode, ResponseStatus responseStatus)
            : base(message)
        {
            this.HttpStatusCode = httpStatusCode;
            this.ResponseStatus = responseStatus;
        }

        /// <summary>
        /// Creates a new HttpResponseException
        /// </summary>
        /// <param name="response">HttpResponseMessage</param>
        /// <returns></returns>
        public static async Task<HttpResponseException> Create(HttpResponseMessage response)
        {
            if (response == null) throw new ArgumentNullException("response");
            if (response.IsSuccessStatusCode) return null;

            var responseContent = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
            var exMessage = $"{response.ReasonPhrase}: {responseContent}";

            var responseStatus = ResponseStatus.Unspecified;

            switch (response.StatusCode)
            {
                case HttpStatusCode.RequestedRangeNotSatisfiable:
                case HttpStatusCode.PreconditionFailed:
                case HttpStatusCode.BadRequest:
                case HttpStatusCode.Gone:
                case HttpStatusCode.NotFound: // If notfound is not an error, it must have been checked already 
                    responseStatus = ResponseStatus.BadRequest;
                    break;
                case HttpStatusCode.ServiceUnavailable:
                case HttpStatusCode.InternalServerError:
                    responseStatus = ResponseStatus.ServerError;
                    break;
                default:
                    responseStatus = ResponseStatus.ConnectionError;
                    break;
            }

            return new HttpResponseException(exMessage, response.StatusCode, responseStatus);
        }
    }
}
