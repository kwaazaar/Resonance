using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Resonance.Models;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Resonance.Web.Controllers
{
    /// <summary>
    /// Publishing events
    /// </summary>
    [Route("publish")]
    public class PublishController : Controller
    {
        private IEventPublisherAsync _publisher;
        private ILogger<PublishController> _logger;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="publisher"></param>
        /// <param name="logger"></param>
        public PublishController(IEventPublisherAsync publisher, ILogger<PublishController> logger)
        {
            _publisher = publisher;
            _logger = logger;
        }

        /// <summary>
        /// Publish a new event to a topic.
        /// </summary>
        /// <param name="name">Name of the topic</param>
        /// <param name="functionalKey">Optional: functionalkey for the event.
        /// NB: Functionalkey is required if subscription need to process it in ordered fashion.
        /// Therefore, if a functional key exists, it's best to specify it here.</param>
        /// <param name="eventName">Optional: Name of the event (eg: OrderCreated)</param>
        /// <param name="publicationDateUtc">Optional: Publicationdate (UTC) for the event (default: systemdate).</param>
        /// <param name="expirationDateUtc">Optional: Expirationdate (UTC) for the event (default: never expires).</param>
        /// <param name="priority">Optional: Priority for the event (default: 100, 1 means higher priority than 100).</param>
        /// <param name="payload">Optional: The actual payload for the event, usually formatted in json or XML, but could be base64-encoded binary data as well.
        /// Payload can be specified in the query which can be convenient when using the GET-method. However, when not passed in the query, the complete body
        /// of the request (POST) is considered the payload.</param>
        /// <returns>The published TopicEvent</returns>
        /// <remarks>To pass headers with the event, add them as HTTP-headers. To pass an eventname, just add it as a header.</remarks>
        [HttpGet("{name}/{functionalKey?}")]
        [HttpPost("{name}/{functionalKey?}")]
        [ProducesResponseType(typeof(TopicEvent), 200)]
        public async Task<IActionResult> Publish([FromRoute]string name, [FromRoute]string functionalKey = null,
            [FromQuery]string eventName = null,
            [FromQuery]DateTime? publicationDateUtc = null, [FromQuery]DateTime? expirationDateUtc = null, [FromQuery] int priority = 100, [FromQuery]string payload = null)
        {
            if (String.IsNullOrWhiteSpace(name))
                return BadRequest("No topicname specified");

            try
            {
                // Set up headers
                var headers = new Dictionary<string, string>();
                foreach (var header in Request.Headers)
                    headers.Add(header.Key, header.Value.ToString()); // Header may appear multiple times

                // Get payload, either from url else consider entire body the payload
                if (payload == null)
                {
                    payload = await new StreamReader(Request.Body).ReadToEndAsync();
                    if (payload != null && payload.Length == 0)
                        payload = null; // Otherwise we would store an empty string
                }

                // Now publish
                var te = await _publisher.PublishAsync(topicName: name,
                    eventName: eventName,
                    publicationDateUtc: publicationDateUtc, 
                    expirationDateUtc: expirationDateUtc, 
                    functionalKey: functionalKey, 
                    priority: priority,
                    headers: headers, 
                    payload: payload);
                return Ok(te);
            }
            catch (ArgumentException argEx)
            {
                return BadRequest(argEx.Message);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.ToString());
                return StatusCode(500);
            }
        }
    }
}
