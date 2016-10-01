using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Resonance.Models;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Resonance.Api.Controllers
{
    [Route("publish")]
    public class PublishController : Controller
    {
        private IEventPublisher _publisher;
        private ILogger<PublishController> _logger;

        public PublishController(IEventPublisher publisher, ILogger<PublishController> logger)
        {
            _publisher = publisher;
            _logger = logger;
        }

        [HttpGet("{name}/{functionalKey?}")]
        [HttpPost("{name}/{functionalKey?}")]
        [ProducesResponseType(typeof(TopicEvent), 200)]
        public async Task<IActionResult> Publish([FromRoute]string name, [FromRoute]string functionalKey = null,
            [FromQuery]DateTime? publicationDateUtc = null, [FromQuery]DateTime? expirationDateUtc = null, [FromQuery]string payload = null)
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
                    payload = await new StreamReader(Request.Body).ReadToEndAsync();

                // Now publish
                var te = await _publisher.Publish(topicName: name, publicationDateUtc: publicationDateUtc, expirationDateUtc: expirationDateUtc, functionalKey: functionalKey, headers: headers, payload: payload);
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
