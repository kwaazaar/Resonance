using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Resonance.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Resonance.Web.Controllers
{
    /// <summary>
    /// Event consumption
    /// </summary>
    [Route("consume")]
    public class ConsumeController : Controller
    {
        private IEventConsumer _consumer;
        private ILogger<ConsumeController> _logger;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="consumer"></param>
        /// <param name="logger"></param>
        public ConsumeController(IEventConsumer consumer, ILogger<ConsumeController> logger)
        {
            _consumer = consumer;
            _logger = logger;
        }

        /// <summary>
        /// Consumes the next available event for the specified subscription
        /// </summary>
        /// <param name="name">Name of the subscription to consume</param>
        /// <param name="visibilityTimeout">Optional: visibility-timeout in seconds (default: 120).</param>
        /// <param name="maxCount">Optional: number of events to consume at once (default: 1).</param>
        /// <returns>List of ConsumableEvent (200) or NotFound (404) when there are no events to consume.</returns>
        [HttpGet("{name}")]
        [ProducesResponseType(typeof(IEnumerable<ConsumableEvent>), 200)]
        public async Task<IActionResult> ConsumeNext(string name, int? visibilityTimeout, int? maxCount)
        {
            try
            {
                var ces = await _consumer.ConsumeNextAsync(name, visibilityTimeout.GetValueOrDefault(120), maxCount.GetValueOrDefault(1));
                if (ces.Count() == 0)
                    return NotFound();
                else
                    return Ok(ces);
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
