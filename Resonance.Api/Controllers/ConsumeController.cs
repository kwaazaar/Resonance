using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Resonance.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Resonance.Api.Controllers
{
    [Route("consume")]
    public class ConsumeController : Controller
    {
        private IEventConsumer _consumer;
        private ILogger<ConsumeController> _logger;

        public ConsumeController(IEventConsumer consumer, ILogger<ConsumeController> logger)
        {
            _consumer = consumer;
            _logger = logger;
        }

        [HttpGet("{name}")]
        [ProducesResponseType(typeof(IEnumerable<ConsumableEvent>), 200)]
        public async Task<IActionResult> ConsumeNext(string name, int? visibilityTimeout, int? maxCount)
        {
            try
            {
                var ces = await _consumer.ConsumeNext(name, visibilityTimeout.GetValueOrDefault(120), maxCount.GetValueOrDefault(1));
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
