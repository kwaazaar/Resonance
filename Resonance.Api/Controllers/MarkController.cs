using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Resonance.Models;
using Microsoft.Extensions.Logging;

namespace Resonance.Api.Controllers
{
    [Route("mark")]
    public class MarkController : Controller
    {
        private IEventConsumer _consumer;
        private ILogger<MarkController> _logger;

        public MarkController(IEventConsumer consumer, ILogger<MarkController> logger)
        {
            _consumer = consumer;
            _logger = logger;
        }

        [HttpGet]
        [Route("consumed/{id}/{deliverykey}")]
        public async Task<IActionResult> MarkConsumed(long id, string deliveryKey)
        {
            if ((id == 0) || String.IsNullOrWhiteSpace(deliveryKey))
                return BadRequest("id and deliverykey must be specified");

            try
            {
                await _consumer.MarkConsumedAsync(id, deliveryKey);
                return Ok();
            }
            catch (ArgumentException argEx)
            {
                return NotFound(argEx.Message);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.ToString());
                return StatusCode(500);
            }
        }

        [HttpGet]
        [Route("failed/{id}/{deliverykey}")]
        public async Task<IActionResult> MarkFailed(long id, string deliveryKey, string reason = null)
        {
            if ((id == 0) || String.IsNullOrWhiteSpace(deliveryKey))
                return BadRequest("id and deliverykey must be specified");

            try
            {
                await _consumer.MarkFailedAsync(id, deliveryKey, Reason.Other(reason ?? string.Empty));
                return Ok();
            }
            catch (ArgumentException argEx)
            {
                return NotFound(argEx.Message);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.ToString());
                return StatusCode(500);
            }
        }
    }
}
