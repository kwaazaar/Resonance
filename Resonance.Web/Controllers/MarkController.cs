using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Resonance.Models;
using Microsoft.Extensions.Logging;

namespace Resonance.Web.Controllers
{
    /// <summary>
    /// Mark events consumed or failed
    /// </summary>
    [Route("mark")]
    public class MarkController : Controller
    {
        private IEventConsumerAsync _consumer;
        private ILogger<MarkController> _logger;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="consumer"></param>
        /// <param name="logger"></param>
        public MarkController(IEventConsumerAsync consumer, ILogger<MarkController> logger)
        {
            _consumer = consumer;
            _logger = logger;
        }

        /// <summary>
        /// Mark the specified event as consumed.
        /// </summary>
        /// <param name="id">The id of the event</param>
        /// <param name="deliveryKey">The deliverykey that was provided when consuming the event</param>
        /// <returns></returns>
        /// <remarks>Make sure to mark the event as consumed BEFORE its visibility timeout expires.
        /// Otherwise another subscriber (or thread) may already be consuming the event again.
        /// If the event has not yet been consumed again, marking it complete will be allowed.</remarks>
        [HttpGet]
        [HttpPost]
        [Route("{id}/{deliverykey}/consumed")]
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

        /// <summary>
        /// Mark the specified event as failed.
        /// </summary>
        /// <param name="id">The id of the event</param>
        /// <param name="deliveryKey">The deliverykey that was provided when consuming the event</param>
        /// <param name="reason">Optional: Reason why it should be marked failed. Although optional,
        /// any information may help troubleshooting later when trying to find out why events were not consumed successfully.</param>
        /// <returns></returns>
        /// <remarks>Make sure to mark the event as failed BEFORE its visibility timeout expires.
        /// Otherwise another subscriber (or thread) may already be consuming the event again.
        /// If the event has not yet been consumed again, marking it failed will be allowed.</remarks>
        [HttpGet]
        [HttpPost]
        [Route("{id}/{deliverykey}/failed")]
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
