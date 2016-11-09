using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Resonance.Repo;
using Resonance.Models;
using Microsoft.Extensions.Logging;

namespace Resonance.Web.Controllers
{
    /// <summary>
    /// Subscription management
    /// </summary>
    [Route("subscriptions")]
    public class SubscriptionController : Controller
    {
        private IEventConsumerAsync _consumer;
        private ILogger<SubscriptionController> _logger;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="consumer"></param>
        /// <param name="logger"></param>
        public SubscriptionController(IEventConsumerAsync consumer, ILogger<SubscriptionController> logger)
        {
            _consumer = consumer;
            _logger = logger;
        }

        /// <summary>
        /// Gets a list of all subscriptions
        /// </summary>
        /// <returns>List of subscriptions</returns>
        [HttpGet]
        [ProducesResponseType(typeof(IEnumerable<Subscription>), 200)]
        public async Task<IActionResult> Get()
        {
            try
            {
                return Ok(await _consumer.GetSubscriptionsAsync());
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.ToString());
                return StatusCode(500);
            }
        }

        /// <summary>
        /// Gets a single subscription
        /// </summary>
        /// <param name="name">Name of the subscription</param>
        /// <returns>The subscription</returns>
        [HttpGet("{name}")]
        [ProducesResponseType(typeof(Subscription), 200)]
        public async Task<IActionResult> Get(string name)
        {
            try
            {
                var sub = await _consumer.GetSubscriptionByNameAsync(name);
                if (sub != null)
                    return Ok(sub);
                else
                    return NotFound();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.ToString());
                return StatusCode(500);
            }
        }

        /// <summary>
        /// Gets subscription statistics
        /// </summary>
        /// <param name="periodStartUtc">Optional: From (UTC)</param>
        /// <param name="periodEndUtc">Optional: Until (UTC)</param>
        /// <returns>The subscription statistics</returns>
        [HttpGet("stats")]
        [ProducesResponseType(typeof(SubscriptionSummary), 200)]
        public async Task<IActionResult> GetStatistics(DateTime? periodStartUtc=null, DateTime? periodEndUtc = null)
        {
            try
            {
                var stats = await _consumer.GetSubscriptionStatisticsAsync(
                    periodStartUtc.GetValueOrDefault(new DateTime(1900,1,1)),
                    periodEndUtc.GetValueOrDefault(DateTime.UtcNow.AddDays(1))); // +1 day just to make sure
                return Ok(stats);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.ToString());
                return StatusCode(500);
            }
        }

        /// <summary>
        /// Adds a new subscription.
        /// </summary>
        /// <param name="sub">The subscription to add. Make sure to add relevant topic subscriptions and to enable those.</param>
        /// <returns>The resulting subscription.</returns>
        [HttpPost]
        [ProducesResponseType(typeof(Subscription), 200)]
        public async Task<IActionResult> Post([FromBody]Subscription sub)
        {
            if (sub != null && TryValidateModel(sub))
            {
                try
                {
                    var existingSubscription = await _consumer.GetSubscriptionByNameAsync(sub.Name);
                    if (existingSubscription != null)
                        return BadRequest("Subscription with this name already exists");

                    if (sub.Id.HasValue)
                    {
                        if (sub.Id.Value == 0)
                            sub.Id = null;
                        else
                            return BadRequest("Id must be null when adding a subscription");
                    }

                    return Ok(await _consumer.AddOrUpdateSubscriptionAsync(sub));
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
            else
                return BadRequest(ModelState);
        }

        /// <summary>
        /// Updates a subscription
        /// </summary>
        /// <param name="name">Name of the subscription to update</param>
        /// <param name="sub">The new defintion of the subscription</param>
        /// <returns>The resulting subscription.</returns>
        [HttpPut("{name}")]
        [ProducesResponseType(typeof(Subscription), 200)]
        public async Task<IActionResult> Put(string name, [FromBody]Subscription sub)
        {
            if (sub != null && TryValidateModel(sub))
            {
                try
                {
                    var existingSub = await _consumer.GetSubscriptionByNameAsync(name);
                    if (existingSub == null)
                        return NotFound($"No subscription found with name {name}");

                    if (sub.Id.HasValue && existingSub.Id.Value != sub.Id.Value)
                        return BadRequest("Id of subscription cannot be modified");

                    sub.Id = existingSub.Id; // Make sure we edit the existing subscription instead of adding a new one

                    return Ok(await _consumer.AddOrUpdateSubscriptionAsync(sub));
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
            else
                return BadRequest(ModelState);
        }

        /// <summary>
        /// Delete a subscription
        /// </summary>
        /// <param name="name">Name of the subscription to delete</param>
        /// <returns></returns>
        [HttpDelete("{name}")]
        public async Task<IActionResult> Delete(string name)
        {
            if (String.IsNullOrWhiteSpace(name))
                return BadRequest("Subscription name not provided");

            try
            {
                var existingSub = await _consumer.GetSubscriptionByNameAsync(name);
                if (existingSub == null)
                    return NotFound($"No subscription found with name {name}");
                else
                {
                    await _consumer.DeleteSubscriptionAsync(existingSub.Id.Value);
                    return Ok();
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.ToString());
                return StatusCode(500);
            }
        }
    }
}
