using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Resonance.Repo;
using Resonance.Models;
using Microsoft.Extensions.Logging;

namespace Resonance.Api.Controllers
{
    [Route("subscriptions")]
    public class SubscriptionController : Controller
    {
        private IEventConsumer _consumer;
        private ILogger<SubscriptionController> _logger;

        public SubscriptionController(IEventConsumer consumer, ILogger<SubscriptionController> logger)
        {
            _consumer = consumer;
            _logger = logger;
        }

        [HttpGet]
        [ProducesResponseType(typeof(IEnumerable<Subscription>), 200)]
        public async Task<IActionResult> Get()
        {
            try
            {
                return Ok(await _consumer.GetSubscriptions());
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.ToString());
                return StatusCode(500);
            }
        }

        [HttpGet("{name}")]
        [ProducesResponseType(typeof(Subscription), 200)]
        public async Task<IActionResult> Get(string name)
        {
            try
            {
                var sub = await _consumer.GetSubscriptionByName(name);
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

        [HttpPost]
        [ProducesResponseType(typeof(Subscription), 200)]
        public async Task<IActionResult> Post([FromBody]Subscription sub)
        {
            if (sub != null && TryValidateModel(sub))
            {
                try
                {
                    return Ok(await _consumer.AddOrUpdateSubscription(sub));
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

        [HttpPut("{name}")]
        [ProducesResponseType(typeof(Subscription), 200)]
        public async Task<IActionResult> Put(string name, [FromBody]Subscription sub)
        {
            if (sub != null && TryValidateModel(sub))
            {
                try
                {
                    var existingSub = await _consumer.GetSubscriptionByName(name);
                    if (existingSub == null)
                        return NotFound($"No subscription found with name {name}");
                    if (existingSub.Id != sub.Id)
                        return BadRequest("Id of subscription cannot be modified");

                    return Ok(await _consumer.AddOrUpdateSubscription(sub));
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

        [HttpDelete("{name}")]
        public async Task<IActionResult> Delete(string name)
        {
            if (String.IsNullOrWhiteSpace(name))
                return BadRequest("Subscription name not provided");

            try
            {
                var existingSub = await _consumer.GetSubscriptionByName(name);
                if (existingSub == null)
                    return NotFound($"No subscription found with name {name}");
                else
                {
                    await _consumer.DeleteSubscription(existingSub.Id.Value);
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
