using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Resonance.Repo;
using Resonance.Models;

// For more information on enabling Web API for empty projects, visit http://go.microsoft.com/fwlink/?LinkID=397860

namespace Resonance.Api.Controllers
{
    [Route("subscriptions")]
    public class SubscriptionController : Controller
    {
        private IEventConsumer _consumer;

        public SubscriptionController(IEventConsumer consumer)
        {
            _consumer = consumer;
        }

        [HttpGet]
        public IEnumerable<Subscription> Get()
        {
            return _consumer.GetSubscriptions();
        }

        // GET api/values/5
        [HttpGet("{name}")]
        public IActionResult Get(string name)
        {
            var sub = _consumer.GetSubscriptionByName(name);
            if (sub != null)
                return Ok(sub);
            else
                return NotFound();
        }

        // POST api/values
        [HttpPost]
        public IActionResult Post([FromBody]Subscription sub)
        {
            if (sub != null && TryValidateModel(sub))
            {
                try
                {
                    return Ok(_consumer.AddOrUpdateSubscription(sub));
                }
                catch (Exception ex)
                {
                    return BadRequest(ex.Message);
                }
            }
            else
                return BadRequest(ModelState);
        }

        [HttpPut("{name}")]
        public IActionResult Put(string name, [FromBody]Subscription sub)
        {
            if (sub != null && TryValidateModel(sub))
            {
                var existingSub = _consumer.GetSubscriptionByName(name);
                if (existingSub == null)
                    return NotFound($"No subscription found with name {name}");
                if (existingSub.Id != sub.Id)
                    return BadRequest("Id of subscription cannot be modified");

                try
                {
                    return Ok(_consumer.AddOrUpdateSubscription(sub));
                }
                catch (Exception ex)
                {
                    return BadRequest(ex.Message);
                }
            }
            else
                return BadRequest(ModelState);
        }

        [HttpDelete("{name}")]
        public IActionResult Delete(string name)
        {
            if (String.IsNullOrWhiteSpace(name))
                return BadRequest("Subscription name not provided");

            var existingSub = _consumer.GetSubscriptionByName(name);
            if (existingSub == null)
                return NotFound($"No subscription found with name {name}");
            else
            {
                _consumer.DeleteSubscription(existingSub.Id);
                return Ok();
            }
        }

        [HttpGet("{name}/consume")]
        public IActionResult Consume(string name, int? visibilityTimeout)
        {
            try
            {
                var ce = _consumer.ConsumeNext(name, visibilityTimeout.GetValueOrDefault(120), 1).FirstOrDefault();
                if (ce == null)
                    return NotFound();
                else
                    return Ok(ce);
            }
            catch (ArgumentException)
            {
                return BadRequest($"Subscription with name {name} not found");
            }
        }
    }
}
