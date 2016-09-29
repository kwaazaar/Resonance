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
    [Route("topics")]
    public class TopicController : Controller
    {
        private IEventPublisher _publisher;
        private ILogger<TopicController> _logger;

        public TopicController(IEventPublisher publisher, ILogger<TopicController> logger)
        {
            _publisher = publisher;
            _logger = logger;
        }

        [HttpGet()]
        public IActionResult GetTopics(string partOfName)
        {
            try
            {
                return Ok(_publisher.GetTopics(partOfName));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.ToString());
                return StatusCode(500);
            }
        }

        [HttpGet("{name}")]
        public IActionResult GetTopic(string name)
        {
            try
            {
                var topic = _publisher.GetTopicByName(name);
                if (topic != null)
                    return Ok(topic);
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
        public IActionResult Post([FromBody]Topic topic)
        {
            if (topic != null && TryValidateModel(topic))
            {
                try
                {
                    return Ok(_publisher.AddOrUpdateTopic(topic));
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
        public IActionResult Put(string name, [FromBody]Topic topic)
        {
            if (topic != null && TryValidateModel(topic))
            {
                try
                {
                    var existingTopic = _publisher.GetTopicByName(name);
                    if (existingTopic == null)
                        return NotFound($"No topic found with name {name}");
                    if (existingTopic.Id != topic.Id)
                        return BadRequest("Id of topic cannot be modified");

                    return Ok(_publisher.AddOrUpdateTopic(topic));
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
        public IActionResult Delete(string name, bool? includingSubscriptions)
        {
            if (String.IsNullOrWhiteSpace(name))
                return BadRequest("Topic name not provided");

            try
            {
                var existingTopic = _publisher.GetTopicByName(name);
                if (existingTopic == null)
                    return NotFound($"No topic found with name {name}");
                else
                {
                    _publisher.DeleteTopic(existingTopic.Id.Value, includingSubscriptions.GetValueOrDefault(true));
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
