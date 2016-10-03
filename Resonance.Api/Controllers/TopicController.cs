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
        [ProducesResponseType(typeof(IEnumerable<Topic>), 200)]
        public async Task<IActionResult> GetTopics(string partOfName)
        {
            try
            {
                return Ok(await _publisher.GetTopicsAsync(partOfName));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.ToString());
                return StatusCode(500);
            }
        }

        [HttpGet("{name}")]
        [ProducesResponseType(typeof(Topic), 200)]
        public async Task<IActionResult> GetTopic(string name)
        {
            try
            {
                var topic = await _publisher.GetTopicByNameAsync(name);
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
        [ProducesResponseType(typeof(Topic), 200)]
        public async Task<IActionResult> Post([FromBody]Topic topic)
        {
            if (topic != null && TryValidateModel(topic))
            {
                try
                {
                    return Ok(await _publisher.AddOrUpdateTopicAsync(topic));
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
        [ProducesResponseType(typeof(Topic), 200)]
        public async Task<IActionResult> Put(string name, [FromBody]Topic topic)
        {
            if (topic != null && TryValidateModel(topic))
            {
                try
                {
                    var existingTopic = await _publisher.GetTopicByNameAsync(name);
                    if (existingTopic == null)
                        return NotFound($"No topic found with name {name}");
                    if (existingTopic.Id != topic.Id)
                        return BadRequest("Id of topic cannot be modified");

                    return Ok(await _publisher.AddOrUpdateTopicAsync(topic));
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
        public async Task<IActionResult> Delete(string name, bool? includingSubscriptions)
        {
            if (String.IsNullOrWhiteSpace(name))
                return BadRequest("Topic name not provided");

            try
            {
                var existingTopic = await _publisher.GetTopicByNameAsync(name);
                if (existingTopic == null)
                    return NotFound($"No topic found with name {name}");
                else
                {
                    await _publisher.DeleteTopicAsync(existingTopic.Id.Value, includingSubscriptions.GetValueOrDefault(true));
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
