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
    /// Topic Management
    /// </summary>
    [Route("topics")]
    public class TopicController : Controller
    {
        private IEventPublisherAsync _publisher;
        private ILogger<TopicController> _logger;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="publisher"></param>
        /// <param name="logger"></param>
        public TopicController(IEventPublisherAsync publisher, ILogger<TopicController> logger)
        {
            _publisher = publisher;
            _logger = logger;
        }

        /// <summary>
        /// Gets a list of topics
        /// </summary>
        /// <param name="partOfName">Optional: part of the name of the topic</param>
        /// <returns>List of topics</returns>
        [HttpGet()]
        [ProducesResponseType(typeof(IEnumerable<Topic>), 200)]
        public async Task<IActionResult> GetTopics(string partOfName = null)
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

        /// <summary>
        /// Gets a topic
        /// </summary>
        /// <param name="name">Name of the topic</param>
        /// <returns>The topic</returns>
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

        /// <summary>
        /// Adds a new topic
        /// </summary>
        /// <param name="topic">Topic to add</param>
        /// <returns>Resulting topic</returns>
        [HttpPost]
        [ProducesResponseType(typeof(Topic), 200)]
        public async Task<IActionResult> Post([FromBody]Topic topic)
        {
            if (topic != null && TryValidateModel(topic))
            {
                try
                {
                    // Name must be unique
                    var topicWithNewName = await _publisher.GetTopicByNameAsync(topic.Name);
                    if (topicWithNewName != null)
                        return BadRequest("Topic with this name already exists");

                    if (topic.Id.HasValue)
                    {
                        if (topic.Id.Value == 0) // Probably using SwaggerUI, so lets be forgiving
                            topic.Id = null;
                        else
                            return BadRequest("Id must be null when adding a topic");
                    }

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

        /// <summary>
        /// Updates an existing topic
        /// </summary>
        /// <param name="name">Name of the existing topic</param>
        /// <param name="topic">Updated version of the topic</param>
        /// <returns>Resulting topic</returns>
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

                    // The new version of the topic cannot have a (different) id
                    if (topic.Id.HasValue && existingTopic.Id.Value != topic.Id.Value)
                        return BadRequest("Id of topic cannot be modified");

                    // Name must be unique
                    var topicWithNewName = await _publisher.GetTopicByNameAsync(topic.Name);
                    if (topicWithNewName != null)
                        return BadRequest("Topic with this name already exists");

                    // Copy the id to make sure we actually UPDATE the topic, instead of adding a new one
                    topic.Id = existingTopic.Id;

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

        /// <summary>
        /// Delete an existing topic
        /// </summary>
        /// <param name="name">Name of the topic</param>
        /// <param name="includingSubscriptions">Optional: Specifies whether to delete its subscriptions as well (default: true).</param>
        /// <returns></returns>
        [HttpDelete("{name}")]
        public async Task<IActionResult> Delete(string name, bool? includingSubscriptions = true)
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
