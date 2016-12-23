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
    [Route("maintenance")]
    public class MaintenanceController : Controller
    {
        private IEventConsumerAsync _consumer;
        private ILogger<MaintenanceController> _logger;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="consumer"></param>
        /// <param name="logger"></param>
        public MaintenanceController(IEventConsumerAsync consumer, ILogger<MaintenanceController> logger)
        {
            _consumer = consumer;
            _logger = logger;
        }

        /// <summary>
        /// Initiates the Housekeeping-process
        /// </summary>
        [HttpGet("housekeeping")]
        public async Task<IActionResult> PerformHouseKeepingTasks()
        {
            try
            {
                await _consumer.PerformHouseKeepingTasksAsync().ConfigureAwait(false);
                return Ok();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.ToString());
                return StatusCode(500);
            }
        }
    }
}
