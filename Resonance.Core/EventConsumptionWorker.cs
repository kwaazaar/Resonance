using Resonance.Models;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Resonance
{
    /// <summary>
    /// Base-class for polling and processing workitems
    /// </summary>
    public class EventConsumptionWorker : IDisposable
    {
        private Task _internalTask;
        private CancellationTokenSource _cancellationToken;
        private int _attempts;
        private DateTime? _suspendedUntilUtc = null;
        private DateTime? _lastHouseKeepingRun = null;
        private object _suspendTimeoutLock = new object();
        private readonly int _minDelayInMs;
        private readonly int _maxDelayInMs;
        private readonly int _batchSize;
        private readonly int _housekeepingIntervalMin;

        private readonly IEventConsumerAsync _eventConsumer;
        private readonly ILogger _logger;
        private readonly string _subscriptionName;
        private readonly Func<ConsumableEvent, Task<ConsumeResult>> _consumeAction;
        private readonly int _visibilityTimeout;

        /// <summary>
        /// Instantiates a PollingTask
        /// </summary>
        /// <param name="subscriptionName"></param>
        /// <param name="minBackOffDelayInMs">Minimum backoff delay in milliseconds. If 0, then processing will use up to 100% CPU!</param>
        /// <param name="maxBackOffDelayInMs">Maximum backoff delay in milliseconds. Backoff-delay will increment exponentially up until this value.</param>
        /// <param name="batchSize">Number of events to process in parallel (> 1 can result in slower processing when ordered delivery is used)</param>
        /// <param name="visibilityTimeout">Number of seconds the business event must be locked</param>
        /// <param name="eventConsumer">EventConsumer instance to use</param>
        /// <param name="logger">ILogger to use for logging purposes</param>
        /// <param name="housekeepingIntervalMin">Interval between housekeeping intervals. Set to 0 to disable housekeeping.</param>
        /// <param name="consumeAction">Action that must be invoked for each event. Make sure it is thread-safe when parallelExecution is enabled!</param>
        public EventConsumptionWorker(IEventConsumerAsync eventConsumer, string subscriptionName,
            Func<ConsumableEvent, Task<ConsumeResult>> consumeAction, int visibilityTimeout = 60,
            ILogger logger = null,
            int minBackOffDelayInMs = 1, int maxBackOffDelayInMs = 60000, int batchSize = 1, int housekeepingIntervalMin = 5)
        {
            if (maxBackOffDelayInMs < minBackOffDelayInMs) throw new ArgumentOutOfRangeException("maxBackOffDelayInSeconds", "maxBackOffDelayInSeconds must be greater than minBackOffDelay");

            this._minDelayInMs = minBackOffDelayInMs;
            this._maxDelayInMs = maxBackOffDelayInMs;
            this._cancellationToken = new CancellationTokenSource();
            this._batchSize = batchSize;
            this._housekeepingIntervalMin = housekeepingIntervalMin;

            this._eventConsumer = eventConsumer;
            this._logger = logger;
            this._subscriptionName = subscriptionName;
            this._consumeAction = consumeAction;
            this._visibilityTimeout = visibilityTimeout;
        }

        /// <summary>
        /// Temporary suspend polling for the specified timeout.
        /// </summary>
        /// <param name="timeout"></param>
        /// <remarks>When for some reason items cannot be processed for a while, polling can be suspended by invoking this method. If the proces was already suspended, the timeout will be added!</remarks>
        /// <returns>Time (UTC) until suspended.</returns>
        protected DateTime Suspend(TimeSpan timeout)
        {
            var suspendedUntilUtc = _suspendedUntilUtc;

            if (!suspendedUntilUtc.HasValue) // if set, we're already suspended
            {
                lock (_suspendTimeoutLock)
                {
                    suspendedUntilUtc = DateTime.UtcNow.Add(timeout);
                    _suspendedUntilUtc = suspendedUntilUtc;
                }
            }

            return suspendedUntilUtc.Value;
        }

        /// <summary>
        /// Sleeps/blocks the thread for a backoff-delay.
        /// </summary>
        /// <remarks>Override GetBackOffDelay to implement custom logic to determine the delay duration.</remarks>
        protected void BackOff()
        {
            var backoffDelay = this.GetBackOffDelay(this._attempts, this._minDelayInMs, this._maxDelayInMs);
            if (backoffDelay.TotalMilliseconds > 0)
            {
                if (_housekeepingIntervalMin > 0 // Enabled
                    && (backoffDelay.TotalMilliseconds == this._maxDelayInMs) // Currently idle/backing off for maxDelay
                    && _lastHouseKeepingRun.GetValueOrDefault() < DateTime.UtcNow.AddMinutes(-1 * _housekeepingIntervalMin)) // More than x minutes passed since last housekeeping run
                {
                    try
                    {
                        LogTrace("Running housekeeping tasks...");
                        _eventConsumer.PerformHouseKeepingTasksAsync().GetAwaiter().GetResult(); // No await, blocking is no problem here.
                        LogTrace("Housekeeping tasks done.");
                        _lastHouseKeepingRun = DateTime.UtcNow;
                    }
                    catch (Exception ex)
                    {
                        LogWarning("Housekeeping tasks have failed. Worker will continue, but this may indicate a serious problem. Details: {0}", ex);
                        _lastHouseKeepingRun = DateTime.UtcNow; // Even though it failed, we still set it, because we don't want it to retry too soon after this error.
                    }
                }

                LogTrace("Backing off for {backoffDelay}", backoffDelay);

                try
                {
                    Task.WaitAll(new Task[] { Task.Delay(backoffDelay) }, _cancellationToken.Token);
                }
                catch (OperationCanceledException) { }
            }
        }

        /// <summary>
        /// Indicates whether polling is running (has been started)
        /// </summary>
        /// <returns></returns>
        public virtual bool IsRunning()
        {
            return (this._internalTask != null
                && this._internalTask.Status == TaskStatus.Running);
        }

        /// <summary>
        /// Executes a workitem and calls Complete on success (no exception and true returned by Execute) or Failed otherwise.
        /// </summary>
        /// <param name="workItem"></param>
        private async Task ExecuteWork(ConsumableEvent workItem)
        {
            bool success = false;
            Exception execEx = null;

            Stopwatch w = new Stopwatch();
            w.Start();
            try
            {
                success = await this.Execute(workItem).ConfigureAwait(false);
                w.Stop();
            }
            catch (Exception ex)
            {
                w.Stop();
                success = false;
                execEx = ex;
            }

            if (success)
                this.Completed(workItem, w.Elapsed);
            else
                this.Failed(workItem, execEx);
        }

        /// <summary>
        /// Determines the backoff timeout/delay.
        /// </summary>
        /// <remarks>By default the delay increases exponentially of every failed attempt</remarks>
        /// <returns>TimeSpan</returns>
        protected virtual TimeSpan GetBackOffDelay(int attempts, int minDelayInMs, int maxDelayInMs)
        {
            double delayInMs = ((0.5 * (Math.Pow(2, (double)attempts) - 1)) * 1000d) + (double)minDelayInMs;
            if (delayInMs > maxDelayInMs)
                delayInMs = maxDelayInMs;

            var milliseconds = Convert.ToInt64(delayInMs);
            TimeSpan timeout = TimeSpan.FromMilliseconds((double)milliseconds);
            return timeout;
        }

        /// <summary>
        /// Starts the processing of workitems.
        /// </summary>
        public void Start()
        {
            if (this._internalTask != null)
                throw new InvalidOperationException("Task is already running or has not yet finished stopping");

            this._cancellationToken = new CancellationTokenSource();
            this._internalTask = Task.Factory.StartNew(() =>
            {
                while (!this._cancellationToken.IsCancellationRequested)
                {
                    var suspendedUntilUtc = _suspendedUntilUtc;
                    if (!suspendedUntilUtc.HasValue || (suspendedUntilUtc.Value < DateTime.UtcNow))
                        this.TryExecuteWorkItems().GetAwaiter().GetResult(); // Execute synchroniously
                    else
                    {
                        LogWarning("Processing is temporary suspended until {suspendedUntilUtc} (UTC).", suspendedUntilUtc.Value);
                        var suspendDuration = suspendedUntilUtc.Value - DateTime.UtcNow;
                        try
                        {
                            // Suspend processing
                            Task.Delay(suspendDuration, _cancellationToken.Token).GetAwaiter().GetResult(); // Synchroniously, but by using Task.Delay, the cancellationtoken can be passed.
                            LogWarning("Processing has been suspended for {seconds} seconds and will now continue.", suspendDuration.TotalSeconds);
                        }
                        catch (OperationCanceledException) { } // Because the delay was cancelled through the _cancellationToken

                        // Clear _suspendedUntilUtc to make sure we continue next time in this look
                        lock (_suspendTimeoutLock)
                            _suspendedUntilUtc = null;
                    }
                }
            }, TaskCreationOptions.LongRunning); // CancellationToken is not passed: it's checked internally (while-loop) for gracefull cancellation
        }

        /// <summary>
        /// Gets and executes workitems
        /// </summary>
        /// <remarks>Invokes PollingException if the TryGetWork or ExecuteWork throw an exception.</remarks>
        private async Task TryExecuteWorkItems()
        {
            try
            {
                var workitems = await this.TryGetWork(this._batchSize).ConfigureAwait(false);
                if (workitems == null || (!workitems.Any<ConsumableEvent>())) // No result or empty list
                {
                    this._attempts++;
                    if (this._attempts == 1) // First time no events were found
                    {
                        LogTrace("No consumable events found. Polling-timeout will increase from {minDelayInMs} till {maxDelayInMs} milliseconds.", _minDelayInMs, _maxDelayInMs);
                    }
                    this.BackOff();
                }
                else
                {
                    this._attempts = 0;

                    if (workitems.Count() > 1)
                    {
                        LogTrace("Processing {items} events in parallel.", workitems.Count());
                        workitems.AsParallel().ForAll(ce => // AsParallel takes care of partitioning, resulting in using less threads than Parallal.ForEach, but still fast enough.
                            Task.Run(async () => // Threadpool task to wait for async parts in inner task (ExecuteWork)
                            {
                                await this.ExecuteWork(ce).ConfigureAwait(false);
                            }).GetAwaiter().GetResult() // Need to block, because ForAll does not
                        );
                    }
                    else
                        await this.ExecuteWork(workitems.First()).ConfigureAwait(false); // Executed on single (current) thread, but theoretically the TryGetWork could have returned more than one workitem

                    this.BackOff(); // Using backoff here as well to pause for minBackoffDelay between batches
                }
            }
            catch (AggregateException aggregateException)
            {
                PollingException(aggregateException);
            }
        }

        /// <summary>
        /// Stops the processing of workitems.
        /// </summary>
        public void Stop()
        {
            this._cancellationToken.Cancel();
            if (this._internalTask.Status == TaskStatus.Running)
                this._internalTask.Wait();

            // We no longer need the task and cancellation token
#if NET452
            this._internalTask.Dispose();
#endif
            this._internalTask = null;
            this._cancellationToken.Dispose();
            this._cancellationToken = null;
        }

        /// <summary>
        /// Disposes the PollingTask
        /// </summary>
        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// IDispose implementation
        /// </summary>
        /// <param name="disposing"></param>
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (this._internalTask != null)
                {
                    if (this._internalTask.Status == TaskStatus.Running)
                    {
                        Stop(); // Also disposes the task
                    }
                    else // Never started or completed/failed/cancelled, so just dispose
                    {
#if NET452
                        this._internalTask.Dispose();
#endif
                    }
                }
                this._cancellationToken.Dispose();
            }
            this._internalTask = null;
        }

        /// <summary>
        /// Try get (a list of) workitems
        /// </summary>
        /// <param name="maxWorkItems">The maximum number of workitems to be returned</param>
        /// <returns>A list of workitems or null/empty list when no work to be done</returns>
        protected virtual async Task<IEnumerable<ConsumableEvent>> TryGetWork(int maxWorkItems)
        {
            try
            {
                var consEvent = await _eventConsumer.ConsumeNextAsync(_subscriptionName, _visibilityTimeout, maxWorkItems).ConfigureAwait(false);
                return consEvent;
            }
            catch (Exception ex)
            {
                LogError("Failed to get consumable event: {exception}", ex);
                return null;
            }
        }

        /// <summary>
        /// Invoked to actually execute a single workitem
        /// </summary>
        /// <param name="workItem"></param>
        protected virtual async Task<bool> Execute(ConsumableEvent workItem)
        {
            // Only process if still invisible (serial processing of a list of workitems may cause workitems to expire before being processed)
            if (workItem.InvisibleUntilUtc > DateTime.UtcNow)
            {
                return await ExecuteConcrete(workItem).ConfigureAwait(false);
            }
            else
            {
                LogWarning("ConsumableEvent with id {Id} has expired: InvisibleUntilUtc ({InvisibleUntilUtc}) < UtcNow ({UtcNow}).",
                    workItem.Id, workItem.InvisibleUntilUtc, DateTime.UtcNow);
                return false;
            }
        }

        private async Task<bool> ExecuteConcrete(ConsumableEvent ce)
        {
            ConsumeResult result = null;
            bool mustRollback = false;

            try
            {
                LogTrace("Processing event with id {Id} and functional key {FunctionalKey}.",
                    ce.Id, ce.FunctionalKey != null ? ce.FunctionalKey : "n/a");
                result = await _consumeAction(ce).ConfigureAwait(false);
            }
            catch (Exception procEx)
            {
                result = ConsumeResult.Failed(procEx.ToString());
            }


            if (result.ResultType == ConsumeResultType.Succeeded)
            {
                bool markedConsumed = false;

                try
                {
                    await _eventConsumer.MarkConsumedAsync(ce.Id, ce.DeliveryKey).ConfigureAwait(false);
                    markedConsumed = true;
                    LogTrace("Event consumption succeeded for event with id {Id} and functional key {FunctionalKey}.",
                        ce.Id, ce.FunctionalKey != null ? ce.FunctionalKey : "n/a");
                }
                catch (Exception ex)
                {
                    LogError("Failed to mark event consumed with id {Id} and functional key {FunctionalKey}, cause event to be processes again! Details: {Exception}.",
                        ce.Id, ce.FunctionalKey != null ? ce.FunctionalKey : "n/a", ex);
                    // mustRollback hoeft eigenlijk niet geset te worden, want markedComplete zal niet meer true zijn
                    mustRollback = true;
                }

                if (!markedConsumed)
                    mustRollback = true;
            }
            else if (result.ResultType == ConsumeResultType.Failed)
            {
                mustRollback = true;
                LogError("Exception occurred while processing event with id {Id} and functional key {FunctionalKey}: {Reason}.",
                    ce.Id, ce.FunctionalKey != null ? ce.FunctionalKey : "n/a", result.Reason);

                try
                {
                    await _eventConsumer.MarkFailedAsync(ce.Id, ce.DeliveryKey, Reason.Other(result.Reason)).ConfigureAwait(false);
                }
                catch (Exception) { } // Swallow, want is geen ramp als deze toch opnieuw wordt aangeboden (dan wordt hij alsnog corrupt gemeld)
            }
            else if (result.ResultType == ConsumeResultType.MustSuspend)
            {
                mustRollback = true;
                var suspendedUntilUtc = this.Suspend(result.SuspendDuration.GetValueOrDefault(TimeSpan.FromSeconds(60)));
                LogError("Event consumption failed for event with id {Id} and functional key {FunctionalKey}. Processing should (continue to) suspend. Details: {Reason}.",
                    ce.Id, ce.FunctionalKey != null ? ce.FunctionalKey : "n/a", result.Reason);
            }
            // MustRetry does nothing: default behaviour when not marked consumed/failed

            return (result.ResultType == ConsumeResultType.Succeeded && !mustRollback);
        }

        /// <summary>
        /// Optional: Invoked when a workitem has been successfully executed
        /// </summary>
        /// <param name="workItem"></param>
        protected virtual void Completed(ConsumableEvent workItem, TimeSpan elapsed)
        { }

        /// <summary>
        /// Optional: Invoked when a workitem failed to execute successfully
        /// </summary>
        /// <param name="workItem"></param>
        /// <param name="execEx"></param>
        protected virtual void Failed(ConsumableEvent workItem, Exception execEx)
        { }

        /// <summary>
        /// Optional: Invoked when the TryGetWork or ExecuteWork throw an exception
        /// </summary>
        /// <param name="pollingEx"></param>
        protected virtual void PollingException(Exception pollingEx)
        {
            LogError("Polling exception occurred: {Exception}", pollingEx);
        }

        #region Logging helpers
        private void LogTrace(string text, params object[] args)
        {
            if (_logger != null)
                _logger.LogTrace(text, args);
        }

        private void LogInformation(string text, params object[] args)
        {
            if (_logger != null)
                _logger.LogInformation(text, args);
        }

        private void LogError(string text, params object[] args)
        {
            if (_logger != null)
                _logger.LogError(text, args);
        }

        private void LogWarning(string text, params object[] args)
        {
            if (_logger != null)
                _logger.LogWarning(text, args);
        }
        #endregion
    }
}

