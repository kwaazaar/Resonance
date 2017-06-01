using Resonance.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Resonance.Repo
{
    public abstract class BaseEventingRepo
    {
        /// <summary>
        /// Min datetime to use (Sql Server does not support any lower value than this)
        /// </summary>
        public static DateTime MinDateTime { get { return new DateTime(1753, 1, 1); } }

        /// <summary>
        /// Max datetime to use
        /// </summary>
        public static DateTime MaxDateTime { get { return new DateTime(9999, 12, 31); } }

        public async Task<TopicEvent> PublishTopicEventAsync(TopicEvent newTopicEvent, bool logTopicEvent, IEnumerable<Subscription> subscriptionsMatching, DateTime? deliveryDelayedUntilUtc)
        {
            await BeginTransactionAsync().ConfigureAwait(false);

            try
            {
                PrepareTopicEvent(newTopicEvent);

                if (logTopicEvent)
                {
                    var topicEventId = await AddTopicEventAsync(newTopicEvent).ConfigureAwait(false);
                    newTopicEvent.Id = topicEventId;
                }

                // Create tasks for each subscription
                var subTasks = new List<Task<Int64>>();
                foreach (var subscription in subscriptionsMatching)
                {
                    // By default a SubscriptionEvent takes expirationdate of TopicEvent
                    var subExpirationDateUtc = newTopicEvent.ExpirationDateUtc;

                    // If subscription has its own TTL, it will be applied, but may never exceed the TopicEvent expiration
                    if (subscription.TimeToLive.HasValue)
                    {
                        subExpirationDateUtc = newTopicEvent.PublicationDateUtc.Value.AddSeconds(subscription.TimeToLive.Value);
                        if (newTopicEvent.ExpirationDateUtc < subExpirationDateUtc)
                            subExpirationDateUtc = newTopicEvent.ExpirationDateUtc;
                    }

                    // Delivery can be initially delayed, but it cannot exceed the expirationdate (would be useless)
                    DateTime subDeliveryDelayedUntilUtc = MinDateTime;
                    if (deliveryDelayedUntilUtc.HasValue) // Passed values always go overwrite subscription settings
                        subDeliveryDelayedUntilUtc = deliveryDelayedUntilUtc.Value;
                    else if (subscription.DeliveryDelay.HasValue)
                        subDeliveryDelayedUntilUtc = newTopicEvent.PublicationDateUtc.Value.AddSeconds(subscription.DeliveryDelay.Value);

                    var newSubscriptionEvent = new SubscriptionEvent
                    {
                        SubscriptionId = subscription.Id.Value,
                        TopicEventId = newTopicEvent.Id,
                        EventName = newTopicEvent.EventName,
                        PublicationDateUtc = newTopicEvent.PublicationDateUtc.Value,
                        FunctionalKey = newTopicEvent.FunctionalKey,
                        Priority = newTopicEvent.Priority,
                        PayloadId = newTopicEvent.PayloadId,
                        Payload = null, // Only used when consuming
                        ExpirationDateUtc = subExpirationDateUtc,
                        DeliveryDelayedUntilUtc = subDeliveryDelayedUntilUtc,
                        DeliveryCount = 0,
                        DeliveryKey = string.Empty,
                        InvisibleUntilUtc = MinDateTime,
                    };

                    if (ParallelQueriesSupport)
                        subTasks.Add(AddSubscriptionEventAsync(newSubscriptionEvent));
                    else
                        await AddSubscriptionEventAsync(newSubscriptionEvent).ConfigureAwait(false);
                };

                if (subTasks.Count > 0) // Wait for all tasks (if any) to complete
                    await Task.WhenAll(subTasks.ToArray()).ConfigureAwait(false); // No timeout: db-commandtimeout will do

                await CommitTransactionAsync().ConfigureAwait(false);

                // Return the topic event
                return newTopicEvent;
            }
            catch (AggregateException aggrEx)
            {
                await RollbackTransactionAsync().ConfigureAwait(false);
                var firstInnerEx = aggrEx.InnerExceptions.FirstOrDefault();
                if (firstInnerEx != null)
                    throw firstInnerEx; // Do we want this?
                else
                    throw;
            }
            catch (Exception)
            {
                await RollbackTransactionAsync().ConfigureAwait(false);
                throw;
            }
        }

        private void PrepareTopicEvent(TopicEvent newTopicEvent)
        {
            if (!newTopicEvent.PublicationDateUtc.HasValue)
                newTopicEvent.PublicationDateUtc = DateTime.UtcNow;
            if (newTopicEvent.FunctionalKey == null)
                newTopicEvent.FunctionalKey = string.Empty;
        }

        protected abstract Task<Int64> AddSubscriptionEventAsync(SubscriptionEvent newSubscriptionEvent);
        protected abstract Task<Int64> AddTopicEventAsync(TopicEvent newTopicEvent);

        #region EventingRepo stuff that does not need to be available outside the repo implementations, so not part of IEventingRepo
        protected abstract bool ParallelQueriesSupport { get; }
        protected abstract Task BeginTransactionAsync();
        protected abstract Task RollbackTransactionAsync();
        protected abstract Task CommitTransactionAsync();
        #endregion
    }
}
