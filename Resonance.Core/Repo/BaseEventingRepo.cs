using Resonance.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Resonance.Repo
{
    public abstract class BaseEventingRepo
    {
        public async Task<TopicEvent> PublishTopicEventAsync(TopicEvent newTopicEvent, IEnumerable<Subscription> subscriptionsMatching)
        {
            await BeginTransactionAsync().ConfigureAwait(false);

            try
            {
                var topicEventId = await AddTopicEventAsync(newTopicEvent).ConfigureAwait(false);
                newTopicEvent.Id = topicEventId;

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
                        if (newTopicEvent.ExpirationDateUtc.HasValue && (newTopicEvent.ExpirationDateUtc.Value < subExpirationDateUtc))
                            subExpirationDateUtc = newTopicEvent.ExpirationDateUtc;
                    }

                    // Delivery can be initially delayed, but it cannot exceed the expirationdate (would be useless)
                    var deliveryDelayedUntilUtc = subscription.DeliveryDelay.HasValue
                        ? newTopicEvent.PublicationDateUtc.Value.AddSeconds(subscription.DeliveryDelay.Value)
                        : default(DateTime?);

                    var newSubscriptionEvent = new SubscriptionEvent
                    {
                        SubscriptionId = subscription.Id.Value,
                        TopicEventId = newTopicEvent.Id.Value,
                        EventName = newTopicEvent.EventName,
                        PublicationDateUtc = newTopicEvent.PublicationDateUtc.Value,
                        FunctionalKey = newTopicEvent.FunctionalKey,
                        Priority = newTopicEvent.Priority,
                        PayloadId = newTopicEvent.PayloadId,
                        Payload = null, // Only used when consuming
                        ExpirationDateUtc = subExpirationDateUtc,
                        DeliveryDelayedUntilUtc = deliveryDelayedUntilUtc,
                        DeliveryCount = 0,
                        DeliveryKey = null,
                        InvisibleUntilUtc = null,
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
