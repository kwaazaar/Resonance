using Resonance.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Resonance.Tests.Consuming
{
    [Collection("EventingRepo")]
    public class SubscriptionTests
    {
        private readonly IEventPublisher _publisher;
        private readonly IEventConsumer _consumer;
        private readonly EventingRepoFactoryFixture _fixture;

        public SubscriptionTests(EventingRepoFactoryFixture fixture)
        {
            _publisher = new EventPublisher(fixture.RepoFactory, DateTimeProvider.Repository, TimeSpan.Zero, SafeExecOptions.NoRetries);
            _consumer = new EventConsumer(fixture.RepoFactory, TimeSpan.Zero, SafeExecOptions.NoRetries);
            _fixture = fixture;
        }

        [Fact]
        public void SubscriptionStatistics()
        {
            // Arrange
            var topicName = "SubStats";
            var subName = topicName + "_Sub1";
            var topic = _publisher.AddOrUpdateTopic(new Topic { Name = topicName });
            var sub1 = _consumer.AddOrUpdateSubscription(new Subscription
            {
                Name = subName,
                Ordered = true,
                MaxDeliveries = 1,
                TopicSubscriptions = new List<TopicSubscription> { new TopicSubscription { TopicId = topic.Id.Value, Enabled = true } },
            });

            var utcNow = DateTime.UtcNow;
            var funcKey = "A";
            // Create situation: failed-other (marked)
            _publisher.Publish(topicName, eventName: topicName + "0", functionalKey: funcKey);
            var ce = _consumer.ConsumeNext(sub1.Name).SingleOrDefault();
            _consumer.MarkFailed(ce.Id, ce.DeliveryKey, Reason.Other("Some other reason"));
            // Create situation: expired
            _publisher.Publish(topicName, eventName: topicName + "1", functionalKey: funcKey, expirationDateUtc: utcNow);
            Thread.Sleep(TimeSpan.FromSeconds(1)); // Expired for sure
            // Create situation: maxDeliveriesReached
            _publisher.Publish(topicName, eventName: topicName + "2", functionalKey: funcKey);
            ce = _consumer.ConsumeNext(sub1.Name, visibilityTimeout: 1).SingleOrDefault();
            Assert.Equal(topicName + "2", ce.EventName);
            Thread.Sleep(TimeSpan.FromSeconds(1 + 1)); // Becomes visible again, but has reached maxDeliveries
            // Create situation: overtaken
            _publisher.Publish(topicName, eventName: topicName + "3", functionalKey: funcKey, publicationDateUtc: utcNow.AddSeconds(2));
            ce = _consumer.ConsumeNext(sub1.Name).SingleOrDefault();
            Assert.Equal(topicName + "3", ce.EventName);
            _publisher.Publish(topicName, eventName: topicName + "4", functionalKey: funcKey, publicationDateUtc: utcNow.AddSeconds(-1));
            _consumer.MarkConsumed(ce.Id, ce.DeliveryKey); // #4 is now overtaken
            _publisher.Publish(topicName, eventName: topicName + "5", functionalKey: funcKey);

            var statsBeforeHouseKeeping = _consumer.GetSubscriptionStatistics(utcNow.AddMinutes(-10), utcNow.AddMinutes(+10));
            Assert.Equal(statsBeforeHouseKeeping.Single(s => s.Subscription.Id.Value == sub1.Id).Open, 4); // 4 open, since housekeeping has not yet moved them (3) to failed
            Assert.Equal(statsBeforeHouseKeeping.Single(s => s.Subscription.Id.Value == sub1.Id).Consumed, 1);
            Assert.Equal(statsBeforeHouseKeeping.Single(s => s.Subscription.Id.Value == sub1.Id).FailedExpired, 0);
            Assert.Equal(statsBeforeHouseKeeping.Single(s => s.Subscription.Id.Value == sub1.Id).FailedMaxDeliveriesReached, 0);
            Assert.Equal(statsBeforeHouseKeeping.Single(s => s.Subscription.Id.Value == sub1.Id).FailedOvertaken, 0);
            Assert.Equal(statsBeforeHouseKeeping.Single(s => s.Subscription.Id.Value == sub1.Id).FailedOther, 1); // Does not need housekeeping to move it
            _consumer.PerformHouseKeepingTasks();
            var statsAfterHouseKeeping = _consumer.GetSubscriptionStatistics(utcNow.AddMinutes(-10), utcNow.AddMinutes(+10));
            Assert.Equal(statsAfterHouseKeeping.Single(s => s.Subscription.Id.Value == sub1.Id).Open, 1);
            Assert.Equal(statsAfterHouseKeeping.Single(s => s.Subscription.Id.Value == sub1.Id).Consumed, 1);
            Assert.Equal(statsAfterHouseKeeping.Single(s => s.Subscription.Id.Value == sub1.Id).FailedExpired, 1);
            Assert.Equal(statsAfterHouseKeeping.Single(s => s.Subscription.Id.Value == sub1.Id).FailedMaxDeliveriesReached, 1);
            Assert.Equal(statsAfterHouseKeeping.Single(s => s.Subscription.Id.Value == sub1.Id).FailedOvertaken, 1);
            Assert.Equal(statsAfterHouseKeeping.Single(s => s.Subscription.Id.Value == sub1.Id).FailedOther, 1);
        }
    }
}
