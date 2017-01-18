using Resonance.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Xunit;

namespace Resonance.Tests.Housekeeping
{
    [Collection("EventingRepo")]
    public class HousekeepingTests
    {
        private readonly IEventPublisher _publisher;
        private readonly IEventConsumer _consumer;
        private readonly EventingRepoFactoryFixture _fixture;

        public HousekeepingTests(EventingRepoFactoryFixture fixture)
        {
            _publisher = new EventPublisher(fixture.RepoFactory);
            _consumer = new EventConsumer(fixture.RepoFactory);
            _fixture = fixture;
        }

        [Fact]
        public void Expiration()
        {
            // Arrange
            var topicName = "HousekeepingTests.Expiration";
            var subName = topicName + "_Sub1";
            var topic = _publisher.AddOrUpdateTopic(new Topic { Name = topicName });
            var sub1 = _consumer.AddOrUpdateSubscription(new Subscription
            {
                Name = subName,
                TopicSubscriptions = new List<TopicSubscription> { new TopicSubscription { TopicId = topic.Id.Value, Enabled = true } },
            });

            var utcNow = DateTime.UtcNow;
            _publisher.Publish(topicName, eventName: topicName + "1", expirationDateUtc: utcNow.AddSeconds(5), payload: "1");
            _publisher.Publish(topicName, eventName: topicName + "2", expirationDateUtc: utcNow.AddSeconds(5), payload: "2");
            _publisher.Publish(topicName, eventName: topicName + "3", expirationDateUtc: utcNow.AddMinutes(15), payload: "3");

            var visibilityTimeout = 1; // Must expire asap
            var ce1 = _consumer.ConsumeNext(subName, visibilityTimeout: visibilityTimeout).SingleOrDefault();
            var ce2 = _consumer.ConsumeNext(subName, visibilityTimeout: 10).SingleOrDefault(); // Still invisible after expiration
            var ce3 = _consumer.ConsumeNext(subName, visibilityTimeout: visibilityTimeout).SingleOrDefault();
            Assert.Equal("1", ce1.Payload); // Not yet expired
            Assert.Equal("2", ce2.Payload);
            Assert.Equal("3", ce3.Payload);
            Thread.Sleep(TimeSpan.FromSeconds(5+1));

            _consumer.PerformHouseKeepingTasks();

            // Check that only 1 se in table
            var eventNames = _fixture.GetEventNamesForFailedEvents(sub1.Id.Value);
            Assert.Equal(1, eventNames.Count);
            Assert.Equal(topicName + "1", eventNames.First());
            // 2 has expired, but is still invisible, so may still be processed
        }

        [Fact]
        public void MaxDeliveries()
        {
            // Arrange
            var topicName = "HousekeepingTests.MaxDeliveries";
            var subName = topicName + "_Sub1";
            var topic = _publisher.AddOrUpdateTopic(new Topic { Name = topicName });
            var sub1 = _consumer.AddOrUpdateSubscription(new Subscription
            {
                Name = subName,
                MaxDeliveries = 1,
                TopicSubscriptions = new List<TopicSubscription> { new TopicSubscription { TopicId = topic.Id.Value, Enabled = true } },
            });

            var utcNow = DateTime.UtcNow;
            _publisher.Publish(topicName, eventName: topicName + "1", publicationDateUtc: utcNow, payload: "1");
            _publisher.Publish(topicName, eventName: topicName + "2", publicationDateUtc: utcNow.AddSeconds(1), payload: "2");
            _publisher.Publish(topicName, eventName: topicName + "3", publicationDateUtc: utcNow.AddSeconds(2), payload: "3");

            var visibilityTimeout = 1; // Must expire asap
            var ce1 = _consumer.ConsumeNext(subName, visibilityTimeout: visibilityTimeout).SingleOrDefault();
            var ce2 = _consumer.ConsumeNext(subName, visibilityTimeout: 10).SingleOrDefault();
            Assert.Equal("1", ce1.Payload); // Not yet maxDeliveriesReached
            Assert.Equal("2", ce2.Payload); // Not yet maxDeliveriesReached
            Thread.Sleep(TimeSpan.FromSeconds(1+1)); // ce1 has become visible again, ce2 has not!
            _consumer.PerformHouseKeepingTasks();

            // Check that only 1 se in table
            var eventNames = _fixture.GetEventNamesForFailedEvents(sub1.Id.Value);
            Assert.Equal(1, eventNames.Count);
            Assert.Equal(topicName + "1", eventNames.First());
            // ce2 is still invisible
        }

        [Fact]
        public void Overtaken()
        {
            // Arrange
            var topicName = "HousekeepingTests.Overtaken";
            var subName = topicName + "_Sub1";
            var topic = _publisher.AddOrUpdateTopic(new Topic { Name = topicName });
            var sub1 = _consumer.AddOrUpdateSubscription(new Subscription
            {
                Name = subName,
                Ordered = true,
                TopicSubscriptions = new List<TopicSubscription> { new TopicSubscription { TopicId = topic.Id.Value, Enabled = true } },
            });

            var utcNow = DateTime.UtcNow;
            var funcKey = "A";
            _publisher.Publish(topicName, eventName: topicName + "1", publicationDateUtc: utcNow, functionalKey: funcKey);
            var ce = _consumer.ConsumeNext(subName).SingleOrDefault();
            _consumer.MarkConsumed(ce.Id, ce.DeliveryKey);
            _publisher.Publish(topicName, eventName: topicName + "2", publicationDateUtc: utcNow.AddSeconds(-1), functionalKey: funcKey);
            ce = _consumer.ConsumeNext(subName).SingleOrDefault();
            Assert.Null(ce); // Already overtaken, so should not be delivered

            _consumer.PerformHouseKeepingTasks();

            // Check that only 1 se in table
            var eventNames = _fixture.GetEventNamesForFailedEvents(sub1.Id.Value);
            Assert.Equal(1, eventNames.Count);
            Assert.Equal(topicName + "2", eventNames.First());
        }
    }
}
