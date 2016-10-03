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
    public class StandardOrderingTests
    {
        private readonly IEventPublisher _publisher;
        private readonly IEventConsumer _consumer;

        public StandardOrderingTests(EventingRepoFactoryFixture fixture)
        {
            _publisher = new EventPublisher(fixture.RepoFactory);
            _consumer = new EventConsumer(fixture.RepoFactory);
        }

        [Fact]
        public void PublicationDate_Default()
        {
            // Arrange
            var topicName = "StandardOrderingTests.PublicationDate_Default";
            var subName = topicName + "_Sub1";
            var topic = _publisher.AddOrUpdateTopicAsync(new Topic { Name = topicName }).Result;
            var sub1 = _consumer.AddOrUpdateSubscriptionAsync(new Subscription
            {
                Name = subName, // When ordered not set, delivery should still be ordered on publicationdateutc
                TopicSubscriptions = new List<TopicSubscription> { new TopicSubscription { TopicId = topic.Id.Value, Enabled = true } },
            }).Result;

            _publisher.PublishAsync(topicName, payload: "1").Wait();
            Thread.Sleep(100); // To make sure publicationdateutc is not equal for each item
            _publisher.PublishAsync(topicName, payload: "2").Wait();
            Thread.Sleep(100);
            _publisher.PublishAsync(topicName, payload: "3").Wait();

            var visibilityTimeout = 2;
            var ce1 = _consumer.ConsumeNextAsync(subName, visibilityTimeout: visibilityTimeout).Result.SingleOrDefault();
            var ce2 = _consumer.ConsumeNextAsync(subName, visibilityTimeout: visibilityTimeout).Result.SingleOrDefault();
            var ce3 = _consumer.ConsumeNextAsync(subName, visibilityTimeout: visibilityTimeout).Result.SingleOrDefault();
            Assert.Equal("1", ce1.Payload);
            Assert.Equal("2", ce2.Payload);
            Assert.Equal("3", ce3.Payload);
            _consumer.MarkConsumedAsync(ce2.Id, ce2.DeliveryKey).Wait(); // ce2 should be gone

            Thread.Sleep(TimeSpan.FromSeconds(visibilityTimeout + 1)); // Wait until visibilitytimeout of all items (+1) has expired
            ce1 = _consumer.ConsumeNextAsync(subName, visibilityTimeout: visibilityTimeout).Result.SingleOrDefault();
            ce3 = _consumer.ConsumeNextAsync(subName, visibilityTimeout: visibilityTimeout).Result.SingleOrDefault();
            Assert.Equal("1", ce1.Payload);
            Assert.Equal("3", ce3.Payload);
        }

        [Fact]
        public void PublicationDate_Custom()
        {
            // Arrange
            var topicName = "StandardOrderingTests.PublicationDate_Custom";
            var subName = topicName + "_Sub1";
            var topic = _publisher.AddOrUpdateTopicAsync(new Topic { Name = topicName }).Result;
            var sub1 = _consumer.AddOrUpdateSubscriptionAsync(new Subscription
            {
                Name = subName, // When ordered not set, delivery should still be ordered on publicationdateutc
                TopicSubscriptions = new List<TopicSubscription> { new TopicSubscription { TopicId = topic.Id.Value, Enabled = true } },
            }).Result;

            _publisher.PublishAsync(topicName, payload: "1", publicationDateUtc: DateTime.UtcNow.AddSeconds(1)).Wait();
            _publisher.PublishAsync(topicName, payload: "2", publicationDateUtc: DateTime.UtcNow).Wait();
            _publisher.PublishAsync(topicName, payload: "3", publicationDateUtc: DateTime.UtcNow.AddSeconds(2)).Wait();

            var visibilityTimeout = 2;
            var ce1 = _consumer.ConsumeNextAsync(subName, visibilityTimeout: visibilityTimeout).Result.SingleOrDefault();
            var ce2 = _consumer.ConsumeNextAsync(subName, visibilityTimeout: visibilityTimeout).Result.SingleOrDefault();
            var ce3 = _consumer.ConsumeNextAsync(subName, visibilityTimeout: visibilityTimeout).Result.SingleOrDefault();
            Assert.Equal("2", ce1.Payload);
            Assert.Equal("1", ce2.Payload);
            Assert.Equal("3", ce3.Payload);
            _consumer.MarkConsumedAsync(ce1.Id, ce1.DeliveryKey).Wait(); // ce1 should be gone

            Thread.Sleep(TimeSpan.FromSeconds(visibilityTimeout + 1)); // Wait until visibilitytimeout of all items (+1) has expired
            ce2 = _consumer.ConsumeNextAsync(subName, visibilityTimeout: visibilityTimeout).Result.SingleOrDefault();
            ce3 = _consumer.ConsumeNextAsync(subName, visibilityTimeout: visibilityTimeout).Result.SingleOrDefault();
            Assert.Equal("1", ce2.Payload);
            Assert.Equal("3", ce3.Payload);
        }
    }
}
