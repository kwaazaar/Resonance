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
            _consumer = new EventConsumer(fixture.RepoFactory, TimeSpan.Zero, SafeExecOptions.NoRetries); // To test explicit behavior, retries get in the way
        }

        [Fact]
        public void PublicationDate_Default()
        {
            // Arrange
            var topicName = "StandardOrderingTests.PublicationDate_Default";
            var subName = topicName + "_Sub1";
            var topic = _publisher.AddOrUpdateTopic(new Topic { Name = topicName });
            var sub1 = _consumer.AddOrUpdateSubscription(new Subscription
            {
                Name = subName, // When ordered not set, delivery should still be ordered on publicationdateutc
                TopicSubscriptions = new List<TopicSubscription> { new TopicSubscription { TopicId = topic.Id.Value, Enabled = true } },
            });

            _publisher.Publish(topicName, payload: "1");
            Thread.Sleep(100); // To make sure publicationdateutc is not equal for each item
            _publisher.Publish(topicName, payload: "2");
            Thread.Sleep(100);
            _publisher.Publish(topicName, payload: "3");

            var visibilityTimeout = 2;
            var ce1 = _consumer.ConsumeNext(subName, visibilityTimeout: visibilityTimeout).SingleOrDefault();
            var ce2 = _consumer.ConsumeNext(subName, visibilityTimeout: visibilityTimeout).SingleOrDefault();
            var ce3 = _consumer.ConsumeNext(subName, visibilityTimeout: visibilityTimeout).SingleOrDefault();
            Assert.Equal("1", ce1.Payload);
            Assert.Equal("2", ce2.Payload);
            Assert.Equal("3", ce3.Payload);
            _consumer.MarkConsumed(ce2.Id, ce2.DeliveryKey); // ce2 should be gone

            Thread.Sleep(TimeSpan.FromSeconds(visibilityTimeout + 1)); // Wait until visibilitytimeout of all items (+1) has expired
            ce1 = _consumer.ConsumeNext(subName, visibilityTimeout: visibilityTimeout).SingleOrDefault();
            ce3 = _consumer.ConsumeNext(subName, visibilityTimeout: visibilityTimeout).SingleOrDefault();
            Assert.Equal("1", ce1.Payload);
            Assert.Equal("3", ce3.Payload);
        }

        [Fact]
        public void PublicationDate_Custom()
        {
            // Arrange
            var topicName = "StandardOrderingTests.PublicationDate_Custom";
            var subName = topicName + "_Sub1";
            var topic = _publisher.AddOrUpdateTopic(new Topic { Name = topicName });
            var sub1 = _consumer.AddOrUpdateSubscription(new Subscription
            {
                Name = subName, // When ordered not set, delivery should still be ordered on publicationdateutc
                TopicSubscriptions = new List<TopicSubscription> { new TopicSubscription { TopicId = topic.Id.Value, Enabled = true } },
            });

            _publisher.Publish(topicName, payload: "1", publicationDateUtc: DateTime.UtcNow.AddSeconds(1));
            _publisher.Publish(topicName, payload: "2", publicationDateUtc: DateTime.UtcNow);
            _publisher.Publish(topicName, payload: "3", publicationDateUtc: DateTime.UtcNow.AddSeconds(2));

            var visibilityTimeout = 2;
            var ce1 = _consumer.ConsumeNext(subName, visibilityTimeout: visibilityTimeout).SingleOrDefault();
            var ce2 = _consumer.ConsumeNext(subName, visibilityTimeout: visibilityTimeout).SingleOrDefault();
            var ce3 = _consumer.ConsumeNext(subName, visibilityTimeout: visibilityTimeout).SingleOrDefault();
            Assert.Equal("2", ce1.Payload);
            Assert.Equal("1", ce2.Payload);
            Assert.Equal("3", ce3.Payload);
            _consumer.MarkConsumed(ce1.Id, ce1.DeliveryKey); // ce1 should be gone

            Thread.Sleep(TimeSpan.FromSeconds(visibilityTimeout + 1)); // Wait until visibilitytimeout of all items (+1) has expired
            ce2 = _consumer.ConsumeNext(subName, visibilityTimeout: visibilityTimeout).SingleOrDefault();
            ce3 = _consumer.ConsumeNext(subName, visibilityTimeout: visibilityTimeout).SingleOrDefault();
            Assert.Equal("1", ce2.Payload);
            Assert.Equal("3", ce3.Payload);
        }

        [Fact]
        public void PublicationDate_WithPriority()
        {
            // Arrange
            var topicName = "PublicationDate_WithPriority";
            var subName = topicName + "_Sub1";
            var topic = _publisher.AddOrUpdateTopic(new Topic { Name = topicName });
            var sub1 = _consumer.AddOrUpdateSubscription(new Subscription
            {
                Name = subName, // When ordered not set, delivery should still be ordered on publicationdateutc
                TopicSubscriptions = new List<TopicSubscription> { new TopicSubscription { TopicId = topic.Id.Value, Enabled = true } },
            });

            _publisher.Publish(topicName, payload: "1");
            Thread.Sleep(100); // To make sure publicationdateutc is not equal for each item
            _publisher.Publish(topicName, payload: "2", priority: 1); // Higher
            Thread.Sleep(100);
            _publisher.Publish(topicName, payload: "3");

            var visibilityTimeout = 2;
            var ce1 = _consumer.ConsumeNext(subName, visibilityTimeout: visibilityTimeout).SingleOrDefault();
            var ce2 = _consumer.ConsumeNext(subName, visibilityTimeout: visibilityTimeout).SingleOrDefault();
            var ce3 = _consumer.ConsumeNext(subName, visibilityTimeout: visibilityTimeout).SingleOrDefault();
            Assert.Equal("2", ce1.Payload);
            Assert.Equal("1", ce2.Payload);
            Assert.Equal("3", ce3.Payload);
            _consumer.MarkConsumed(ce1.Id, ce1.DeliveryKey); // Once the high prio is gone, the other come again

            Thread.Sleep(TimeSpan.FromSeconds(visibilityTimeout + 1)); // Wait until visibilitytimeout of all items (+1) has expired
            ce1 = _consumer.ConsumeNext(subName, visibilityTimeout: visibilityTimeout).SingleOrDefault();
            ce3 = _consumer.ConsumeNext(subName, visibilityTimeout: visibilityTimeout).SingleOrDefault();
            Assert.Equal("1", ce1.Payload);
            Assert.Equal("3", ce3.Payload);
        }


        [Fact]
        public void ConsumeBatch_AllConsumed()
        {
            // Arrange
            var topicName = "ConsumeBatch_AllConsumed";
            var subName = topicName + "_Sub1";
            var topic = _publisher.AddOrUpdateTopic(new Topic { Name = topicName });
            var sub1 = _consumer.AddOrUpdateSubscription(new Subscription
            {
                Name = subName, // When ordered not set, delivery should still be ordered on publicationdateutc
                TopicSubscriptions = new List<TopicSubscription> { new TopicSubscription { TopicId = topic.Id.Value, Enabled = true } },
            });

            _publisher.Publish(topicName, payload: "1");
            Thread.Sleep(100); // To make sure publicationdateutc is not equal for each item
            _publisher.Publish(topicName, payload: "2");
            Thread.Sleep(100);
            _publisher.Publish(topicName, payload: "3");

            // Consume a batch of 2 items
            var batchSize = 2;
            var visibilityTimeout = 2; // 2 secs
            var ces = _consumer.ConsumeNext(subName, visibilityTimeout: visibilityTimeout, maxCount: batchSize);
            Assert.Equal(batchSize, ces.Count());
            Assert.True(ces.Any(ce => ce.Payload == "1"));
            Assert.True(ces.Any(ce => ce.Payload == "2"));

            // Mark the whole batch consumed
            _consumer.MarkConsumed(ces.Cast<ConsumableEventId>()); // Can be simply cast, since ConsumableEvent derives from ConsumableEventId

            // Check if they are really marked consumed (can no longer be consumed)
            Thread.Sleep(TimeSpan.FromSeconds(visibilityTimeout + 1)); // Wait until visibilitytimeout of all items (+1) has expired
            ces = _consumer.ConsumeNext(subName, maxCount: batchSize); // Batchsize exceeds nr of events available
            Assert.Equal(1, ces.Count());
            Assert.True(ces.Any(ce => ce.Payload == "3"));
        }

        [Fact]
        public void ConsumeBatch_NotAllConsumed()
        {
            // Arrange
            var topicName = "ConsumeBatch_NotAllConsumed";
            var subName = topicName + "_Sub1";
            var topic = _publisher.AddOrUpdateTopic(new Topic { Name = topicName });
            var sub1 = _consumer.AddOrUpdateSubscription(new Subscription
            {
                Name = subName, // When ordered not set, delivery should still be ordered on publicationdateutc
                TopicSubscriptions = new List<TopicSubscription> { new TopicSubscription { TopicId = topic.Id.Value, Enabled = true } },
            });

            _publisher.Publish(topicName, payload: "1");
            Thread.Sleep(100); // To make sure publicationdateutc is not equal for each item
            _publisher.Publish(topicName, payload: "2");
            Thread.Sleep(100);
            _publisher.Publish(topicName, payload: "3");

            // Consume a batch of 2 items
            var batchSize = 2;
            var visibilityTimeout = 2; // 2 secs
            var ces = _consumer.ConsumeNext(subName, visibilityTimeout: visibilityTimeout, maxCount: batchSize);
            Assert.Equal(batchSize, ces.Count());
            Assert.True(ces.Any(ce => ce.Payload == "1"));
            Assert.True(ces.Any(ce => ce.Payload == "2"));

            // Mark only 2 consumed
            var ce1 = ces.Single(ce => ce.Payload == "1");
            _consumer.MarkFailed(ce1.Id, ce1.DeliveryKey, Reason.Other("Fail"));
            _consumer.MarkConsumed(ces.Where(ce => ce.Payload == "2").Cast<ConsumableEventId>());

            // Check if they are really marked consumed (can no longer be consumed)
            Thread.Sleep(TimeSpan.FromSeconds(visibilityTimeout + 1)); // Wait until visibilitytimeout of all items (+1) has expired
            ces = _consumer.ConsumeNext(subName, maxCount: batchSize); // Batchsize exceeds nr of events available
            Assert.Equal(1, ces.Count());
            Assert.True(ces.Any(ce => ce.Payload == "3"));
        }

        [Fact]
        public void ConsumeBatch_AllConsumedFailed()
        {
            // Arrange
            var topicName = "ConsumeBatch_AllConsumedFail";
            var subName = topicName + "_Sub1";
            var topic = _publisher.AddOrUpdateTopic(new Topic { Name = topicName });
            var sub1 = _consumer.AddOrUpdateSubscription(new Subscription
            {
                Name = subName, // When ordered not set, delivery should still be ordered on publicationdateutc
                TopicSubscriptions = new List<TopicSubscription> { new TopicSubscription { TopicId = topic.Id.Value, Enabled = true } },
            });

            _publisher.Publish(topicName, payload: "1");
            Thread.Sleep(100); // To make sure publicationdateutc is not equal for each item
            _publisher.Publish(topicName, payload: "2");
            Thread.Sleep(100);
            _publisher.Publish(topicName, payload: "3");

            // Consume a batch of 2 items
            var batchSize = 2;
            var visibilityTimeout = 2; // 2 secs
            var ces = _consumer.ConsumeNext(subName, visibilityTimeout: visibilityTimeout,  maxCount: batchSize);
            Assert.Equal(batchSize, ces.Count());

            // Mark #2 consumed
            var ce2 = ces.Single(ce => ce.Payload == "2");
            _consumer.MarkConsumed(ce2.Id, ce2.DeliveryKey);

            // Mark the whole batch consumed, including #2
            Assert.ThrowsAny<Exception>(() => _consumer.MarkConsumed(ces.Cast<ConsumableEventId>())); // Will throw either an ArgumentException or (when parallel is enabled for the repo (mssql)) AggregateException

            // Check if marking consumed failed for all
            Thread.Sleep(TimeSpan.FromSeconds(visibilityTimeout + 1)); // Wait until visibilitytimeout of all items (+1) has expired
            ces = _consumer.ConsumeNext(subName, maxCount: batchSize);
            Assert.Equal(2, ces.Count());
            Assert.True(ces.Any(ce => ce.Payload == "1")); // 1 must also be redelivered, since marking consumed has failed
            Assert.True(ces.Any(ce => ce.Payload == "3"));
        }
    }
}
