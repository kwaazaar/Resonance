using Resonance.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Resonance.Tests.Publishing
{
    [Collection("EventingRepo")]
    public class PublishTests
    {
        private readonly IEventPublisher _publisher;
        private readonly IEventConsumer _consumer;

        public PublishTests(EventingRepoFactoryFixture fixture)
        {
            _publisher = new EventPublisher(fixture.RepoFactory);
            _consumer = new EventConsumer(fixture.RepoFactory);
        }

        [Fact]
        public void PublishComplete()
        {
            // Arrange
            var topicName = "Publishing.PublishTests.PublishComplete";
            var topic = _publisher.AddOrUpdateTopic(new Topic { Name = topicName }).Result;

            var publicationDateUtc = DateTime.UtcNow.AddMinutes(1); // Overriding default (which is UtcNow)
            var expirationDateUtc = DateTime.UtcNow.AddMinutes(5);
            var functionalKey = "F1";
            var headers = new Dictionary<string, string>
            {
                { "EventName", "Order.Purchased" },
                { "MessageId", Guid.NewGuid().ToString() },
            };
            var payload = "Hello";

            // Act
            var topicEvent = _publisher.Publish(topicName,
                publicationDateUtc: publicationDateUtc,
                expirationDateUtc: expirationDateUtc,
                functionalKey: functionalKey,
                headers: headers,
                payload: payload).Result;

            // Assert
            Assert.NotNull(topicEvent.Id);
            Assert.Equal(topic.Id.Value, topicEvent.TopicId);
            Assert.Equal(publicationDateUtc, topicEvent.PublicationDateUtc);
            Assert.Equal(expirationDateUtc, topicEvent.ExpirationDateUtc);
            Assert.Equal(functionalKey, topicEvent.FunctionalKey);
            Assert.NotNull(topicEvent.Headers);
            Assert.Equal(headers.Count, topicEvent.Headers.Count);
            Assert.True(headers.All(h => topicEvent.Headers.Any(teH => teH.Key == h.Key)));
            Assert.True(headers.All(h => h.Value == topicEvent.Headers[h.Key]));
            Assert.NotNull(topicEvent.PayloadId); // Cannot check payload here
        }

        [Fact]
        public void PublishMinimal()
        {
            // Arrange
            var topicName = "Publishing.PublishTests.PublishMinimal";
            var topic = _publisher.AddOrUpdateTopic(new Topic { Name = topicName }).Result;

            // Act
            var topicEvent = _publisher.Publish(topicName).Result;
            Thread.Sleep(TimeSpan.FromMilliseconds(200)); // To make sure that DateTime.UtcNow returns a later datetime than during the Publish-call

            // Assert
            Assert.NotNull(topicEvent.Id);
            Assert.Equal(topic.Id.Value, topicEvent.TopicId);
            Assert.NotNull(topicEvent.PublicationDateUtc);
            Assert.True(topicEvent.PublicationDateUtc < DateTime.UtcNow); // Older than now
            Assert.True(topicEvent.PublicationDateUtc > DateTime.UtcNow.AddMinutes(-1)); // Less than a minute old
            Assert.Null(topicEvent.ExpirationDateUtc);
            Assert.Null(topicEvent.FunctionalKey);
            Assert.Null(topicEvent.Headers);
            Assert.Null(topicEvent.PayloadId);
        }
    }
}
