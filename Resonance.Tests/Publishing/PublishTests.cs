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
            var topic = _publisher.AddOrUpdateTopicAsync(new Topic { Name = topicName }).Result;
            var sub = _consumer.AddOrUpdateSubscription(new Subscription
            {
                Name = Guid.NewGuid().ToString(),
                TopicSubscriptions = new List<TopicSubscription> { new TopicSubscription { TopicId = topic.Id.Value, Enabled = true, } }
            });

            var publicationDateUtc = DateTime.UtcNow.AddMinutes(1); // Overriding default (which is UtcNow)
            var expirationDateUtc = DateTime.UtcNow.AddMinutes(5);
            var functionalKey = "F1";
            var priority = 10;
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
                priority: priority,
                headers: headers,
                payload: payload);

            // Assert
            Assert.NotNull(topicEvent.Id);
            Assert.Equal(topic.Id.Value, topicEvent.TopicId);
            Assert.Equal(publicationDateUtc, topicEvent.PublicationDateUtc);
            Assert.Equal(expirationDateUtc, topicEvent.ExpirationDateUtc);
            Assert.Equal(functionalKey, topicEvent.FunctionalKey);
            Assert.Equal(priority, topicEvent.Priority);
            Assert.NotNull(topicEvent.Headers);
            Assert.Equal(headers.Count, topicEvent.Headers.Count);
            Assert.True(headers.All(h => topicEvent.Headers.Any(teH => teH.Key == h.Key)));
            Assert.True(headers.All(h => h.Value == topicEvent.Headers[h.Key]));
            Assert.NotNull(topicEvent.PayloadId); // Cannot check payload here

            // Act
            var consumableEvent = _consumer.ConsumeNext(sub.Name).SingleOrDefault();

            // Assert
            Assert.NotNull(consumableEvent);
            Assert.NotNull(consumableEvent.Id);
            Assert.Equal(functionalKey, consumableEvent.FunctionalKey);
            Assert.Equal(payload, consumableEvent.Payload);
            // Other properties do not exist on an consumable event
        }

        [Fact]
        public void PublishMinimal()
        {
            // Arrange
            var topicName = "Publishing.PublishTests.PublishMinimal";
            var topic = _publisher.AddOrUpdateTopicAsync(new Topic { Name = topicName }).Result;
            var sub = _consumer.AddOrUpdateSubscription(new Subscription
            {
                Name = Guid.NewGuid().ToString(),
                TopicSubscriptions = new List<TopicSubscription> { new TopicSubscription { TopicId = topic.Id.Value, Enabled = true, } }
            });

            // Act
            var topicEvent = _publisher.PublishAsync(topicName).Result;
            Thread.Sleep(TimeSpan.FromMilliseconds(200)); // To make sure that DateTime.UtcNow returns a later datetime than during the Publish-call

            // Assert
            Assert.NotNull(topicEvent.Id);
            Assert.Equal(topic.Id.Value, topicEvent.TopicId);
            Assert.NotNull(topicEvent.PublicationDateUtc);
            Assert.True(topicEvent.PublicationDateUtc < DateTime.UtcNow); // Older than now
            Assert.True(topicEvent.PublicationDateUtc > DateTime.UtcNow.AddMinutes(-1)); // Less than a minute old
            Assert.Null(topicEvent.ExpirationDateUtc);
            Assert.Null(topicEvent.FunctionalKey);
            Assert.Equal(0, topicEvent.Priority);
            Assert.Null(topicEvent.Headers);
            Assert.Null(topicEvent.PayloadId);

            // Act
            var consumableEvent = _consumer.ConsumeNext(sub.Name).SingleOrDefault();

            // Assert
            Assert.NotNull(consumableEvent);
            Assert.NotNull(consumableEvent.Id);
            Assert.Null(consumableEvent.FunctionalKey);
            Assert.Null(consumableEvent.Payload);
            // Other properties do not exist on an consumable event
        }

        [Fact]
        public void PublishMultipleSubscribersNoFilter()
        {
            // Arrange
            var topicName = "PublishMultipleSubscribersNoFilter";
            var topic1 = _publisher.AddOrUpdateTopic(new Topic { Name = topicName + "1" });
            var topic2 = _publisher.AddOrUpdateTopic(new Topic { Name = topicName + "2" });
            var sub1 = _consumer.AddOrUpdateSubscription(new Subscription
            {
                Name = Guid.NewGuid().ToString(),
                TopicSubscriptions = new List<TopicSubscription>
                {
                    new TopicSubscription { TopicId = topic1.Id.Value, Enabled = true },
                },
            });
            var sub2 = _consumer.AddOrUpdateSubscription(new Subscription
            {
                Name = Guid.NewGuid().ToString(),
                TopicSubscriptions = new List<TopicSubscription>
                {
                    new TopicSubscription { TopicId = topic1.Id.Value, Enabled = true },
                },
            });
            var sub3 = _consumer.AddOrUpdateSubscription(new Subscription
            {
                Name = Guid.NewGuid().ToString(),
                TopicSubscriptions = new List<TopicSubscription>
                {
                    new TopicSubscription { TopicId = topic2.Id.Value, Enabled = true },
                },
            });
            // Result:
            // Topic1: Sub1 + Sub2
            // Topic2: Sub3

            // Act
            var te1 = _publisher.Publish(topicName + "1", functionalKey: "1"); // We (ab)use functionalkey for correlation
            var te2 = _publisher.Publish(topicName + "2", functionalKey: "2");

            // Assert
            var se1 = _consumer.ConsumeNext(sub1.Name).SingleOrDefault();
            Assert.NotNull(se1);
            Assert.Equal(te1.FunctionalKey, se1.FunctionalKey);
            var se2 = _consumer.ConsumeNext(sub2.Name).SingleOrDefault();
            Assert.NotNull(se2);
            Assert.Equal(te1.FunctionalKey, se2.FunctionalKey); // te1 is also sent to sub2
            var se3 = _consumer.ConsumeNext(sub3.Name).SingleOrDefault();
            Assert.NotNull(se3);
            Assert.Equal(te2.FunctionalKey, se3.FunctionalKey); // te1 is also sent to sub2

            Assert.Null(_consumer.ConsumeNext(sub1.Name).SingleOrDefault()); // No more events for all subscribers (previously consumed events are still invisible
            Assert.Null(_consumer.ConsumeNext(sub2.Name).SingleOrDefault());
            Assert.Null(_consumer.ConsumeNext(sub3.Name).SingleOrDefault());
        }

        [Fact]
        public void PublishSingleSubscriberExactFilter()
        {
            // Arrange
            var topicName = "PublishSingleSubscriberExactFilter";
            var topic1 = _publisher.AddOrUpdateTopic(new Topic { Name = topicName });
            var sub1 = _consumer.AddOrUpdateSubscription(new Subscription
            {
                Name = Guid.NewGuid().ToString(),
                TopicSubscriptions = new List<TopicSubscription>
                {
                    new TopicSubscription
                    {
                        TopicId = topic1.Id.Value, Enabled = true,
                        Filtered = true,
                        Filters = new List<TopicSubscriptionFilter>
                        {
                            new TopicSubscriptionFilter { Header="Headr1", MatchExpression="HdrVALUE", }
                        },
                    },
                },
            });

            // Act
            var te1 = _publisher.Publish(topicName, functionalKey: "1", headers: new Dictionary<string, string> { { "Headr1", "HdrValue" }, { "Headr2", "HdrValue" } }); // We (ab)use functionalkey for correlation
            var te2 = _publisher.Publish(topicName, functionalKey: "2", headers: new Dictionary<string, string> { });

            // Assert
            var se1 = _consumer.ConsumeNext(sub1.Name).SingleOrDefault();
            Assert.NotNull(se1);
            Assert.Equal(te1.FunctionalKey, se1.FunctionalKey); // Must match, filters are always case insensitive
            var se2 = _consumer.ConsumeNext(sub1.Name).SingleOrDefault();
            Assert.Null(se2); // Does not have "Headr1"-header
            _consumer.MarkConsumed(se1.Id, se1.DeliveryKey);

            // Disable the filter
            sub1.TopicSubscriptions[0].Filtered = false;
            sub1 = _consumer.AddOrUpdateSubscription(sub1);
            Assert.False(sub1.TopicSubscriptions[0].Filtered);

            var te3 = _publisher.Publish(topicName, functionalKey: "3", headers: new Dictionary<string, string> { });
            var se3 = _consumer.ConsumeNext(sub1.Name).SingleOrDefault();
            Assert.NotNull(se3); // Must be delivered: filter may not match, but it's disabled
            Assert.Equal(te3.FunctionalKey, se3.FunctionalKey);
            _consumer.MarkConsumed(se3.Id, se3.DeliveryKey);

            var te4 = _publisher.Publish(topicName, functionalKey: "4");
            var se4 = _consumer.ConsumeNext(sub1.Name).SingleOrDefault();
            Assert.NotNull(se4); // Similar test as with se3, but now the whole header-parameter is missing
            Assert.Equal(te4.FunctionalKey, se4.FunctionalKey);
        }

        [Fact]
        public void PublishSingleSubscriberStartsWithFilter()
        {
            // Arrange
            var topicName = "PublishSingleSubscriberStartsWithFilter";
            var topic1 = _publisher.AddOrUpdateTopic(new Topic { Name = topicName });
            var sub1 = _consumer.AddOrUpdateSubscription(new Subscription
            {
                Name = Guid.NewGuid().ToString(),
                TopicSubscriptions = new List<TopicSubscription>
                {
                    new TopicSubscription
                    {
                        TopicId = topic1.Id.Value, Enabled = true,
                        Filtered = true,
                        Filters = new List<TopicSubscriptionFilter>
                        {
                            new TopicSubscriptionFilter { Header="Headr1", MatchExpression="HdR*", }
                        },
                    },
                },
            });

            // Act
            var te1 = _publisher.Publish(topicName, functionalKey: "1", headers: new Dictionary<string, string> { { "Headr1", "HdrValueA" } }); // We (ab)use functionalkey for correlation
            var te2 = _publisher.Publish(topicName, functionalKey: "2", headers: new Dictionary<string, string> { { "Headr1", "ValueAHdr" } });
            var te3 = _publisher.Publish(topicName, functionalKey: "3", headers: new Dictionary<string, string> { });

            // Assert
            var se1 = _consumer.ConsumeNext(sub1.Name).SingleOrDefault();
            Assert.NotNull(se1); // Value starts with "Hdr"
            Assert.Equal(te1.FunctionalKey, se1.FunctionalKey);
            var se2 = _consumer.ConsumeNext(sub1.Name).SingleOrDefault();
            Assert.Null(se2);
            var se3 = _consumer.ConsumeNext(sub1.Name).SingleOrDefault();
            Assert.Null(se3);
        }

        [Fact]
        public void PublishSingleSubscriberEndsWithFilter()
        {
            // Arrange
            var topicName = "PublishSingleSubscriberEndsWithFilter";
            var topic1 = _publisher.AddOrUpdateTopic(new Topic { Name = topicName });
            var sub1 = _consumer.AddOrUpdateSubscription(new Subscription
            {
                Name = Guid.NewGuid().ToString(),
                TopicSubscriptions = new List<TopicSubscription>
                {
                    new TopicSubscription
                    {
                        TopicId = topic1.Id.Value, Enabled = true,
                        Filtered = true,
                        Filters = new List<TopicSubscriptionFilter>
                        {
                            new TopicSubscriptionFilter { Header="Headr1", MatchExpression="*VALUEa", }
                        },
                    },
                },
            });

            // Act
            var te1 = _publisher.Publish(topicName, functionalKey: "1", headers: new Dictionary<string, string> { { "Headr1", "HdrValueA" } }); // We (ab)use functionalkey for correlation
            var te2 = _publisher.Publish(topicName, functionalKey: "2", headers: new Dictionary<string, string> { { "Headr1", "HdrValueB" } });
            var te3 = _publisher.Publish(topicName, functionalKey: "3", headers: new Dictionary<string, string> { });

            // Assert
            var se1 = _consumer.ConsumeNext(sub1.Name).SingleOrDefault();
            Assert.NotNull(se1); // Value ends with "ValueA" (case insensitive)
            Assert.Equal(te1.FunctionalKey, se1.FunctionalKey);
            var se2 = _consumer.ConsumeNext(sub1.Name).SingleOrDefault();
            Assert.Null(se2);
            var se3 = _consumer.ConsumeNext(sub1.Name).SingleOrDefault();
            Assert.Null(se3);
        }

        [Fact]
        public void PublishSingleSubscriberStartsWithEndsWithFilter()
        {
            // Arrange
            var topicName = "PublishSingleSubscriberSWEWFilter";
            var topic1 = _publisher.AddOrUpdateTopic(new Topic { Name = topicName });
            var sub1 = _consumer.AddOrUpdateSubscription(new Subscription
            {
                Name = Guid.NewGuid().ToString(),
                TopicSubscriptions = new List<TopicSubscription>
                {
                    new TopicSubscription
                    {
                        TopicId = topic1.Id.Value, Enabled = true,
                        Filtered = true,
                        Filters = new List<TopicSubscriptionFilter>
                        {
                            new TopicSubscriptionFilter { Header="Headr1", MatchExpression="*VaLuE*", }
                        },
                    },
                },
            });

            // Act
            var te1 = _publisher.Publish(topicName, functionalKey: "1", headers: new Dictionary<string, string> { { "Headr1", "XValueY" } }); // 1, 3 and 4 must match
            var te2 = _publisher.Publish(topicName, functionalKey: "2", headers: new Dictionary<string, string> { { "Headr2", "MValueN" } });
            var te3 = _publisher.Publish(topicName, functionalKey: "3", headers: new Dictionary<string, string> { { "Headr1", "PValue" } });
            var te4 = _publisher.Publish(topicName, functionalKey: "4", headers: new Dictionary<string, string> { { "Headr1", "ValueQ" } });
            var te5 = _publisher.Publish(topicName, functionalKey: "5", headers: new Dictionary<string, string> { { "Headr1", "NoMatch" } });
            var te6 = _publisher.Publish(topicName, functionalKey: "6", headers: new Dictionary<string, string> { });

            // Assert
            var se1 = _consumer.ConsumeNext(sub1.Name).SingleOrDefault();
            Assert.NotNull(se1);
            Assert.Equal(te1.FunctionalKey, se1.FunctionalKey);
            var se3 = _consumer.ConsumeNext(sub1.Name).SingleOrDefault();
            Assert.NotNull(se3);
            Assert.Equal(te3.FunctionalKey, se3.FunctionalKey);
            var se4 = _consumer.ConsumeNext(sub1.Name).SingleOrDefault();
            Assert.NotNull(se4);
            Assert.Equal(te4.FunctionalKey, se4.FunctionalKey);
            var seNext = _consumer.ConsumeNext(sub1.Name).SingleOrDefault();
            Assert.Null(seNext);
        }
    }
}