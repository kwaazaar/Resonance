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

        public DateTime MaxDateTime { get { return new DateTime(9999, 12, 31); } }

        public PublishTests(EventingRepoFactoryFixture fixture)
        {
            _publisher = new EventPublisher(fixture.RepoFactory, DateTimeProvider.Repository, TimeSpan.Zero, InvokeOptions.NoRetries);
            _consumer = new EventConsumer(fixture.RepoFactory, TimeSpan.Zero, InvokeOptions.NoRetries);
        }

        [Fact]
        public void PublishComplete()
        {
            // Arrange
            var topicName = "Publishing.PublishTests.PublishComplete";
            var topic = _publisher.AddOrUpdateTopic(new Topic { Name = topicName });
            var sub = _consumer.AddOrUpdateSubscription(new Subscription
            {
                Name = Guid.NewGuid().ToString(),
                TopicSubscriptions = new List<TopicSubscription> { new TopicSubscription { TopicId = topic.Id.Value, Enabled = true, } }
            });

            var publicationDateUtc = DateTime.UtcNow.AddMinutes(1); // Overriding default (which is UtcNow)
            var expirationDateUtc = DateTime.UtcNow.AddMinutes(5);
            var eventName = "Name of the Event";
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
                eventName: eventName,
                publicationDateUtc: publicationDateUtc,
                expirationDateUtc: expirationDateUtc,
                functionalKey: functionalKey,
                priority: priority,
                headers: headers,
                payload: payload);

            // Assert
            Assert.NotNull(topicEvent.Id);
            Assert.Equal(topic.Id.Value, topicEvent.TopicId);
            Assert.Equal(eventName, topicEvent.EventName);
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
            Assert.Equal(eventName, consumableEvent.EventName);
            Assert.Equal(functionalKey, consumableEvent.FunctionalKey);
            Assert.Equal(payload, consumableEvent.Payload);
            // Other properties do not exist on an consumable event
        }

        [Fact]
        public void PublishMinimal()
        {
            // Arrange
            var topicName = "Publishing.PublishTests.PublishMinimal";
            var topic = _publisher.AddOrUpdateTopic(new Topic { Name = topicName });
            var sub = _consumer.AddOrUpdateSubscription(new Subscription
            {
                Name = Guid.NewGuid().ToString(),
                TopicSubscriptions = new List<TopicSubscription> { new TopicSubscription { TopicId = topic.Id.Value, Enabled = true, } }
            });

            // Act
            var topicEvent = _publisher.Publish(topicName);
            Thread.Sleep(TimeSpan.FromMilliseconds(200)); // To make sure that DateTime.UtcNow returns a later datetime than during the Publish-call

            // Assert
            Assert.NotNull(topicEvent.Id);
            Assert.Null(topicEvent.EventName);
            Assert.Equal(topic.Id.Value, topicEvent.TopicId);
            Assert.NotNull(topicEvent.PublicationDateUtc);
            Assert.True(topicEvent.PublicationDateUtc < DateTime.UtcNow); // Older than now
            Assert.True(topicEvent.PublicationDateUtc > DateTime.UtcNow.AddMinutes(-1)); // Less than a minute old
            Assert.Equal(MaxDateTime, topicEvent.ExpirationDateUtc);
            Assert.Equal(string.Empty, topicEvent.FunctionalKey);
            Assert.Equal(100, topicEvent.Priority);
            Assert.Null(topicEvent.Headers);
            Assert.Null(topicEvent.PayloadId);

            // Act
            var consumableEvent = _consumer.ConsumeNext(sub.Name).SingleOrDefault();

            // Assert
            Assert.NotNull(consumableEvent);
            Assert.NotNull(consumableEvent.Id);
            Assert.Null(consumableEvent.EventName);
            Assert.Equal(string.Empty, consumableEvent.FunctionalKey);
            Assert.Null(consumableEvent.Payload);
            // Other properties do not exist on an consumable event
        }

        [Fact]
        public void PublishNoLog()
        {
            // Arrange
            var topicName = "Publishing.PublishTests.PublishNoLog";
            var topic = _publisher.AddOrUpdateTopic(new Topic { Name = topicName, Log = false });
            var sub = _consumer.AddOrUpdateSubscription(new Subscription
            {
                Name = Guid.NewGuid().ToString(),
                TopicSubscriptions = new List<TopicSubscription> { new TopicSubscription { TopicId = topic.Id.Value, Enabled = true, } }
            });

            // Act
            var topicEvent = _publisher.Publish(topicName);
            Thread.Sleep(TimeSpan.FromMilliseconds(200)); // To make sure that DateTime.UtcNow returns a later datetime than during the Publish-call

            // Assert
            Assert.Null(topicEvent.Id);
            Assert.Null(topicEvent.EventName);
            Assert.Equal(topic.Id.Value, topicEvent.TopicId);
            Assert.NotNull(topicEvent.PublicationDateUtc);
            Assert.True(topicEvent.PublicationDateUtc < DateTime.UtcNow); // Older than now
            Assert.True(topicEvent.PublicationDateUtc > DateTime.UtcNow.AddMinutes(-1)); // Less than a minute old
            Assert.Equal(MaxDateTime, topicEvent.ExpirationDateUtc);
            Assert.Equal(string.Empty, topicEvent.FunctionalKey);
            Assert.Equal(100, topicEvent.Priority);
            Assert.Null(topicEvent.Headers);
            Assert.Null(topicEvent.PayloadId);

            // Act
            var consumableEvent = _consumer.ConsumeNext(sub.Name).SingleOrDefault();

            // Assert
            Assert.NotNull(consumableEvent);
            Assert.NotNull(consumableEvent.Id);
            Assert.Null(consumableEvent.EventName);
            Assert.Equal(string.Empty, consumableEvent.FunctionalKey);
            Assert.Null(consumableEvent.Payload);
            // Other properties do not exist on an consumable event

            // Consume it, to make sure the missing topicEvent is not a problem
            _consumer.MarkConsumed(consumableEvent.Id, consumableEvent.DeliveryKey);
        }

        [Fact]
        public void PublishTakeEventNameFromHeaders()
        {
            // Arrange
            var topicName = "Publishing.PublishTakeEventNameFromHeaders";
            var topic = _publisher.AddOrUpdateTopic(new Topic { Name = topicName });
            var sub = _consumer.AddOrUpdateSubscription(new Subscription
            {
                Name = Guid.NewGuid().ToString(),
                TopicSubscriptions = new List<TopicSubscription> { new TopicSubscription { TopicId = topic.Id.Value, Enabled = true, } }
            });

            var eventNameFromHeader = "Order.Purchased";
            var headers = new Dictionary<string, string>
            {
                { "EventName", eventNameFromHeader },
                { "MessageId", Guid.NewGuid().ToString() },
            };

            // Act
            var topicEvent = _publisher.Publish(topicName,
                eventName: null, // Not passed
                headers: headers);

            // Assert
            Assert.NotNull(topicEvent.Id);
            Assert.Equal(topic.Id.Value, topicEvent.TopicId);
            Assert.NotNull(topicEvent.Headers);
            Assert.Equal(headers.Count, topicEvent.Headers.Count);
            Assert.True(headers.All(h => topicEvent.Headers.Any(teH => teH.Key == h.Key)));
            Assert.True(headers.All(h => h.Value == topicEvent.Headers[h.Key]));
            Assert.Equal(eventNameFromHeader, topicEvent.EventName);

            // Act
            var consumableEvent = _consumer.ConsumeNext(sub.Name).SingleOrDefault();

            // Assert
            Assert.NotNull(consumableEvent);
            Assert.NotNull(consumableEvent.Id);
            Assert.Equal(eventNameFromHeader, consumableEvent.EventName);
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
            _publisher.AddOrUpdateTopic(topic1); // To make sure cached subscription-config is cleared

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
        public void PublishSingleSubscriberExactFilter_Not()
        {
            // Arrange
            var topicName = "PublishSingleSubscriberExactFilter_Not";
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
                            new TopicSubscriptionFilter { Header="Headr1", MatchExpression="HdrVALUE", NotMatch = true }
                        },
                    },
                },
            });

            // Act
            var te1 = _publisher.Publish(topicName, functionalKey: "1", headers: new Dictionary<string, string> { { "Headr1", "HdrValue" }, { "Headr2", "HdrValue" } }); // We (ab)use functionalkey for correlation
            var te2 = _publisher.Publish(topicName, functionalKey: "2", headers: new Dictionary<string, string> { }); // Absent
            var te3 = _publisher.Publish(topicName, functionalKey: "3", headers: new Dictionary<string, string> { { "Headr1", "NotMatching" } }); // Present and not matching

            // Assert
            var se = _consumer.ConsumeNext(sub1.Name, visibilityTimeout: 1).SingleOrDefault();
            Assert.NotNull(se);
            Assert.Equal(te2.FunctionalKey, se.FunctionalKey); // Header absent, so match!
            _consumer.MarkConsumed(se.Id, se.DeliveryKey);
            se = _consumer.ConsumeNext(sub1.Name, visibilityTimeout: 1).SingleOrDefault();
            Assert.NotNull(se);
            Assert.Equal(te3.FunctionalKey, se.FunctionalKey); // Header present and different, so match!
            _consumer.MarkConsumed(se.Id, se.DeliveryKey);
            se = _consumer.ConsumeNext(sub1.Name, visibilityTimeout: 1).SingleOrDefault();
            Assert.Null(se); // Nothing left

            // Disable the filter
            sub1.TopicSubscriptions[0].Filtered = false;
            sub1 = _consumer.AddOrUpdateSubscription(sub1);
            Assert.False(sub1.TopicSubscriptions[0].Filtered);
            _publisher.AddOrUpdateTopic(topic1); // To make sure cached subscription-config is cleared

            var te4 = _publisher.Publish(topicName, functionalKey: "4", headers: new Dictionary<string, string> { { "Headr1", "HdrValue" }, { "Headr2", "HdrValue" } }); // We (ab)use functionalkey for correlation
            var te5 = _publisher.Publish(topicName, functionalKey: "5", headers: new Dictionary<string, string> { }); // Absent
            var te6 = _publisher.Publish(topicName, functionalKey: "6", headers: new Dictionary<string, string> { { "Headr1", "NotMatching" } }); // Present and not matching

            // Filter disabled, so everything should match
            var se4 = _consumer.ConsumeNext(sub1.Name).SingleOrDefault();
            var se5 = _consumer.ConsumeNext(sub1.Name).SingleOrDefault();
            var se6 = _consumer.ConsumeNext(sub1.Name).SingleOrDefault();
            Assert.NotNull(se4); // Must be delivered: filter may not match, but it's disabled
            Assert.Equal(te4.FunctionalKey, se4.FunctionalKey);
            Assert.NotNull(se5); // Must be delivered: filter may not match, but it's disabled
            Assert.Equal(te5.FunctionalKey, se5.FunctionalKey);
            Assert.NotNull(se6); // Must be delivered: filter may not match, but it's disabled
            Assert.Equal(te6.FunctionalKey, se6.FunctionalKey);
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
        public void PublishSingleSubscriberStartsWithFilter_Not()
        {
            // Arrange
            var topicName = "PublishSingleSubscriberStartsWithFilter_Not";
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
                            new TopicSubscriptionFilter { Header="Headr1", MatchExpression="HdR*", NotMatch = true }
                        },
                    },
                },
            });

            // Act
            var te1 = _publisher.Publish(topicName, functionalKey: "1", headers: new Dictionary<string, string> { { "Headr1", "HdrValueA" } }); // Does not match
            var te2 = _publisher.Publish(topicName, functionalKey: "2", headers: new Dictionary<string, string> { { "Headr1", "ValueAHdr" } }); // Matches
            var te3 = _publisher.Publish(topicName, functionalKey: "3", headers: new Dictionary<string, string> { }); // Matches

            // Assert
            var se = _consumer.ConsumeNext(sub1.Name).SingleOrDefault();
            Assert.NotNull(se);
            Assert.Equal(te2.FunctionalKey, se.FunctionalKey);
            se = _consumer.ConsumeNext(sub1.Name).SingleOrDefault();
            Assert.NotNull(se);
            Assert.Equal(te3.FunctionalKey, se.FunctionalKey);
            se = _consumer.ConsumeNext(sub1.Name).SingleOrDefault();
            Assert.Null(se);
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
        public void PublishSingleSubscriberEndsWithFilter_Not()
        {
            // Arrange
            var topicName = "PublishSingleSubscriberEndsWithFilter_Not";
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
                            new TopicSubscriptionFilter { Header="Headr1", MatchExpression="*VALUEa", NotMatch = true }
                        },
                    },
                },
            });

            // Act
            var te1 = _publisher.Publish(topicName, functionalKey: "1", headers: new Dictionary<string, string> { { "Headr1", "HdrValueA" } }); // No match
            var te2 = _publisher.Publish(topicName, functionalKey: "2", headers: new Dictionary<string, string> { { "Headr1", "HdrValueB" } }); // Match
            var te3 = _publisher.Publish(topicName, functionalKey: "3", headers: new Dictionary<string, string> { }); // Match

            // Assert
            var se = _consumer.ConsumeNext(sub1.Name).SingleOrDefault();
            Assert.NotNull(se); // Value ends with "ValueA" (case insensitive)
            Assert.Equal(te2.FunctionalKey, se.FunctionalKey);
            se = _consumer.ConsumeNext(sub1.Name).SingleOrDefault();
            Assert.NotNull(se); // Value ends with "ValueA" (case insensitive)
            Assert.Equal(te3.FunctionalKey, se.FunctionalKey);
            se = _consumer.ConsumeNext(sub1.Name).SingleOrDefault();
            Assert.Null(se);
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

        [Fact]
        public void PublishSingleSubscriberStartsWithEndsWithFilter_Not()
        {
            // Arrange
            var topicName = "PublishSingleSubscriberSWEWFilter_Not";
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
                            new TopicSubscriptionFilter { Header="Headr1", MatchExpression="*VaLuE*", NotMatch = true}
                        },
                    },
                },
            });

            // Act
            var te1 = _publisher.Publish(topicName, functionalKey: "1", headers: new Dictionary<string, string> { { "Headr1", "XValueY" } }); // No match
            var te2 = _publisher.Publish(topicName, functionalKey: "2", headers: new Dictionary<string, string> { { "Headr2", "MValueN" } }); // Match
            var te3 = _publisher.Publish(topicName, functionalKey: "3", headers: new Dictionary<string, string> { { "Headr1", "PValue" } }); // No match
            var te4 = _publisher.Publish(topicName, functionalKey: "4", headers: new Dictionary<string, string> { { "Headr1", "ValueQ" } }); // No match
            var te5 = _publisher.Publish(topicName, functionalKey: "5", headers: new Dictionary<string, string> { { "Headr1", "NoMatch" } }); // Matches
            var te6 = _publisher.Publish(topicName, functionalKey: "6", headers: new Dictionary<string, string> { }); // Matches

            // Assert
            var se = _consumer.ConsumeNext(sub1.Name).SingleOrDefault();
            Assert.NotNull(se);
            Assert.Equal(te2.FunctionalKey, se.FunctionalKey);
            se = _consumer.ConsumeNext(sub1.Name).SingleOrDefault();
            Assert.NotNull(se);
            Assert.Equal(te5.FunctionalKey, se.FunctionalKey);
            se = _consumer.ConsumeNext(sub1.Name).SingleOrDefault();
            Assert.NotNull(se);
            Assert.Equal(te6.FunctionalKey, se.FunctionalKey);
            se = _consumer.ConsumeNext(sub1.Name).SingleOrDefault();
            Assert.Null(se);
        }
    }
}