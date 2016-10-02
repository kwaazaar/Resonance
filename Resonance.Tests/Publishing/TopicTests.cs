using Resonance.Models;
using Resonance.Repo;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Resonance.Tests.Publishing
{
    [Collection("EventingRepo")]
    public class TopicTests
    {
        private readonly IEventPublisher _publisher;
        private readonly IEventConsumer _consumer;

        public TopicTests(EventingRepoFactoryFixture fixture)
        {
            _publisher = new EventPublisher(fixture.RepoFactory);
            _consumer = new EventConsumer(fixture.RepoFactory);
        }

        [Fact]
        public void AddTopic()
        {
            // Arrange
            var topicName = "Publishing.TopicTests.AddTopic";
            var topicNotes = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";

            // Act
            var returnedTopic = _publisher.AddOrUpdateTopicAsync(new Topic { Name = topicName, Notes = topicNotes }).Result;

            // Assert
            Assert.Equal(topicName, returnedTopic.Name);
            Assert.Equal(topicNotes, returnedTopic.Notes);
            Assert.True(returnedTopic.Id.HasValue);
            Assert.True(returnedTopic.Id.Value > 0);
        }

        [Fact]
        public void UpdateTopic()
        {
            // Arrange
            var topicName = "Publishing.TopicTests.UpdateTopic";
            var topicNotes = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";

            // Act
            var addedTopic = _publisher.AddOrUpdateTopicAsync(new Topic { Name = topicName, Notes = topicNotes }).Result;
            var topicToBeUpdated = new Topic { Id = addedTopic.Id, Name = addedTopic.Name + "_updated", Notes = "updated_" + addedTopic.Notes };
            var updatedTopic = _publisher.AddOrUpdateTopicAsync(topicToBeUpdated).Result;

            // Assert
            Assert.Equal(updatedTopic.Id.Value, addedTopic.Id.Value); // Use id from addedTopic, to make sure the id was not modified by the EventPublisher itself
            Assert.Equal(updatedTopic.Name, topicToBeUpdated.Name);
            Assert.Equal(updatedTopic.Notes, topicToBeUpdated.Notes);
        }

        [Fact]
        public void DeleteTopic()
        {
            // Arrange
            var topicName = "Publishing.TopicTests.DeleteTopic";
            var topicNotes = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";

            // Act
            var addedTopic = _publisher.AddOrUpdateTopicAsync(new Topic { Name = topicName, Notes = topicNotes }).Result;
            _publisher.DeleteTopicAsync(addedTopic.Id.Value, true).Wait();

            // Assert
            Assert.Null(_publisher.GetTopicAsync(addedTopic.Id.Value).Result);

            // Act
            var addedTopicWithSubscriptions = _publisher.AddOrUpdateTopicAsync(new Topic { Name = topicName + "_WithSubs", Notes = topicNotes }).Result;
            var sub1 = _consumer.AddOrUpdateSubscriptionAsync(new Subscription
            {
                Name = addedTopicWithSubscriptions.Name + "_Sub1",
                TopicSubscriptions = new List<TopicSubscription>
                {
                    new TopicSubscription
                    {
                        TopicId = addedTopicWithSubscriptions.Id.Value, Enabled = true, Filtered = true,
                        Filters = new List<TopicSubscriptionFilter>
                        {
                            new TopicSubscriptionFilter { Header = "EventType", MatchExpression = "Order.*" },
                        },
                    },
                },
            }).Result;
            _publisher.PublishAsync(addedTopicWithSubscriptions.Name, payload: "test").Wait(); // Make sure there are also TopicEvents and SubscriptionEvents

            // Assert/act
            Assert.ThrowsAny<Exception>(() => _publisher.DeleteTopicAsync(addedTopicWithSubscriptions.Id.Value, false).Wait());
            Assert.NotNull(_publisher.GetTopicAsync(addedTopicWithSubscriptions.Id.Value).Result);

            // Act
            _publisher.DeleteTopicAsync(addedTopicWithSubscriptions.Id.Value, true).Wait();
            
            // Assert
            Assert.Null(_publisher.GetTopicAsync(addedTopicWithSubscriptions.Id.Value).Result);
        }

        [Fact]
        public void GetTopicById()
        {
            // Arrange
            var topicName = "Publishing.TopicTests.GetTopicById";
            _publisher.AddOrUpdateTopicAsync(new Topic { Name = Guid.NewGuid().ToString() }).Wait(); // Add another to make sure it actually finds it
            var topicId = _publisher.AddOrUpdateTopicAsync(new Topic { Name = topicName }).Result.Id.Value;

            // Act
            var topic = _publisher.GetTopicAsync(topicId).Result;

            // Assert
            Assert.NotNull(topic);
            Assert.Equal(topicId, topic.Id.Value);
        }

        [Fact]
        public void GetTopicByName()
        {
            // Arrange
            var topicName = "Publishing.TopicTests.GetTopicByName";
            _publisher.AddOrUpdateTopicAsync(new Topic { Name = topicName + "!" }).Wait(); // Add look-a-likes
            _publisher.AddOrUpdateTopicAsync(new Topic { Name = "!" + topicName }).Wait();
            var topicToBeFound = _publisher.AddOrUpdateTopicAsync(new Topic { Name = topicName }).Result;

            // Act
            var topic = _publisher.GetTopicByNameAsync(topicName).Result;

            // Assert
            Assert.NotNull(topic);
            Assert.Equal(topicToBeFound.Id.Value, topic.Id.Value);
            Assert.Equal(topicToBeFound.Name, topic.Name);
            Assert.Equal(topicToBeFound.Notes, topic.Notes);
        }

        [Fact]
        public void GetTopics()
        {
            // Arrange
            var topicName = "Publishing.TopicTests.GetTopics";
            _publisher.AddOrUpdateTopicAsync(new Topic { Name = Guid.NewGuid().ToString() }).Wait();
            _publisher.AddOrUpdateTopicAsync(new Topic { Name = topicName + "1" }).Wait();
            _publisher.AddOrUpdateTopicAsync(new Topic { Name = topicName + "2" }).Wait();
            _publisher.AddOrUpdateTopicAsync(new Topic { Name = topicName + "3" }).Wait();

            // Act
            var topics = _publisher.GetTopicsAsync().Result;

            // Assert
            Assert.NotNull(topics);
            Assert.True(topics.Count() >= 4); // Maybe more, which were added by other tests
            Assert.True(topics.Any((t) => t.Name == topicName + "1"));
            Assert.True(topics.Any((t) => t.Name == topicName + "2"));
            Assert.True(topics.Any((t) => t.Name == topicName + "3"));

            // Act
            topics = _publisher.GetTopicsAsync(topicName).Result;
            Assert.NotNull(topics);
            Assert.True(topics.Count() == 3);
            Assert.True(topics.Any((t) => t.Name == topicName + "1"));
            Assert.True(topics.Any((t) => t.Name == topicName + "2"));
            Assert.True(topics.Any((t) => t.Name == topicName + "3"));
        }
    }
}
