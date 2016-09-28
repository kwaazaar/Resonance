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

        public TopicTests(EventingRepoFactoryFixture fixture)
        {
            _publisher = new EventPublisher(fixture.RepoFactory);
        }

        [Fact]
        public void AddTopic()
        {
            // Arrange
            var topicName = "Publishing.AddTopic";
            var topicNotes = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";

            // Act
            var returnedTopic = _publisher.AddOrUpdateTopic(new Topic { Name = topicName, Notes = topicNotes });

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
            var topicName = "Publishing.UpdateTopic";
            var topicNotes = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";

            // Act
            var addedTopic = _publisher.AddOrUpdateTopic(new Topic { Name = topicName, Notes = topicNotes });
            var topicToBeUpdated = new Topic { Id = addedTopic.Id, Name = addedTopic.Name + "_updated", Notes = "updated_" + addedTopic.Notes };
            var updatedTopic = _publisher.AddOrUpdateTopic(topicToBeUpdated);

            // Assert
            Assert.Equal(updatedTopic.Id.Value, addedTopic.Id.Value); // Use id from addedTopic, to make sure the id was not modified by the EventPublisher itself
            Assert.Equal(updatedTopic.Name, topicToBeUpdated.Name);
            Assert.Equal(updatedTopic.Notes, topicToBeUpdated.Notes);
        }

        [Fact]
        public void DeleteTopic()
        {
            // Arrange
            var topicName = "Publishing.DeleteTopic";
            var topicNotes = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";

            // Act
            var addedTopic = _publisher.AddOrUpdateTopic(new Topic { Name = topicName, Notes = topicNotes });
            _publisher.DeleteTopic(addedTopic.Id.Value, true); // No subscriptions to delete, must add to future test

            // Assert
            Assert.Null(_publisher.GetTopic(addedTopic.Id.Value));
        }

        [Fact]
        public void GetTopicById()
        {
            // Arrange
            var topicName = "Publishing.GetTopicById";
            _publisher.AddOrUpdateTopic(new Topic { Name = Guid.NewGuid().ToString() }); // Add another to make sure it actually finds it
            var topicId = _publisher.AddOrUpdateTopic(new Topic { Name = topicName }).Id.Value;

            // Act
            var topic = _publisher.GetTopic(topicId);

            // Assert
            Assert.NotNull(topic);
            Assert.Equal(topicId, topic.Id.Value);
        }

        [Fact]
        public void GetTopicByName()
        {
            // Arrange
            var topicName = "Publishing.GetTopicByName";
            _publisher.AddOrUpdateTopic(new Topic { Name = topicName + "!" }); // Add look-a-likes
            _publisher.AddOrUpdateTopic(new Topic { Name = "!" + topicName });
            var topicToBeFound = _publisher.AddOrUpdateTopic(new Topic { Name = topicName });

            // Act
            var topic = _publisher.GetTopicByName(topicName);

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
            var topicName = "Publishing.GetTopics";
            _publisher.AddOrUpdateTopic(new Topic { Name = Guid.NewGuid().ToString() });
            _publisher.AddOrUpdateTopic(new Topic { Name = topicName + "1" });
            _publisher.AddOrUpdateTopic(new Topic { Name = topicName + "2" });
            _publisher.AddOrUpdateTopic(new Topic { Name = topicName + "3" });

            // Act
            var topics = _publisher.GetTopics();

            // Assert
            Assert.NotNull(topics);
            Assert.True(topics.Count() >= 4); // Maybe more, which were added by other tests
            Assert.True(topics.Any((t) => t.Name == topicName + "1"));
            Assert.True(topics.Any((t) => t.Name == topicName + "2"));
            Assert.True(topics.Any((t) => t.Name == topicName + "3"));

            // Act
            topics = _publisher.GetTopics(topicName);
            Assert.NotNull(topics);
            Assert.True(topics.Count() == 3);
            Assert.True(topics.Any((t) => t.Name == topicName + "1"));
            Assert.True(topics.Any((t) => t.Name == topicName + "2"));
            Assert.True(topics.Any((t) => t.Name == topicName + "3"));
        }
    }
}
