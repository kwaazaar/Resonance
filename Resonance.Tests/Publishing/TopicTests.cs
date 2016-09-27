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
        IEventingRepoFactory _repoFactory;

        public TopicTests(EventingRepoFactoryFixture fixture)
        {
            _repoFactory = fixture.RepoFactory;
        }

        [Fact]
        public void AddTopic()
        {
            var topicName = "Publishing.AddTopic";
            var topicNotes = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";

            using (var repo = _repoFactory.CreateRepo())
            {
                var returnedTopic = repo.AddOrUpdateTopic(new Topic { Name = topicName, Notes = topicNotes });

                // Checked returned topic
                Assert.Equal(topicName, returnedTopic.Name);
                Assert.Equal(topicNotes, returnedTopic.Notes);
                Assert.True(returnedTopic.Id.HasValue);
                Assert.True(returnedTopic.Id.Value > 0);

                var loadedTopic = repo.GetTopicByName(topicName);
                Assert.NotNull(loadedTopic);
                Assert.Equal(topicName, loadedTopic.Name);
                Assert.Equal(topicNotes, loadedTopic.Notes);
                Assert.True(loadedTopic.Id.HasValue);
                Assert.True(loadedTopic.Id.Value > 0);
            }
        }
    }
}
