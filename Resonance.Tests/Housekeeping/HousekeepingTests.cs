using Dapper;
using Microsoft.Extensions.Configuration;
using MySql.Data.MySqlClient;
using Resonance.Models;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
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

        private List<string> GetEventNamesForFailedEvents(Int64 subscriptionId)
        {
            var useMySql = (_fixture.Configuration["UseMySql"] == "true"); // Can be set from environment variable
            var connectionString = _fixture.Configuration.GetConnectionString(useMySql ? "Resonance.MySql" : "Resonance.MsSql");


            if (useMySql)
            {
                using (var conn = new MySqlConnection(connectionString))
                {
                    conn.Open();
                    return conn.Query<string>("select EventName from FailedSubscriptionEvent where SubscriptionId = @subscriptionId;", new { subscriptionId = subscriptionId }).ToList();
                }
            }
            else
            {
                using (var conn = new SqlConnection(connectionString))
                {
                    conn.Open();
                    return conn.Query<string>("select EventName from FailedSubscriptionEvent where SubscriptionId = @subscriptionId;", new { subscriptionId = subscriptionId }).ToList();
                }
            }
        }

        [Fact]
        public void Expiration()
        {
            // Arrange
            var topicName = "HousekeepingTests.Expiration";
            var subName = topicName + "_Sub1";
            var topic = _publisher.AddOrUpdateTopicAsync(new Topic { Name = topicName }).Result;
            var sub1 = _consumer.AddOrUpdateSubscriptionAsync(new Subscription
            {
                Name = subName,
                TopicSubscriptions = new List<TopicSubscription> { new TopicSubscription { TopicId = topic.Id.Value, Enabled = true } },
            }).Result;

            var utcNow = DateTime.UtcNow;
            _publisher.Publish(topicName, eventName: topicName + "1", expirationDateUtc: utcNow.AddSeconds(5), payload: "1");
            _publisher.Publish(topicName, eventName: topicName + "2", expirationDateUtc: utcNow.AddMinutes(15), payload: "2");

            var visibilityTimeout = 1; // Must expire asap
            var ce1 = _consumer.ConsumeNextAsync(subName, visibilityTimeout: visibilityTimeout).Result.SingleOrDefault();
            var ce2 = _consumer.ConsumeNextAsync(subName, visibilityTimeout: visibilityTimeout).Result.SingleOrDefault();
            Assert.Equal("1", ce1.Payload); // Not yet expired
            Assert.Equal("2", ce2.Payload);
            Thread.Sleep(TimeSpan.FromSeconds(5+1));

            _consumer.PerformHouseKeepingTasks();

            // Check that only 1 se in table
            var eventNames = GetEventNamesForFailedEvents(sub1.Id.Value);
            Assert.Equal(1, eventNames.Count);
            Assert.Equal(topicName + "1", eventNames.First());

        }

        [Fact]
        public void MaxDeliveries()
        {
            // Arrange
            var topicName = "HousekeepingTests.MaxDeliveries";
            var subName = topicName + "_Sub1";
            var topic = _publisher.AddOrUpdateTopicAsync(new Topic { Name = topicName }).Result;
            var sub1 = _consumer.AddOrUpdateSubscriptionAsync(new Subscription
            {
                Name = subName,
                MaxDeliveries = 1,
                TopicSubscriptions = new List<TopicSubscription> { new TopicSubscription { TopicId = topic.Id.Value, Enabled = true } },
            }).Result;

            var utcNow = DateTime.UtcNow;
            _publisher.Publish(topicName, eventName: topicName + "1", publicationDateUtc: utcNow, payload: "1");
            _publisher.Publish(topicName, eventName: topicName + "2", publicationDateUtc: utcNow.AddSeconds(1), payload: "2");

            var visibilityTimeout = 1; // Must expire asap
            var ce1 = _consumer.ConsumeNextAsync(subName, visibilityTimeout: visibilityTimeout).Result.SingleOrDefault();
            Assert.Equal("1", ce1.Payload); // Not yet maxDeliveriesReached

            _consumer.PerformHouseKeepingTasks();

            // Check that only 1 se in table
            var eventNames = GetEventNamesForFailedEvents(sub1.Id.Value);
            Assert.Equal(1, eventNames.Count);
            Assert.Equal(topicName + "1", eventNames.First());
        }
    }
}
