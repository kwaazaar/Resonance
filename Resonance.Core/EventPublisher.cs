using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Resonance.Models;
using Newtonsoft.Json;

namespace Resonance
{
    public class EventPublisher : IEventPublisher
    {
        private IEventingRepo _repo;

        public EventPublisher(IEventingRepo repo)
        {
            _repo = repo;
        }

        public TopicEvent Publish(string topicName, DateTime? PublicationDateUtc = default(DateTime?), DateTime? ExpirationDateUtc = default(DateTime?), string FunctionalKey = null, string payload = null)
        {
            var topic = _repo.GetTopicByName(topicName);
            if (topic == null)
                throw new ArgumentException($"Topic with name {topicName} not found", "topicName");

            // Store payload
            string payloadId = (payload != null) ? _repo.StorePayload(payload) : null;

            // Store topic event
            var newTopicEvent = new TopicEvent
            {
                TopicId = topic.Id,
                PublicationDateUtc = PublicationDateUtc.GetValueOrDefault(DateTime.UtcNow),
                FunctionalKey = FunctionalKey,
                ExpirationDateUtc = ExpirationDateUtc,
                PayloadId = payloadId,
            };
            string id = _repo.AddTopicEvent(newTopicEvent);

            // Return the topic event
            newTopicEvent.Id = id;
            return newTopicEvent;
        }

        public TopicEvent Publish<T>(string topicName, DateTime? publicationDateUtc = default(DateTime?), DateTime? expirationDateUtc = default(DateTime?), string functionalKey = null, T payload = null) where T:class
        {
            string payloadAsString = null;
            if (payload != null)
                payloadAsString = JsonConvert.SerializeObject(payload); // No specific parameters: the consumer must understand the json as well

            return Publish(topicName, publicationDateUtc, expirationDateUtc, functionalKey, payloadAsString);
        }
    }
}
