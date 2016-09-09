using Resonance.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Resonance
{
    public interface IEventingRepo
    {
        // Transaction Management
        /// <summary>
        /// Starts a new transaction
        /// </summary>
        void BeginTransaction();
        void RollbackTransaction();
        void CommitTransaction();

        // Topic management
        IEnumerable<Topic> GetTopics(string partOfName = null);
        Topic GetTopic(string id);
        Topic GetTopicByName(string name);
        Topic AddOrUpdateTopic(Topic topic);
        void DeleteTopic(string id, bool inclSubscriptions);

        // Subscription management
        IEnumerable<Subscription> GetSubscriptions(string partOfName =  null, string topicId = null);
        Subscription GetSubscription(string id);
        Subscription AddOrUpdateSubscription(Subscription subscription);
        void DeleteSubscription(string id);

        // Stats
        IEnumerable<TopicStats> GetTopicStatistics(string id);

        // Publication
        string StorePayload(string payload);
        string AddTopicEvent(TopicEvent topicEvent);
        string AddSubscriptionEvent(SubscriptionEvent subscriptionEvent);

    }
}
