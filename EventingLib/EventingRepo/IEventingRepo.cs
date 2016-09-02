using EventingLib.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventingLib.EventingRepo
{
    interface IEventingRepo
    {
        // Topic management
        IEnumerable<Topic> GetTopics(string partOfName);
        Topic GetTopic(string id);
        Topic AddOrUpdateTopic(Topic topic);
        void DeleteTopic(string id, bool inclSubscriptions);

        // Subscription management
        IEnumerable<Subscription> GetSubcriptions(string partOfName, string topicId);
        Subscription GetSubscription(string id);
        Subscription AddOrUpdateSubscription(Subscription subscription);
        void DeleteSubscription(string id);

        // Stats
        IEnumerable<TopicStats> GetTopicStatistics(string id);
    }
}
