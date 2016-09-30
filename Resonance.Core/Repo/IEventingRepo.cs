using Resonance.Repo.InternalModels;
using Resonance.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Resonance.Repo
{
    public interface IEventingRepo : IDisposable
    {
        // Transaction Management
        /// <summary>
        /// Starts a new transaction
        /// </summary>
        void BeginTransaction();
        void RollbackTransaction();
        void CommitTransaction();

        // Topic & subscription management
        Task<IEnumerable<Topic>> GetTopics(string partOfName = null);
        Task<Topic> GetTopic(Int64 id);
        Task<Topic> GetTopicByName(string name);
        Task<Topic> AddOrUpdateTopic(Topic topic);
        Task DeleteTopic(Int64 id, bool inclSubscriptions);
        Task<IEnumerable<Subscription>> GetSubscriptions(Int64? topicId = null);
        Task<Subscription> GetSubscription(Int64 id);
        Task<Subscription> GetSubscriptionByName(string name);
        Task<Subscription> AddOrUpdateSubscription(Subscription subscription);
        Task DeleteSubscription(Int64 id);

        // Publication
        Task<Int64> StorePayload(string payload);
        Task<string> GetPayload(Int64 id);
        Task<int> DeletePayload(Int64 id);
        Task<Int64> AddTopicEvent(TopicEvent topicEvent);
        Task<Int64> AddSubscriptionEvent(SubscriptionEvent subscriptionEvent);

        // Consumption
        //IEnumerable<SubscriptionEventIdentifier> FindConsumableEventsForSubscription(Subscription subscription, int maxCount);
        //bool TryLockConsumableEvent(SubscriptionEventIdentifier sId, string deliveryKey, DateTime invisibleUntilUtc);
        Task<IEnumerable<ConsumableEvent>> ConsumeNext(string subscriptionName, int visibilityTimeout, int maxCount = 1);
        Task MarkConsumed(Int64 id, string deliveryKey);
        Task MarkFailed(Int64 id, string deliveryKey, Reason reason);
    }
}
