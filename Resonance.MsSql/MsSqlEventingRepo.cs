using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using Dapper;
using Resonance;
using Resonance.Models;

namespace Resonance.Repo
{
    public class MsSqlEventingRepo : IEventingRepo
    {
        protected readonly IDbConnection _conn;

        public MsSqlEventingRepo(IDbConnection conn)
        {
            _conn = conn;
        }

        public Subscription AddOrUpdateSubscription(Subscription subscription)
        {
            throw new NotImplementedException();
        }

        public Topic AddOrUpdateTopic(Topic topic)
        {
            throw new NotImplementedException();
        }

        public void DeleteSubscription(string id)
        {
            throw new NotImplementedException();
        }

        public void DeleteTopic(string id, bool inclSubscriptions)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<Subscription> GetSubcriptions(string partOfName, string topicId)
        {
            return null;
        }

        public Subscription GetSubscription(string id)
        {
            throw new NotImplementedException();
        }

        public Topic GetTopic(string id)
        {
            
            throw new NotImplementedException();
        }

        public IEnumerable<Topic> GetTopics(string partOfName)
        {
            if (partOfName != null)
            {
                var parameters = new Dictionary<string, object>
                {
                    { "@partOfName", $"%{partOfName}%"}
                };
                return _conn.Query<Topic>("select * from Topic" +
                    " where Name like @partOfName", parameters);
            }
            else
                return _conn.Query<Topic>("select * from Topic");
        }

        public IEnumerable<TopicStats> GetTopicStatistics(string id)
        {
            throw new NotImplementedException();
        }
    }
}
