using Microsoft.Extensions.Configuration;
using MySql.Data.MySqlClient;
using Resonance.Repo;
using Resonance.Repo.Database;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;

namespace Resonance.Tests
{
    public class EventingRepoFactoryFixture : BaseFixture, IDisposable
    {
        public IEventingRepoFactory RepoFactory { get; set; }

        public EventingRepoFactoryFixture()
            : base()
        {
            var useMySql = (this.Configuration["UseMySql"] == "true"); // Can be set from environment variable
            Console.WriteLine("Running tests on " + (useMySql ? "MySQL" : "MS SQL Server"));
            var connectionString = this.Configuration.GetConnectionString(useMySql ? "Resonance.MySql" : "Resonance.MsSql");

            if (useMySql)
            {
                RepoFactory = new MySqlEventingRepoFactory(connectionString);
                using (var conn = new MySqlConnection(connectionString))
                {
                    conn.Open();
                    CleanDb(conn);
                }
            }
            else
            {
                RepoFactory = new MsSqlEventingRepoFactory(connectionString);
                using (var conn = new SqlConnection(connectionString))
                {
                    conn.Open();
                    CleanDb(conn);
                }
            }
        }

        private void CleanDb(IDbConnection conn)
        {
            var cmd = conn.CreateCommand();
            cmd.CommandText = "delete from LastConsumedSubscriptionEvent;"
                + "delete from ConsumedSubscriptionEvent;"
                + "delete from FailedSubscriptionEvent;"
                + "delete from SubscriptionEvent;"
                + "delete from TopicEvent;"
                + "delete from EventPayload;"
                + "delete from TopicSubscriptionFilter;"
                + "delete from TopicSubscription;"
                + "delete from Subscription;"
                + "delete from Topic;";
            cmd.ExecuteNonQuery();
        }

        public void Dispose()
        {
            Dispose(true);   
        }
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                // Nothing to do (yet)
            }
        }
    }
}
