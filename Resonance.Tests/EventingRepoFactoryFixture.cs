using Dapper;
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

        public void CleanDb(IDbConnection conn)
        {
            var cmd = conn.CreateCommand();
            cmd.CommandText = "delete from lastconsumedsubscriptionevent;"
                + "delete from consumedsubscriptionevent;"
                + "delete from failedsubscriptionevent;"
                + "delete from subscriptionevent;"
                + "delete from topicevent;"
                + "delete from eventpayload;"
                + "delete from topicsubscriptionfilter;"
                + "delete from topicsubscription;"
                + "delete from subscription;"
                + "delete from topic;";
            cmd.ExecuteNonQuery();
        }

        public List<string> GetEventNamesForFailedEvents(Int64 subscriptionId)
        {
            var useMySql = (Configuration["UseMySql"] == "true"); // Can be set from environment variable
            var connectionString = Configuration.GetConnectionString(useMySql ? "Resonance.MySql" : "Resonance.MsSql");


            if (useMySql)
            {
                using (var conn = new MySqlConnection(connectionString))
                {
                    conn.Open();
                    return conn.Query<string>("select EventName from failedsubscriptionevent where SubscriptionId = @subscriptionId;", new { subscriptionId = subscriptionId }).ToList();
                }
            }
            else
            {
                using (var conn = new SqlConnection(connectionString))
                {
                    conn.Open();
                    return conn.Query<string>("select EventName from failedsubscriptionevent where SubscriptionId = @subscriptionId;", new { subscriptionId = subscriptionId }).ToList();
                }
            }
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
