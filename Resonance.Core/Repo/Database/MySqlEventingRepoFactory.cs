using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace Resonance.Repo.Database
{
    public class MySqlEventingRepoFactory : IEventingRepoFactory
    {
        private readonly string _connectionString;
        private readonly int _maxRetriesOnDeadlock;

        public MySqlEventingRepoFactory(string connectionString)
            : this(connectionString, 1)
        {
        }

        public MySqlEventingRepoFactory(string connectionString, int maxRetriesOnDeadlock)
        {
            _connectionString = connectionString;
            _maxRetriesOnDeadlock = maxRetriesOnDeadlock;
        }

        public IEventingRepo CreateRepo()
        {
            return new MySqlEventingRepo(new MySqlConnection(_connectionString), _maxRetriesOnDeadlock);
        }
    }
}
