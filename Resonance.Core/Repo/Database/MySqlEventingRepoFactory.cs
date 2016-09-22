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

        public MySqlEventingRepoFactory(string connectionString)
        {
            _connectionString = connectionString;
        }

        public IEventingRepo CreateRepo()
        {
            return new MySqlEventingRepo(new MySqlConnection(_connectionString));
        }
    }
}
