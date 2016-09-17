using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;

namespace Resonance.Repo.Database
{
    public class MsSqlEventingRepoFactory : IEventingRepoFactory
    {
        private readonly string _connectionString;

        public MsSqlEventingRepoFactory(string connectionString)
        {
            _connectionString = connectionString;
        }

        public IEventingRepo CreateRepo()
        {
            return new MsSqlEventingRepo(new SqlConnection(_connectionString));
        }
    }
}
