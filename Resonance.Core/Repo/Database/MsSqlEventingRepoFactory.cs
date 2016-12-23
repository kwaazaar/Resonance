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
        private readonly TimeSpan _commandTimeout;

        /// <summary>
        /// MsSql Repository Factory
        /// </summary>
        /// <param name="connectionString">Connectionstring to use</param>
        /// <remarks>A commandtimeout of 30 seconds is applied.</remarks>
        public MsSqlEventingRepoFactory(string connectionString)
            : this(connectionString, TimeSpan.FromSeconds(30))
        {
        }

        /// <summary>
        /// MsSql Repository Factory
        /// </summary>
        /// <param name="connectionString">Connectionstring to use</param>
        /// <param name="commandTimeout">Commandtimeout to use</param>
        public MsSqlEventingRepoFactory(string connectionString, TimeSpan commandTimeout)
        {
            _connectionString = connectionString;
            _commandTimeout = commandTimeout;
        }

        /// <summary>
        /// Create a repository instance
        /// </summary>
        /// <returns></returns>
        public IEventingRepo CreateRepo()
        {
            return new MsSqlEventingRepo(new SqlConnection(_connectionString), _commandTimeout);
        }
    }
}
