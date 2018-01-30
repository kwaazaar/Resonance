using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace Resonance.Repo.Database
{
    public class MySqlEventingRepoFactory : BaseEventingRepoFactory, IEventingRepoFactory
    {
        private readonly string _connectionString;
        private readonly int _maxRetriesOnDeadlock;
        private readonly TimeSpan _commandTimeout;

        /// <summary>
        /// MySql Repository Factory
        /// </summary>
        /// <param name="connectionString">Connectionstring to use</param>
        /// <remarks>A default connectiontimeout of 30 seconds will be used. On deadlock a maximum of one retry will be performed.</remarks>
        public MySqlEventingRepoFactory(string connectionString)
            : this(connectionString, TimeSpan.FromSeconds(30), 1)
        {
        }

        /// <summary>
        /// MySql Repository Factory
        /// </summary>
        /// <param name="connectionString">Connectionstring to use</param>
        /// <param name="commandTimeout">Commandtimeout to use</param>
        /// <param name="maxRetriesOnDeadlock">Specifies the maximum number of retries to perform in case of a deadlock situation.</param>
        public MySqlEventingRepoFactory(string connectionString, TimeSpan commandTimeout, int maxRetriesOnDeadlock)
        {
            _connectionString = connectionString;
            _commandTimeout = commandTimeout;
            _maxRetriesOnDeadlock = maxRetriesOnDeadlock;
        }

        /// <summary>
        /// Creates a repository instance
        /// </summary>
        /// <returns></returns>
        public override IEventingRepo CreateRepo()
        {
            return new MySqlEventingRepo(new MySqlConnection(_connectionString), _commandTimeout, _maxRetriesOnDeadlock);
        }
    }
}
