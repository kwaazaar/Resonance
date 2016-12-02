using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Resonance.Repo
{
    public enum RepoError : int
    {
        /// <summary>
        /// Unspecified or miscellanious error
        /// </summary>
        Unspecified = 0,

        /// <summary>
        /// The repo is unavailable, possibly because of a connection failure or authentication failure
        /// </summary>
        Unavailable = 1,

        /// <summary>
        /// The repo is too busy, possibly because of repeating deadlocks, or too many connections.
        /// </summary>
        TooBusy = 2,
    }

    /// <summary>
    /// Repository exception
    /// </summary>
    public class RepoException : Exception
    {
        /// <summary>
        /// Classification of the error that caused this exception
        /// </summary>
        public RepoError Error { get; protected set; }

        public RepoException()
            : this("A repository error has occurred")
        {
        }

        public RepoException(string message)
            : base(message)
        {
            this.Error = RepoError.Unspecified;
        }

        public RepoException(string message, Exception innerException)
            : this(message, innerException, RepoError.Unspecified)
        {
        }

        public RepoException(Exception innerException)
            : this(innerException.Message)
        {
        }

        public RepoException(Exception innerException, RepoError repoError)
            : this(innerException.Message)
        {
            this.Error = repoError;
        }

        public RepoException(string message, RepoError repoError)
            : this(message)
        {
            this.Error = repoError;
        }

        public RepoException(string message, Exception innerException, RepoError repoError)
            : base(message, innerException)
        {
            this.Error = repoError;
        }
    }
}
