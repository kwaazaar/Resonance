using System;

namespace Resonance.Models
{
    public class RetryPolicy
    {
        /// <summary>
        /// Maximum number of retries
        /// </summary>
        public int Retries { get; set; }

        /// <summary>
        /// Backoff-period between retries
        /// </summary>
        public TimeSpan InitialBackoffPeriod { get; set; }

        /// <summary>
        /// When true, backoff-period will double for each next retry (exponential)
        /// </summary>
        public bool IncrementalBackoff { get; set; }

        /// <summary>
        /// Create a retry policy
        /// </summary>
        /// <param name="retries">Max number of retries (nr of attempts will be retries + 1)</param>
        /// <param name="initialBackoffPeriod">Backoff-period between retries</param>
        /// <param name="incrementalBackoff">When true, backoff-period will double for each next retry (exponential)</param>
        public RetryPolicy(int retries, TimeSpan initialBackoffPeriod, bool incrementalBackoff)
        {
            if (retries < 0) throw new ArgumentOutOfRangeException(nameof(retries), "retries cannot be less than 0");
            if (initialBackoffPeriod.Ticks < 0) throw new ArgumentOutOfRangeException(nameof(initialBackoffPeriod), "initialBackoffPeriod must be 0 or greater");
            if (incrementalBackoff && initialBackoffPeriod.Ticks == 0) throw new ArgumentException("incrementalBackoff required initialBackoffPeriod to be greater than 0");

            Retries = retries;
            IncrementalBackoff = incrementalBackoff;
            InitialBackoffPeriod = initialBackoffPeriod;
        }

        /// <summary>
        /// 3 retries, 300ms incremental backoff period
        /// </summary>
        public static RetryPolicy Default => new RetryPolicy(3, TimeSpan.FromMilliseconds(300), true);
        /// <summary>
        /// No retries
        /// </summary>
        public static RetryPolicy None => new RetryPolicy(0, TimeSpan.Zero, false);
        /// <summary>
        /// 10 retries, 10 ms static backoff period
        /// </summary>
        public static RetryPolicy Quick => new RetryPolicy(10, TimeSpan.FromMilliseconds(10), false);
        /// <summary>
        /// 60 retries, 100ms static backoff period
        /// </summary>
        public static RetryPolicy Persevere => new RetryPolicy(60, TimeSpan.FromMilliseconds(100), false);
    }
}
