using System;

namespace Resonance.Models
{
    public class RetryPolicy
    {
        public int Retries { get; set; }
        public TimeSpan InitialBackoffPeriod { get; set; }
        public bool IncrementalBackoff { get; set; }

        public RetryPolicy(int retries, TimeSpan initialBackoffPeriod, bool incrementalBackoff)
        {
            if (retries < 0) throw new ArgumentOutOfRangeException(nameof(retries), "retries cannot be less than 0");
            if (initialBackoffPeriod.Ticks < 0) throw new ArgumentOutOfRangeException(nameof(initialBackoffPeriod), "initialBackoffPeriod must be 0 or greater");
            if (incrementalBackoff && initialBackoffPeriod.Ticks == 0) throw new ArgumentException("incrementalBackoff required initialBackoffPeriod to be greater than 0");

            Retries = retries;
            IncrementalBackoff = incrementalBackoff;
            InitialBackoffPeriod = initialBackoffPeriod;
        }

        public static RetryPolicy Default => new RetryPolicy(3, TimeSpan.FromMilliseconds(300), true);
        public static RetryPolicy None => new RetryPolicy(0, TimeSpan.Zero, false);
        public static RetryPolicy Quick => new RetryPolicy(10, TimeSpan.FromMilliseconds(10), false);
        public static RetryPolicy Persevere => new RetryPolicy(60, TimeSpan.FromMilliseconds(100), false);
    }
}
