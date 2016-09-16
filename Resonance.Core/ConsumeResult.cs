using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Resonance
{

    public enum ConsumeResultType : int
    {
        Succeeded = 0,
        MustRetry = 1,
        Failed = 2,
        MustSuspend = 3,
    }

    public class ConsumeResult
    {
        public ConsumeResultType ResultType { get; private set; }
        public string Reason { get; private set; }
        public TimeSpan? SuspendDuration { get; private set; }

        private ConsumeResult()
        {

        }

        public static ConsumeResult Succeeded
        {
            get
            {
                return new ConsumeResult
                {
                    ResultType = ConsumeResultType.Succeeded,
                };
            }
        }

        public static ConsumeResult MustRetry(string reason)
        {
            return new ConsumeResult
            {
                ResultType = ConsumeResultType.MustRetry,
                Reason = reason,
            };
        }

        public static ConsumeResult Failed(string reason)
        {
            return new ConsumeResult
            {
                ResultType = ConsumeResultType.Failed,
                Reason = reason,
            };
        }

        public static ConsumeResult MustSuspend(TimeSpan suspendDuration, string reason = null)
        {
            return new ConsumeResult
            {
                ResultType = ConsumeResultType.MustSuspend,
                SuspendDuration = suspendDuration,
                Reason = reason,
            };
        }
    }
}
