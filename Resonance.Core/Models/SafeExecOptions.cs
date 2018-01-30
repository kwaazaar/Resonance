using System;

namespace Resonance.Models
{
    public class SafeExecOptions
    {
        public RetryPolicy RetryPolicy { get; set; }
        public Action<Exception> ErrorAction { get; set; }

        public SafeExecOptions(RetryPolicy retryPolicy, Action<Exception> errorAction)
        {
            RetryPolicy = retryPolicy ?? throw new ArgumentNullException(nameof(retryPolicy));
            ErrorAction = errorAction;
        }

        public static SafeExecOptions Default => new SafeExecOptions(RetryPolicy.Default, null);
        public static SafeExecOptions NoRetries => new SafeExecOptions(RetryPolicy.None, null);
        public static SafeExecOptions Persevere => new SafeExecOptions(RetryPolicy.Persevere, null);
    }
}
