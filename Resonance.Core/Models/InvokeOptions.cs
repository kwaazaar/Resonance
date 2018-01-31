using System;

namespace Resonance.Models
{
    public class InvokeOptions
    {
        /// <summary>
        /// Retry Policy
        /// </summary>
        public RetryPolicy RetryPolicy { get; set; }

        /// <summary>
        /// Action to invoke when an exception occurred and the invoke is about to be retried. Arguments are the original exception, attempt, max attempts.</param>
        /// </summary>
        public Action<Exception, int, int> ErrorAction { get; set; }

        /// <summary>
        /// Create an InvokeOptions structure
        /// </summary>
        /// <param name="retryPolicy">RetryPolicy</param>
        /// <param name="errorAction">Optional: action to invoke when an exception occurred and the invoke is about to be retried. Arguments are the original exception, attempt, max attempts.</param>
        public InvokeOptions(RetryPolicy retryPolicy, Action<Exception, int, int> errorAction)
        {
            RetryPolicy = retryPolicy ?? throw new ArgumentNullException(nameof(retryPolicy));
            ErrorAction = errorAction;
        }

        /// <summary>
        /// RetryPolicy.Default and no error-action
        /// </summary>
        public static InvokeOptions Default => new InvokeOptions(RetryPolicy.Default, null);

        /// <summary>
        /// RetryPolicy.None and no error-action
        /// </summary>
        public static InvokeOptions NoRetries => new InvokeOptions(RetryPolicy.None, null);

        /// <summary>
        /// RetryPolicy.Persevere and no error-action
        /// </summary>
        public static InvokeOptions Persevere => new InvokeOptions(RetryPolicy.Persevere, null);
    }
}
