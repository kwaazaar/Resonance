using Resonance.Models;
using System;
using System.Threading.Tasks;

namespace Resonance.Repo
{
    public abstract class BaseEventingRepoFactory : IEventingRepoFactory
    {
        public abstract IEventingRepo CreateRepo();

        public async Task InvokeFuncAsync(Func<IEventingRepo, Task> repoAction, InvokeOptions options)
        {
            int attempts = 0;
            bool success = false;
            TimeSpan nextBackoff = options.RetryPolicy.InitialBackoffPeriod;

            do
            {
                attempts++;
                try
                {
                    using (var repo = CreateRepo())
                        await repoAction(repo).ConfigureAwait(false);
                    success = true;
                }
                catch (Exception ex)
                {
                    // When not yet last attempt
                    if (attempts < (options.RetryPolicy.Retries + 1))
                    {
                        // pass the exeception to the erroraction (if any)
                        if (options.ErrorAction != null)
                        {
                            SafeInvoke(() => options.ErrorAction(ex, attempts, options.RetryPolicy.Retries + 1));
                        }

                        // Backoff
                        if (nextBackoff != TimeSpan.Zero)
                        {
                            await Task.Delay(nextBackoff);
                            if (options.RetryPolicy.IncrementalBackoff)
                                nextBackoff += nextBackoff; // Double it
                        }
                    }
                    else
                        throw; // On last attempt, just throw the exception
                }
            } while (!success && attempts < (options.RetryPolicy.Retries + 1));
        }

        public async Task<T> InvokeFuncAsync<T>(Func<IEventingRepo, Task<T>> repoAction, InvokeOptions options)
        {
            T result = default(T);

            int attempts = 0;
            bool success = false;
            TimeSpan nextBackoff = options.RetryPolicy.InitialBackoffPeriod;

            do
            {
                attempts++;
                try
                {
                    using (var repo = CreateRepo())
                        result = await repoAction(repo).ConfigureAwait(false);
                    success = true;
                }
                catch (Exception ex)
                {
                    // When not yet last attempt
                    if (attempts < (options.RetryPolicy.Retries + 1))
                    {
                        // pass the exeception to the erroraction (if any)
                        if (options.ErrorAction != null)
                        {
                            SafeInvoke(() => options.ErrorAction(ex, attempts, options.RetryPolicy.Retries + 1));
                        }

                        // Backoff
                        if (nextBackoff != TimeSpan.Zero)
                        {
                            await Task.Delay(nextBackoff);
                            if (options.RetryPolicy.IncrementalBackoff)
                                nextBackoff += nextBackoff; // Double it
                        }
                    }
                    else
                        throw; // On last attempt, just throw the exception
                }
            } while (!success && attempts < (options.RetryPolicy.Retries + 1));

            return result;
        }

        private Exception SafeInvoke(Action action)
        {
            try
            {
                action();
                return null;
            }
            catch (Exception ex)
            {
                return ex;
            }
        }
    }
}
