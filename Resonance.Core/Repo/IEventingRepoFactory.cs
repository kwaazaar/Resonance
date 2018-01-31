using Resonance.Models;
using System;
using System.Threading.Tasks;

namespace Resonance.Repo
{
    public interface IEventingRepoFactory
    {
        IEventingRepo CreateRepo();
        Task<T> InvokeFuncAsync<T>(Func<IEventingRepo, Task<T>> repoAction, InvokeOptions options);
        Task InvokeFuncAsync(Func<IEventingRepo, Task> repoAction, InvokeOptions options);
    }
}
