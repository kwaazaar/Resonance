using Resonance.Models;
using System;
using System.Threading.Tasks;

namespace Resonance.Repo
{
    public interface IEventingRepoFactory
    {
        IEventingRepo CreateRepo();
        Task<T> SafeExecAsync<T>(Func<IEventingRepo, Task<T>> repoAction, SafeExecOptions options);
        Task SafeExecAsync(Func<IEventingRepo, Task> repoAction, SafeExecOptions options);
    }
}
