using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Resonance.Repo
{
    public interface IEventingRepoFactory
    {
        IEventingRepo CreateRepo();
    }
}
