using Dapper;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Resonance.Repo.Database
{
    internal static class MsSqlUtil
    {
        public static DbString ToDbKey(this string key)
        {
            return new DbString { Value = key, IsFixedLength = false, Length = 36, IsAnsi = true };
        }
    }
}
