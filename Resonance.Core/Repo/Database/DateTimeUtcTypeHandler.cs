using Dapper;
using System;
using System.Collections.Generic;
using System.Text;
using System.Data;

namespace Resonance.Repo.Database
{
    public class DateTimeUtcTypeHandler : SqlMapper.TypeHandler<DateTime>
    {
        public override void SetValue(IDbDataParameter parameter, DateTime value)
        {
            parameter.Value = value; // ToUniversalTime?
        }

        public override DateTime Parse(object value)
        {
            return DateTime.SpecifyKind((DateTime)value, DateTimeKind.Utc);
        }
    }
}
