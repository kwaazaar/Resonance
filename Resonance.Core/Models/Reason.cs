using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Resonance.Models
{
    /// <summary>
    /// 0=Unknown, 1=Expired, 2=MaxRetriesReached, 3=Other
    /// </summary>
    public enum ReasonType : int
    {
        Unknown = 0,
        Expired = 1,
        MaxRetriesReached = 2,
        Other = 3,
    }

    public class Reason
    {
        public ReasonType Type { get; set; }
        public string ReasonText { get; set; }

        public static Reason Expired
        {
            get
            {
                return new Reason
                {
                    Type = ReasonType.Expired
                };
            }
        }

        public static Reason MaxRetriesReached
        {
            get
            {
                return new Reason
                {
                    Type = ReasonType.MaxRetriesReached,
                };
            }
        }

        public static Reason Other(string reasonText)
        {
            return new Reason
            {
                Type = ReasonType.Other,
                ReasonText = reasonText,
            };
        }
    }
}
