using System;
using System.Collections.Generic;
using System.Text;

namespace Resonance
{
    /// <summary>
    /// Provider for DateTime
    /// </summary>
    public enum DateTimeProvider
    {
        /// <summary>
        /// Repository provides datetimes (default)
        /// </summary>
        Repository = 0,

        /// <summary>
        /// System-clock provides datetimes. Watch out when using multiple (virtual) systems on the same repository, because of system clock differences.
        /// </summary>
        SystemClock = 1,
    }
}
