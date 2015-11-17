// -----------------------------------------------------------------------
//  <copyright file="DevelopmentHelper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

using Raven.Abstractions;

namespace Raven.Database.Util
{
    internal static class DevelopmentHelper
    {
        public static void TimeBomb()
        {
<<<<<<< HEAD
            if (SystemTime.UtcNow > new DateTime(2015, 12, 6))
=======
            if (SystemTime.UtcNow > new DateTime(2015, 12, 1))
>>>>>>> e2a896b0dffc7db7ed332d891d3adc8431570648
                throw new NotImplementedException("Development time bomb.");
        }
    }
}
