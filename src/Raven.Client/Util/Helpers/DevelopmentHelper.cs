﻿using System;

namespace Raven.Client.Util.Helpers
{
    internal static class DevelopmentHelper
    {
        public static void TimeBomb()
        {
            if (SystemTime.UtcNow > new DateTime(2017, 8, 1))
                throw new NotImplementedException("Development time bomb.");
        }
    }
}