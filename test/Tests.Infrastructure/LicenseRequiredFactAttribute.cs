using System;
using Xunit;

namespace Tests.Infrastructure
{
    public class LicenseRequiredFactAttribute : FactAttribute
    {
        internal static readonly bool HasLicense;

        internal static string SkipMessage = "Requires License to be set via 'RAVEN_LICENSE' or 'RAVEN_LICENSE_PATH' environment variable.";

        static LicenseRequiredFactAttribute()
        {
            HasLicense =
                string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("RAVEN_LICENSE")) == false ||
                string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("RAVEN.LICENSE")) == false || 
                string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("RAVEN_LICENSE_PATH")) == false || 
                string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("RAVEN.LICENSE.PATH")) == false;
        }

        public override string Skip
        {
            get
            {
                if (ShouldSkip())
                    return SkipMessage;

                return null;
            }
        }

        internal static bool ShouldSkip()
        {
            return HasLicense == false;
        }
    }
}
