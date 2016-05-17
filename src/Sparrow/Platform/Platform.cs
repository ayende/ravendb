using System;
using System.Runtime.InteropServices;

namespace Sparrow.Platform
{
    public static class Platform
    {
        public static readonly bool RunningOnPosix = RuntimeInformation.IsOSPlatform(OSPlatform.Linux) ||
                                                     RuntimeInformation.IsOSPlatform(OSPlatform.OSX);

        public static readonly bool CanPrefetch = IsWindows8OrNewer();

        private static bool IsWindows8OrNewer()
        {
            var winString = "Windows ";
            var os = RuntimeInformation.OSDescription;

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows) == false)
                return false;

            var idx = os.IndexOf(winString, StringComparison.OrdinalIgnoreCase);
            if (idx < 0)
                return false;

            var ver = os.Substring(idx + winString.Length);
            return ver != null && Convert.ToInt32(ver) >= 6.2; // 6.2 is win8, 6.1 win7..
        }
    }
}