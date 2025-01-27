namespace Sparrow.Server.Platform;

public static class PalConfiguration
{
    public static int IoRingQueueSize = 32;
    public static bool LowPriorityIo = false;
    public static Pal.RvnWriteMode WriteMode = Pal.RvnWriteMode.Auto;
}
