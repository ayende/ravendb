using System;

namespace Voron
{
    [Flags]
    public enum TransactionPersistenceModeFlags
    {
        None = 0,
        Encrypted = 1,
        LinkedJournalsRecord = 16,
        HasFreePages = 32
    }
}
