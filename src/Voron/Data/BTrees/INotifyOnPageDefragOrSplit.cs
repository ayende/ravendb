namespace Voron.Data.BTrees;

public interface INotifyOnPageDefragOrSplit
{
    void PageDefragOrSplit(long p);
}
