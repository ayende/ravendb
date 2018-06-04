namespace Tryouts.Corax
{
    public enum ContainerType : byte
    {
        None = 0,
        Bitmap = (byte)'B',
        Array = (byte)'A',
        RunLength = (byte)'R',
        Skip = (byte)'S'
    }
}
