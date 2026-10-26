namespace Sparrow.Json
{
    /// <summary>
    /// Implemented by output streams that can use the total size of the content before they get its first byte.
    /// </summary>
    internal interface IContentLengthHint
    {
        /// <summary>
        /// Called before the first byte is written, with the size of all the content that follows.
        /// </summary>
        /// <returns>true when the content needs no explicit flush, because the owner of the stream flushes it when it completes (an HTTP response)</returns>
        bool OnContentLength(long length);
    }
}
