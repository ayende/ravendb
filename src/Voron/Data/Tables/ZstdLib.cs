using System;
using System.IO;
using System.Runtime.InteropServices;

namespace Voron.Data.Tables
{
    internal static unsafe class ZstdLib
    {
        private const string DllName = @"C:\Users\ayende\Downloads\zstd-v1.4.4-win64\dll\libzstd.dll";

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        private static extern UIntPtr ZSTD_compressBound(UIntPtr srcSize);


        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        private static extern ulong ZSTD_getFrameContentSize(void* src, UIntPtr srcSize);
        
        const ulong ZSTD_CONTENTSIZE_UNKNOWN = unchecked(0UL - 1);
        const ulong ZSTD_CONTENTSIZE_ERROR = unchecked(0UL - 2);
        
        public static int GetDecompressedSize(Span<byte> compressed)
        {
            fixed (byte* srcPtr = compressed)
            {
                ulong size = ZSTD_getFrameContentSize(srcPtr, (UIntPtr)compressed.Length);
                if (size == ZSTD_CONTENTSIZE_ERROR || size == ZSTD_CONTENTSIZE_UNKNOWN)
                    throw new InvalidDataException("Unable to get the content size from ZSTD value");

                return (int)size;
            }
        }

        public static long GetMaxCompression(long size)
        {
            return (long)ZSTD_compressBound((UIntPtr)size);
        }

        public static int GetMaxCompression(int size)
        {
            return (int)ZSTD_compressBound((UIntPtr)size);
        }

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        private static extern void* ZSTD_createCCtx();
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        private static extern UIntPtr ZSTD_freeCCtx(void* cctx);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern uint ZSTD_isError(UIntPtr code);
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr ZSTD_getErrorName(UIntPtr code);
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern UIntPtr ZSTD_compressCCtx(void* ctx, byte* dst, UIntPtr dstCapacity, byte* src, UIntPtr srcSize, int compressionLevel);
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern UIntPtr ZSTD_decompressDCtx(UIntPtr ctx, UIntPtr dst, UIntPtr dstCapacity, UIntPtr src, UIntPtr srcSize);


        private static void AssertSuccess(UIntPtr v)
        {
            if (ZSTD_isError(v) != 0)
                throw new InvalidOperationException(Marshal.PtrToStringAnsi(ZSTD_getErrorName(v)));
        }

        public static ZstdContext CreateContext()
        {
            var handle = ZSTD_createCCtx();
            if (handle == null)
                throw new OutOfMemoryException("Could not allocate a ZSTD context");

            return new ZstdContext(handle);
        }

        public struct ZstdContext : IDisposable
        {
            private void* _handle;
            public int CompressionLevel;

            public ZstdContext(void* handle)
            {
                _handle = handle;
                CompressionLevel = 3;
            }

            public int Compress(Span<byte> src, Span<byte> dst)
            {
                fixed (byte* srcPtr = src)
                fixed (byte* dstPtr = dst)
                {
                    var result = ZSTD_compressCCtx(_handle, dstPtr, (UIntPtr)dst.Length, srcPtr, (UIntPtr)src.Length, CompressionLevel);
                    AssertSuccess(result);
                    return (int)result;
                }
            }

            public void Dispose()
            {
                if (_handle != null)
                    ZSTD_freeCCtx(_handle);
                _handle = null;
            }
        }
    }
}
