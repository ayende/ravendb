﻿using Raven.Abstractions.FileSystem;
using System.Threading.Tasks;

namespace Raven.Client.FileSystem.Impl
{
    internal interface IFilesOperation 
    {
        string FileName { get; }

        Task<FileHeader> Execute(IAsyncFilesSession session);
    }
}
