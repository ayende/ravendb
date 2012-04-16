﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RavenFS.Client
{
    [Serializable]
    public class SynchronizationReport
    {
        public string FileName { get; set; }
        public long BytesTransfered { get; set; }
        public long BytesCopied { get; set; }
        public long NeedListLength { get; set; }
        public Exception Exception { get; set; }
    }
}
