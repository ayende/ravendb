using System;
using System.IO;
using System.IO.Compression;
using Raven.Abstractions.Connection;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Abstractions.Smuggler
{
    public class SmugglerSpinFileWriter : IDisposable
    {
        private readonly long splitOutfileSize;
        private readonly bool shouldSplitStream;
        private readonly string filePath;

        private Stream stream;
        private StreamWriter streamWriter;
        private CountingStream countingStream;
        private GZipStream gZipStream;
        private JsonTextWriter jsonWriter;

        private string lastPropertyName;
        private int spinFileNumber;
        private const long MB = 1;
        // private const long MB = 1024L*1024;


        public SmugglerSpinFileWriter(Stream stream, long splitOutfileSize, bool shouldSplitStream, string filePath)
        {
            this.shouldSplitStream = shouldSplitStream;
            this.splitOutfileSize = shouldSplitStream == false ? 0 : splitOutfileSize; // applicable only for owned files-only streams
            this.filePath = filePath;
            this.stream = stream;

            OpenNewStream();
        }

        private void OpenNewStream()
        {
            gZipStream = new GZipStream(stream, CompressionMode.Compress, leaveOpen: true);
            countingStream = new CountingStream(gZipStream);
            streamWriter = new StreamWriter(countingStream);
            jsonWriter = new JsonTextWriter(streamWriter)
            {
                Formatting = Formatting.Indented
            };
        }

        public JsonTextWriter GetJsonWriter()
        {
            if (splitOutfileSize == 0)
                return jsonWriter;

            if (countingStream.NumberOfWrittenBytes < splitOutfileSize*MB) // in MB
                return jsonWriter;

            // reopen with next file:
            CloseStream();
            stream = getNextFileNameStream();
            OpenNewStream();

            WriteStartObject();
            WritePropertyName(lastPropertyName);

            return jsonWriter;
        }

        private void CloseStream()
        {
            WriteEndArray();
            WriteEndObject();

            DisposeAllStreams();
        }

        private Stream getNextFileNameStream()
        {
            string nextFilename = $"{filePath}.{++spinFileNumber}";
            return File.Create(nextFilename);
        }
    

        private void DisposeAllStreams()
        {
            streamWriter?.Dispose();
            streamWriter = null;
            countingStream?.Dispose();
            countingStream = null;
            gZipStream?.Dispose();
            gZipStream = null;
            if (shouldSplitStream == true) // else caller should dispose handle dispose (might not own the stream)
            {
                stream?.Dispose();
                stream = null;
            }
        }

        public void Dispose()
        {
            DisposeAllStreams();
        }

        public void WritePropertyName(string item)
        {
            this.lastPropertyName = item;
            jsonWriter.WritePropertyName(item);
            jsonWriter.WriteStartArray();
        }

        public void WriteEndArray()
        {
            jsonWriter.WriteEndArray();
        }

        public void WriteStartObject()
        {
            jsonWriter.WriteStartObject();
        }

        public void WriteEndObject()
        {
            jsonWriter.WriteEndObject();
            streamWriter.Flush();
        }
    }
}
