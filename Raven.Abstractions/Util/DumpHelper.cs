using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;

namespace Raven.Abstractions.Util
{	
	//credit goes to http://blogs.microsoft.co.il/sasha/2008/05/28/programmatically-generating-a-dump-file/
	public static class DumpHelper
	{
		public enum DumpType
		{
			MiniDumpNormal = 0,
			MiniDumpWithDataSegs = 1,
			MiniDumpWithFullMemory = 2,
			MiniDumpWithHandleData = 4,
			MiniDumpFilterMemory = 8,
			MiniDumpScanMemory = 16,
			MiniDumpWithUnloadedModules = 32,
			MiniDumpWithIndirectlyReferencedMemory = 64,
			MiniDumpFilterModulePaths = 128,
			MiniDumpWithProcessThreadData = 256,
			MiniDumpWithPrivateReadWriteMemory = 512,
			MiniDumpWithoutOptionalData = 1024,
			MiniDumpWithFullMemoryInfo = 2048,
			MiniDumpWithThreadInfo = 4096,
			MiniDumpWithCodeSegs = 8192,
		}
		[DllImportAttribute("dbghelp.dll")]
		[return: MarshalAsAttribute(UnmanagedType.Bool)]
		private static extern bool MiniDumpWriteDump(
			[In] IntPtr hProcess,
			uint ProcessId,
			SafeFileHandle hFile,
			DumpType DumpType,
			[In] IntPtr ExceptionParam,
			[In] IntPtr UserStreamParam,
			[In] IntPtr CallbackParam);

		public static void WriteTinyDumpForThisProcess(string fileName)
		{
			WriteDumpForThisProcess(fileName, DumpType.MiniDumpNormal);
		}

		public static void WriteFullDumpForThisProcess(string fileName)
		{
			WriteDumpForThisProcess(fileName, DumpType.MiniDumpWithFullMemory);
		}

		public static void WriteDumpForThisProcess(string fileName, DumpType dumpType)
		{
			WriteDumpForProcess(Process.GetCurrentProcess(), fileName, dumpType);
		}

		public static void WriteTinyDumpForProcess(Process process, string fileName)
		{
			WriteDumpForProcess(process, fileName, DumpType.MiniDumpNormal);
		}

		public static void WriteFullDumpForProcess(Process process, string fileName)
		{
			WriteDumpForProcess(process, fileName, DumpType.MiniDumpWithFullMemory);
		}

		public static void WriteDumpForProcess(Process process, string fileName, DumpType dumpType)
		{
			using (FileStream fs = File.Create(fileName))
			{
				if (!MiniDumpWriteDump(Process.GetCurrentProcess().Handle,
					(uint)process.Id, fs.SafeFileHandle, dumpType,
					IntPtr.Zero, IntPtr.Zero, IntPtr.Zero))
				{
					throw new Win32Exception(Marshal.GetLastWin32Error(), "Error calling MiniDumpWriteDump.");
				}
			}
		}
	}
}
