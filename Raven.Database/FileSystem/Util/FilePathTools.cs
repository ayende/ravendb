﻿using System;
using System.IO;
using Raven.Database.Extensions;

namespace Raven.Database.FileSystem.Util
{
	public static class FilePathTools
	{
		public static string MakeSureEndsWithSlash(string filePath)
		{
			return filePath.TrimEnd('\\') + "\\";
		}

	    public static string StripWorkingDirectory(string workDir, string dir)
	    {
	        if (dir.StartsWith(workDir, StringComparison.OrdinalIgnoreCase))
	            return "\t\\" + dir.Substring(workDir.Length);
	        return dir;
	    }

		public static string ApplyWorkingDirectoryToPathAndMakeSureThatItEndsWithSlash(string workingDirectory, string path)
		{
			if (string.IsNullOrEmpty(workingDirectory) || string.IsNullOrEmpty(path))
				return path;

			if (Path.IsPathRooted(path) == false)
			{
				if (path.StartsWith(@"~/") || path.StartsWith(@"~\"))
				{
					path = path
						.Replace(@"~/", workingDirectory)
						.Replace(@"~\", workingDirectory);
				}
				else
				{
					path = Path.Combine(workingDirectory, path);
				}
			}

			

			return MakeSureEndsWithSlash(path.ToFullPath());
		}

        public static string MakeSureEndsWithSlash(string filePath)
        {
            return filePath.TrimEnd('\\') + "\\";
        }
	}
}