using System;
using System.Text;
#if SILVERLIGHT || NETFX_CORE
using Raven.Client.Silverlight.MissingFromSilverlight;
#else
using Raven.Abstractions.Util.Encryptors;
#endif

namespace Raven.Client.Connection
{
	public static class ServerHash
	{
		public static string GetServerHash(string url)
		{
			var bytes = Encoding.UTF8.GetBytes(url);
			return BitConverter.ToString(GetHash(bytes));
		}

		private static byte[] GetHash(byte[] bytes)
		{
#if SILVERLIGHT || NETFX_CORE
			return MD5Core.GetHash(bytes);
#else
			return Encryptor.Current.Hash.Compute(bytes);
#endif
		}
	}
}