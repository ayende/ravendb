using System;

namespace Raven.Database.Extensions
{
	internal static class DateTimeExtensions
	{
		public static long ToUnixTime(this DateTime time)
		{
			return (long) time.ToUniversalTime().Subtract(new DateTime(1970, 1, 1, 0, 0, 0)).TotalMilliseconds;
		}

		public static DateTime ToDateTime(this byte[] bytes)
		{
			return DateTime.FromBinary(BitConverter.ToInt64(bytes, 0));
		}
	}
}
