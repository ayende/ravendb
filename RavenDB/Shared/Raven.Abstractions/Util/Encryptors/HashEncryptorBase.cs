namespace Raven.Abstractions.Util.Encryptors
{
	using System.Security.Cryptography;

	public abstract class HashEncryptorBase
	{
		public byte[] ComputeHash(HashAlgorithm algorithm, byte[] bytes)
		{
			using (algorithm)
			{
				return algorithm.ComputeHash(bytes);
			}
		}
	}
}