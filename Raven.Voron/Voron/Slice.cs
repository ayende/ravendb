using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;
using Voron.Impl;
using Voron.Trees;

namespace Voron
{
	[StructLayout(LayoutKind.Explicit, Pack = 1)]
	public struct PrefixedSliceHeader
	{
		[FieldOffset(0)]
		public byte PrefixId;

		[FieldOffset(1)]
		public ushort PrefixUsage;

		[FieldOffset(3)]
		public ushort NonPrefixedDataSize;
	}

	public unsafe delegate int SliceComparer(byte* a, byte* b, int size);

	public unsafe class Slice
	{
		public static Slice AfterAllKeys = new Slice(SliceOptions.AfterAllKeys);
		public static Slice BeforeAllKeys = new Slice(SliceOptions.BeforeAllKeys);
		public static Slice Empty = new Slice(new byte[0]);

		private readonly byte[] _array;

		public SliceOptions Options;

		public const byte NonPrefixedId = 0xff;

		private readonly PrefixedSliceHeader* _prefixHeader;
		private readonly byte* _prefixedBase;

		public readonly ushort Size;
		public readonly byte* NonPrefixedData;

		public Slice NewPrefix = null;
		private PrefixNode _prefix;

		public Slice(SliceOptions options)
		{
			Options = options;
			Size = 0;
			NonPrefixedData = null;
			_array = null;
			
			_prefixHeader = null;
			NonPrefixedData = null;
		}

		public Slice(byte* key, ushort size)
		{
			Size = size;
			Options = SliceOptions.Key;
			_array = null;
			NonPrefixedData = key;
		}

		public Slice(byte[] key) : this(key, (ushort)key.Length)
		{
			
		}

		public Slice(Slice other, ushort size)
		{
			if (other._array != null)
				_array = other._array;
			else
				NonPrefixedData = other.NonPrefixedData;

			Options = other.Options;
			Size = size;
		}

		public Slice(byte[] key, ushort size)
		{
			if (key == null) throw new ArgumentNullException("key");
			Size = size;
			Options = SliceOptions.Key;
			NonPrefixedData = null;
			_array = key;
		}

		internal Slice(NodeHeader* node)
		{
			if (node->KeySize > 0)
			{
				_prefixedBase = (byte*)node + Constants.NodeHeaderSize;
				_prefixHeader = (PrefixedSliceHeader*)_prefixedBase;

				NonPrefixedData = _prefixedBase + Constants.PrefixedSliceHeaderSize;

				Size = node->KeySize;
			}
			else
			{
				Size = 0;
			}

			Options = SliceOptions.Key;
		}

		internal static Slice WithEmptyPrefix(Slice key)
		{
			return new Slice(key);
		}

		//TODO arek
		private Slice(Slice key)
		{
			_prefixedBase = (byte*)Marshal.AllocHGlobal(Constants.PrefixedSliceHeaderSize + key.Size).ToPointer();
			_prefixHeader = (PrefixedSliceHeader*)_prefixedBase;

			_prefixHeader->PrefixId = NonPrefixedId;
			_prefixHeader->PrefixUsage = 0;
			_prefixHeader->NonPrefixedDataSize = key.Size;

			NonPrefixedData = _prefixedBase + Constants.PrefixedSliceHeaderSize;
			key.CopyTo(NonPrefixedData);

			Options = key.Options;
			Size = (ushort)(Constants.PrefixedSliceHeaderSize + _prefixHeader->NonPrefixedDataSize);
		}

		internal Slice(byte prefixId, ushort prefixUsage, Slice key)
		{
			var nonPrefixedSize = (ushort)(key.Size - prefixUsage);
			_prefixedBase = (byte*)Marshal.AllocHGlobal(Constants.PrefixedSliceHeaderSize + nonPrefixedSize).ToPointer();
			_prefixHeader = (PrefixedSliceHeader*)_prefixedBase;

			_prefixHeader->PrefixId = prefixId;
			_prefixHeader->PrefixUsage = prefixUsage;
			_prefixHeader->NonPrefixedDataSize = nonPrefixedSize;

			NonPrefixedData = _prefixedBase + Constants.PrefixedSliceHeaderSize;
			key.CopyTo(prefixUsage, NonPrefixedData, 0, _prefixHeader->NonPrefixedDataSize);

			Options = key.Options;
			Size = (ushort)(Constants.PrefixedSliceHeaderSize + _prefixHeader->NonPrefixedDataSize);
		}

		public bool IsPrefixed
		{
			get { return _prefixHeader != null; }
		}

		internal byte PrefixId
		{
			get { return _prefixHeader->PrefixId; }
			set { _prefixHeader->PrefixId = value; }
		}

		internal ushort PrefixUsage
		{
			get { return _prefixHeader->PrefixUsage; }
		}

		internal ushort NonPrefixedDataSize
		{
			get { return _prefixHeader->NonPrefixedDataSize; }
		}

		public bool Equals(Slice other)
		{
			return Compare(other) == 0;
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != GetType()) return false;
			return Equals((Slice)obj);
		}

		public override int GetHashCode()
		{
			if (_array != null)
				return ComputeHashArray();
			return ComputeHashPointer();
		}

		private int ComputeHashPointer()
		{
			unchecked
			{
				const int p = 16777619;
				int hash = (int)2166136261;

				for (int i = 0; i < Size; i++)
					hash = (hash ^ NonPrefixedData[i]) * p;

				hash += hash << 13;
				hash ^= hash >> 7;
				hash += hash << 3;
				hash ^= hash >> 17;
				hash += hash << 5;
				return hash;
			}
		}

		private int ComputeHashArray()
		{
			unchecked
			{
				const int p = 16777619;
				int hash = (int)2166136261;

				for (int i = 0; i < Size; i++)
					hash = (hash ^ _array[i]) * p;

				hash += hash << 13;
				hash ^= hash >> 7;
				hash += hash << 3;
				hash ^= hash >> 17;
				hash += hash << 5;
				return hash;
			}
		}

		public override string ToString()
		{
			// this is used for debug purposes only
			if (Options != SliceOptions.Key)
				return Options.ToString();

			if (IsPrefixed == false)
			{
				if (_array != null)
					return Encoding.UTF8.GetString(_array, 0, Size);

				return new string((sbyte*) NonPrefixedData, 0, Size, Encoding.UTF8);
			}

			if (_prefix != null)
				return _prefix.Value + new string((sbyte*) NonPrefixedData, 0, NonPrefixedDataSize, Encoding.UTF8);

			if (PrefixId == NonPrefixedId)
				return new string((sbyte*) NonPrefixedData, 0, NonPrefixedDataSize, Encoding.UTF8);

			return string.Format("prefix_id: {0} [usage: {1}], non_prefixed: {2}", PrefixId, PrefixUsage, new string((sbyte*)NonPrefixedData, 0, NonPrefixedDataSize, Encoding.UTF8));
		}

		internal void SetPrefix(PrefixNode prefix)
		{
			_prefix = prefix;
		}

		public int Compare(Slice other)
		{
			Debug.Assert(Options == SliceOptions.Key);
			Debug.Assert(other.Options == SliceOptions.Key);

			var keySize = IsPrefixed ? Size - Constants.PrefixedSliceHeaderSize + PrefixUsage : Size;
			var otherKeySize = other.IsPrefixed ? other.Size - Constants.PrefixedSliceHeaderSize + other.PrefixUsage : other.Size;

			var r = CompareData(other, NativeMethods.memcmp, (ushort) Math.Min(keySize, otherKeySize));
			if (r != 0)
				return r;

			return keySize - otherKeySize;
		}

		public bool StartsWith(Slice other)
		{
			if (Size < other.Size)
				return false;
			return CompareData(other, NativeMethods.memcmp, other.Size) == 0;
		}

		//private int ComparePrefix(Slice other, SliceComparer cmp, out ushort comparedBytes)
		//{
		//	Debug.Assert(IsPrefixed);

		//	comparedBytes = Math.Min(PrefixUsage, other.PrefixUsage);

		//	return cmp(_prefix.ValuePtr, other._prefix.ValuePtr, comparedBytes);
		//}

		private int ComparePrefixes(Slice other, SliceComparer cmp, int count)
		{
			if (count == 0)
				return 0;

			return cmp(_prefix.ValuePtr, other._prefix.ValuePtr, count);
		}

		private int ComparePrefixWithNonPrefixedData(Slice other, SliceComparer cmp, int prefixOffset, int count)
		{
			Debug.Assert(IsPrefixed);

			if (count == 0)
				return 0;

			return cmp(_prefix.ValuePtr + prefixOffset, other.NonPrefixedData, count);
		}

		private int CompareNonPrefixedData(Slice other, SliceComparer cmp, int nonPrefixedDataOffset, int count)
		{
			if (count == 0)
				return 0;

			if(other._prefixHeader != null)
				return cmp(NonPrefixedData + nonPrefixedDataOffset, other.NonPrefixedData, count);

			fixed (byte* ptr = other._array)
				return cmp(NonPrefixedData + nonPrefixedDataOffset, ptr, count);
		}

		private int CompareData(Slice other, SliceComparer cmp, ushort size)
		{
			if (IsPrefixed == false && other.IsPrefixed == false)
			{
				fixed (byte* a = _array)
				{
					fixed (byte* b = other._array)
					{
						return cmp(_array != null ? a : NonPrefixedData, other._array != null ? b : other.NonPrefixedData, size);
					}
				}
			}

			if (IsPrefixed && other.IsPrefixed)
			{
				// compare prefixes
				var comparedPrefixBytes = Math.Min(PrefixUsage, other.PrefixUsage);
				var r = ComparePrefixes(other, cmp, comparedPrefixBytes);

				if (r != 0)
					return r;
				
				// compare prefix and non prefix bytes
				size -= comparedPrefixBytes;

				if (PrefixUsage > comparedPrefixBytes)
				{
					var remainingPrefix = Math.Min(Math.Min(PrefixUsage - comparedPrefixBytes, other.NonPrefixedDataSize), size);

					r = ComparePrefixWithNonPrefixedData(other, cmp, comparedPrefixBytes, remainingPrefix);

					if (r != 0)
						return r;

					// compare non prefixed data

					size -= (ushort) remainingPrefix;
					 //TODO arek review Min(Min) - isn't size enough
					r = other.CompareNonPrefixedData(this, cmp, remainingPrefix, Math.Min(Math.Min(other.NonPrefixedDataSize - remainingPrefix, NonPrefixedDataSize), size));

					return r * -1;
				}
				
				if(other.PrefixUsage > comparedPrefixBytes)
				{
					var remainingPrefix = Math.Min(Math.Min(other.PrefixUsage - comparedPrefixBytes, NonPrefixedDataSize), size);

					r = other.ComparePrefixWithNonPrefixedData(this, cmp, comparedPrefixBytes, remainingPrefix);

					if (r != 0)
						return r * -1;

					// compare non prefixed data

					size -= (ushort)remainingPrefix;
					//TODO arek review Min(Min) - isn't size enough
					r = CompareNonPrefixedData(other, cmp, remainingPrefix, Math.Min(Math.Min(NonPrefixedDataSize - remainingPrefix, other.NonPrefixedDataSize), size));

					return r;
				}

				// both prefixes were equal, now compare non prefixed data

				r = CompareNonPrefixedData(other, cmp, 0, size);

				return r;
			}

			if (IsPrefixed == false && other.IsPrefixed)
			{
				var prefixLength = Math.Min(other.PrefixUsage, size);

				var r = other.ComparePrefixWithNonPrefixedData(other, cmp, 0, prefixLength);

				if (r != 0)
					return r * -1;

				// compare non prefixed data

				size -= prefixLength;

				r = other.CompareNonPrefixedData(this, cmp, prefixLength, size);

				return r * -1;
			}

			if (IsPrefixed && other.IsPrefixed == false)
			{
				var prefixLength = Math.Min(PrefixUsage, size);

				var r = ComparePrefixWithNonPrefixedData(this, cmp, 0, prefixLength);

				if (r != 0)
					return r;

				// compare non prefixed data

				size -= prefixLength;

				r = CompareNonPrefixedData(other, cmp, prefixLength, size);

				return r;
			}

			throw new NotImplementedException();
		}

		private class SlicePrefixMatcher
		{
			private readonly int _maxPrefixLength;

			public SlicePrefixMatcher(int maxPrefixLength)
			{
				_maxPrefixLength = maxPrefixLength;
			}

			public int MatchedBytes { get; private set; }

			public int MatchPrefix(byte* a, byte* b, int size)
			{
				MatchedBytes = 0;

				for (var i = 0; i < Math.Min(_maxPrefixLength, size); i++)
				{
					try
					{
						if (*a == *b)
							MatchedBytes++;
						else
							break;
					}
					catch (Exception e)
					{
						Console.WriteLine(e);
					}

					a++;
					b++;
				}

				return 0;
			} 
		}

		public int FindPrefixSize(Slice other)
		{
			var keySize = IsPrefixed ? Size - Constants.PrefixedSliceHeaderSize + PrefixUsage : Size;
			var otherKeySize = other.IsPrefixed ? other.Size - Constants.PrefixedSliceHeaderSize + other.PrefixUsage : other.Size;

			// TODO arek - 

			var slicePrefixMatcher = new SlicePrefixMatcher(Math.Min(keySize, otherKeySize));

			CompareData(other, slicePrefixMatcher.MatchPrefix, (ushort) Math.Min(keySize, otherKeySize));

			return slicePrefixMatcher.MatchedBytes;
		}

		public static implicit operator Slice(string s)
		{
			return new Slice(Encoding.UTF8.GetBytes(s));
		}

		public void CopyTo(byte* dest)
		{
			if (IsPrefixed == false)
			{
				if (_array == null)
				{
					NativeMethods.memcpy(dest, NonPrefixedData, Size);
					return;
				}
				fixed (byte* a = _array)
				{
					NativeMethods.memcpy(dest, a, Size);
				}
			}
			else
			{
					NativeMethods.memcpy(dest, _prefixedBase, Size);
			}
		}

		public void CopyTo(byte[] dest)
		{
			if (IsPrefixed == false)
			{
				if (_array == null)
				{
					fixed (byte* p = dest)
						NativeMethods.memcpy(p, NonPrefixedData, Size);
					return;
				}
				Buffer.BlockCopy(_array, 0, dest, 0, Size);
			}
			else
			{
				// TODO arek
				throw new NotImplementedException();
			}
		}

		public void CopyTo(int from, byte[] dest, int offset, int count)
		{
			if (from + count > Size)
				throw new ArgumentOutOfRangeException("from", "Cannot copy data after the end of the slice");
			if(offset + count > dest.Length)
				throw new ArgumentOutOfRangeException("from", "Cannot copy data after the end of the buffer");

			if (_array == null)
			{
				fixed (byte* p = dest)
					NativeMethods.memcpy(p + offset, NonPrefixedData + from, count);
				return;
			}
			Buffer.BlockCopy(_array, from, dest, offset, count);
		}

		public void CopyTo(int from, byte* dest, int offset, int count)
		{
			if (from + count > Size)
				throw new ArgumentOutOfRangeException("from", "Cannot copy data after the end of the slice");

			if (_array == null)
			{
				NativeMethods.memcpy(dest + offset, NonPrefixedData + from, count);
				return;
			}

			fixed (byte* p = _array)
				NativeMethods.memcpy(dest + offset, p + from, count);
		}

		public Slice Clone()
		{
			var buffer = new byte[Size];
			if (_array == null)
			{
				fixed (byte* dest = buffer)
				{
					NativeMethods.memcpy(dest, NonPrefixedData, Size);
				}
			}
			else
			{
				Buffer.BlockCopy(_array, 0, buffer, 0, Size);
			}
			return new Slice(buffer);
		}

	    public ValueReader CreateReader()
	    {
            if(_array != null)
                return new ValueReader(_array, Size);

	        return new ValueReader(NonPrefixedData, Size);
	    }
	}
}