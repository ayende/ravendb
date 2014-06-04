using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
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

	// TODO arek - maybe introduce Flags concept to avoid checks like if(HasPrefixHeader), if(IsPrefixed == false)
	// TODO arek - remember about calling Marshal.FreeHGlobal
	public unsafe class Slice
	{
		public static Slice AfterAllKeys = new Slice(SliceOptions.AfterAllKeys);
		public static Slice BeforeAllKeys = new Slice(SliceOptions.BeforeAllKeys);
		public static Slice Empty = new Slice(new byte[0]);

		private readonly byte[] _array;

		public SliceOptions Options;

		public const byte NonPrefixedId = 0xff;

		private readonly PrefixedSliceHeader* _prefixHeader;

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
			if (other.HasPrefixHeader == false)
			{
				if (other._array != null)
					_array = other._array;
				else
					NonPrefixedData = other.NonPrefixedData;

				Size = size;
			}
			else
			{
				if (other.IsPrefixed == false)
				{
					NonPrefixedData = other.NonPrefixedData;
					Size = size;
				}
				else
				{
					Debug.Assert(other._prefix != null);

					//var count = Math.Min(other.PrefixUsage + other.NonPrefixedDataSize, size); TODO arek

					NonPrefixedData = (byte*)Marshal.AllocHGlobal(other.PrefixUsage + other.NonPrefixedDataSize).ToPointer(); // TODO arek - need to take into account 'size' parameter

					other._prefix.Value.CopyTo(NonPrefixedData, other.PrefixUsage);

					NativeMethods.memcpy(NonPrefixedData + other.PrefixUsage, other.NonPrefixedData, other.NonPrefixedDataSize);

					Size = size;
				}
			}

			Options = other.Options;
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
				_prefixHeader = (PrefixedSliceHeader*)((byte*)node + Constants.NodeHeaderSize);

				NonPrefixedData = (byte*)_prefixHeader + Constants.PrefixedSliceHeaderSize;

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
			var a = new Slice(key);

			return a;
		}

		//TODO arek - do the review of this ctor and move to the calling method
		private Slice(Slice key)
		{
			if (key.HasPrefixHeader)
			{
				if (key.IsPrefixed == false)
				{
					_prefixHeader = (PrefixedSliceHeader*) Marshal.AllocHGlobal(Constants.PrefixedSliceHeaderSize).ToPointer();

					_prefixHeader->PrefixId = NonPrefixedId;
					_prefixHeader->PrefixUsage = 0;
					_prefixHeader->NonPrefixedDataSize = key.NonPrefixedDataSize;

					NonPrefixedData = key.NonPrefixedData;

					Options = key.Options;
					Size = (ushort) (Constants.PrefixedSliceHeaderSize + _prefixHeader->NonPrefixedDataSize);
				}
				else
				{
					Debug.Assert(key._prefix != null);

					_prefixHeader = (PrefixedSliceHeader*)Marshal.AllocHGlobal(Constants.PrefixedSliceHeaderSize + key.NonPrefixedDataSize + key.PrefixUsage).ToPointer();

					_prefixHeader->PrefixId = NonPrefixedId;
					_prefixHeader->PrefixUsage = 0;
					_prefixHeader->NonPrefixedDataSize = (ushort) (key.NonPrefixedDataSize + key.PrefixUsage);

					NonPrefixedData = (byte*)_prefixHeader + Constants.PrefixedSliceHeaderSize;
					key._prefix.Value.CopyTo(NonPrefixedData, key.PrefixUsage);
					NativeMethods.memcpy(NonPrefixedData + key.PrefixUsage, key.NonPrefixedData, key.NonPrefixedDataSize);

					Options = key.Options;
					Size = (ushort)(Constants.PrefixedSliceHeaderSize + _prefixHeader->NonPrefixedDataSize);
				}
			}
			else
			{
				_prefixHeader = (PrefixedSliceHeader*)Marshal.AllocHGlobal(Constants.PrefixedSliceHeaderSize + key.Size).ToPointer();

				_prefixHeader->PrefixId = NonPrefixedId;
				_prefixHeader->PrefixUsage = 0;
				_prefixHeader->NonPrefixedDataSize = key.Size;

				NonPrefixedData = (byte*)_prefixHeader + Constants.PrefixedSliceHeaderSize;
				key.CopyTo(NonPrefixedData);

				Options = key.Options;
				Size = (ushort)(Constants.PrefixedSliceHeaderSize + _prefixHeader->NonPrefixedDataSize);
			}
		}

		internal Slice(byte prefixId, ushort prefixUsage, Slice key)
		{
			if (key.HasPrefixHeader == false)
			{
				var nonPrefixedSize = (ushort) (key.Size - prefixUsage);
				_prefixHeader = (PrefixedSliceHeader*) Marshal.AllocHGlobal(Constants.PrefixedSliceHeaderSize + nonPrefixedSize).ToPointer();

				_prefixHeader->PrefixId = prefixId;
				_prefixHeader->PrefixUsage = prefixUsage;
				_prefixHeader->NonPrefixedDataSize = nonPrefixedSize;

				NonPrefixedData = (byte*)_prefixHeader + Constants.PrefixedSliceHeaderSize;
				key.CopyTo(prefixUsage, NonPrefixedData, 0, _prefixHeader->NonPrefixedDataSize);

				Options = key.Options;
				Size = (ushort) (Constants.PrefixedSliceHeaderSize + _prefixHeader->NonPrefixedDataSize);
			}
			else
			{
				if (key.IsPrefixed)
				{
					Debug.Assert(key._prefix != null);

					var nonPrefixedSize = (ushort)(key.PrefixUsage + key.NonPrefixedDataSize - prefixUsage);
					_prefixHeader = (PrefixedSliceHeader*) Marshal.AllocHGlobal(Constants.PrefixedSliceHeaderSize).ToPointer();

					_prefixHeader->PrefixId = prefixId;
					_prefixHeader->PrefixUsage = prefixUsage;
					_prefixHeader->NonPrefixedDataSize = nonPrefixedSize;

					if (prefixUsage == key.PrefixUsage)
					{
						NonPrefixedData = key.NonPrefixedData;
					}
					else if (prefixUsage > key.PrefixUsage)
					{
						NonPrefixedData = key.NonPrefixedData + (prefixUsage - key.PrefixUsage);
					}
					else
					{
						NonPrefixedData = (byte*) Marshal.AllocHGlobal(nonPrefixedSize).ToPointer();

						var prefixPart = key.PrefixUsage - prefixUsage;

						key._prefix.Value.CopyTo(prefixUsage, NonPrefixedData, 0, prefixPart);
						NativeMethods.memcpy(NonPrefixedData + prefixPart, key.NonPrefixedData, nonPrefixedSize - prefixPart);
					}

					Options = key.Options;
					Size = (ushort)(Constants.PrefixedSliceHeaderSize + _prefixHeader->NonPrefixedDataSize);
				}
				else
				{
					var nonPrefixedSize = (ushort)(key.NonPrefixedDataSize - prefixUsage);
					_prefixHeader = (PrefixedSliceHeader*) Marshal.AllocHGlobal(Constants.PrefixedSliceHeaderSize).ToPointer();

					_prefixHeader->PrefixId = prefixId;
					_prefixHeader->PrefixUsage = prefixUsage;
					_prefixHeader->NonPrefixedDataSize = nonPrefixedSize;

					NonPrefixedData = key.NonPrefixedData + prefixUsage;

					Options = key.Options;
					Size = (ushort)(Constants.PrefixedSliceHeaderSize + _prefixHeader->NonPrefixedDataSize);
				}
			}
		}

		public bool HasPrefixHeader
		{
			get { return _prefixHeader != null; }
		}

		public bool IsPrefixed
		{
			get { return PrefixId != NonPrefixedId; }
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

			if (HasPrefixHeader == false)
			{
				if (_array != null)
					return Encoding.UTF8.GetString(_array, 0, Size);

				return new string((sbyte*) NonPrefixedData, 0, Size, Encoding.UTF8);
			}

			if (_prefix != null)
				return new Slice(_prefix.Value, PrefixUsage) + new string((sbyte*) NonPrefixedData, 0, NonPrefixedDataSize, Encoding.UTF8);

			if (IsPrefixed == false)
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

			var keySize = HasPrefixHeader ? Size - Constants.PrefixedSliceHeaderSize + PrefixUsage : Size;
			var otherKeySize = other.HasPrefixHeader ? other.Size - Constants.PrefixedSliceHeaderSize + other.PrefixUsage : other.Size;

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
			Debug.Assert(HasPrefixHeader);

			if (count == 0)
				return 0;

			fixed (byte* ptr = other._array)
				return cmp(_prefix.ValuePtr + prefixOffset, other._array != null ? ptr : other.NonPrefixedData, count);
		}

		private int CompareNonPrefixedData(int offset, Slice other, int otherOffset, SliceComparer cmp, int count)
		{
			if (count == 0)
				return 0;

			if(other._prefixHeader != null)
				return cmp(NonPrefixedData + offset, other.NonPrefixedData + otherOffset, count);

			fixed (byte* ptr = other._array)
				return cmp(NonPrefixedData + offset, (other._array != null ? ptr : other.NonPrefixedData) + otherOffset, count);
		}

		private int CompareData(Slice other, SliceComparer cmp, ushort size)
		{
			if (HasPrefixHeader == false && other.HasPrefixHeader == false)
			{
				fixed (byte* a = _array)
				{
					fixed (byte* b = other._array)
					{
						return cmp(_array != null ? a : NonPrefixedData, other._array != null ? b : other.NonPrefixedData, size);
					}
				}
			}

			if (HasPrefixHeader && other.HasPrefixHeader)
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
					r = other.CompareNonPrefixedData(remainingPrefix, this, 0, cmp, Math.Min(Math.Min(other.NonPrefixedDataSize - remainingPrefix, NonPrefixedDataSize), size));

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
					r = CompareNonPrefixedData(remainingPrefix, other, 0, cmp, Math.Min(Math.Min(NonPrefixedDataSize - remainingPrefix, other.NonPrefixedDataSize), size));

					return r;
				}

				// both prefixes were equal, now compare non prefixed data

				r = CompareNonPrefixedData(0, other, 0, cmp, size);

				return r;
			}

			if (HasPrefixHeader == false && other.HasPrefixHeader)
			{
				var prefixLength = Math.Min(other.PrefixUsage, size);

				var r = other.ComparePrefixWithNonPrefixedData(this, cmp, 0, prefixLength);

				if (r != 0)
					return r * -1;

				// compare non prefixed data

				size -= prefixLength;

				r = other.CompareNonPrefixedData(0, this, prefixLength, cmp, size); // TODO arek - last check

				return r * -1;
			}

			if (HasPrefixHeader && other.HasPrefixHeader == false)
			{
				var prefixLength = Math.Min(PrefixUsage, size);

				var r = ComparePrefixWithNonPrefixedData(other, cmp, 0, prefixLength);

				if (r != 0)
					return r;

				// compare non prefixed data

				size -= prefixLength;

				r = CompareNonPrefixedData(0, other, prefixLength, cmp, size);

				return r;
			}

			throw new NotSupportedException();
		}

		private class SlicePrefixMatcher : IDisposable
		{
			private readonly int _maxPrefixLength;

			public SlicePrefixMatcher(int maxPrefixLength)
			{
				_maxPrefixLength = maxPrefixLength;
				MatchedBytes = 0;
			}

			public int MatchedBytes { get; private set; }

			public int MatchPrefix(byte* a, byte* b, int size)
			{
				for (var i = 0; i < Math.Min(_maxPrefixLength, size); i++)
				{
					if (*a == *b)
						MatchedBytes++;
					else
						break;

					a++;
					b++;
				}

				return 0;
			}

			public void Dispose()
			{
			}
		}

		public int FindPrefixSize(Slice other)
		{
			var keySize = HasPrefixHeader ? Size - Constants.PrefixedSliceHeaderSize + PrefixUsage : Size;
			var otherKeySize = other.HasPrefixHeader ? other.Size - Constants.PrefixedSliceHeaderSize + other.PrefixUsage : other.Size;

			// TODO arek - 

			using (var slicePrefixMatcher = new SlicePrefixMatcher(Math.Min(keySize, otherKeySize)))
			{
				CompareData(other, slicePrefixMatcher.MatchPrefix, (ushort) Math.Min(keySize, otherKeySize));

				return slicePrefixMatcher.MatchedBytes;
			}
		}

		public static implicit operator Slice(string s)
		{
			return new Slice(Encoding.UTF8.GetBytes(s));
		}

		public void CopyTo(byte* dest, int? count = null)
		{
			if (HasPrefixHeader == false)
			{
				if (_array == null)
				{
					NativeMethods.memcpy(dest, NonPrefixedData, count ?? Size);
					return;
				}
				fixed (byte* a = _array)
				{
					NativeMethods.memcpy(dest, a, count ?? Size);
				}
			}
			else
			{
				Debug.Assert(count == null); // TODO arek - temp assertion

				NativeMethods.memcpy(dest, (byte*) _prefixHeader, Constants.PrefixedSliceHeaderSize);
				NativeMethods.memcpy(dest + Constants.PrefixedSliceHeaderSize, NonPrefixedData, NonPrefixedDataSize);
			}
		}

		public void CopyTo(byte[] dest)
		{
			if (HasPrefixHeader == false)
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
				throw new NotImplementedException("TODO arek");
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

			if (HasPrefixHeader == false)
			{
				if (_array == null)
				{
					NativeMethods.memcpy(dest + offset, NonPrefixedData + from, count);
					return;
				}

				fixed (byte* p = _array)
					NativeMethods.memcpy(dest + offset, p + from, count);
			}
			else
			{
				// TODO arek
				throw new NotImplementedException("TODO arek");
			}
		}

		public Slice Clone()
		{
			Debug.Assert(HasPrefixHeader == false);

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
		    if (HasPrefixHeader == false)
		    {
			    if (_array != null)
				    return new ValueReader(_array, Size);

			    return new ValueReader(NonPrefixedData, Size);
		    }

		    if (IsPrefixed)
		    {
				Debug.Assert(_prefix != null);

			    var key = new byte[PrefixUsage + NonPrefixedDataSize];

			    fixed (byte* p = key)
			    { // TODO arek - fix it
				    NativeMethods.memcpy(p, _prefix.ValuePtr, PrefixUsage);
				    NativeMethods.memcpy(p + PrefixUsage, NonPrefixedData, NonPrefixedDataSize);
			    }

				return new ValueReader(key, key.Length);
		    }

			return new ValueReader(NonPrefixedData, NonPrefixedDataSize);
	    }
	}
}