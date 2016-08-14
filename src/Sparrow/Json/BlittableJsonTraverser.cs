﻿using System;
using System.Collections;
using System.Collections.Generic;

namespace Sparrow.Json
{
    public class BlittableJsonTraverser
    {
        public static BlittableJsonTraverser Default = new BlittableJsonTraverser();

        private const char PropertySeparator = '.';
        private const char CollectionSeparator = ',';

        private readonly char[] _separators =
        {
            PropertySeparator,
            CollectionSeparator

        };

        public BlittableJsonTraverser(char[] nonDefaultSeparators = null)
        {
            if (nonDefaultSeparators != null)
                _separators = nonDefaultSeparators;
        }

        public bool TryRead(BlittableJsonReaderObject docReader, StringSegment path, out object result)
        {
            var indexOfFirstSeparator = path.IndexOfAny(_separators, 0);
            object reader;

            //if not found -> indexOfFirstSeparator == -1 -> take whole includePath as segment
            if (docReader.TryGetMember(path.SubSegment(0, indexOfFirstSeparator), out reader) == false)
            {
                result = null;
                return false;
            }

            if (indexOfFirstSeparator == -1)
            {
                result = reader;
                return true;
            }

            var pathSegment = path.SubSegment(indexOfFirstSeparator + 1);

            switch (path[indexOfFirstSeparator])
            {
                case PropertySeparator:
                    var subObject = reader as BlittableJsonReaderObject;
                    if (subObject != null)
                    {
                        return TryRead(subObject, pathSegment, out result);
                    }

                    BlittableJsonReaderArray array;
                    switch (pathSegment)
                    {
                        case "Length":
                            var lazyStringValue = reader as LazyStringValue;
                            if (lazyStringValue != null)
                            {
                                result = lazyStringValue.Size;
                                return true;
                            }

                            var lazyCompressedStringValue = reader as LazyCompressedStringValue;
                            if (lazyCompressedStringValue != null)
                            {
                                result = lazyCompressedStringValue.UncompressedSize;
                                return true;
                            }

                            array = reader as BlittableJsonReaderArray;
                            if (array != null)
                            {
                                result = array.Length;
                                return true;
                            }
                            break;
                        case "Count":
                            array = reader as BlittableJsonReaderArray;
                            if (array != null)
                            {
                                result = array.Length;
                                return true;
                            }
                            break;
                    }

                    throw new InvalidOperationException($"Invalid path. After the property separator ('{PropertySeparator}') {reader?.GetType()?.FullName ?? "null"} object has been ancountered instead of {nameof(BlittableJsonReaderObject)}.");
                case CollectionSeparator:
                    var subArray = reader as BlittableJsonReaderArray;
                    if (subArray != null)
                    {
                        result = ReadArray(subArray, pathSegment);
                        return true;
                    }

                    throw new InvalidOperationException($"Invalid path. After the collection separator ('{CollectionSeparator}') {reader?.GetType()?.FullName ?? "null"}  object has been ancountered instead of {nameof(BlittableJsonReaderArray)}.");
                default:
                    throw new NotSupportedException($"Unhandled separator character: {path[indexOfFirstSeparator]}");
            }
        }

        private IEnumerable<object> ReadArray(BlittableJsonReaderArray array, StringSegment pathSegment)
        {
            // ReSharper disable once ForCanBeConvertedToForeach
            for (int i = 0; i < array.Length; i++)
            {
                var item = array[i];
                var arrayObject = item as BlittableJsonReaderObject;
                if (arrayObject != null)
                {
                    object result;
                    if (TryRead(arrayObject, pathSegment, out result))
                    {
                        var enumerable = result as IEnumerable;

                        if (enumerable != null)
                        {
                            foreach (var nestedItem in enumerable)
                            {
                                yield return nestedItem;
                            }
                        }
                        else
                        {
                            yield return result;
                        }
                    }
                }
                else
                {
                    var arrayReader = item as BlittableJsonReaderArray;
                    if (arrayReader != null)
                    {
                        var indexOfFirstSeparatorInSubIndex = pathSegment.IndexOfAny(_separators, 0);
                        var subSegment = pathSegment.SubSegment(indexOfFirstSeparatorInSubIndex + 1);

                        foreach (var nestedItem in ReadArray(arrayReader, subSegment))
                        {
                            yield return nestedItem;
                        }
                    }
                    else
                    {
                        yield return item;
                    }
                }
            }
        }
    }
}