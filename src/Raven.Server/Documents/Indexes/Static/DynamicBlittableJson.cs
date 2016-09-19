using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Linq;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Static
{
    public class DynamicBlittableJson : DynamicObject, IEnumerable<object>, IBlittableJsonContainer
    {
        public BlittableJsonReaderObject BlittableJson { get; private set; }

        private LazyStringValue _key;

        public DynamicBlittableJson(Document document)
        {
            Set(document);
        }

        public DynamicBlittableJson(BlittableJsonReaderObject blittableJson)
        {
            BlittableJson = blittableJson;
        }

        public void Set(Document document)
        {
            _key = document.Key;
            BlittableJson = document.Data;
        }

        public bool ContainsKey(string key)
        {
            return BlittableJson.GetPropertyNames().Contains(key);
        }

        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            var name = binder.Name;
            return TryGetByName(name, out result);
        }

        public override bool TryGetIndex(GetIndexBinder binder, object[] indexes, out object result)
        {
            return TryGetByName((string)indexes[0], out result);
        }

        private bool TryGetByName(string name, out object result)
        {
            if (name == Constants.Indexing.Fields.DocumentIdFieldName || name == "Id")
            {
                if (_key == null)
                {
                    if (BlittableJson.TryGetMember(name, out result) == false)
                        result = DynamicNullObject.Null;

                    return true;
                }

                result = _key;
                return true;
            }

            var getResult = BlittableJson.TryGetMember(name, out result);

            if (getResult == false && (name == Constants.Metadata.Id || name == Constants.Metadata.Etag))
            {
                result = BlittableJson.Modifications[name];
                getResult = result != null;
            }

            if (result == null && name == "HasValue")
            {
                result = getResult;
                return true;
            }

            if (getResult && result == null)
            {
                result = DynamicNullObject.ExplicitNull;
                return true;
            }

            if (getResult == false)
            {
                result = DynamicNullObject.Null;
                return true;
            }

            result = TypeConverter.ToDynamicType(result);
            return true;
        }

        public object this[string key]
        {
            get
            {
                if (Constants.Headers.LastModified.Equals(key, StringComparison.OrdinalIgnoreCase)) // TODO - avoid two headers for last doc modification
                    key = Constants.Headers.RavenLastModified;

                object result;
                if (TryGetByName(key, out result) == false)
                    throw new InvalidOperationException($"Could not get '{key}' value of dynamic object");

                return result;
            }
        }

        public T Value<T>(string key)
        {
            return TypeConverter.Convert<T>(this[key], false);
        }

        public IEnumerator<object> GetEnumerator()
        {
            foreach (var propertyName in BlittableJson.GetPropertyNames())
            {
                yield return new KeyValuePair<object, object>(propertyName, TypeConverter.ToDynamicType(BlittableJson[propertyName]));
            }
        }

        public IEnumerable<object> Select(Func<object, object> func)
        {
            return new DynamicArray(Enumerable.Select(this, func));
        }

        public IEnumerable<object> OrderBy(Func<object, object> func)
        {
            return new DynamicArray(Enumerable.OrderBy(this, func));
        }

        public override string ToString()
        {
            return BlittableJson.ToString();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;

            return Equals((DynamicBlittableJson)obj);
        }

        protected bool Equals(DynamicBlittableJson other)
        {
            return Equals(BlittableJson, other.BlittableJson);
        }

        public override int GetHashCode()
        {
            return BlittableJson?.GetHashCode() ?? 0;
        }
    }
}