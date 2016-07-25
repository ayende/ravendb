using System;
using System.Collections.Generic;
using System.IO;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server
{
    public static class ChangeVectorExtensions
    {
        public static BlittableJsonReaderArray ToBlittableJsonArray(this ChangeVectorEntry[] changeVector, DocumentsOperationContext context)
        {
            var array = new DynamicJsonArray();
            foreach (var entry in changeVector)
            {
                array.Add(new DynamicJsonValue
                {
                    ["DbId"] = entry.DbId.ToString(),
                    ["Etag"] = entry.Etag
                });
            }

            return context.ReadArray(array, "ToBlittableJson() Extension");
        }

        public static ChangeVectorEntry[] FromBlittableJsonArray(this BlittableJsonReaderArray array, DocumentsOperationContext context)
        {
            var result = new ChangeVectorEntry[array.Length];
            var index = 0;
            foreach (BlittableJsonReaderObject doc in array)
            {
                doc.ThrowIfMissingProperty("DbId");
                doc.ThrowIfMissingProperty("Etag");

                string dbIdAsString;
                if(!doc.TryGet("DbId",out dbIdAsString))
                    throw new InvalidDataException($"Tried to fetch DbId property from {doc}, but it was not a string.");

                if (!Guid.TryParse(dbIdAsString, out result[index].DbId))
                    throw new InvalidDataException($"Tried to parse Guid from {dbIdAsString}, but failed.");

                long etag;
                if (!doc.TryGet("Etag", out etag))
                    throw new InvalidDataException($"Tried to fetch Etag property from {doc}, but it was not a number.");

                result[index].Etag = etag;
                index++;
            }

            return result;
        }

        public static void ThrowIfMissingProperty(this BlittableJsonReaderObject obj, string propertyName)
        {
            object _;
            if(!obj.TryGet(propertyName,out _))
                throw new MissingMemberException($"Property named '{propertyName}' is missing from blittable json document -> {{{obj.ToString()}}}");
        }

        public static bool UpdateLargerEtagIfRelevant(this ChangeVectorEntry[] changeVector,
                   Dictionary<Guid, long> maxEtagsPerDbId)
        {
            var changeVectorUpdated = false;
            for (int i = 0; i < changeVector.Length; i++)
            {
                long dbEtag;
                if (maxEtagsPerDbId.TryGetValue(changeVector[i].DbId, out dbEtag) == false)
                    continue;
                maxEtagsPerDbId.Remove(changeVector[i].DbId);
                if (dbEtag > changeVector[i].Etag)
                {
                    changeVectorUpdated = true;
                    changeVector[i].Etag = dbEtag;
                }
            }

            return changeVectorUpdated;
        }

        public static ChangeVectorEntry[] InsertNewEtagsIfRelevant(
            this ChangeVectorEntry[] changeVector,
            Dictionary<Guid, long> maxEtagsPerDbId,
            out bool hasResized)
        {
            hasResized = false;

            if (maxEtagsPerDbId.Count <= 0)
                return changeVector;

            hasResized = true;
            var oldSize = changeVector.Length;
            Array.Resize(ref changeVector, oldSize + maxEtagsPerDbId.Count);

            foreach (var kvp in maxEtagsPerDbId)
            {
                changeVector[oldSize++] = new ChangeVectorEntry
                {
                    DbId = kvp.Key,
                    Etag = kvp.Value,
                };
            }

            return changeVector;
        }
    }
}
