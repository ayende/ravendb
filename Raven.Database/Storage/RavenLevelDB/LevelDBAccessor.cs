using System;

namespace Raven.Database.Storage.RavenLevelDB
{
    public class LevelDBAccessor
    {
        private readonly LevelDBStorage storage;
        private readonly LevelDBTransaction transaction;

        public LevelDBAccessor(LevelDBStorage storage, LevelDBTransaction transaction)
        {
            this.storage = storage;
            this.transaction = transaction;
        }

        public Object Get(string id)
        {
            return this.storage.Get(id, this.transaction);
        }

        public void Put(string id, Object obj)
        {
            this.storage.Put(id, obj, this.transaction);
        }

        public void Delete(string id)
        {
            this.storage.Delete(id, this.transaction);
        }
    }
}