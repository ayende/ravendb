# End-to-end HNSW vs Centroids comparison through a RavenDB server. VECTORS_DIR holds X.npy (vectors), Q.npy (queries) and truth.npy (exact neighbors).
# usage: compare_server.py <server-url> [load] ; builds both indexes one after the other and reports
# indexing time, index size on disk, recall@10 and query latency (server-side and client-side).
import numpy as np, requests, json, sys, time, os
from concurrent.futures import ThreadPoolExecutor

URL = sys.argv[1] if len(sys.argv) > 1 else 'http://127.0.0.1:8095'
DB = 'vec'
S = requests.Session()
DATA = os.environ.get('VECTORS_DIR', '/work/ravendb/vecexp')
Q = np.load(f'{DATA}/Q.npy'); T = np.load(f'{DATA}/truth.npy')[:, :10]
N = np.load(f'{DATA}/X.npy', mmap_mode='r').shape[0]

def load():
    S.put(f'{URL}/admin/databases?name={DB}&replicationFactor=1', json={'DatabaseName': DB})
    X = np.load(f'{DATA}/X.npy')
    def batch(start):
        cmds = [{'Id': f'vectors/{i}', 'Type': 'PUT', 'ChangeVector': None,
                 'Document': {'Embedding': X[i].tolist(), '@metadata': {'@collection': 'Vectors'}}}
                for i in range(start, min(start + 500, len(X)))]
        requests.post(f'{URL}/databases/{DB}/bulk_docs', data=json.dumps({'Commands': cmds})).raise_for_status()
    t0 = time.time()
    with ThreadPoolExecutor(8) as ex:
        list(ex.map(batch, range(0, len(X), 500)))
    print(f'loaded {len(X)} documents in {time.time() - t0:.0f}s', flush=True)

def build(name, strategy, extra):
    vec = {'Dimensions': int(Q.shape[1]), 'SourceEmbeddingType': 'Single', 'DestinationEmbeddingType': 'Single', 'IndexingStrategy': strategy, **extra}
    d = {'Name': name, 'Type': 'Map', 'Maps': ['from d in docs.Vectors select new { Embedding = CreateVector(d.Embedding) }'],
         'Fields': {'Embedding': {'Vector': vec}}, 'Configuration': {'Indexing.Static.SearchEngineType': 'Corax'}}
    S.put(f'{URL}/databases/{DB}/admin/indexes', json={'Indexes': [d]}).raise_for_status()
    t0 = time.time()
    while True:
        st = S.get(f'{URL}/databases/{DB}/indexes/stats', params={'name': name}).json()['Results'][0]
        if st.get('IsStale') is False and st.get('EntriesCount', 0) >= N:
            break
        time.sleep(1)
    elapsed = time.time() - t0
    return elapsed, index_size(name)

def index_size(name):
    report = S.get(f'{URL}/databases/{DB}/debug/storage/report').json()
    for env in report.get('Results', []):
        if env.get('Type') == 'Index' and env.get('Name') == name:
            r = env['Report']
            return r['DataFile']['UsedSpaceInBytes'], r['DataFile']['AllocatedSpaceInBytes']
    return None, None

def query(name, cands, n=1000):
    rql = f"from index '{name}' where vector.search(Embedding, $q, 0, {cands}) limit 10"
    def one(i):
        t0 = time.perf_counter()
        r = S.post(f'{URL}/databases/{DB}/queries?metadataOnly=true', json={'Query': rql, 'QueryParameters': {'q': Q[i].tolist()}})
        r.raise_for_status()
        j = r.json()
        ids = [int(x['@metadata']['@id'].split('/')[1]) for x in j['Results']]
        return len(set(ids) & set(T[i].tolist())) / 10, j.get('DurationInMs', 0), time.perf_counter() - t0
    for i in range(100):
        one(i)
    res = [one(i) for i in range(n)]
    wall = np.array([c for _, _, c in res]) * 1000
    return np.mean([a for a, _, _ in res]), np.mean([b for _, b, _ in res]), np.median(wall), np.percentile(wall, 99)

if __name__ == '__main__':
    if 'load' in sys.argv:
        load()
    for name, strategy, extra in (('Hnsw', 'Hnsw', {'NumberOfEdges': 16, 'NumberOfCandidatesForIndexing': 100}),
                                  ('Centroids', 'Centroids', {})):
        elapsed, (used, allocated) = build(name, strategy, extra)
        print(f'[{name}] indexed {N:,} vectors in {elapsed:.1f}s ({N / elapsed:,.0f}/s), index data file used {used / 2**20:.0f} MB, allocated {allocated / 2**20:.0f} MB', flush=True)
        for cands in (16, 32, 64, 128):
            rec, server_ms, p50, p99 = query(name, cands)
            print(f'[{name}] numberOfCandidates={cands:4d} recall@10 {rec:.3f}  server avg {server_ms:.2f} ms  client p50 {p50:.2f} ms  p99 {p99:.2f} ms', flush=True)
