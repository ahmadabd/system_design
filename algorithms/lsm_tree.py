import os
import json
import time
import shutil
from typing import Dict, Any, Optional, List, Tuple
from algorithms.bloom_filter import BloomFilter

TOMBSTONE = "__LSM_TOMBSTONE__"


class SSTable:
    """
    Sorted String Table (SSTable):
    An immutable, sorted on-disk file containing key-value pairs,
    a sparse index for fast seeks, and an embedded Bloom Filter to prevent disk reads.
    """
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.sparse_index: List[Tuple[str, int]] = []  # (key, record_index)
        self.bloom_filter: Optional[BloomFilter] = None
        self.min_key: Optional[str] = None
        self.max_key: Optional[str] = None
        self.records: List[Tuple[str, Any, float]] = []  # (key, value, timestamp)
        
        if os.path.exists(filepath):
            self._load()

    @classmethod
    def create_from_memtable(
        cls, 
        filepath: str, 
        memtable_data: Dict[str, Tuple[Any, float]], 
        index_interval: int = 16
    ) -> "SSTable":
        """
        Creates and serializes a new immutable SSTable from sorted MemTable entries.
        """
        table = cls.__new__(cls)
        table.filepath = filepath
        table.records = []
        table.sparse_index = []

        keys = sorted(memtable_data.keys())
        if not keys:
            raise ValueError("Cannot create SSTable from empty memtable")

        table.min_key = keys[0]
        table.max_key = keys[-1]

        # Initialize embedded Bloom filter with 1% false positive rate
        table.bloom_filter = BloomFilter(expected_elements=max(len(keys), 10), false_positive_rate=0.01)

        for idx, key in enumerate(keys):
            val, ts = memtable_data[key]
            table.records.append((key, val, ts))
            table.bloom_filter.add(key)
            if idx % index_interval == 0:
                table.sparse_index.append((key, idx))

        table._save()
        return table

    def _save(self) -> None:
        """Serializes SSTable to disk."""
        data = {
            "min_key": self.min_key,
            "max_key": self.max_key,
            "sparse_index": self.sparse_index,
            "bloom_filter": list(self.bloom_filter.to_bytes()),
            "bloom_expected": self.bloom_filter.expected_elements,
            "bloom_p": self.bloom_filter.false_positive_rate,
            "records": self.records
        }
        temp_path = f"{self.filepath}.tmp"
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(data, f)
        os.replace(temp_path, self.filepath)

    def _load(self) -> None:
        """Loads SSTable metadata, sparse index, and Bloom filter into memory."""
        with open(self.filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        self.min_key = data["min_key"]
        self.max_key = data["max_key"]
        self.sparse_index = [tuple(item) for item in data["sparse_index"]]
        raw_bloom_bytes = bytes(data["bloom_filter"])
        self.bloom_filter = BloomFilter.from_bytes(
            raw_bloom_bytes, 
            expected_elements=data["bloom_expected"], 
            false_positive_rate=data["bloom_p"]
        )
        self.records = [tuple(rec) for rec in data["records"]]

    def get(self, key: str) -> Tuple[bool, Optional[Any], float]:
        """
        Queries the SSTable for a key.
        Returns: (found, value, timestamp)
        """
        # 1. Key Range Check
        if self.min_key is None or self.max_key is None:
            return (False, None, 0.0)
        if key < self.min_key or key > self.max_key:
            return (False, None, 0.0)

        # 2. Bloom Filter Check (Zero disk/memory search if missing)
        if not self.bloom_filter.exists(key):
            return (False, None, 0.0)

        # 3. Binary Search with Sparse Index Range
        low_idx = 0
        high_idx = len(self.records)

        for idx_key, rec_idx in self.sparse_index:
            if idx_key <= key:
                low_idx = rec_idx
            else:
                high_idx = rec_idx
                break

        # Search within bounded slice [low_idx, high_idx]
        for i in range(low_idx, high_idx):
            r_key, r_val, r_ts = self.records[i]
            if r_key == key:
                return (True, r_val, r_ts)
            elif r_key > key:
                break

        return (False, None, 0.0)


class LSMTree:
    """
    A full educational and functional Log-Structured Merge-tree (LSM Tree) storage engine.
    
    Architecture:
    - MemTable: In-memory write buffer (sorted).
    - Write-Ahead Log (WAL): Sequential append-only disk log for durability.
    - SSTables: Immutable sorted disk files with embedded Bloom filters.
    - Background Compaction: Merge-sorts multiple SSTables, purging tombstones and old versions.
    """
    def __init__(
        self, 
        data_dir: str, 
        memtable_threshold: int = 5, 
        compaction_threshold: int = 4
    ):
        self.data_dir = data_dir
        self.memtable_threshold = memtable_threshold
        self.compaction_threshold = compaction_threshold
        
        os.makedirs(self.data_dir, exist_ok=True)
        self.wal_path = os.path.join(self.data_dir, "wal.log")
        
        # MemTable: dict of key -> (value, timestamp)
        self.memtable: Dict[str, Tuple[Any, float]] = {}
        self.sstables: List[SSTable] = []
        self._sstable_seq = 0
        
        # Load existing SSTables and recover from WAL
        self._load_existing_sstables()
        self._recover_from_wal()

    def _load_existing_sstables(self) -> None:
        """Discovers and loads all existing SSTables from data directory."""
        files = sorted(
            [f for f in os.listdir(self.data_dir) if f.startswith("sstable_") and f.endswith(".db")]
        )
        self.sstables = []
        for filename in files:
            filepath = os.path.join(self.data_dir, filename)
            self.sstables.append(SSTable(filepath))
            # Track max sequence id
            seq_part = filename.replace("sstable_", "").replace(".db", "")
            if seq_part.isdigit():
                self._sstable_seq = max(self._sstable_seq, int(seq_part) + 1)

    def _recover_from_wal(self) -> None:
        """Replays WAL entries to restore un-flushed MemTable state on crash recovery."""
        if not os.path.exists(self.wal_path):
            return

        with open(self.wal_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                entry = json.loads(line)
                key = entry["key"]
                val = entry["val"]
                ts = entry["ts"]
                self.memtable[key] = (val, ts)

    def _append_wal(self, key: str, val: Any, ts: float) -> None:
        """Appends a mutation sequentially to WAL on disk."""
        with open(self.wal_path, "a", encoding="utf-8") as f:
            f.write(json.dumps({"key": key, "val": val, "ts": ts}) + "\n")
            f.flush()

    def put(self, key: str, value: Any) -> None:
        """Writes or updates a key-value pair."""
        ts = time.time()
        self._append_wal(key, value, ts)
        self.memtable[key] = (value, ts)

        if len(self.memtable) >= self.memtable_threshold:
            self.flush()

    def delete(self, key: str) -> None:
        """Deletes a key by writing a Tombstone."""
        ts = time.time()
        self._append_wal(key, TOMBSTONE, ts)
        self.memtable[key] = (TOMBSTONE, ts)

        if len(self.memtable) >= self.memtable_threshold:
            self.flush()

    def get(self, key: str) -> Optional[Any]:
        """
        Reads a key with LSM hierarchy search:
        1. Check MemTable (fastest, newest).
        2. Check SSTables from newest to oldest (filtered by Bloom Filter).
        """
        # 1. Search MemTable
        if key in self.memtable:
            val, _ = self.memtable[key]
            return None if val == TOMBSTONE else val

        # 2. Search SSTables from Newest to Oldest
        for sstable in reversed(self.sstables):
            found, val, _ = sstable.get(key)
            if found:
                return None if val == TOMBSTONE else val

        return None

    def flush(self) -> None:
        """Flushes in-memory MemTable to a new immutable SSTable file on disk."""
        if not self.memtable:
            return

        sstable_filename = f"sstable_{self._sstable_seq:05d}.db"
        self._sstable_seq += 1
        sstable_path = os.path.join(self.data_dir, sstable_filename)

        new_sstable = SSTable.create_from_memtable(sstable_path, self.memtable)
        self.sstables.append(new_sstable)

        # Clear MemTable and truncate WAL
        self.memtable.clear()
        if os.path.exists(self.wal_path):
            open(self.wal_path, "w").close()

        # Trigger compaction if SSTable count exceeds threshold
        if len(self.sstables) >= self.compaction_threshold:
            self.compact()

    def compact(self) -> None:
        """
        Performs Merge-Sort Compaction:
        Merges all SSTables into a single new consolidated SSTable,
        keeping only the latest versions and purging tombstones.
        """
        if len(self.sstables) < 2:
            return

        merged_records: Dict[str, Tuple[Any, float]] = {}

        # Merge from oldest to newest so newer timestamps overwrite older
        for sstable in self.sstables:
            for key, val, ts in sstable.records:
                if key not in merged_records or ts > merged_records[key][1]:
                    merged_records[key] = (val, ts)

        # Purge tombstones during compaction
        active_records = {
            k: (v, ts) for k, (v, ts) in merged_records.items() if v != TOMBSTONE
        }

        old_sstables = list(self.sstables)

        if active_records:
            compacted_filename = f"sstable_{self._sstable_seq:05d}.db"
            self._sstable_seq += 1
            compacted_path = os.path.join(self.data_dir, compacted_filename)
            new_sstable = SSTable.create_from_memtable(compacted_path, active_records)
            self.sstables = [new_sstable]
        else:
            self.sstables = []

        # Remove old SSTable files
        for old_sstable in old_sstables:
            if os.path.exists(old_sstable.filepath):
                try:
                    os.remove(old_sstable.filepath)
                except OSError:
                    pass

    def close(self) -> None:
        """Flushes remaining data and closes."""
        self.flush()

    def destroy(self) -> None:
        """Completely wipes data directory."""
        if os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir)


def demo():
    print("=== LSM Tree Storage Engine Demonstration ===")
    test_dir = "/tmp/lsm_tree_demo"
    lsm = LSMTree(data_dir=test_dir, memtable_threshold=3, compaction_threshold=3)

    # 1. Writes
    print("\n1. Writing keys to MemTable and WAL...")
    lsm.put("user:101", {"name": "Alice", "role": "admin"})
    lsm.put("user:102", {"name": "Bob", "role": "user"})
    lsm.put("user:103", {"name": "Charlie", "role": "user"}) # Triggers flush to SSTable 1

    lsm.put("user:104", {"name": "David", "role": "user"})
    lsm.put("user:101", {"name": "Alice Cooper", "role": "admin"}) # Update
    lsm.delete("user:102") # Triggers flush to SSTable 2

    # 2. Reads
    print("\n2. Querying Keys:")
    print("user:101 (Updated):", lsm.get("user:101"))
    print("user:102 (Deleted/Tombstone):", lsm.get("user:102"))
    print("user:103 (From SSTable 1):", lsm.get("user:103"))
    print("user:999 (Non-existent, skipped by Bloom Filter):", lsm.get("user:999"))

    print(f"\nSSTables count before compaction: {len(lsm.sstables)}")
    lsm.compact()
    print(f"SSTables count after compaction: {len(lsm.sstables)}")
    print("user:101 after compaction:", lsm.get("user:101"))
    print("user:102 after compaction:", lsm.get("user:102"))

    lsm.destroy()


if __name__ == "__main__":
    demo()
