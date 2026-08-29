import os
import shutil
import tempfile
import pytest
from algorithms.lsm_tree import LSMTree


@pytest.fixture
def lsm_instance():
    temp_dir = tempfile.mkdtemp()
    tree = LSMTree(data_dir=temp_dir, memtable_threshold=3, compaction_threshold=3)
    yield tree
    tree.destroy()


def test_lsm_tree_put_get_memtable(lsm_instance):
    """Verify in-memory write and read before flushing."""
    lsm_instance.put("product:1", {"name": "Laptop", "price": 999.0})
    lsm_instance.put("product:2", {"name": "Mouse", "price": 25.0})

    assert lsm_instance.get("product:1") == {"name": "Laptop", "price": 999.0}
    assert lsm_instance.get("product:2") == {"name": "Mouse", "price": 25.0}
    assert lsm_instance.get("product:99") is None


def test_lsm_tree_flush_and_sstable_reads(lsm_instance):
    """Verify that exceeding memtable threshold flushes to SSTables and reads still succeed."""
    # Insert 5 items (threshold is 3, so first 3 trigger flush to SSTable 0, next 2 stay in memtable)
    for i in range(5):
        lsm_instance.put(f"key:{i}", f"val_{i}")

    assert len(lsm_instance.sstables) >= 1

    # Verify reads across both SSTable and MemTable
    for i in range(5):
        assert lsm_instance.get(f"key:{i}") == f"val_{i}"


def test_lsm_tree_delete_and_tombstones(lsm_instance):
    """Verify deletion writes tombstone and hides previously stored values."""
    lsm_instance.put("user:42", {"email": "test@example.com"})
    lsm_instance.flush() # Force into SSTable

    assert lsm_instance.get("user:42") == {"email": "test@example.com"}

    # Delete
    lsm_instance.delete("user:42")
    assert lsm_instance.get("user:42") is None


def test_lsm_tree_manual_compaction(lsm_instance):
    """Verify merge-sort compaction merges multiple SSTables into one and removes tombstones."""
    lsm_instance.compaction_threshold = 10

    # Flush 1
    lsm_instance.put("a", "old_a")
    lsm_instance.put("b", "b_val")
    lsm_instance.flush()

    # Flush 2: update 'a' and delete 'b'
    lsm_instance.put("a", "new_a")
    lsm_instance.delete("b")
    lsm_instance.flush()

    # Flush 3: add 'c'
    lsm_instance.put("c", "c_val")
    lsm_instance.flush()

    assert len(lsm_instance.sstables) == 3

    # Trigger compaction
    lsm_instance.compact()

    assert len(lsm_instance.sstables) == 1
    assert lsm_instance.get("a") == "new_a"
    assert lsm_instance.get("b") is None  # Tombstone purged
    assert lsm_instance.get("c") == "c_val"


def test_lsm_tree_auto_compaction_on_threshold(lsm_instance):
    """Verify that hitting compaction_threshold triggers automatic compaction during flush."""
    lsm_instance.compaction_threshold = 3

    lsm_instance.put("k1", "v1")
    lsm_instance.flush()
    assert len(lsm_instance.sstables) == 1

    lsm_instance.put("k2", "v2")
    lsm_instance.flush()
    assert len(lsm_instance.sstables) == 2

    # 3rd flush hits threshold 3 and auto-compacts to 1
    lsm_instance.put("k3", "v3")
    lsm_instance.flush()
    assert len(lsm_instance.sstables) == 1
    assert lsm_instance.get("k1") == "v1"
    assert lsm_instance.get("k2") == "v2"
    assert lsm_instance.get("k3") == "v3"


def test_lsm_tree_wal_crash_recovery():
    """Verify that un-flushed writes in MemTable are recovered from WAL on restart."""
    temp_dir = tempfile.mkdtemp()
    
    # 1. First instance writes without flushing
    tree1 = LSMTree(data_dir=temp_dir, memtable_threshold=100)
    tree1.put("session:abc", {"user_id": 999})
    tree1.put("session:xyz", {"user_id": 888})
    # Simulate sudden crash / process exit without tree1.close() or flush
    
    # 2. Second instance opens same data directory and recovers
    tree2 = LSMTree(data_dir=temp_dir, memtable_threshold=100)
    assert tree2.get("session:abc") == {"user_id": 999}
    assert tree2.get("session:xyz") == {"user_id": 888}
    
    tree2.destroy()
