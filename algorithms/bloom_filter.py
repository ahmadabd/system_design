import math
import hashlib
from typing import List, Optional, Any, Union


class BloomFilter:
    """
    A space-efficient, probabilistic data structure for set membership testing.
    
    Guarantees:
    - Zero False Negatives: If exists(key) returns False, the key is definitely NOT in the set.
    - Controllable False Positives: If exists(key) returns True, the key PROBABLY is in the set.
    
    Uses Kirsch-Mitzenmacher double-hashing technique:
        gi(x) = (h1(x) + i * h2(x)) mod m
    to generate k independent hash functions from two 128-bit hashes.
    """
    def __init__(self, expected_elements: int = 10000, false_positive_rate: float = 0.01):
        if expected_elements <= 0:
            raise ValueError("expected_elements must be > 0")
        if not (0 < false_positive_rate < 1):
            raise ValueError("false_positive_rate must be between 0 and 1 (exclusive)")

        self.expected_elements = expected_elements
        self.false_positive_rate = false_positive_rate
        
        # Calculate optimal bit array size (m) and number of hash functions (k)
        # m = - (n * ln(p)) / (ln(2)^2)
        self.size = int(- (expected_elements * math.log(false_positive_rate)) / (math.log(2) ** 2))
        self.size = max(self.size, 8)
        
        # k = (m / n) * ln(2)
        self.num_hashes = int((self.size / expected_elements) * math.log(2))
        self.num_hashes = max(self.num_hashes, 1)
        
        # Bytearray representation: 8 bits per byte
        self._byte_count = (self.size + 7) // 8
        self.bit_array = bytearray(self._byte_count)
        self.count = 0

    def _get_hashes(self, item: str) -> List[int]:
        """
        Derives `num_hashes` distinct bit positions using double hashing.
        """
        item_bytes = str(item).encode("utf-8")
        # Generate two 64-bit independent hashes using sha256
        digest = hashlib.sha256(item_bytes).digest()
        h1 = int.from_bytes(digest[:8], byteorder="big")
        h2 = int.from_bytes(digest[8:16], byteorder="big")
        
        # If h2 is 0 or even, adjust to ensure good distribution
        if h2 == 0:
            h2 = 1

        indices = []
        for i in range(self.num_hashes):
            combined_hash = (h1 + i * h2) % self.size
            indices.append(combined_hash)
        return indices

    def add(self, item: str) -> None:
        """Adds an element to the Bloom filter."""
        for bit_index in self._get_hashes(item):
            byte_index = bit_index // 8
            bit_offset = bit_index % 8
            self.bit_array[byte_index] |= (1 << bit_offset)
        self.count += 1

    def exists(self, item: str) -> bool:
        """
        Tests membership.
        Returns False if the item is GUARANTEED not in the set.
        Returns True if the item is PROBABLY in the set.
        """
        for bit_index in self._get_hashes(item):
            byte_index = bit_index // 8
            bit_offset = bit_index % 8
            if not (self.bit_array[byte_index] & (1 << bit_offset)):
                return False
        return True

    def __contains__(self, item: str) -> bool:
        return self.exists(item)

    def to_bytes(self) -> bytes:
        """Serializes the bit array for storage or transmission."""
        return bytes(self.bit_array)

    @classmethod
    def from_bytes(
        cls, 
        raw_bytes: bytes, 
        expected_elements: int = 10000, 
        false_positive_rate: float = 0.01
    ) -> "BloomFilter":
        """Restores a BloomFilter instance from serialized bytes."""
        instance = cls(expected_elements=expected_elements, false_positive_rate=false_positive_rate)
        if len(raw_bytes) != len(instance.bit_array):
            raise ValueError(f"Byte length mismatch: expected {len(instance.bit_array)}, got {len(raw_bytes)}")
        instance.bit_array = bytearray(raw_bytes)
        return instance

    def clear(self) -> None:
        """Resets the bit array."""
        self.bit_array = bytearray(self._byte_count)
        self.count = 0

    @property
    def estimated_false_positive_rate(self) -> float:
        """Calculates current false positive probability based on current fill rate."""
        # 1. Count set bits
        set_bits = sum(bin(byte).count("1") for byte in self.bit_array)
        fill_ratio = set_bits / self.size
        # p = (1 - e^(-k * n / m))^k ≈ (set_bits / m)^k
        return float(fill_ratio ** self.num_hashes)


class CountingBloomFilter(BloomFilter):
    """
    Counting Bloom Filter:
    Replaces single bits with 8-bit counters to support DELETIONS.
    """
    def __init__(self, expected_elements: int = 10000, false_positive_rate: float = 0.01):
        super().__init__(expected_elements=expected_elements, false_positive_rate=false_positive_rate)
        # 8-bit counter per slot (0 to 255)
        self.counters = bytearray(self.size)

    def add(self, item: str) -> None:
        """Adds an item and increments corresponding counters."""
        for index in self._get_hashes(item):
            if self.counters[index] < 255:
                self.counters[index] += 1
            # Update underlying bit array
            byte_index = index // 8
            bit_offset = index % 8
            self.bit_array[byte_index] |= (1 << bit_offset)
        self.count += 1

    def remove(self, item: str) -> bool:
        """
        Removes an item by decrementing counters.
        Returns True if item was possibly present and decremented, False if definitely not present.
        """
        if not self.exists(item):
            return False

        for index in self._get_hashes(item):
            if self.counters[index] > 0:
                self.counters[index] -= 1
            # If counter hit 0, clear bit in bit array
            if self.counters[index] == 0:
                byte_index = index // 8
                bit_offset = index % 8
                self.bit_array[byte_index] &= ~(1 << bit_offset)

        self.count = max(0, self.count - 1)
        return True


def demo():
    print("=== Bloom Filter Demonstration ===")
    bf = BloomFilter(expected_elements=1000, false_positive_rate=0.01)
    print(f"Optimal Size: {bf.size} bits ({len(bf.bit_array)} bytes)")
    print(f"Optimal Hash Functions: {bf.num_hashes}")

    # Add 1000 elements
    for i in range(1000):
        bf.add(f"product_id_{i}")

    # 1. Test True Positives
    all_found = all(f"product_id_{i}" in bf for i in range(1000))
    print(f"All 1000 inserted items detected (Zero False Negatives): {all_found}")

    # 2. Test False Positives
    false_positives = sum(1 for i in range(1000, 2000) if f"product_id_{i}" in bf)
    print(f"False Positives out of 1000 uninserted queries: {false_positives} ({false_positives / 10:.2f}%)")


if __name__ == "__main__":
    demo()
