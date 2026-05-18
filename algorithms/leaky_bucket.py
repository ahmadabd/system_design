import time
from typing import Tuple

class LeakyBucketRateLimiter:
    """
    A lazy-leak implementation of the Leaky Bucket rate limiting algorithm.
    
    DIFFERENCE FROM TOKEN BUCKET:
    - Token Bucket: Holds a pool of tokens. It allows immediate bursts up to capacity 
      before enforcing limits, suitable for APIs that allow fast short-term spikes.
    - Leaky Bucket: Represents a queue that leaks requests at a strict, uniform rate. 
      It smooths out spikes completely, providing a stable flow rate downstream.
    """
    def __init__(self, capacity: float, leak_rate: float):
        """
        :param capacity: Maximum buffer size of the bucket (queue capacity).
        :param leak_rate: Rate at which the bucket leaks requests per second (flow rate).
        """
        self.capacity = capacity
        self.leak_rate = leak_rate
        
        self.water = 0.0                      # Bucket starts empty (0 requests queued)
        self.last_leak_time = time.time()     # Timestamp of last request

    def _leak(self):
        """Lazily drains water (requests) from the bucket based on elapsed time."""
        now = time.time()
        elapsed = now - self.last_leak_time
        
        # Calculate water drained: elapsed * leak_rate
        drained = elapsed * self.leak_rate
        self.water = max(0.0, self.water - drained)
        self.last_leak_time = now

    def acquire(self) -> Tuple[bool, float]:
        """
        Attempts to add a new request (1 unit of water) into the leaky bucket.
        
        :return: A tuple of (is_allowed: bool, current_water: float)
        """
        self._leak()
        
        # Check if bucket has capacity to accept another request
        if self.water + 1.0 <= self.capacity:
            self.water += 1.0
            return True, self.water
            
        return False, self.water

# --- Demonstration and Testing ---
if __name__ == "__main__":
    print("=========================================")
    print("Testing Leaky Bucket Rate Limiter")
    print("=========================================")

    # Create a leaky bucket with capacity = 3.0 requests, leaking at 1.0 request/sec
    limiter = LeakyBucketRateLimiter(capacity=3.0, leak_rate=1.0)
    print(f"Bucket initialized: Capacity = {limiter.capacity}, Leak Rate = {limiter.leak_rate}/sec\n")

    # 1. Simulate a rapid burst of 5 requests
    print("Simulating a rapid burst of requests:")
    for i in range(1, 6):
        allowed, queued = limiter.acquire()
        status = "QUEUED / ALLOWED" if allowed else "BLOCKED (Bucket Full)"
        print(f"Request {i:02d} | Status: {status:<22} | Queue Size (Water): {queued:.2f}")
        time.sleep(0.1) # small delay

    # 2. Wait for 2.2 seconds to allow the queue to leak (drain)
    sleep_duration = 2.2
    print(f"\n--- Sleeping for {sleep_duration} seconds to allow the bucket to leak... ---")
    time.sleep(sleep_duration)

    # 3. Try to queue more requests
    print("\nAttempting new requests after leak:")
    for i in range(6, 10):
        allowed, queued = limiter.acquire()
        status = "QUEUED / ALLOWED" if allowed else "BLOCKED (Bucket Full)"
        print(f"Request {i:02d} | Status: {status:<22} | Queue Size (Water): {queued:.2f}")
        time.sleep(0.2)
