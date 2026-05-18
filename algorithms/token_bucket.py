import time
from typing import Tuple

class TokenBucketRateLimiter:
    """
    A lazy-refill implementation of the Token Bucket rate limiting algorithm.
    
    RATIONALE:
    Instead of spawning background worker threads to constantly top up the bucket
    (which consumes unnecessary CPU resources), this implementation calculates
    the bucket's state lazily on each incoming request based on elapsed time.
    """
    def __init__(self, capacity: float, refill_rate: float):
        """
        :param capacity: Maximum number of tokens the bucket can hold (burst limit).
        :param refill_rate: Number of tokens added to the bucket per second.
        """
        self.capacity = capacity
        self.refill_rate = refill_rate
        
        self.tokens = capacity                # Start with a full bucket
        self.last_refill_time = time.time()   # Track last request timestamp

    def _refill(self):
        """Lazily tops up the bucket based on the time elapsed since the last request."""
        now = time.time()
        elapsed = now - self.last_refill_time
        
        # Calculate new token total: capacity + elapsed * refill_rate
        new_tokens = self.tokens + (elapsed * self.refill_rate)
        self.tokens = min(self.capacity, new_tokens)
        self.last_refill_time = now

    def acquire(self, tokens_needed: int = 1) -> Tuple[bool, float]:
        """
        Attempts to acquire the specified number of tokens from the bucket.
        
        :param tokens_needed: The quantity of tokens requested.
        :return: A tuple of (is_allowed: bool, current_tokens: float)
        """
        self._refill()
        
        if self.tokens >= tokens_needed:
            self.tokens -= tokens_needed
            return True, self.tokens
        
        return False, self.tokens

# --- Demonstration and Testing ---
if __name__ == "__main__":
    print("=========================================")
    print("Testing Token Bucket Rate Limiter")
    print("=========================================")

    # Create a bucket with capacity = 5 tokens, refilling at 2 tokens/sec
    limiter = TokenBucketRateLimiter(capacity=5.0, refill_rate=2.0)
    print(f"Bucket initialized: Capacity = {limiter.capacity}, Refill Rate = {limiter.refill_rate}/sec\n")

    # 1. Simulate a rapid burst of 7 requests (burst capacity is 5)
    for i in range(1, 8):
        allowed, remaining = limiter.acquire(1)
        status = "ALLOWED" if allowed else "BLOCKED (Rate Limited)"
        print(f"Request {i:02d} | Status: {status:<22} | Tokens Remaining: {remaining:.2f}")
        time.sleep(0.05) # minimal delay

    # 2. Sleep for 1.5 seconds to allow the bucket to refill
    sleep_duration = 1.5
    print(f"\n--- Sleeping for {sleep_duration} seconds to allow partial refill... ---")
    time.sleep(sleep_duration)

    # 3. Try to acquire more tokens
    print("\nAttempting new requests after refill:")
    for i in range(8, 13):
        allowed, remaining = limiter.acquire(1)
        status = "ALLOWED" if allowed else "BLOCKED (Rate Limited)"
        print(f"Request {i:02d} | Status: {status:<22} | Tokens Remaining: {remaining:.2f}")
        time.sleep(0.1)
