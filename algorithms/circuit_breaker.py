import time
import random
from typing import Callable, Any, Optional

class CircuitBreakerOpenError(Exception):
    """Raised when the circuit breaker is OPEN and short-circuits execution."""
    pass

class CircuitBreaker:
    """
    A standalone, educational implementation of the Circuit Breaker pattern.
    
    Manages three states:
    - CLOSED: Normal operation. Requests flow through. Consecutive failures are tracked.
    - OPEN: Service is unhealthy. Fast-fails immediately without hitting the service.
    - HALF-OPEN: Recovery timeout elapsed. Allows a single probe request to check health.
    """
    def __init__(
        self, 
        failure_threshold: int = 3, 
        recovery_timeout: float = 5.0
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        
        self.state = "CLOSED"  # CLOSED, OPEN, HALF-OPEN
        self.failure_count = 0
        self.last_state_change = time.time()

    def _transition_to(self, new_state: str):
        old_state = self.state
        self.state = new_state
        self.last_state_change = time.time()
        print(f"\n>>> [STATE TRANSITION] {old_state} -> {new_state} <<<\n")

    def execute(self, func: Callable, *args, **kwargs) -> Any:
        current_time = time.time()
        
        # 1. State: OPEN Check
        if self.state == "OPEN":
            # Check if recovery timeout has elapsed
            if current_time - self.last_state_change > self.recovery_timeout:
                self._transition_to("HALF-OPEN")
            else:
                # Fast-fail
                raise CircuitBreakerOpenError(
                    f"Circuit Breaker is OPEN. Fast-failing request. "
                    f"({self.recovery_timeout - (current_time - self.last_state_change):.1f}s until retry allowed)"
                )

        # 2. Execute target function
        try:
            print(f"[Execution Attempt] Breaker State: {self.state}")
            result = func(*args, **kwargs)
            
            # On Success:
            if self.state == "HALF-OPEN":
                # Probe call succeeded! Reset circuit to CLOSED
                print("[Probe Success] Downstream service recovered!")
                self.failure_count = 0
                self._transition_to("CLOSED")
            elif self.state == "CLOSED":
                # Clean up any transient failure counts
                self.failure_count = 0
                
            return result

        except Exception as e:
            # On Failure:
            self.failure_count += 1
            print(f"[Execution Failure] Captured error: '{e}'. (Failures count: {self.failure_count}/{self.failure_threshold})")
            
            if self.state == "CLOSED":
                if self.failure_count >= self.failure_threshold:
                    print("[Threshold Reached] Tripping circuit breaker to OPEN!")
                    self._transition_to("OPEN")
            elif self.state == "HALF-OPEN":
                # Probe call failed. Immediately trip back to OPEN
                print("[Probe Failure] Downstream service still unhealthy!")
                self._transition_to("OPEN")
                
            raise e

# --- Demonstration and Testing ---
if __name__ == "__main__":
    print("=========================================")
    print("Testing Circuit Breaker State Machine")
    print("=========================================")
    
    # 70% chance of failing simulated service
    def unreliable_external_service(should_fail: bool):
        if should_fail:
            raise RuntimeError("Simulated Database Timeout / Outage!")
        return "SUCCESSFUL_SERVICE_RESPONSE"

    # Instantiate breaker: trip after 3 failures, 3 seconds recovery timeout
    breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=3.0)

    # 1. Force 3 failures in a row
    for i in range(1, 4):
        try:
            print(f"\n--- Request {i} ---")
            breaker.execute(unreliable_external_service, should_fail=True)
        except Exception as e:
            print(f"Outcome: Request threw exception.")

    # 2. Check that the 4th request immediately fast-fails (OPEN state)
    try:
        print("\n--- Request 4 (Should Fast-Fail instantly) ---")
        breaker.execute(unreliable_external_service, should_fail=False)
    except CircuitBreakerOpenError as e:
        print(f"Outcome: Fast-failed! Error: '{e}'")

    # 3. Wait for recovery timeout to pass
    print("\nSleeping for 3.5 seconds to exceed recovery timeout...")
    time.sleep(3.5)

    # 4. Request 5 should probe (HALF-OPEN state) and transition to CLOSED upon success
    try:
        print("\n--- Request 5 (Probe Request, should succeed) ---")
        response = breaker.execute(unreliable_external_service, should_fail=False)
        print(f"Outcome: Success! Response: {response}")
    except Exception as e:
        print(f"Outcome: Exception raised: {e}")

    # 5. Confirm it's fully closed and functioning again
    print("\n--- Request 6 (Back to normal CLOSED state) ---")
    response = breaker.execute(unreliable_external_service, should_fail=False)
    print(f"Outcome: Success! Response: {response}")
