import os
import time
import random
import sys
import tempfile
import signal

def flaky_environment_simulation():
    """
    Simulates a ghost in the machine:
    1. Randomly deletes environment variables.
    2. Creates stale lock files with random PIDs.
    """
    if random.random() < 0.3:
        if "CRITICAL_VAR" in os.environ:
            del os.environ["CRITICAL_VAR"]
            
    lock_file = "/tmp/ghost.lock"
    # Simulate a stale lock from a crashed process (random PID)
    if random.random() < 0.5 and not os.path.exists(lock_file):
        with open(lock_file, "w") as f:
            # Write a PID that is unlikely to be the current one, e.g., 99999
            f.write("99999") 

def robust_automation():
    """
    Runs automation with environmental checks and workarounds.
    """
    MAX_RETRIES = 5
    lock_file = "/tmp/ghost.lock"
    
    # Ensure environment variable exists initially
    os.environ["CRITICAL_VAR"] = "12345"
    
    for attempt in range(1, MAX_RETRIES + 1):
        print(f"Attempt {attempt}...")
        
        # 1. Check & Clear Lock File
        if os.path.exists(lock_file):
            print("  Lock file detected. Checking staleness...")
            try:
                with open(lock_file, "r") as f:
                    pid_str = f.read().strip()
                    if not pid_str:
                         pid = -1
                    else:
                         pid = int(pid_str)
                
                # Check if process is running
                try:
                    os.kill(pid, 0)
                    print(f"  Process {pid} is alive. Waiting...")
                    time.sleep(1)
                    continue
                except OSError:
                    print(f"  Process {pid} not found (Stale lock). Removing.")
                    os.remove(lock_file)
            except (ValueError, FileNotFoundError):
                print("  Invalid lock file content. Removing.")
                if os.path.exists(lock_file):
                    os.remove(lock_file)

        # 2. Check Environment
        if "CRITICAL_VAR" not in os.environ:
            print("  CRITICAL_VAR missing! Restoring...")
            os.environ["CRITICAL_VAR"] = "12345"
            
        # 3. Run Operation (Simulated)
        try:
            # Inject chaos
            flaky_environment_simulation()
            
            # verify environment again *after* chaos but before critical section
            if "CRITICAL_VAR" not in os.environ:
                 print("  Environment corrupted during execution. Retrying...")
                 continue

            # Check if lock appeared during execution
            if os.path.exists(lock_file):
                 print("  Lock file appeared during execution. Retrying...")
                 continue

            print("  Operation successful!")
            return True
            
        except Exception as e:
            print(f"  Error: {e}")
            time.sleep(1)
            
    return False

if __name__ == "__main__":
    if robust_automation():
        print("Automation Completed Successfully.")
    else:
        print("Automation Failed.")
