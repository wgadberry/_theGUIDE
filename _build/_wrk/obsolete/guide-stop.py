#!/usr/bin/env python3
"""
theGuide Worker Shutdown Script

Stops all running theGuide worker processes.

Usage:
    python guide-stop.py              # Stop all workers (with confirmation)
    python guide-stop.py --force      # Stop without confirmation
    python guide-stop.py --list       # List running workers only
    python guide-stop.py --worker X   # Stop specific worker(s)
"""

import argparse
import subprocess
import sys
import os

# =============================================================================
# Configuration
# =============================================================================

# Worker script patterns to match
WORKER_PATTERNS = [
    "guide-producer.py",
    "guide-shredder.py",
    "guide-funder.py",
    "guide-sync-funding.py",
    "guide-pool-enricher.py",
    "guide-load-programs.py",
    "guide-price-loader.py",
    "guide-backfill-tokens.py",
    "guide-market-loader.py",
    "guide-analytics.py",
    "guide-clipper.py",
    "guide-circular-flow.py",
    "guide-address-classifier.py",
    "guide-wallet-hunter.py",
    "guide-token-forensic.py",
    "guide-mint-scanner.py",
    "guide-to-networkx.py",
    "guide-welcome.py",
]


# =============================================================================
# Helper Functions
# =============================================================================

def print_header(text: str):
    """Print a section header"""
    print(f"\n{'='*60}")
    print(f"  {text}")
    print(f"{'='*60}")


def print_status(name: str, status: bool, details: str = ""):
    """Print a status line"""
    icon = "[OK]" if status else "[X]"
    detail_str = f" ({details})" if details else ""
    print(f"  {icon} {name}{detail_str}")


def get_running_workers() -> list:
    """Get list of running worker processes"""
    workers = []

    try:
        # Use wmic to get process list with command line
        result = subprocess.run(
            ["wmic", "process", "where", "name='python.exe'", "get", "processid,commandline"],
            capture_output=True,
            text=True,
            timeout=10
        )

        for line in result.stdout.strip().split('\n'):
            line = line.strip()
            if not line or line.startswith('CommandLine'):
                continue

            for pattern in WORKER_PATTERNS:
                if pattern in line:
                    # Extract PID (last number in line)
                    parts = line.split()
                    if parts:
                        try:
                            pid = int(parts[-1])
                            workers.append({
                                "pid": pid,
                                "script": pattern,
                                "cmdline": line[:80] + "..." if len(line) > 80 else line
                            })
                        except ValueError:
                            pass
                    break

    except Exception as e:
        print(f"  [X] Error getting process list: {e}")

    return workers


def kill_process(pid: int) -> bool:
    """Kill a process by PID"""
    try:
        result = subprocess.run(
            ["taskkill", "/F", "/PID", str(pid)],
            capture_output=True,
            text=True,
            timeout=10
        )
        return result.returncode == 0
    except Exception:
        return False


def close_worker_windows():
    """Close PowerShell windows with theGuide titles"""
    try:
        # Find windows with "theGuide" in title and close them
        ps_cmd = '''
        Get-Process powershell | Where-Object {$_.MainWindowTitle -like "*theGuide*"} | ForEach-Object {
            $_.CloseMainWindow() | Out-Null
            Write-Output $_.Id
        }
        '''
        result = subprocess.run(
            ["powershell", "-Command", ps_cmd],
            capture_output=True,
            text=True,
            timeout=10
        )
        closed = [p for p in result.stdout.strip().split('\n') if p.strip()]
        return len(closed)
    except Exception:
        return 0


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='theGuide Worker Shutdown Script'
    )
    parser.add_argument('--force', '-f', action='store_true',
                        help='Stop without confirmation')
    parser.add_argument('--list', '-l', action='store_true',
                        help='List running workers only')
    parser.add_argument('--worker', '-w', action='append',
                        help='Stop specific worker(s) by name')

    args = parser.parse_args()

    print_header("theGuide Worker Manager")

    # Get running workers
    workers = get_running_workers()

    if not workers:
        print("\n  No theGuide workers currently running.")
        return 0

    # Filter by specific workers if requested
    if args.worker:
        workers = [w for w in workers if any(p in w['script'] for p in args.worker)]
        if not workers:
            print(f"\n  No matching workers found for: {', '.join(args.worker)}")
            return 0

    # List mode
    if args.list:
        print(f"\n  Found {len(workers)} running worker(s):\n")
        print(f"  {'PID':<8} {'Script':<30} {'Command'}")
        print(f"  {'-'*8} {'-'*30} {'-'*40}")
        for w in workers:
            print(f"  {w['pid']:<8} {w['script']:<30} {w['cmdline'][:40]}")
        return 0

    # Stop mode
    print(f"\n  Found {len(workers)} running worker(s):\n")
    for w in workers:
        print(f"    - {w['script']} (PID: {w['pid']})")

    # Confirm
    if not args.force:
        print()
        try:
            confirm = input("  Stop all workers? (y/n): ").strip().lower()
            if confirm != 'y':
                print("\n  Aborted.")
                return 0
        except (KeyboardInterrupt, EOFError):
            print("\n\n  Aborted.")
            return 0

    # Stop workers
    print_header("Stopping Workers")

    stopped = 0
    failed = 0

    for w in workers:
        if kill_process(w['pid']):
            print_status(f"{w['script']} (PID: {w['pid']})", True, "stopped")
            stopped += 1
        else:
            print_status(f"{w['script']} (PID: {w['pid']})", False, "failed to stop")
            failed += 1

    # Also try to close worker windows
    closed_windows = close_worker_windows()
    if closed_windows:
        print(f"\n  Closed {closed_windows} worker window(s)")

    # Summary
    print_header("Summary")
    print(f"  Stopped: {stopped}")
    if failed:
        print(f"  Failed:  {failed}")

    if failed == 0:
        print("\n  [OK] All workers stopped successfully!")
    else:
        print(f"\n  [X] {failed} worker(s) failed to stop")

    print()
    return 0 if failed == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
