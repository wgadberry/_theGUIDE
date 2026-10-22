#!/usr/bin/env python3
"""
theGuide Environment Setup - Master Setup Script

Runs all setup steps in sequence:
  1. Check and install dependencies
  2. Start Docker containers (MySQL, RabbitMQ)
  3. Create database schema and load seed data
  4. Create RabbitMQ queues

Usage:
    python 00-setup-all.py              # Full setup
    python 00-setup-all.py --skip-docker    # Skip Docker (containers already running)
    python 00-setup-all.py --skip-deps      # Skip dependency check
    python 00-setup-all.py --dry-run        # Show what would be done
"""

import argparse
import subprocess
import sys
import os
import time

# =============================================================================
# Configuration
# =============================================================================

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

SETUP_STEPS = [
    {
        "name": "Check Dependencies",
        "script": "01-check-dependencies.py",
        "args": ["--install"],
        "skip_flag": "skip_deps",
    },
    {
        "name": "Start Docker Containers",
        "script": "01-check-dependencies.py",
        "args": ["--start-docker"],
        "skip_flag": "skip_docker",
        "wait_after": 15,  # Wait for services to initialize
    },
    {
        "name": "Create Database Schema",
        "script": "02-create-schema.py",
        "args": ["--with-seed"],
        "skip_flag": None,
    },
    {
        "name": "Create RabbitMQ Queues",
        "script": "03-create-queues.py",
        "args": [],
        "skip_flag": None,
    },
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


def run_step(step: dict, dry_run: bool = False) -> bool:
    """Run a single setup step"""
    script_path = os.path.join(SCRIPT_DIR, step["script"])

    if not os.path.exists(script_path):
        print_status(step["name"], False, "script not found")
        return False

    cmd = [sys.executable, script_path] + step.get("args", [])

    if dry_run:
        print(f"  [DRY] Would run: {' '.join(cmd)}")
        return True

    print(f"  Running: {step['script']} {' '.join(step.get('args', []))}")
    print()

    try:
        result = subprocess.run(cmd, cwd=SCRIPT_DIR)
        success = result.returncode == 0

        if success and step.get("wait_after"):
            print(f"\n  Waiting {step['wait_after']}s for services to initialize...")
            time.sleep(step["wait_after"])

        return success
    except Exception as e:
        print_status(step["name"], False, str(e))
        return False


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='theGuide Environment Setup - Master Setup Script'
    )
    parser.add_argument('--skip-docker', action='store_true',
                        help='Skip Docker container startup')
    parser.add_argument('--skip-deps', action='store_true',
                        help='Skip dependency check')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be done without executing')
    parser.add_argument('--step', type=int,
                        help='Run only specific step (1-4)')

    args = parser.parse_args()

    print("\n" + "="*60)
    print("""
         _   _          ____       _     _
        | |_| |__   ___|  _ \\ _   _(_) __| | ___
        | __| '_ \\ / _ \\ | | | | | | |/ _` |/ _ \\
        | |_| | | |  __/ |_| | |_| | | (_| |  __/
         \\__|_| |_|\\___|____/ \\__,_|_|\\__,_|\\___|

              Environment Setup Wizard
    """)
    print("="*60)

    if args.dry_run:
        print("\n  MODE: DRY RUN")

    # Build skip flags
    skip_flags = {
        "skip_docker": args.skip_docker,
        "skip_deps": args.skip_deps,
    }

    # Run steps
    total_steps = 0
    successful_steps = 0
    failed_steps = []

    for i, step in enumerate(SETUP_STEPS, 1):
        # Skip if specific step requested
        if args.step and args.step != i:
            continue

        # Check skip flag
        skip_flag = step.get("skip_flag")
        if skip_flag and skip_flags.get(skip_flag):
            print_header(f"Step {i}: {step['name']} [SKIPPED]")
            continue

        print_header(f"Step {i}: {step['name']}")
        total_steps += 1

        if run_step(step, args.dry_run):
            successful_steps += 1
            print_status(step["name"], True, "complete")
        else:
            failed_steps.append(step["name"])
            print_status(step["name"], False, "failed")

            # Ask to continue on failure
            if not args.dry_run:
                try:
                    cont = input("\n  Continue with remaining steps? (y/n): ").strip().lower()
                    if cont != 'y':
                        break
                except (KeyboardInterrupt, EOFError):
                    print("\n\n  Aborted.")
                    break

    # Summary
    print_header("Setup Summary")
    print(f"  Steps completed: {successful_steps}/{total_steps}")

    if failed_steps:
        print(f"  Failed steps: {', '.join(failed_steps)}")
        print("\n  [X] Setup completed with errors")
    else:
        print("\n  [OK] Setup completed successfully!")
        print()
        print("  Next steps:")
        print("    1. cd _wrk")
        print("    2. python guide-welcome.py")
        print("    3. Enter a mint address to start the pipeline")

    print()
    return 0 if not failed_steps else 1


if __name__ == '__main__':
    sys.exit(main())
