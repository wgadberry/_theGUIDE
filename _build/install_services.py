#!/usr/bin/env python3
"""
T16O Exchange Guide Services - Batch Installer

Installs, starts, stops, or removes all Guide Windows services.

Uses NSSM (Non-Sucking Service Manager) for reliable Windows service management.
Works with any Python installation including Windows Store Python.

Usage:
    python install_services.py install     # Install all services
    python install_services.py start       # Start all services
    python install_services.py stop        # Stop all services
    python install_services.py remove      # Remove all services
    python install_services.py status      # Show service status
    python install_services.py restart     # Restart all services

    python install_services.py install gateway producer  # Install specific services
    python install_services.py start shredder            # Start specific service
"""

import sys
import subprocess
import argparse
import os
import shutil
import urllib.request
import zipfile
from typing import List, Optional

# Paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
WORKER_DIR = os.path.join(SCRIPT_DIR, '_wrk')
NSSM_DIR = os.path.join(SCRIPT_DIR, 'tools', 'nssm')
NSSM_EXE = os.path.join(NSSM_DIR, 'nssm.exe')
NSSM_URL = 'https://nssm.cc/release/nssm-2.24.zip'

# Use python.org Python (not Windows Store) for services
# Priority: Python314 (working) > Python313 > fallback to current
PYTHON_PATHS = [
    r'C:\Users\wgadb\AppData\Local\Programs\Python\Python314\python.exe',
    r'C:\Python314\python.exe',
]
PYTHON_EXE = next((p for p in PYTHON_PATHS if os.path.exists(p)), sys.executable)

# Required dependencies for workers (not for installer itself)
REQUIRED_PACKAGES = {
    'flask': 'flask',
    'pika': 'pika',
    'mysql-connector-python': 'mysql.connector',
    'requests': 'requests',
}


def ensure_nssm() -> bool:
    """
    Ensure NSSM is available. Download if missing.
    Returns True if NSSM is ready, False otherwise.
    """
    if os.path.exists(NSSM_EXE):
        return True

    print("\n=== Setting up NSSM (Non-Sucking Service Manager) ===\n")
    print(f"  Downloading from {NSSM_URL}...")

    try:
        os.makedirs(NSSM_DIR, exist_ok=True)
        zip_path = os.path.join(NSSM_DIR, 'nssm.zip')

        # Download
        urllib.request.urlretrieve(NSSM_URL, zip_path)

        # Extract
        with zipfile.ZipFile(zip_path, 'r') as zf:
            # Find the win64 nssm.exe in the archive
            for name in zf.namelist():
                if name.endswith('win64/nssm.exe'):
                    # Extract to our target location
                    with zf.open(name) as src, open(NSSM_EXE, 'wb') as dst:
                        dst.write(src.read())
                    break

        # Cleanup zip
        os.remove(zip_path)

        if os.path.exists(NSSM_EXE):
            print(f"  NSSM installed to {NSSM_EXE}\n")
            return True
        else:
            print(f"  Failed to extract NSSM")
            return False

    except Exception as e:
        print(f"  Error downloading NSSM: {e}")
        print(f"  Download manually from https://nssm.cc/download")
        return False


def check_dependencies() -> bool:
    """
    Check that all required packages are installed.
    If missing, prompt to install them automatically.
    Returns True if all dependencies are met, False otherwise.
    """
    missing = []

    for package, import_name in REQUIRED_PACKAGES.items():
        try:
            __import__(import_name)
        except ImportError:
            missing.append(package)

    if not missing:
        return True

    print("\n=== Missing Dependencies ===\n")
    print("  The following packages are required:\n")
    for pkg in missing:
        print(f"    - {pkg}")

    # Auto-install missing packages using the target Python
    print(f"\n  Installing missing packages to {PYTHON_EXE}...")

    try:
        result = subprocess.run(
            [PYTHON_EXE, '-m', 'pip', 'install'] + missing,
            capture_output=True,
            text=True,
            timeout=120
        )

        if result.returncode == 0:
            print(f"  Installed successfully.\n")
            return True
        else:
            print(f"\n  Installation failed:")
            print(f"    {result.stderr.strip()}")
            print(f"\n  Try manually: {PYTHON_EXE} -m pip install {' '.join(missing)}")
            return False

    except subprocess.TimeoutExpired:
        print(f"\n  Installation timed out.")
        print(f"  Try manually: {PYTHON_EXE} -m pip install {' '.join(missing)}")
        return False
    except Exception as e:
        print(f"\n  Installation error: {e}")
        print(f"  Try manually: {PYTHON_EXE} -m pip install {' '.join(missing)}")
        return False


# Service definitions in startup order
# Each service maps to a worker script with specific args
SERVICES = [
    {
        'name': 'gateway',
        'svc_name': 'T16OExchange.Guide.Gateway',
        'display': 'T16O Exchange - Guide Gateway',
        'script': 'guide-gateway.py',
        'args': ['--with-queue-consumer', '--with-response-consumer'],
    },
    {
        'name': 'producer',
        'svc_name': 'T16OExchange.Guide.Producer',
        'display': 'T16O Exchange - Guide Producer',
        'script': 'guide-producer.py',
        'args': ['--queue-consumer'],
    },
    {
        'name': 'decoder',
        'svc_name': 'T16OExchange.Guide.Decoder',
        'display': 'T16O Exchange - Guide Decoder',
        'script': 'guide-decoder.py',
        'args': ['--queue-consumer'],
    },
    {
        'name': 'detailer',
        'svc_name': 'T16OExchange.Guide.Detailer',
        'display': 'T16O Exchange - Guide Detailer',
        'script': 'guide-detailer.py',
        'args': ['--queue-consumer'],
    },
    {
        'name': 'shredder',
        'svc_name': 'T16OExchange.Guide.Shredder',
        'display': 'T16O Exchange - Guide Shredder',
        'script': 'guide-shredder.py',
        'args': ['--daemon'],
    },
    {
        'name': 'funder',
        'svc_name': 'T16OExchange.Guide.Funder',
        'display': 'T16O Exchange - Guide Funder',
        'script': 'guide-funder.py',
        'args': ['--queue-consumer'],
    },
    {
        'name': 'aggregator',
        'svc_name': 'T16OExchange.Guide.Aggregator',
        'display': 'T16O Exchange - Guide Aggregator',
        'script': 'guide-aggregator.py',
        'args': ['--queue-consumer'],
    },
    {
        'name': 'enricher',
        'svc_name': 'T16OExchange.Guide.Enricher',
        'display': 'T16O Exchange - Guide Enricher',
        'script': 'guide-enricher.py',
        'args': ['--queue-consumer'],
    },
]


def get_services(names: Optional[List[str]] = None) -> List[dict]:
    """Get service definitions, optionally filtered by name"""
    if not names:
        return SERVICES
    return [s for s in SERVICES if s['name'] in names]


def run_nssm(args: List[str], capture: bool = True) -> subprocess.CompletedProcess:
    """Run NSSM with given arguments"""
    cmd = [NSSM_EXE] + args
    if capture:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    else:
        return subprocess.run(cmd, timeout=30)


def check_service_status(svc_name: str) -> str:
    """Check Windows service status using sc query"""
    try:
        result = subprocess.run(
            ['sc', 'query', svc_name],
            capture_output=True,
            text=True,
            timeout=10
        )
        if 'RUNNING' in result.stdout:
            return 'RUNNING'
        elif 'STOPPED' in result.stdout:
            return 'STOPPED'
        elif 'does not exist' in result.stderr.lower() or result.returncode != 0:
            return 'NOT INSTALLED'
        else:
            return 'UNKNOWN'
    except Exception:
        return 'ERROR'


def cmd_install(services: List[dict]):
    """Install services using NSSM"""
    print("\n=== Installing T16O Exchange Guide Services ===\n")

    if not ensure_nssm():
        print("  Cannot proceed without NSSM")
        return

    python_exe = PYTHON_EXE
    print(f"  Using Python: {python_exe}")
    print(f"  Worker dir:   {WORKER_DIR}\n")
    
    # Verify Python exists
    if not os.path.exists(python_exe):
        print(f"  ERROR: Python not found at {python_exe}")
        return
    
    success = 0
    failed = 0

    for svc in services:
        svc_name = svc['svc_name']
        script_path = os.path.join(WORKER_DIR, svc['script'])
        
        # Build AppParameters: "script_path arg1 arg2"
        app_params = f'"{script_path}" ' + ' '.join(svc['args'])

        print(f"  [{svc['name']}] Installing {svc['display']}...")

        # Check if script exists
        if not os.path.exists(script_path):
            print(f"    Error: Script not found: {script_path}")
            failed += 1
            continue

        # Check if already installed
        if check_service_status(svc_name) != 'NOT INSTALLED':
            print(f"    Already installed (use 'remove' first to reinstall)")
            success += 1
            continue

        try:
            # Install service: nssm install <servicename> <python.exe>
            # NSSM expects just the executable, not the script
            result = run_nssm(['install', svc_name, python_exe])

            if result.returncode == 0:
                # Set AppParameters (the script and its arguments)
                run_nssm(['set', svc_name, 'AppParameters', app_params])
                # Set display name
                run_nssm(['set', svc_name, 'DisplayName', svc['display']])
                # Set description
                run_nssm(['set', svc_name, 'Description', f'T16O Exchange Guide {svc["name"].capitalize()} worker'])
                # Set startup directory (critical for relative imports/paths)
                run_nssm(['set', svc_name, 'AppDirectory', WORKER_DIR])
                # Set auto-start on boot
                run_nssm(['set', svc_name, 'Start', 'SERVICE_AUTO_START'])
                # Set auto-restart on failure
                run_nssm(['set', svc_name, 'AppExit', 'Default', 'Restart'])
                # Set restart delay (5 seconds)
                run_nssm(['set', svc_name, 'AppRestartDelay', '5000'])
                # Set stdout/stderr logs
                log_dir = os.path.join(SCRIPT_DIR, 'logs')
                os.makedirs(log_dir, exist_ok=True)
                run_nssm(['set', svc_name, 'AppStdout', os.path.join(log_dir, f'{svc["name"]}.log')])
                run_nssm(['set', svc_name, 'AppStderr', os.path.join(log_dir, f'{svc["name"]}.err.log')])
                # Append to logs instead of overwriting
                run_nssm(['set', svc_name, 'AppStdoutCreationDisposition', '4'])
                run_nssm(['set', svc_name, 'AppStderrCreationDisposition', '4'])

                print(f"  [{svc['name']}] Installed successfully")
                success += 1
            else:
                print(f"    Error: {result.stderr.strip() if result.stderr else result.stdout.strip()}")
                failed += 1

        except Exception as e:
            print(f"    Exception: {e}")
            failed += 1

    print(f"\n=== Installation Complete: {success} success, {failed} failed ===")
    print(f"\n  Next: Run 'python install_services.py start' to start all services")


def cmd_start(services: List[dict]):
    """Start services using NSSM"""
    print("\n=== Starting T16O Exchange Guide Services ===\n")

    if not ensure_nssm():
        print("  Cannot proceed without NSSM")
        return

    success = 0
    failed = 0

    for svc in services:
        svc_name = svc['svc_name']
        print(f"  [{svc['name']}] Starting {svc['display']}...")

        status = check_service_status(svc_name)
        if status == 'NOT INSTALLED':
            print(f"    Not installed (run 'install' first)")
            failed += 1
            continue
        if status == 'RUNNING':
            print(f"    Already running")
            success += 1
            continue

        try:
            result = run_nssm(['start', svc_name])
            if result.returncode == 0:
                print(f"  [{svc['name']}] Started successfully")
                success += 1
            else:
                print(f"    Error: {result.stderr.strip() if result.stderr else 'Failed to start'}")
                failed += 1
        except Exception as e:
            print(f"    Exception: {e}")
            failed += 1

    print(f"\n=== Startup Complete: {success} success, {failed} failed ===")
    print(f"\n  Check logs in: {os.path.join(SCRIPT_DIR, 'logs')}")


def cmd_stop(services: List[dict]):
    """Stop services using NSSM (reverse order)"""
    print("\n=== Stopping T16O Exchange Guide Services ===\n")

    if not ensure_nssm():
        print("  Cannot proceed without NSSM")
        return

    success = 0
    failed = 0

    # Stop in reverse order
    for svc in reversed(services):
        svc_name = svc['svc_name']
        print(f"  [{svc['name']}] Stopping {svc['display']}...")

        status = check_service_status(svc_name)
        if status == 'NOT INSTALLED':
            print(f"    Not installed")
            success += 1
            continue
        if status == 'STOPPED':
            print(f"    Already stopped")
            success += 1
            continue

        try:
            result = run_nssm(['stop', svc_name])
            if result.returncode == 0:
                print(f"  [{svc['name']}] Stopped successfully")
                success += 1
            else:
                print(f"    Error: {result.stderr.strip() if result.stderr else 'Failed to stop'}")
                failed += 1
        except Exception as e:
            print(f"    Exception: {e}")
            failed += 1

    print(f"\n=== Shutdown Complete: {success} success, {failed} failed ===")


def cmd_remove(services: List[dict]):
    """Remove services using NSSM (reverse order)"""
    print("\n=== Removing T16O Exchange Guide Services ===\n")

    if not ensure_nssm():
        print("  Cannot proceed without NSSM")
        return

    # Stop first
    cmd_stop(services)

    print()  # Blank line after stop

    success = 0
    failed = 0

    # Remove in reverse order
    for svc in reversed(services):
        svc_name = svc['svc_name']
        print(f"  [{svc['name']}] Removing {svc['display']}...")

        status = check_service_status(svc_name)
        if status == 'NOT INSTALLED':
            print(f"    Not installed")
            success += 1
            continue

        try:
            result = run_nssm(['remove', svc_name, 'confirm'])
            if result.returncode == 0:
                print(f"  [{svc['name']}] Removed successfully")
                success += 1
            else:
                print(f"    Error: {result.stderr.strip() if result.stderr else 'Failed to remove'}")
                failed += 1
        except Exception as e:
            print(f"    Exception: {e}")
            failed += 1

    print(f"\n=== Removal Complete: {success} success, {failed} failed ===")


def cmd_status(services: List[dict]):
    """Show service status"""
    print("\n=== T16O Exchange Guide Services Status ===\n")
    print(f"  Python: {PYTHON_EXE}\n")

    print(f"  {'Service':<15} {'Status':<15} {'Display Name'}")
    print(f"  {'-'*15} {'-'*15} {'-'*40}")

    for svc in services:
        status = check_service_status(svc['svc_name'])
        status_display = {
            'RUNNING': '\033[92mRUNNING\033[0m',       # Green
            'STOPPED': '\033[93mSTOPPED\033[0m',       # Yellow
            'NOT INSTALLED': '\033[91mNOT INSTALLED\033[0m',  # Red
            'UNKNOWN': '\033[90mUNKNOWN\033[0m',       # Gray
            'ERROR': '\033[91mERROR\033[0m',           # Red
        }.get(status, status)

        print(f"  {svc['name']:<15} {status_display:<24} {svc['display']}")

    print()


def cmd_restart(services: List[dict]):
    """Restart services"""
    cmd_stop(services)
    cmd_start(services)


def main():
    parser = argparse.ArgumentParser(
        description='T16O Exchange Guide Services - Batch Installer',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python install_services.py install           # Install all services
    python install_services.py start             # Start all services
    python install_services.py stop              # Stop all services
    python install_services.py status            # Show service status
    python install_services.py install gateway   # Install specific service
    python install_services.py restart shredder  # Restart specific service
"""
    )

    parser.add_argument('command',
                        choices=['install', 'start', 'stop', 'remove', 'status', 'restart'],
                        help='Command to execute')
    parser.add_argument('services', nargs='*',
                        help='Specific services to operate on (default: all)')

    args = parser.parse_args()

    # Check worker dependencies (except for status which is read-only)
    if args.command not in ('status', 'remove', 'stop'):
        if not check_dependencies():
            return 1

    # Get services to operate on
    services = get_services(args.services if args.services else None)

    if not services:
        print(f"Error: Unknown service(s): {args.services}")
        print(f"Available services: {[s['name'] for s in SERVICES]}")
        return 1

    # Execute command
    commands = {
        'install': cmd_install,
        'start': cmd_start,
        'stop': cmd_stop,
        'remove': cmd_remove,
        'status': cmd_status,
        'restart': cmd_restart,
    }

    commands[args.command](services)
    return 0


if __name__ == '__main__':
    sys.exit(main())
