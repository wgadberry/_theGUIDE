#!/usr/bin/env python3
"""
theGuide Environment Setup - Step 1: Check and Install Dependencies

Verifies all required CLI tools, Python packages, and services are available.
Optionally installs missing Python packages and starts Docker containers.

Usage:
    python 01-check-dependencies.py                    # Check only
    python 01-check-dependencies.py --install          # Install missing Python packages
    python 01-check-dependencies.py --start-docker     # Start Docker containers
    python 01-check-dependencies.py --install --start-docker  # Full setup
    python 01-check-dependencies.py --verbose          # Show detailed output
"""

import subprocess
import sys
import shutil
from typing import Tuple, List, Optional

# =============================================================================
# Configuration
# =============================================================================

# Minimum Python version required
MIN_PYTHON_VERSION = (3, 10)

# Required Python packages (pip install name, import name, description)
REQUIRED_PACKAGES = [
    ("mysql-connector-python", "mysql.connector", "MySQL database connector"),
    ("pika", "pika", "RabbitMQ AMQP client"),
    ("requests", "requests", "HTTP client library"),
    ("networkx", "networkx", "Graph analysis library"),
    ("matplotlib", "matplotlib", "Plotting library"),
    ("orjson", "orjson", "Fast JSON library"),
]

# Optional packages (nice to have)
OPTIONAL_PACKAGES = [
    ("numpy", "numpy", "Numerical computing"),
    ("pandas", "pandas", "Data analysis"),
    ("scipy", "scipy", "Scientific computing"),
]

# Required CLI tools
REQUIRED_CLI_TOOLS = [
    ("python", ["python", "--version"], "Python interpreter"),
    ("pip", ["pip", "--version"], "Python package manager"),
    ("git", ["git", "--version"], "Version control"),
    ("docker", ["docker", "--version"], "Container runtime"),
    ("curl", ["curl", "--version"], "HTTP client"),
]

# Service endpoints to check
SERVICE_ENDPOINTS = {
    "mysql": {"host": "localhost", "port": 3396},
    "rabbitmq": {"host": "localhost", "port": 5692},
    "rabbitmq_mgmt": {"host": "localhost", "port": 15692},
}


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


def check_python_version() -> Tuple[bool, str]:
    """Check Python version meets minimum requirement"""
    version = sys.version_info[:2]
    meets_req = version >= MIN_PYTHON_VERSION
    version_str = f"{version[0]}.{version[1]}"
    min_str = f"{MIN_PYTHON_VERSION[0]}.{MIN_PYTHON_VERSION[1]}"

    if meets_req:
        return True, f"v{version_str}"
    else:
        return False, f"v{version_str} (requires >= {min_str})"


def check_package_installed(import_name: str) -> Tuple[bool, Optional[str]]:
    """Check if a Python package is installed"""
    try:
        module = __import__(import_name.split('.')[0])
        version = getattr(module, '__version__', 'unknown')
        return True, version
    except ImportError:
        return False, None


def install_package(pip_name: str) -> bool:
    """Install a Python package via pip"""
    try:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", pip_name, "-q"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        return True
    except subprocess.CalledProcessError:
        return False


def check_cli_tool(cmd: List[str]) -> Tuple[bool, Optional[str]]:
    """Check if a CLI tool is available"""
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=10
        )
        # Extract version from first line
        output = result.stdout.strip() or result.stderr.strip()
        first_line = output.split('\n')[0] if output else ""
        return True, first_line[:50]
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        return False, None


def check_port_open(host: str, port: int) -> bool:
    """Check if a TCP port is open"""
    import socket
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except:
        return False


def check_mysql_connection() -> Tuple[bool, str]:
    """Check MySQL database connection"""
    try:
        import mysql.connector
        conn = mysql.connector.connect(
            host="localhost",
            port=3396,
            user="root",
            password="rootpassword",
            database="t16o_db",
            connection_timeout=5
        )
        cursor = conn.cursor()
        cursor.execute("SELECT VERSION()")
        version = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return True, f"MySQL {version}"
    except Exception as e:
        return False, str(e)[:40]


def check_rabbitmq_connection() -> Tuple[bool, str]:
    """Check RabbitMQ connection"""
    try:
        import pika
        credentials = pika.PlainCredentials('admin', 'admin123')
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host='localhost',
                port=5692,
                credentials=credentials,
                connection_attempts=1,
                retry_delay=1,
                socket_timeout=5
            )
        )
        connection.close()
        return True, "Connected"
    except Exception as e:
        return False, str(e)[:40]


# =============================================================================
# Main Check Functions
# =============================================================================

def check_all_cli_tools(verbose: bool = False) -> int:
    """Check all required CLI tools"""
    print_header("CLI Tools")
    missing = 0

    for name, cmd, desc in REQUIRED_CLI_TOOLS:
        found, version = check_cli_tool(cmd)
        if found:
            detail = version if verbose else ""
            print_status(f"{name} - {desc}", True, detail)
        else:
            print_status(f"{name} - {desc}", False, "not found")
            missing += 1

    return missing


def check_all_packages(install: bool = False, verbose: bool = False) -> int:
    """Check all required Python packages"""
    print_header("Python Packages (Required)")
    missing = 0

    for pip_name, import_name, desc in REQUIRED_PACKAGES:
        found, version = check_package_installed(import_name)

        if found:
            detail = f"v{version}" if verbose else ""
            print_status(f"{pip_name} - {desc}", True, detail)
        else:
            if install:
                print(f"  → Installing {pip_name}...", end=" ")
                if install_package(pip_name):
                    print("OK")
                    print_status(f"{pip_name} - {desc}", True, "installed")
                else:
                    print("FAILED")
                    print_status(f"{pip_name} - {desc}", False, "install failed")
                    missing += 1
            else:
                print_status(f"{pip_name} - {desc}", False, "not installed")
                missing += 1

    return missing


def check_optional_packages(verbose: bool = False) -> int:
    """Check optional Python packages"""
    print_header("Python Packages (Optional)")
    missing = 0

    for pip_name, import_name, desc in OPTIONAL_PACKAGES:
        found, version = check_package_installed(import_name)
        if found:
            detail = f"v{version}" if verbose else ""
            print_status(f"{pip_name} - {desc}", True, detail)
        else:
            print_status(f"{pip_name} - {desc}", False, "not installed (optional)")
            missing += 1

    return missing


def check_services(verbose: bool = False) -> int:
    """Check required services"""
    print_header("Services")
    missing = 0

    # Check ports first
    for name, endpoint in SERVICE_ENDPOINTS.items():
        port_open = check_port_open(endpoint["host"], endpoint["port"])
        if port_open:
            print_status(f"{name} port {endpoint['port']}", True, "listening")
        else:
            print_status(f"{name} port {endpoint['port']}", False, "not listening")
            missing += 1

    # Check actual connections if ports are open
    if check_port_open("localhost", 3396):
        success, detail = check_mysql_connection()
        print_status("MySQL connection", success, detail)
        if not success:
            missing += 1

    if check_port_open("localhost", 5692):
        success, detail = check_rabbitmq_connection()
        print_status("RabbitMQ connection", success, detail)
        if not success:
            missing += 1

    return missing


def start_docker_containers() -> bool:
    """Start Docker containers using docker-compose"""
    import os
    script_dir = os.path.dirname(os.path.abspath(__file__))
    compose_file = os.path.join(script_dir, "docker-compose.yml")

    if not os.path.exists(compose_file):
        print(f"  [X] docker-compose.yml not found at {compose_file}")
        return False

    print(f"  Starting containers from {compose_file}...")
    try:
        result = subprocess.run(
            ["docker-compose", "-f", compose_file, "up", "-d"],
            capture_output=True,
            text=True,
            timeout=120
        )
        if result.returncode == 0:
            print("  [OK] Containers started successfully")
            print("  Waiting 10s for services to initialize...")
            import time
            time.sleep(10)
            return True
        else:
            print(f"  [X] Failed to start containers: {result.stderr[:100]}")
            return False
    except Exception as e:
        print(f"  [X] Error starting containers: {e}")
        return False


def check_docker_containers(verbose: bool = False) -> int:
    """Check Docker containers"""
    print_header("Docker Containers")

    try:
        result = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}\t{{.Status}}"],
            capture_output=True,
            text=True,
            timeout=10
        )

        if result.returncode != 0:
            print_status("Docker", False, "not running or no permission")
            return 1

        containers = {}
        for line in result.stdout.strip().split('\n'):
            if line:
                parts = line.split('\t')
                if len(parts) >= 2:
                    containers[parts[0]] = parts[1]

        # Look for MySQL and RabbitMQ containers
        mysql_found = any('mysql' in name.lower() for name in containers.keys())
        rabbit_found = any('rabbit' in name.lower() for name in containers.keys())

        missing = 0
        for name, status in containers.items():
            is_up = 'Up' in status
            print_status(name, is_up, status[:30] if verbose else "")
            if not is_up:
                missing += 1

        if not mysql_found:
            print_status("t16o_v1_mysql", False, "container not found")
            missing += 1
        if not rabbit_found:
            print_status("t16o_v1_rabbitmq", False, "container not found")
            missing += 1

        return missing

    except Exception as e:
        print_status("Docker check", False, str(e)[:30])
        return 1


# =============================================================================
# Main
# =============================================================================

def main():
    import argparse

    parser = argparse.ArgumentParser(
        description='theGuide Environment Setup - Check and Install Dependencies'
    )
    parser.add_argument('--install', action='store_true',
                        help='Install missing Python packages')
    parser.add_argument('--start-docker', action='store_true',
                        help='Start Docker containers if not running')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Show detailed version information')

    args = parser.parse_args()

    print("\n" + "="*60)
    print("  theGuide Environment Dependency Checker")
    print("="*60)

    total_issues = 0

    # Check Python version
    print_header("Python Version")
    py_ok, py_version = check_python_version()
    print_status(f"Python {py_version}", py_ok)
    if not py_ok:
        total_issues += 1

    # Check CLI tools
    total_issues += check_all_cli_tools(args.verbose)

    # Check required packages
    total_issues += check_all_packages(args.install, args.verbose)

    # Check optional packages
    check_optional_packages(args.verbose)

    # Start Docker containers if requested
    if args.start_docker:
        print_header("Starting Docker Containers")
        start_docker_containers()

    # Check Docker containers
    total_issues += check_docker_containers(args.verbose)

    # Check services
    total_issues += check_services(args.verbose)

    # Summary
    print_header("Summary")
    if total_issues == 0:
        print("  [OK] All required dependencies are available!")
        print("  [OK] theGuide environment is ready.")
    else:
        print(f"  [X] Found {total_issues} issue(s) that need attention.")
        if not args.install:
            print("\n  Tip: Run with --install to install missing Python packages:")
            print(f"       python {sys.argv[0]} --install")
        print("\n  For Docker/services, ensure containers are running:")
        print("       docker-compose up -d")

    print()
    return 0 if total_issues == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
