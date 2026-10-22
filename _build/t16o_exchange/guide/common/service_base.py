"""
Windows Service Base Class for T16O Exchange Guide Services

Uses pywin32 to create native Windows services that wrap Python workers.
Each service manages its worker's lifecycle (start, stop, pause, resume).

Installation:
    python -m t16o_exchange.guide.<service>Service install
    python -m t16o_exchange.guide.<service>Service start
    python -m t16o_exchange.guide.<service>Service stop
    python -m t16o_exchange.guide.<service>Service remove
"""

import os
import sys
import time
import logging
import threading
import importlib.util
from abc import abstractmethod
from typing import Optional, Callable, Any

# Windows service imports
try:
    import win32serviceutil
    import win32service
    import win32event
    import servicemanager
    HAS_WIN32 = True
except ImportError:
    HAS_WIN32 = False
    # Stub classes for development/testing on non-Windows
    class win32serviceutil:
        class ServiceFramework:
            pass
        @staticmethod
        def HandleCommandLine(*args, **kwargs):
            pass

    class win32service:
        SERVICE_STOP_PENDING = 0x00000003
        SERVICE_RUNNING = 0x00000004
        SERVICE_START_PENDING = 0x00000002

    class win32event:
        WAIT_OBJECT_0 = 0
        @staticmethod
        def CreateEvent(*args, **kwargs):
            return None
        @staticmethod
        def SetEvent(*args, **kwargs):
            pass
        @staticmethod
        def WaitForSingleObject(*args, **kwargs):
            return 0

    class servicemanager:
        @staticmethod
        def LogInfoMsg(msg):
            print(f"[INFO] {msg}")
        @staticmethod
        def LogErrorMsg(msg):
            print(f"[ERROR] {msg}")


# Path to worker scripts
WORKER_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
    '_wrk'
)


class GuideServiceBase(win32serviceutil.ServiceFramework):
    """
    Base class for all Guide Windows services.

    Subclasses must define:
        _svc_name_: Windows service name (e.g., 'T16OExchange.Guide.Funder')
        _svc_display_name_: Display name in Services panel
        _svc_description_: Service description
        worker_name: Name of worker (e.g., 'funder')
    """

    # Subclasses must override these
    _svc_name_ = 'T16OExchange.Guide.Base'
    _svc_display_name_ = 'T16O Exchange - Guide Base'
    _svc_description_ = 'T16O Exchange Guide Base Service'

    # Worker configuration
    worker_name: str = None  # e.g., 'funder', 'shredder'
    worker_args: list = []   # Additional args to pass to worker

    def __init__(self, args):
        if HAS_WIN32:
            win32serviceutil.ServiceFramework.__init__(self, args)
            self.stop_event = win32event.CreateEvent(None, 0, 0, None)
        else:
            self.stop_event = threading.Event()

        self.is_running = False
        self.worker_thread: Optional[threading.Thread] = None
        self.worker_stop_flag = threading.Event()

        # Setup logging
        self._setup_logging()

    def _setup_logging(self):
        """Configure logging for the service"""
        log_dir = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
            'logs'
        )
        os.makedirs(log_dir, exist_ok=True)

        log_file = os.path.join(log_dir, f'{self.worker_name or "service"}.log')

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(self._svc_name_)

    def SvcStop(self):
        """Handle service stop request"""
        self.logger.info(f"Service stop requested: {self._svc_name_}")

        if HAS_WIN32:
            self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
            win32event.SetEvent(self.stop_event)
        else:
            self.stop_event.set()

        self.is_running = False
        self.worker_stop_flag.set()

        # Wait for worker thread to finish
        if self.worker_thread and self.worker_thread.is_alive():
            self.worker_thread.join(timeout=30)

        self.logger.info(f"Service stopped: {self._svc_name_}")

    def SvcDoRun(self):
        """Main service entry point"""
        if HAS_WIN32:
            servicemanager.LogInfoMsg(f"Starting service: {self._svc_name_}")

        self.logger.info(f"Service starting: {self._svc_name_}")
        self.is_running = True

        try:
            self._run_worker()
        except Exception as e:
            self.logger.error(f"Service error: {e}")
            if HAS_WIN32:
                servicemanager.LogErrorMsg(f"Service error: {e}")
            raise

    def _run_worker(self):
        """
        Run the worker in a separate thread.
        The worker module is loaded dynamically from _wrk directory.
        """
        if not self.worker_name:
            raise ValueError("worker_name must be set by subclass")

        # Load worker module
        worker_file = os.path.join(WORKER_PATH, f'guide-{self.worker_name}.py')
        if not os.path.exists(worker_file):
            raise FileNotFoundError(f"Worker not found: {worker_file}")

        self.logger.info(f"Loading worker: {worker_file}")

        # Import worker module dynamically
        spec = importlib.util.spec_from_file_location(f'guide_{self.worker_name}', worker_file)
        worker_module = importlib.util.module_from_spec(spec)
        sys.modules[f'guide_{self.worker_name}'] = worker_module
        spec.loader.exec_module(worker_module)

        # Determine entry point based on worker type
        from .config import SERVICE_REGISTRY
        worker_info = SERVICE_REGISTRY.get(self.worker_name, {})
        entry_func_name = worker_info.get('worker_entry', 'run_queue_consumer')

        if not hasattr(worker_module, entry_func_name):
            # Fallback: try main() with service args
            if hasattr(worker_module, 'main'):
                entry_func_name = 'main'
            else:
                raise AttributeError(f"Worker {self.worker_name} has no {entry_func_name} or main()")

        entry_func = getattr(worker_module, entry_func_name)

        # Start worker in thread
        self.worker_thread = threading.Thread(
            target=self._worker_wrapper,
            args=(entry_func,),
            name=f'Worker-{self.worker_name}',
            daemon=True
        )
        self.worker_thread.start()

        # Wait for stop signal
        if HAS_WIN32:
            while self.is_running:
                result = win32event.WaitForSingleObject(self.stop_event, 5000)
                if result == win32event.WAIT_OBJECT_0:
                    break
        else:
            while self.is_running and not self.stop_event.is_set():
                time.sleep(1)

    def _worker_wrapper(self, entry_func: Callable):
        """
        Wrapper to run worker function with proper error handling.
        Injects stop flag for graceful shutdown.
        """
        try:
            self.logger.info(f"Worker started: {self.worker_name}")

            # Build args for worker
            # Most workers use argparse, so we simulate CLI args
            original_argv = sys.argv
            sys.argv = [f'guide-{self.worker_name}.py'] + self._get_worker_args()

            try:
                # Call worker entry function
                # For queue consumers, we pass stop event if possible
                result = entry_func()
            finally:
                sys.argv = original_argv

            self.logger.info(f"Worker finished: {self.worker_name} (result={result})")

        except Exception as e:
            self.logger.error(f"Worker error: {self.worker_name} - {e}")
            import traceback
            self.logger.error(traceback.format_exc())

    def _get_worker_args(self) -> list:
        """
        Get command-line arguments for the worker.
        Subclasses can override to customize.
        """
        from .config import SERVICE_REGISTRY
        worker_info = SERVICE_REGISTRY.get(self.worker_name, {})

        args = list(self.worker_args)

        # Skip auto-args for workers that manage their own CLI (supervisor pattern)
        if not worker_info.get('auto_args', True):
            return args

        # Add default args based on worker type
        if worker_info.get('has_queue', True):
            if '--queue-consumer' not in args:
                args.append('--queue-consumer')
        else:
            # DB poller (shredder)
            if '--daemon' not in args:
                args.append('--daemon')

        return args


def create_service_class(
    worker_name: str,
    service_name: str = None,
    display_name: str = None,
    description: str = None,
    worker_args: list = None
) -> type:
    """
    Factory function to create a service class for a worker.

    Args:
        worker_name: Name of worker (e.g., 'funder')
        service_name: Windows service name
        display_name: Display name in Services panel
        description: Service description
        worker_args: Additional args for worker

    Returns:
        Service class that can be installed as Windows service
    """
    svc_name = service_name or f'T16OExchange.Guide.{worker_name.capitalize()}'
    svc_display = display_name or f'T16O Exchange - Guide {worker_name.capitalize()}'
    svc_desc = description or f'T16O Exchange Guide {worker_name.capitalize()} Service'

    class DynamicService(GuideServiceBase):
        _svc_name_ = svc_name
        _svc_display_name_ = svc_display
        _svc_description_ = svc_desc
        worker_name = worker_name

    DynamicService.worker_args = worker_args or []

    return DynamicService


def run_service_command_line(service_class: type):
    """
    Handle command-line for service installation/management.

    Usage:
        python service.py install  - Install the service
        python service.py start    - Start the service
        python service.py stop     - Stop the service
        python service.py remove   - Remove the service
        python service.py debug    - Run in debug mode (console)
    """
    if not HAS_WIN32:
        print("Windows service support requires pywin32")
        print("Install with: pip install pywin32")
        print("\nRunning in console mode for testing...")

        # Run in console mode for testing
        service = service_class([])
        try:
            service.SvcDoRun()
        except KeyboardInterrupt:
            service.SvcStop()
        return

    if len(sys.argv) == 1:
        # Started by Windows SCM
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(service_class)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        # Command line
        win32serviceutil.HandleCommandLine(service_class)
