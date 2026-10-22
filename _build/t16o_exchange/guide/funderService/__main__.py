"""
Allow running as: python -m t16o_exchange.guide.funderService
"""
from .service import FunderService, run_service_command_line

if __name__ == '__main__':
    run_service_command_line(FunderService)
