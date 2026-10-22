"""
Allow running as: python -m t16o_exchange.guide.aggregatorService
"""
from .service import AggregatorService, run_service_command_line

if __name__ == '__main__':
    run_service_command_line(AggregatorService)
