"""
Allow running as: python -m t16o_exchange.guide.producerService
"""
from .service import ProducerService, run_service_command_line

if __name__ == '__main__':
    run_service_command_line(ProducerService)
