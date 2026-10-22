"""
Allow running as: python -m t16o_exchange.guide.decoderService
"""
from .service import DecoderService, run_service_command_line

if __name__ == '__main__':
    run_service_command_line(DecoderService)
