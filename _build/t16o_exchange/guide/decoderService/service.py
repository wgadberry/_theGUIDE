"""
Decoder Windows Service

Installation:
    python -m t16o_exchange.guide.decoderService install
    python -m t16o_exchange.guide.decoderService start
    python -m t16o_exchange.guide.decoderService stop
    python -m t16o_exchange.guide.decoderService remove
"""

from ..common.service_base import GuideServiceBase, run_service_command_line


class DecoderService(GuideServiceBase):
    """Windows service wrapper for Guide Decoder"""

    _svc_name_ = 'T16OExchange.Guide.Decoder.Queue'
    _svc_display_name_ = 'T16O Exchange - Guide Decoder Queue'
    _svc_description_ = 'T16O Exchange Guide Decoder Service - Decodes transaction data from Solana'

    worker_name = 'decoder'
    worker_args = []


if __name__ == '__main__':
    run_service_command_line(DecoderService)
