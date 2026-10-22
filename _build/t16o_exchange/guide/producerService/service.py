"""
Producer Windows Service

Installation:
    python -m t16o_exchange.guide.producerService install
    python -m t16o_exchange.guide.producerService start
    python -m t16o_exchange.guide.producerService stop
    python -m t16o_exchange.guide.producerService remove
"""

from ..common.service_base import GuideServiceBase, run_service_command_line


class ProducerService(GuideServiceBase):
    """Windows service wrapper for Guide Producer"""

    _svc_name_ = 'T16OExchange.Guide.Producer'
    _svc_display_name_ = 'T16O Exchange - Guide Producer'
    _svc_description_ = 'T16O Exchange Guide Producer Service - Fetches transaction signatures from Solana RPC'

    worker_name = 'producer'
    worker_args = ['--queue-consumer']


if __name__ == '__main__':
    run_service_command_line(ProducerService)
