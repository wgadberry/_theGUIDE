"""
Detailer Windows Service

Installation:
    python -m t16o_exchange.guide.detailerService install
    python -m t16o_exchange.guide.detailerService start
    python -m t16o_exchange.guide.detailerService stop
    python -m t16o_exchange.guide.detailerService remove
"""

from ..common.service_base import GuideServiceBase, run_service_command_line


class DetailerService(GuideServiceBase):
    """Windows service wrapper for Guide Detailer"""

    _svc_name_ = 'T16OExchange.Guide.Detailer.Queue'
    _svc_display_name_ = 'T16O Exchange - Guide Detailer Queue'
    _svc_description_ = 'T16O Exchange Guide Detailer Service - Fetches transaction details and balance changes'

    worker_name = 'detailer'
    worker_args = []


if __name__ == '__main__':
    run_service_command_line(DetailerService)
