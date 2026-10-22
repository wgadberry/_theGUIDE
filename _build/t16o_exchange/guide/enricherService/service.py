"""
Enricher Windows Service

Installation:
    python -m t16o_exchange.guide.enricherService install
    python -m t16o_exchange.guide.enricherService start
    python -m t16o_exchange.guide.enricherService stop
    python -m t16o_exchange.guide.enricherService remove
"""

from ..common.service_base import GuideServiceBase, run_service_command_line


class EnricherService(GuideServiceBase):
    """Windows service wrapper for Guide Enricher"""

    _svc_name_ = 'T16OExchange.Guide.Enricher.Queue'
    _svc_display_name_ = 'T16O Exchange - Guide Enricher Queue'
    _svc_description_ = 'T16O Exchange Guide Enricher Service - Enriches tokens, pools, and programs'

    worker_name = 'enricher'
    worker_args = []


if __name__ == '__main__':
    run_service_command_line(EnricherService)
