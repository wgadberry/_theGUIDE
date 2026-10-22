"""
Funder Windows Service

Installation:
    python -m t16o_exchange.guide.funderService install
    python -m t16o_exchange.guide.funderService start
    python -m t16o_exchange.guide.funderService stop
    python -m t16o_exchange.guide.funderService remove
"""

from ..common.service_base import GuideServiceBase, run_service_command_line


class FunderService(GuideServiceBase):
    """Windows service wrapper for Guide Funder"""

    _svc_name_ = 'T16OExchange.Guide.Funder.Queue'
    _svc_display_name_ = 'T16O Exchange - Guide Funder Queue'
    _svc_description_ = 'T16O Exchange Guide Funder Service - Discovers funding wallets for addresses'

    worker_name = 'funder'
    worker_args = []


if __name__ == '__main__':
    run_service_command_line(FunderService)
