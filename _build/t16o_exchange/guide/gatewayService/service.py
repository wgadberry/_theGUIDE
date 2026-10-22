"""
Gateway Windows Service

Installation:
    python -m t16o_exchange.guide.gatewayService install
    python -m t16o_exchange.guide.gatewayService start
    python -m t16o_exchange.guide.gatewayService stop
    python -m t16o_exchange.guide.gatewayService remove
"""

from ..common.service_base import GuideServiceBase, run_service_command_line


class GatewayService(GuideServiceBase):
    """Windows service wrapper for Guide Gateway"""

    _svc_name_ = 'T16OExchange.Guide.Gateway.Queue'
    _svc_display_name_ = 'T16O Exchange - Guide Gateway Queue'
    _svc_description_ = 'T16O Exchange Guide Gateway Service - Request orchestrator for theGuide pipeline'

    worker_name = 'gateway'
    worker_args = ['--with-queue-consumer', '--with-response-consumer']


if __name__ == '__main__':
    run_service_command_line(GatewayService)
