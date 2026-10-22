"""
Aggregator Windows Service

Installation:
    python -m t16o_exchange.guide.aggregatorService install
    python -m t16o_exchange.guide.aggregatorService start
    python -m t16o_exchange.guide.aggregatorService stop
    python -m t16o_exchange.guide.aggregatorService remove
"""

from ..common.service_base import GuideServiceBase, run_service_command_line


class AggregatorService(GuideServiceBase):
    """Windows service wrapper for Guide Aggregator"""

    _svc_name_ = 'T16OExchange.Guide.Aggregator.Queue'
    _svc_display_name_ = 'T16O Exchange - Guide Aggregator Queue'
    _svc_description_ = 'T16O Exchange Guide Aggregator Service - Guide edges & token participant sync'

    worker_name = 'aggregator'
    worker_args = []


if __name__ == '__main__':
    run_service_command_line(AggregatorService)
