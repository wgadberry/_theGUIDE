"""
Shredder Windows Service

Installation:
    python -m t16o_exchange.guide.shredderService install
    python -m t16o_exchange.guide.shredderService start
    python -m t16o_exchange.guide.shredderService stop
    python -m t16o_exchange.guide.shredderService remove
"""

from ..common.service_base import GuideServiceBase, run_service_command_line


class ShredderService(GuideServiceBase):
    """Windows service wrapper for Guide Shredder"""

    _svc_name_ = 'T16OExchange.Guide.Shredder.Queue'
    _svc_display_name_ = 'T16O Exchange - Guide Shredder Queue'
    _svc_description_ = 'T16O Exchange Guide Shredder Service - Processes staging table records'

    worker_name = 'shredder'
    worker_args = ['--daemon', '--batch-size', '50']  # DB poller mode


if __name__ == '__main__':
    run_service_command_line(ShredderService)
