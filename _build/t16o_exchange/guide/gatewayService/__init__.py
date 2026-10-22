"""
T16O Exchange Guide Gateway Service

Request orchestrator for theGuide pipeline.
Consumes from: mq.guide.gateway.request
Publishes to: mq.guide.gateway.response
"""

from .service import GatewayService

__all__ = ['GatewayService']
