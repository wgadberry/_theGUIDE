"""
T16O Exchange Guide Funder Service

Funding wallet discovery for theGuide pipeline.
Consumes from: mq.guide.funder.request
Publishes to: mq.guide.funder.response
"""

from .service import FunderService

__all__ = ['FunderService']
