"""
T16O Exchange Guide Enricher Service

Data enrichment for theGuide pipeline.
Consumes from: mq.guide.enricher.request
Publishes to: mq.guide.enricher.response
"""

from .service import EnricherService

__all__ = ['EnricherService']
