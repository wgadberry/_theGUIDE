"""
T16O Exchange Guide Producer Service

Signature fetcher for theGuide pipeline.
Consumes from: mq.guide.producer.request
Publishes to: mq.guide.producer.response
Cascades to: mq.guide.decoder.request, mq.guide.detailer.request
"""

from .service import ProducerService

__all__ = ['ProducerService']
