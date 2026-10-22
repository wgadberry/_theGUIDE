"""
T16O Exchange Guide Aggregator Service

Data aggregation for theGuide pipeline.
Consumes from: mq.guide.aggregator.request
Publishes to: mq.guide.aggregator.response
"""

from .service import AggregatorService

__all__ = ['AggregatorService']
