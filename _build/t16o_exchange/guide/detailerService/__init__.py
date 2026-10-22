"""
T16O Exchange Guide Detailer Service

Transaction detail fetcher for theGuide pipeline.
Consumes from: mq.guide.detailer.request
Publishes to: mq.guide.detailer.response
Writes to: t16o_db_staging.txs (tx_state=16)
"""

from .service import DetailerService

__all__ = ['DetailerService']
