"""
T16O Exchange Guide Shredder Service

Staging table processor for theGuide pipeline.
Polls: t16o_db_staging.txs
Mode: DB poller (no queue)
"""

from .service import ShredderService

__all__ = ['ShredderService']
