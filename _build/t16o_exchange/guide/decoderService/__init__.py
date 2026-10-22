"""
T16O Exchange Guide Decoder Service

Transaction decoder for theGuide pipeline.
Consumes from: mq.guide.decoder.request
Publishes to: mq.guide.decoder.response
Writes to: t16o_db_staging.txs (tx_state=8)
"""

from .service import DecoderService

__all__ = ['DecoderService']
