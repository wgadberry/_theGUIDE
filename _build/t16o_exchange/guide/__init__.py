"""
T16O Exchange Guide Services

Windows service wrappers for theGuide pipeline workers.

Services:
    - gatewayService: Request orchestrator
    - producerService: Signature fetcher
    - decoderService: Transaction decoder
    - detailerService: Transaction detail fetcher
    - funderService: Funding wallet discovery
    - aggregatorService: Data aggregator
    - enricherService: Data enricher
    - shredderService: Staging table processor
"""

__all__ = [
    'gatewayService',
    'producerService',
    'decoderService',
    'detailerService',
    'funderService',
    'aggregatorService',
    'enricherService',
    'shredderService',
]
