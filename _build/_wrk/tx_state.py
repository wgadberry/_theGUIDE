"""
tx_state.py: Transaction processing state bitmask constants

Each constant represents a bit in the tx.tx_state column (varchar, implicitly cast to int for bitwise ops).
Use bitwise OR (|) to set bits, bitwise AND (&) to check bits.

Example:
    from tx_state import TxState

    # Set multiple phases
    new_state = TxState.SHREDDED | TxState.DECODED | TxState.GUIDE_EDGES

    # Check if phase is complete
    if current_state & TxState.TOKENS_ENRICHED:
        print("Tokens already enriched")

    # Check if phase is missing
    if not (current_state & TxState.FUNDING_COMPLETE):
        print("Needs funding resolution")
"""


class TxState:
    """Bitmask constants for tx.tx_state column"""

    # Bit 0: Basic transaction data inserted
    SHREDDED = 1

    # Bit 1: Solscan decoded actions fetched
    DECODED = 2

    # Bit 2: tx_guide edges created (transfers, swaps, activities)
    GUIDE_EDGES = 4

    # Bit 3: New addresses sent to funding queue
    ADDRESSES_QUEUED = 8

    # Bit 4: tx_swap records created
    SWAPS_PARSED = 16

    # Bit 5: tx_transfer records created
    TRANSFERS_PARSED = 32

    # Bit 6: Detail enrichment complete
    DETAILED = 64

    # Bit 7: All token metadata fetched
    TOKENS_ENRICHED = 128

    # Bit 8: All pool/AMM data fetched
    POOLS_ENRICHED = 256

    # Bit 9: All addresses have funding info
    FUNDING_COMPLETE = 512

    # Bit 10: Addresses classified
    CLASSIFIED = 1024

    # Composite states for convenience
    SHREDDER_COMPLETE = SHREDDED | DECODED | GUIDE_EDGES | ADDRESSES_QUEUED | SWAPS_PARSED | TRANSFERS_PARSED  # = 63
    FULLY_PROCESSED = (1 << 11) - 1  # All 11 bits set = 2047

    @staticmethod
    def has_phase(current_state: int, phase: int) -> bool:
        """Check if a phase bit is set"""
        return (current_state or 0) & phase == phase

    @staticmethod
    def missing_phase(current_state: int, phase: int) -> bool:
        """Check if a phase bit is NOT set"""
        return (current_state or 0) & phase == 0

    @staticmethod
    def set_phase(current_state: int, phase: int) -> int:
        """Set a phase bit and return new state"""
        return (current_state or 0) | phase

    @staticmethod
    def clear_phase(current_state: int, phase: int) -> int:
        """Clear a phase bit and return new state"""
        return (current_state or 0) & ~phase

    @staticmethod
    def get_phases(current_state: int) -> list:
        """Get list of phase names that are set"""
        phases = []
        if current_state & TxState.SHREDDED:
            phases.append('SHREDDED')
        if current_state & TxState.DECODED:
            phases.append('DECODED')
        if current_state & TxState.GUIDE_EDGES:
            phases.append('GUIDE_EDGES')
        if current_state & TxState.ADDRESSES_QUEUED:
            phases.append('ADDRESSES_QUEUED')
        if current_state & TxState.SWAPS_PARSED:
            phases.append('SWAPS_PARSED')
        if current_state & TxState.TRANSFERS_PARSED:
            phases.append('TRANSFERS_PARSED')
        if current_state & TxState.DETAILED:
            phases.append('DETAILED')
        if current_state & TxState.TOKENS_ENRICHED:
            phases.append('TOKENS_ENRICHED')
        if current_state & TxState.POOLS_ENRICHED:
            phases.append('POOLS_ENRICHED')
        if current_state & TxState.FUNDING_COMPLETE:
            phases.append('FUNDING_COMPLETE')
        if current_state & TxState.CLASSIFIED:
            phases.append('CLASSIFIED')
        return phases


# SQL helper for updates (use in f-strings)
SQL_SET_STATE = "tx_state = tx_state | {phase}"
SQL_CHECK_MISSING = "tx_state & {phase} = 0"
SQL_CHECK_HAS = "tx_state & {phase} = {phase}"
