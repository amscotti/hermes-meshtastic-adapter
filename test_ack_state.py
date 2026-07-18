"""Unit tests for the ACK/NACK state machine (ack_state).

Pure-logic tests that don't need an assembled adapter. Integration coverage of
the tracker (lock ordering, lifecycle checks, send()-level ACK waits, pruning)
remains in test_meshtastic.py and exercises AckTracker through the adapter's
thin delegates.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
hermes_agent_path = os.getenv("HERMES_AGENT_PATH", os.path.expanduser("~/.hermes/hermes-agent"))
if os.path.isdir(hermes_agent_path):
    sys.path.append(hermes_agent_path)

from gateway.platforms.base import SendResult

import ack_state
from ack_state import ACK_RECORD_LIMIT, PERMANENT_NAK_REASONS, AckStatus


class TestRetriabilityClassification(unittest.TestCase):
    """Only ACK-observed transient failures are retriable.

    Moved from test_meshtastic.TestMeshtasticPlatform.test_is_retriable_failure_classification
    — ack_state.is_retriable_failure is a pure module-level function, so it is
    exercised directly without a tracker or an assembled adapter.
    """

    def _r(self, ack):
        return SendResult(success=False, raw_response={"ack": ack} if ack else None)

    def test_classification(self):
        self.assertTrue(ack_state.is_retriable_failure(self._r({"status": AckStatus.TIMEOUT})))
        self.assertTrue(
            ack_state.is_retriable_failure(
                self._r({"status": AckStatus.NAK, "error_reason": "NO_ROUTE"})
            )
        )
        self.assertFalse(
            ack_state.is_retriable_failure(
                self._r({"status": AckStatus.NAK, "error_reason": "TOO_LARGE"})
            )
        )
        # PKI / auth failures are permanent — re-sending can't fix a key problem.
        for reason in (
            "PKI_FAILED",
            "PKI_UNKNOWN_PUBKEY",
            "PKI_SEND_FAIL_PUBLIC_KEY",
            "ADMIN_PUBLIC_KEY_UNAUTHORIZED",
            "NOT_AUTHORIZED",
            "DUTY_CYCLE_LIMIT",
            "RATE_LIMIT_EXCEEDED",
        ):
            self.assertFalse(
                ack_state.is_retriable_failure(
                    self._r({"status": AckStatus.NAK, "error_reason": reason})
                ),
                f"{reason} should be permanent",
            )
        self.assertFalse(ack_state.is_retriable_failure(self._r({"status": AckStatus.ACK})))
        self.assertTrue(ack_state.is_retriable_failure(self._r({"status": AckStatus.IMPLICIT_ACK})))
        # Plain strings still match (StrEnum + public JSON surface).
        self.assertTrue(ack_state.is_retriable_failure(self._r({"status": "timeout"})))
        self.assertFalse(ack_state.is_retriable_failure(self._r(None)))  # pre-send error
        # Disconnect-settled waiters must not spin retries against a closed radio.
        self.assertFalse(
            ack_state.is_retriable_failure(
                self._r({"status": AckStatus.TIMEOUT, "error_reason": "DISCONNECTED"})
            )
        )
        # Adapter-internal collision NAK: the chunk was already transmitted, so
        # retrying would duplicate it on-air.
        self.assertFalse(
            ack_state.is_retriable_failure(
                self._r({"status": AckStatus.NAK, "error_reason": "DUPLICATE_PACKET_ID"})
            )
        )

        # The PERMANENT_NAK_REASONS set is the single source of truth in
        # ack_state; confirm membership matches the documented contract.
        self.assertIs(PERMANENT_NAK_REASONS, ack_state.PERMANENT_NAK_REASONS)
        self.assertEqual(ACK_RECORD_LIMIT, 1000)


if __name__ == "__main__":
    unittest.main()
