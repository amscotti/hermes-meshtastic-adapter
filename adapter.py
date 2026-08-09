"""
Meshtastic Platform Adapter for Hermes Agent.

Connects to Meshtastic LoRa nodes over USB-serial or TCP/IP and bridges them
with the Hermes gateway runner.
"""

import asyncio
import importlib
import logging
import math
import os
import socket
import sys
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future as ConcurrentFuture
from concurrent.futures import InvalidStateError as ConcurrentInvalidStateError
from types import ModuleType
from typing import Any, cast

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, MessageEvent, MessageType, SendResult

try:
    from . import ack_state
except ImportError:
    import ack_state

try:
    from . import telemetry_db
except ImportError:
    import telemetry_db

try:
    from . import chunking
except ImportError:
    import chunking

try:
    from . import mock_interface
except ImportError:
    import mock_interface

try:
    from . import node_freshness
except ImportError:
    import node_freshness

try:
    from . import transport
except ImportError:
    import transport

try:
    from . import inbound
except ImportError:
    import inbound

try:
    from . import send_path
except ImportError:
    import send_path

try:
    from . import connection
except ImportError:
    import connection

try:
    from . import solicited
except ImportError:
    import solicited

logger = logging.getLogger(__name__)

# Value snapshots re-exported so existing imports/tests keep resolving.
# transport reads its own module attributes at call time (the library can be
# pip-installed and re-imported after this module loads — see
# transport.ensure_meshtastic_library), so tests must patch
# transport.HAS_MESHTASTIC / transport.pub, not these adapter copies, and the
# adapter's subscription path reads transport.* itself.
HAS_MESHTASTIC = transport.HAS_MESHTASTIC
pub = transport.pub
DEFAULT_TCP_PORT = transport.DEFAULT_TCP_PORT
_DaemonTransportExecutor = transport._DaemonTransportExecutor


class MeshLinkLost(Exception):
    """Raised into an in-flight solicited request when the transport drops.

    Distinct from a timeout: the node never got the chance to answer over the
    connection the request went out on, so we fail fast instead of waiting out
    the full timeout.
    """


# Protobuf message types for solicited requests. Optional like the rest of the
# meshtastic dependency — the tests exercise the request path with the mock.
try:
    from meshtastic.protobuf import mesh_pb2, portnums_pb2, telemetry_pb2
except ImportError:  # pragma: no cover - optional dependency in tests
    mesh_pb2 = portnums_pb2 = telemetry_pb2 = cast(Any, None)

# TCP keepalive probing for node links. The library's own heartbeat runs every
# 300s, so without this a link that dies silently (WiFi drop, node reboot, RST
# we never see) stays "connected" for up to five minutes — or until our next
# send fails. These settings surface it in ~KEEPALIVE_IDLE + COUNT*INTERVAL
# seconds instead, at the cost of a probe packet a node would otherwise never
# receive. Kept modest for that reason: LoRa nodes are not chatty peers.
KEEPALIVE_IDLE_SECS = 30
KEEPALIVE_INTERVAL_SECS = 10
KEEPALIVE_FAIL_COUNT = 3

# A link that comes back within this many seconds was reset by a node that
# stayed up (a socket-level RST); anything longer means the node itself was
# gone — a reboot, a WiFi drop, or a power cycle. Worth separating in the log:
# the first is a transport hiccup, the second is the node's own health.
# Canonical value lives in connection.py (classify_link_drop); re-exported for
# back-compat.
SOCKET_RESET_MAX_OUTAGE_SECS = connection.SOCKET_RESET_MAX_OUTAGE_SECS

# _standalone_send waits this long for the reconnect task to open an interface.
# Real serial/TCP constructors block until node info arrives (seconds), and
# connect() returns before the daemon transport worker finishes the open.
_STANDALONE_OPEN_TIMEOUT_SECS = 15.0

# ACK bookkeeping state machine lives in ack_state; AckStatus is re-exported
# here so existing imports from adapter keep resolving.
AckStatus = ack_state.AckStatus


# --- Mock Implementation for Testing / Dry Run ---
MockSerialInterface = mock_interface.MockSerialInterface
MockLocalNode = mock_interface.MockLocalNode


class MeshtasticAdapter(BasePlatformAdapter):
    """
    Meshtastic platform adapter. Bridges Meshtastic LoRa radios
    with Hermes async message routing.
    """

    MAX_MESSAGE_LENGTH = chunking.MAX_MESSAGE_LENGTH
    DEFAULT_CHUNK_BYTES = chunking.DEFAULT_CHUNK_BYTES

    # This adapter chunks long replies natively in send() (numbered LoRa-safe
    # chunks), so the gateway delivery router must hand us the full payload
    # instead of truncating it at max_message_length.
    splits_long_messages = True

    # LoRa has no edit primitive. Hermes uses this for streaming; tool-progress
    # pairing with edit_message is documented on edit_message itself.
    SUPPORTS_MESSAGE_EDITING = False

    # Upper bound on retained ACK/NACK bookkeeping records to avoid unbounded
    # memory growth on a long-running gateway. Oldest non-pending records evict
    # first. Aliases ack_state.ACK_RECORD_LIMIT (single source of truth); kept as
    # a class attribute so tests can override it per-instance, and AckTracker
    # reads it via its adapter back-reference.
    ACK_RECORD_LIMIT = ack_state.ACK_RECORD_LIMIT

    # Upper bound on the per-node "observed" overlay (live last_heard / signal
    # freshness). Aliases node_freshness.OBSERVED_NODE_LIMIT (single source of
    # truth); kept as a class attribute so tests/subclasses can override it
    # (the factory seam _create_node_freshness passes it through).
    OBSERVED_NODE_LIMIT = node_freshness.OBSERVED_NODE_LIMIT

    # Bounded receive path: the inbound queue sheds its OLDEST entry when full.
    INCOMING_QUEUE_MAXSIZE = 1000

    # Bounded outbound path: the offline queue (drained on reconnect) evicts its
    # OLDEST entry when full. Documented in README/CLAUDE.md as "bounded at 100".
    OUTBOUND_QUEUE_MAXSIZE = 100

    # Cap on queued+running SQLite writes; when full the newest observation is
    # dropped (freshness already captured it in memory).
    DB_WRITE_MAX_QUEUE = 128

    # Cap on in-flight handle_message tasks (authorized-text flood defense).
    MESSAGE_TASK_LIMIT = 256

    # A transiently-failing queued message is retried this many times before it
    # is dropped, so a stuck interface cannot block the rest of the queue.
    DRAIN_MAX_ATTEMPTS = 3

    # Re-warn for an unauthorized node at most once per window (log-volume bound).
    UNAUTHORIZED_REWARN_SECS = 300.0

    @property
    def message_len_fn(self):
        return lambda text: len(str(text).encode("utf-8"))

    # --- ACK state delegates -------------------------------------------------
    # The ACK bookkeeping dicts and _ack_lock live on self._ack_tracker
    # (ack_state.AckTracker). send() and tests read them through these
    # read-only properties, and the state-machine methods route through the
    # thin delegates below. Lock acquisition order and lifecycle checks remain
    # entirely inside AckTracker.

    @property
    def _pending_acks(self) -> dict[str, dict[str, Any]]:
        return self._ack_tracker._pending_acks

    @property
    def _ack_responses(self) -> dict[str, dict[str, Any]]:
        return self._ack_tracker._ack_responses

    @property
    def _ack_tokens(self) -> dict[str, object]:
        return self._ack_tracker._ack_tokens

    @property
    def _ack_response_tokens(self) -> dict[str, object]:
        return self._ack_tracker._ack_response_tokens

    @property
    def _ack_inflight_tokens(self) -> dict[object, int]:
        return self._ack_tracker._ack_inflight_tokens

    @property
    def _early_ack_packets(self) -> dict[object, tuple[dict, str, str, int]]:
        return self._ack_tracker._early_ack_packets

    @property
    def _ack_futures(self) -> dict[str, ConcurrentFuture]:
        return self._ack_tracker._ack_futures

    @property
    def _ack_lock(self) -> threading.Lock:
        return self._ack_tracker._ack_lock

    @property
    def _response_waiters(self) -> dict[tuple[str, str], list[ConcurrentFuture]]:
        """Read-only bridge to the solicited tracker's registry (tests item-read it)."""
        return self._solicited._response_waiters

    @property
    def _response_lock(self) -> threading.Lock:
        """Read-only bridge to the solicited tracker's lock."""
        return self._solicited._response_lock

    def _maybe_record_pubsub_ack(self, packet: dict) -> bool:
        return self._ack_tracker._maybe_record_pubsub_ack(packet)

    def _track_pending_ack(
        self,
        pkt_id: str | None,
        dest: str,
        content: str,
        *,
        create_future: bool = False,
        send_token: object | None = None,
    ) -> ConcurrentFuture | None:
        return self._ack_tracker._track_pending_ack(
            pkt_id, dest, content, create_future=create_future, send_token=send_token
        )

    def _fail_pending_acks(self, reason: str = "DISCONNECTED") -> None:
        self._ack_tracker._fail_pending_acks(reason)

    def get_ack_status(self, packet_id: str) -> dict[str, Any] | None:
        return self._ack_tracker.get_ack_status(packet_id)

    def _make_ack_callback(self, dest: str, content: str):
        return self._ack_tracker._make_ack_callback(dest, content)

    def _make_ack_callback_for_send(
        self,
        dest: str,
        content: str,
        send_token: object | None,
        lifecycle_id: int | None = None,
    ):
        return self._ack_tracker._make_ack_callback_for_send(
            dest, content, send_token, lifecycle_id
        )

    def _record_ack_response(
        self,
        packet: dict,
        dest: str,
        content: str,
        *,
        send_token: object | None = None,
        lifecycle_id: int | None = None,
    ) -> None:
        self._ack_tracker._record_ack_response(
            packet, dest, content, send_token=send_token, lifecycle_id=lifecycle_id
        )

    def _set_ack_future_result(self, future: ConcurrentFuture, record: dict[str, Any]) -> None:
        self._ack_tracker._set_ack_future_result(future, record)

    async def _wait_for_ack(
        self,
        pkt_id: str,
        future: ConcurrentFuture,
        timeout: float,
    ) -> dict[str, Any]:
        return await self._ack_tracker._wait_for_ack(pkt_id, future, timeout)

    def _is_retriable_failure(self, result: SendResult) -> bool:
        return ack_state.is_retriable_failure(result)

    def _ack_wait_config(self, metadata: dict[str, Any] | None) -> tuple[bool, float]:
        return ack_state.ack_wait_config(metadata)

    def _send_retries(self, metadata: dict[str, Any] | None) -> int:
        return ack_state.send_retries(metadata)

    def _retry_backoff(self) -> float:
        return ack_state.retry_backoff()

    @property
    def enforces_own_access_policy(self) -> bool:
        """This adapter gates inbound traffic itself in ``_on_receive``.

        Tells the gateway's ``_is_user_authorized`` that it may trust an
        already-gated Meshtastic event. The gateway only actually trusts when
        ``_dm_policy`` resolves to ``"allowlist"`` (see below), mirroring
        WeCom/Weixin/WhatsApp — defense-in-depth on top of the env allowlist
        wired via the registry's ``allowed_users_env``.
        """
        return True

    @property
    def _dm_policy(self) -> str:
        """Effective DM access policy read by the gateway trust path.

        ``"allowlist"`` when a node allowlist is active (the gateway then trusts
        the adapter's own intake gate); ``"open"`` when ``allow_all`` is set.
        With no allowlist and ``allow_all=False`` the adapter default-denies at
        intake, so the gateway never sees such traffic — "open" is inert there.
        """
        if self.allowed_nodes and not self.allow_all:
            return "allowlist"
        return "open"

    @property
    def _group_policy(self) -> str:
        """Effective group/channel access policy read by the gateway trust path.

        Meshtastic channel broadcasts map to ``chat_type="group"`` and pass
        through the same ``_is_authorized_node`` intake gate as DMs, so the
        effective policy is identical.
        """
        return self._dm_policy

    def format_tool_event(
        self, event: Any, *, mode: str = "all", preview_max_len: int = 40
    ) -> str | None:
        """Render a short emoji tool blurb for LoRa (not the full args dump).

        Hermes gateway defaults emit long lines like
        ``🔍 Searching the web for <long query>``. Over mesh we only want a
        one-line blurb (emoji + verb); the final answer still delivers in full.
        Returns None when the event is unusable so the dispatcher can drop it.
        """
        del mode, preview_max_len
        try:
            from agent.display import get_tool_emoji, get_tool_verb
        except ImportError:
            get_tool_emoji = None  # type: ignore[assignment]
            get_tool_verb = None  # type: ignore[assignment]

        tool_name = getattr(event, "tool_name", None) or ""
        if not tool_name:
            return None
        emoji = "⚙️"
        if get_tool_emoji is not None:
            try:
                emoji = get_tool_emoji(tool_name, default="⚙️") or "⚙️"
            except Exception as exc:
                logger.debug("get_tool_emoji(%r) failed: %s", tool_name, exc)
        verb = None
        if get_tool_verb is not None:
            try:
                verb = get_tool_verb(tool_name)
            except Exception as exc:
                logger.debug("get_tool_verb(%r) failed: %s", tool_name, exc)
                verb = None
        if verb:
            return f"{emoji} {verb}"
        return f"{emoji} {tool_name}"

    @staticmethod
    def _compact_tool_progress_line(content: str) -> str:
        """Shrink a gateway tool-progress line to a short LoRa blurb.

        The classic progress_callback path does not call ``format_tool_event``;
        it builds full verb+preview strings and ``send()``s them. Cap those to
        emoji + verb (drop `` for <preview>`` / long args) so each tool is one
        short mesh packet instead of a multi-chunk dump.
        """
        text = (content or "").strip()
        if not text:
            return content
        # Progress is single-line chrome; multi-line dumps (approval walls,
        # fenced terminal blocks) are not compacted here — they need other
        # handling. Only rewrite short single-line progress.
        if "\n" in text:
            return content
        # "🔍 Searching the web for long query…" → "🔍 Searching the web"
        if " for " in text:
            head, _, _rest = text.partition(" for ")
            head = head.strip()
            if head:
                return head
        # "⚙️ tool_name: \"preview…\"" → "⚙️ tool_name"
        if ": " in text and len(text) > 48:
            head, _, _rest = text.partition(": ")
            head = head.strip()
            if head:
                return head
        # No structured tool-progress separator matched. This could be a real
        # emoji-leading reply (e.g. an emoji-heavy user-facing message), not
        # gateway chrome — do not guess-and-truncate. Return the original so the
        # normal chunking path applies instead of silently mangling real content.
        return content

    def __init__(self, config: PlatformConfig, **kwargs):
        platform = Platform("meshtastic")
        super().__init__(config=config, platform=platform)

        # Read plugin configuration from env or config.yaml extra
        extra = getattr(config, "extra", {}) or {}

        self.serial_port = os.getenv("MESHTASTIC_SERIAL_PORT") or extra.get("serial_port") or "auto"
        self.baud_rate = int(os.getenv("MESHTASTIC_BAUD_RATE") or extra.get("baud_rate", 115200))
        # meshtastic.serial_interface hardcodes 115200 on the pyserial open —
        # MESHTASTIC_BAUD_RATE is accepted for setup-UI parity / future use but
        # is not applied to the library constructor today.
        if self.baud_rate != 115200:
            logger.warning(
                "MESHTASTIC_BAUD_RATE=%s is ignored: the meshtastic library always "
                "opens serial at 115200.",
                self.baud_rate,
            )

        # Optional TCP/IP transport for WiFi/Ethernet-capable nodes. When a host
        # is configured the adapter connects over TCP instead of serial; the two
        # transports are mutually exclusive (one connection at a time).
        self.tcp_host = (os.getenv("MESHTASTIC_TCP_HOST") or extra.get("tcp_host") or "").strip()
        self.tcp_port = int(
            os.getenv("MESHTASTIC_TCP_PORT") or extra.get("tcp_port") or DEFAULT_TCP_PORT
        )

        # Access control list (Allowed node IDs, e.g. '!da1b1613')
        allowed_nodes_raw = (
            os.getenv("MESHTASTIC_ALLOWED_NODES")
            or os.getenv("MESHTASTIC_ALLOWED_USERS")
            or extra.get("allowed_nodes")
            or extra.get("allowed_users")
            or ""
        )
        self.allow_all = (
            os.getenv("MESHTASTIC_ALLOW_ALL_USERS", "").lower() in ("1", "true", "yes")
            if os.getenv("MESHTASTIC_ALLOW_ALL_USERS")
            else extra.get("allow_all_users", False)
        )

        # Whether to answer channel/broadcast messages. Default False: the agent
        # replies to direct messages only and never posts into a shared public
        # channel (which wastes mesh airtime and is visible to everyone). Set
        # MESHTASTIC_ALLOW_CHANNELS=true to opt in.
        self.allow_channels = (
            os.getenv("MESHTASTIC_ALLOW_CHANNELS", "").lower() in ("1", "true", "yes")
            if os.getenv("MESHTASTIC_ALLOW_CHANNELS")
            else extra.get("allow_channels", False)
        )

        self.allowed_nodes: set[str] = set()
        if allowed_nodes_raw:
            parts = [p.strip().lower() for p in str(allowed_nodes_raw).split(",") if p.strip()]
            for p in parts:
                self.allowed_nodes.add(p)
                # If they omitted the leading '!', support matching it too
                if not p.startswith("!"):
                    self.allowed_nodes.add(f"!{p}")
                else:
                    self.allowed_nodes.add(p.lstrip("!"))

        # Hermes gateway re-checks the allowlist env with exact string equality
        # (no case fold, no bang-normalization). Expand the env so its second
        # gate accepts the same forms we accept at intake. See authz_mixin.
        self._expand_allowlist_env_for_gateway()
        # Cron/notification delivery passes MESHTASTIC_HOME_CHANNEL through as
        # the chat id; fix the all-too-common missing-prefix misconfiguration.
        self._expand_home_channel_env_for_gateway()

        # Nodes already warned about unauthorized access, mapped to the last
        # warn time — a per-node re-warn window bounds the log volume.
        self._unauthorized_warned: dict[str, float] = {}

        if not transport.HAS_MESHTASTIC:
            logger.error(
                "meshtastic library is NOT installed in the gateway's Python "
                "environment — the radio will NOT work and connections fail. "
                "The adapter attempts one automatic install "
                "(MESHTASTIC_AUTOINSTALL=0 to disable), or run manually:\n"
                "  %s -m pip install -r %s",
                sys.executable,
                transport._requirements_path(),
            )

        # Live-observed per-node overlay (last_heard / signal learned from the
        # packet stream), keyed by node id. Fed in _on_receive for EVERY heard
        # node and layered over the library's node DB by the mesh_* tools.
        self._node_freshness = self._create_node_freshness()

        # Receive-stage packet pipeline (normalization → freshness overlay →
        # self-echo filter → observability routing → authz pre-check) lives in
        # inbound.InboundProcessor. It runs synchronously on the platform loop;
        # SQLite writes are delegated back through the _run_db_write writers so
        # they stay off the loop. The processor never imports the adapter —
        # everything it needs is injected here.
        self._inbound = inbound.InboundProcessor(
            normalize_id=self._normalize_node_id,
            freshness=self._node_freshness,
            allow_all=lambda: self.allow_all,
            allowed_nodes=lambda: self.allowed_nodes,
            write_signal=lambda nid, snr, rssi, hops: self._run_db_write(
                lambda: telemetry_db.log_signal(nid, snr, rssi, hops)
            ),
            write_telemetry=lambda nid, decoded: self._run_db_write(
                lambda: inbound.log_telemetry_packet(nid, decoded)
            ),
            write_position=lambda nid, decoded: self._run_db_write(
                lambda: inbound.log_position_packet(nid, decoded)
            ),
        )

        # Active hardware connections mapping: devPath -> interface.
        # _iface_lock protects only short map/state operations; slow Meshtastic
        # I/O is serialized by the lifecycle's single daemon transport worker.
        self._interfaces: dict[str, Any] = {}
        self._iface_lock = threading.Lock()
        self._transport_executor: _DaemonTransportExecutor | None = None
        self._pubsub_subscribed = False
        self._lifecycle_id = 0
        self._lifecycle_lock = threading.Lock()
        self._disconnecting = False
        # Waiters poll the shared completion future (_disconnect_future), not
        # this Event, so concurrent disconnects never occupy default-executor
        # workers. The Event remains only as a teardown-completion signal for
        # tests/diagnostics.
        self._disconnect_done = threading.Event()
        self._disconnect_done.set()
        self._disconnect_future: ConcurrentFuture | None = None
        self._disconnect_task: asyncio.Task | None = None
        self._disconnect_owner_loop: asyncio.AbstractEventLoop | None = None
        self._disconnect_interfaces: list[tuple[str, Any]] = []
        self._disconnect_close_started = False

        # Outbound message queue for temporary drops (Phase 3 Task 2)
        # Bounded at 100 messages, oldest-first eviction
        self._outbound_queue: list[dict[str, Any]] = []
        self._queue_lock = threading.Lock()
        # Bound for _run_db_write's executor backlog (see DB_WRITE_MAX_QUEUE).
        self._db_write_slots = threading.Semaphore(self.DB_WRITE_MAX_QUEUE)
        # ACK/NACK tracking state machine: owns the 7 ACK dicts + _ack_lock.
        # Exposed on the adapter via read-only property delegates below so
        # send() and tests can keep reading self._ack_lock / self._pending_acks.
        self._ack_tracker = ack_state.AckTracker(self)

        # Link health: which socket already has keepalive armed (the library
        # replaces it on self-heal), when each target went down, and a running
        # tally that separates socket resets from the node actually being away.
        self._keepalive_socket_id: int | None = None
        self._link_down_since: dict[str, float] = {}
        self._link_drop_counts: dict[str, int] = {"socket_reset": 0, "node_absent": 0}

        # Waiters for *solicited* replies (telemetry / position / traceroute
        # requested with wantResponse) live in solicited.SolicitedRequestTracker;
        # the registry and lock are exposed via read-only property delegates
        # below so call sites and tests can keep reading self._response_waiters.
        self._solicited = solicited.SolicitedRequestTracker(
            normalize_node_id=self._normalize_node_id,
            interfaces_provider=self.get_interfaces,
            executor_provider=self._transport_executor_for_solicit,
            link_lost_exc=MeshLinkLost,
        )

        # Pause state: when set, the reconnect loop releases the node's socket
        # and stops reconnecting so another client can take the node's limited
        # TCP slot; _pause_until arms an auto-resume. Guarded by _pause_lock.
        self._paused = False
        self._pause_until: float | None = None
        self._pause_lock = threading.Lock()

        # Platform loop: set in connect(). Owns _incoming_queue, reconnect /
        # drain tasks, and the pubsub→queue bridge. Send/ACK waiters may run on
        # a *different* loop (agent session). ACK completion uses
        # concurrent.futures (loop-independent); transport I/O is serialized
        # on the daemon worker (not the platform loop).
        self.loop: asyncio.AbstractEventLoop | None = None
        self._cross_loop_send_logged = False
        self._reconnect_tasks: dict[str, asyncio.Task] = {}
        self._queue_drain_task: asyncio.Task | None = None
        self._running = False

        # Incoming queue and tasks for thread-safe bridge
        self._incoming_queue: asyncio.Queue | None = None
        self._incoming_consumer_task: asyncio.Task | None = None
        self._message_tasks: set[asyncio.Task] = set()

        # Initialise SQLite telemetry DB
        telemetry_db.init_db()
        logger.info("MeshtasticAdapter initialized.")

    @property
    def name(self) -> str:
        return "Meshtastic"

    def get_interfaces(self) -> list[Any]:
        """Return the active serial/BLE interface instances."""
        with self._iface_lock:
            return list(self._interfaces.values())

    def _has_interfaces(self) -> bool:
        with self._iface_lock:
            return bool(self._interfaces)

    def _register_interface(
        self, target: str, iface: Any, *, lifecycle_id: int | None = None
    ) -> bool:
        """Register ``iface`` only if this lifecycle still owns ``target``."""
        with self._iface_lock:
            if (
                not self._running
                or target in self._interfaces
                or (lifecycle_id is not None and lifecycle_id != self._lifecycle_id)
            ):
                return False
            self._interfaces[target] = iface
        return True

    def _pop_interface_for_lifecycle(
        self, target: str, lifecycle_id: int
    ) -> tuple[bool, Any | None]:
        """Remove ``target`` only while ``lifecycle_id`` still owns adapter state.

        Stays on the adapter: it is a two-lock (lifecycle -> iface) read-modify-
        write over adapter state, so it cannot be extracted without either
        leaking the lock-ordering invariant or parameterizing a whole lock pair.
        """
        with self._lifecycle_lock:
            if lifecycle_id != self._lifecycle_id or not self._running:
                return False, None
            with self._iface_lock:
                return True, self._interfaces.pop(target, None)

    @staticmethod
    def _close_interfaces_serialized(interfaces: list[Any]) -> None:
        """Test-compat delegate — body moved to transport.close_interfaces_serialized."""
        transport.close_interfaces_serialized(interfaces)

    async def _close_interfaces(self, interfaces: list[Any]) -> None:
        """Delegate — body moved to transport.close_interfaces (test-compat)."""
        with self._lifecycle_lock:
            executor = self._transport_executor
        # cast(Any): the dual-import binds ``transport`` to a union of two
        # module objects, so the nominal _DaemonTransportExecutor type cannot
        # cross this boundary; the value itself is unchanged.
        await transport.close_interfaces(
            interfaces, cast(Any, executor), self._executor_shutdown_timeout()
        )

    @staticmethod
    def _open_cancel_timeout() -> float:
        """Seconds to wait for a cancelled open before abandoning the await.

        ``0`` means do not wait (abandon immediately). The constructor still
        runs on the daemon transport worker and closes a stale result via
        lifecycle_id. Override with MESHTASTIC_OPEN_CANCEL_TIMEOUT.
        """
        return connection.open_cancel_timeout()

    @staticmethod
    def _executor_shutdown_timeout() -> float:
        """Seconds to wait for transport-worker drain / close during disconnect.

        ``0`` means do not wait. Override with MESHTASTIC_EXECUTOR_SHUTDOWN_TIMEOUT.
        """
        return connection.executor_shutdown_timeout()

    @staticmethod
    def _open_timeout() -> float:
        """Seconds to bound the success-path open (``0`` disables the bound)."""
        return connection.open_timeout()

    async def _shutdown_transport_executor(self, executor: _DaemonTransportExecutor) -> None:
        """Delegate — body moved to transport.shutdown_transport_executor (test-compat)."""
        # cast(Any): same dual-import union boundary as _close_interfaces.
        await transport.shutdown_transport_executor(
            cast(Any, executor), self._executor_shutdown_timeout()
        )

    def _drop_interface_if_dead_serialized(self, target: str, iface: Any) -> bool | None:
        """Delegate — body moved to transport.drop_interface_if_dead_serialized."""
        return transport.drop_interface_if_dead_serialized(
            target,
            iface,
            interfaces=self._interfaces,
            iface_lock=self._iface_lock,
            is_alive=self._interface_is_alive,
        )

    def _open_and_register_interface(self, target: str, lifecycle_id: int) -> Any | None:
        """Open on a worker, then atomically adopt or close a stale result.

        A timed-out open can still finish on the daemon worker and register the
        target while a retry is already in flight. The retry's open then fails
        ``_register_interface`` because the key is taken — without recovering
        the live iface, ``_reconnect_loop`` would treat ``None`` as terminal
        and exit while the interface stays registered with no liveness poll.
        """
        iface = self._open_interface(target)
        if self._register_interface(target, iface, lifecycle_id=lifecycle_id):
            return iface
        # Registration lost the race. Prefer the already-registered iface for
        # this lifecycle; otherwise close our orphan and report failure.
        with self._lifecycle_lock:
            active = self._running and lifecycle_id == self._lifecycle_id
            with self._iface_lock:
                existing = self._interfaces.get(target) if active else None
        transport.close_interfaces_serialized([iface])
        return existing

    def _subscribe_pubsub(self) -> None:
        """Subscribe once per adapter lifecycle, independent of interface count.

        Reads transport.HAS_MESHTASTIC / transport.pub at call time (not the
        module-level snapshots): ensure_meshtastic_library() may pip-install
        and re-import the library after this module was imported.
        """
        if not transport.HAS_MESHTASTIC or not transport.pub or self._pubsub_subscribed:
            return
        pub = transport.pub
        pub.subscribe(self._on_receive_pubsub, "meshtastic.receive")
        pub.subscribe(self._on_connection_lost, "meshtastic.connection.lost")
        pub.subscribe(self._on_connection_established, "meshtastic.connection.established")
        self._pubsub_subscribed = True

    def _unsubscribe_pubsub(self) -> None:
        if not transport.HAS_MESHTASTIC or not transport.pub or not self._pubsub_subscribed:
            return
        pub = transport.pub
        pub.unsubscribe(self._on_receive_pubsub, "meshtastic.receive")
        pub.unsubscribe(self._on_connection_lost, "meshtastic.connection.lost")
        pub.unsubscribe(self._on_connection_established, "meshtastic.connection.established")
        self._pubsub_subscribed = False

    def _schedule_on_loop(
        self,
        loop: asyncio.AbstractEventLoop | None,
        callback: Callable[..., Any],
        *args: Any,
        what: str = "callback",
    ) -> bool:
        """Thread-safe schedule onto ``loop``. Returns False if skipped.

        Used from meshtastic pubsub / radio callback threads to touch asyncio
        state. Logs at debug when the target loop is missing, not running, or
        closes between the check and ``call_soon_threadsafe`` (TOCTOU race).
        """
        if loop is None:
            logger.debug("Skipping %s: target loop is None", what)
            return False
        try:
            if not loop.is_running():
                logger.debug("Skipping %s: target loop not running (loop=%r)", what, loop)
                return False
            loop.call_soon_threadsafe(callback, *args)
            return True
        except RuntimeError as exc:
            # Loop closed between is_running() and call_soon_threadsafe.
            logger.debug("Skipping %s: %s", what, exc)
            return False

    def _cancel_task_threadsafe(self, task: asyncio.Task) -> None:
        """Cancel a task regardless of which loop owns it.

        ``Task.cancel()`` touches the owning loop's internal state and is only
        safe to call from that loop's thread. Disconnect teardown may run on a
        follower loop that took over after the platform loop's owner task was
        cancelled/stranded; in that case foreign-loop tasks are cancelled via
        ``call_soon_threadsafe``. A stopped-but-open loop accepts the callback
        and cancels the task before it can resume into a later lifecycle.
        """
        loop = task.get_loop()
        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            current_loop = None
        if loop is current_loop:
            task.cancel()
            return
        if task.done() or loop.is_closed():
            return
        try:
            # Unlike _schedule_on_loop (the inbound bridge), cancellation must
            # also be queued on a stopped loop in case it is restarted later.
            loop.call_soon_threadsafe(task.cancel)
        except RuntimeError as exc:
            logger.debug("Skipping task cancellation: %s", exc)

    def _lifecycle_is_active(self, lifecycle_id: int) -> bool:
        """Return whether ``lifecycle_id`` still owns adapter loop tasks."""
        with self._lifecycle_lock:
            return self._running and self._lifecycle_id == lifecycle_id

    def _run_db_write(self, fn: Callable[[], None]) -> None:
        """Run a blocking telemetry DB write off the event loop when one is available.

        Inbound processing runs on the platform loop. The target callables
        swallow their own exceptions, so the executor future is intentionally
        fire-and-forget. The semaphore bounds queued+running writes so a packet
        flood cannot grow the default executor's backlog without limit; a write
        dropped while the budget is full is acceptable — the packet's freshness
        was already captured in memory and only the newest observation is lost.
        """
        loop = self.loop
        if loop is None:
            fn()
            return
        if not self._db_write_slots.acquire(blocking=False):
            logger.debug("Dropping Meshtastic DB write: executor backlog full")
            return
        try:
            future = loop.run_in_executor(None, fn)
        except Exception:
            self._db_write_slots.release()
            raise
        future.add_done_callback(lambda _fut: self._db_write_slots.release())

    def _is_authorized_node(self, node_id: str) -> bool:
        # Test-compat delegate — receive logic lives in inbound.py
        # (InboundProcessor); keep until moved tests drop the dependency.
        """Check if a node ID is permitted to speak with the bot."""
        return inbound.is_authorized_node(
            node_id, allow_all=self.allow_all, allowed_nodes=self.allowed_nodes
        )

    def _warn_unauthorized_node(self, sender: str) -> None:
        """Log an unauthorized sender at most once per re-warn window.

        Every unauthorized TEXT packet used to log a warning — a flood from one
        node on a shared channel would write a disk line per packet. Warn on the
        first sighting, then debug until ``UNAUTHORIZED_REWARN_SECS`` elapses.
        """
        now = time.time()
        last = self._unauthorized_warned.get(sender)
        if last is None or now - last >= self.UNAUTHORIZED_REWARN_SECS:
            self._unauthorized_warned[sender] = now
            logger.warning("Unauthorized node ID %s skipped.", sender)
        else:
            logger.debug("Unauthorized node ID %s skipped (repeated).", sender)

    @staticmethod
    def _normalize_node_id(node_id: Any) -> str | None:
        """Canonicalize a Meshtastic node id to ``!`` + lowercase 8-hex when possible.

        Hermes gateway allowlist matching is exact (no case fold / bang
        normalization), so inbound ``user_id`` / DM chat_ids must be stable and
        match the ``!aabbccdd`` form operators put in MESHTASTIC_ALLOWED_NODES.
        Numeric node numbers and ``!``-prefixed hex (any case) are normalized;
        other string forms are lowercased as-is.
        """
        if node_id is None:
            return None
        # bool is a subclass of int — don't treat True/False as node numbers.
        if isinstance(node_id, bool):
            return str(node_id).lower()
        if isinstance(node_id, int):
            # Node numbers are unsigned 32-bit. Reject out-of-range values so a
            # hostile envelope cannot mint a bogus id (e.g. `!-0000001` from a
            # negative, or a value colliding with an allowed node) — such a
            # sender is dropped instead.
            if 0 <= node_id < 2**32:
                return f"!{node_id:08x}"
            return None
        text = str(node_id).strip()
        if not text:
            return None
        low = text.lower()
        bare = low[1:] if low.startswith("!") else low
        if len(bare) == 8 and all(c in "0123456789abcdef" for c in bare):
            return f"!{bare}"
        return low

    @staticmethod
    def _expand_allowlist_env_for_gateway() -> None:
        """Expand MESHTASTIC_ALLOWED_NODES so Hermes' exact-match gate accepts our forms.

        The adapter accepts node ids with/without ``!`` and any case. Hermes
        ``_is_user_authorized`` reads ``allowed_users_env`` (MESHTASTIC_ALLOWED_NODES)
        with exact equality and no normalization. Expanding that env keeps the
        gateway double-check aligned with adapter intake. Legacy
        MESHTASTIC_ALLOWED_USERS is still read for adapter-local allowlisting but
        is not the gateway's auth env, so it is left untouched here.
        """
        raw = os.getenv("MESHTASTIC_ALLOWED_NODES", "").strip()
        if not raw:
            return
        expanded: set[str] = set()
        for part in raw.split(","):
            p = part.strip()
            if not p:
                continue
            expanded.add(p)
            low = p.lower()
            expanded.add(low)
            bare = low.lstrip("!")
            if bare:
                expanded.add(bare)
                expanded.add(f"!{bare}")
        os.environ["MESHTASTIC_ALLOWED_NODES"] = ",".join(sorted(expanded))

    @staticmethod
    def _expand_home_channel_env_for_gateway() -> None:
        """Normalize MESHTASTIC_HOME_CHANNEL so cron delivery reaches the adapter.

        Hermes cron/notification delivery passes the value of
        MESHTASTIC_HOME_CHANNEL straight through as the chat id, and the
        adapter's send path requires the ``meshtastic:`` prefix (chat ids are
        ``meshtastic:!aabbccdd`` DMs or ``meshtastic:channel:0`` broadcasts).
        A bare node id or ``channel:N`` silently fails with "Invalid chat_id
        format" — warn and rewrite the env so the misconfiguration fixes
        itself. Mirrors _expand_allowlist_env_for_gateway.
        """
        raw = os.getenv("MESHTASTIC_HOME_CHANNEL", "").strip()
        if not raw or raw.startswith("meshtastic:"):
            return
        low = raw.lower()
        if low.startswith("channel:"):
            fixed = f"meshtastic:{raw}"
        elif low.startswith("!") or (len(low) == 8 and all(c in "0123456789abcdef" for c in low)):
            # Bare node id, with or without the leading '!' — normalize the same
            # way the allowlist expansion does.
            fixed = f"meshtastic:{MeshtasticAdapter._normalize_node_id(raw) or raw}"
        else:
            logger.warning(
                "MESHTASTIC_HOME_CHANNEL=%r is not a meshtastic chat id; expected "
                "'meshtastic:!aabbccdd' (node DM) or 'meshtastic:channel:0' (broadcast).",
                raw,
            )
            return
        os.environ["MESHTASTIC_HOME_CHANNEL"] = fixed
        logger.warning(
            "MESHTASTIC_HOME_CHANNEL=%r is missing the 'meshtastic:' prefix — using %r instead.",
            raw,
            fixed,
        )

    def _create_node_freshness(self) -> node_freshness.NodeFreshness:
        """Factory seam so tests/subclasses can substitute a bounded overlay."""
        return node_freshness.NodeFreshness()

    def _update_observed(
        self,
        node_id: str,
        rx_time: Any,
        snr: Any,
        rssi: Any,
        hop_count: int | None,
    ) -> None:
        # Test-compat delegate — receive logic lives in inbound.py
        # (InboundProcessor); keep until moved tests drop the dependency.
        self._node_freshness.update(node_id, rx_time, snr, rssi, hop_count)

    def get_observed_node(self, node_id: str) -> dict[str, Any]:
        return self._node_freshness.get(node_id)

    def _get_interface_node_id(self, interface: Any) -> str | None:
        """Test-compat delegate — body moved to inbound.interface_node_id."""
        return inbound.interface_node_id(interface, normalize_id=self._normalize_node_id)

    def _load_tools_module(self) -> ModuleType:
        """Load the companion tools module without colliding with Hermes' tools package."""
        import sys

        if "meshtastic_tools" in sys.modules:
            return sys.modules["meshtastic_tools"]
        if __package__:
            return importlib.import_module(f"{__package__}.mesh_tools")
        return importlib.import_module("mesh_tools")

    def _tools_set_adapter_fn(self) -> Callable[[object | None], None]:
        """Return the companion tools module's set_adapter function."""
        attr_name = "set_adapter"
        return cast(Callable[[object | None], None], getattr(self._load_tools_module(), attr_name))

    def _set_tools_adapter(self, adapter: object | None) -> None:
        """Update the active adapter reference in the companion tools module."""
        self._tools_set_adapter_fn()(adapter)

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        """Connect to the Meshtastic node(s) and start listening."""
        # is_reconnect is part of the base-class contract but ignored here: the
        # only outbound buffer is in-memory and persists across in-process
        # reconnects, so there is no server-side queue to preserve.
        del is_reconnect
        with self._lifecycle_lock:
            if self._running:
                logger.debug("Meshtastic adapter is already connected/connecting")
                return True
            if self._disconnecting:
                logger.warning("Cannot connect Meshtastic adapter while disconnect is in progress")
                return False
            self._running = True
            self._lifecycle_id += 1
            lifecycle_id = self._lifecycle_id
            self._link_down_since.clear()  # drop timestamps belong to the old lifecycle
            if self._transport_executor is None:
                self._transport_executor = _DaemonTransportExecutor(name="meshtastic-transport")
        self.loop = asyncio.get_running_loop()
        self._cross_loop_send_logged = False

        self._set_tools_adapter(self)
        self._subscribe_pubsub()

        # Pass the generation and queue explicitly so a task stranded on an old
        # loop cannot consume a replacement lifecycle's queue after restart.
        # Bounded so a packet flood cannot grow the receive path without limit.
        incoming_queue = asyncio.Queue(maxsize=self.INCOMING_QUEUE_MAXSIZE)
        self._incoming_queue = incoming_queue
        self._incoming_consumer_task = asyncio.create_task(
            self._consume_incoming_queue(lifecycle_id, incoming_queue)
        )

        # Determine targets off-loop: "auto" serial discovery blocks on USB
        # enumeration, which must not stall the platform loop.
        targets = await asyncio.to_thread(self._connection_targets)
        logger.info("Connecting to Meshtastic targets: %s", targets)

        # Start connection routine for each target
        self._reconnect_tasks.clear()
        for target in targets:
            self._reconnect_tasks[target] = asyncio.create_task(
                self._reconnect_loop(target, lifecycle_id)
            )

        # Start queue drain monitoring
        self._queue_drain_task = asyncio.create_task(self._drain_queue_loop(lifecycle_id))

        self._mark_connected()
        return True

    def _connection_targets(self) -> list[str]:
        """Resolve the connection target keys to open (TCP host wins over serial;
        ``auto`` serial discovers ports or falls back to ``mock_port``)."""
        return transport.connection_targets(self.tcp_host, self.tcp_port, self.serial_port)

    def _open_interface(self, target: str) -> Any:
        """Open the serial/TCP interface for a target (delegates to transport)."""
        return transport.open_interface(target)

    def _discover_serial_ports(self) -> list[str]:
        """Discover likely Meshtastic serial devices (delegates to transport)."""
        return transport.discover_serial_ports()

    def _apply_tcp_keepalive(self, iface: Any) -> None:
        """Arm OS-level TCP keepalive on a node socket, re-arming after self-heal.

        ``TCPInterface._reconnect()`` swaps in a brand-new socket on a failed
        read/write and socket options do not survive that, so this runs from the
        liveness poll too, tracking the socket it configured (by identity) to
        stay a no-op otherwise. Best-effort: failures log at debug and the
        library's 300s heartbeat remains the backstop. ``_keepalive_socket_id``
        tracks ONE socket (single-TCP is enforced), fine today.
        """
        sock = getattr(iface, "socket", None)
        if sock is None or not hasattr(sock, "setsockopt"):
            return
        if self._keepalive_socket_id == id(sock):
            return
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            if hasattr(socket, "TCP_KEEPIDLE"):  # Linux, and Windows on 3.13+
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, KEEPALIVE_IDLE_SECS)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, KEEPALIVE_INTERVAL_SECS)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, KEEPALIVE_FAIL_COUNT)
            elif hasattr(socket, "SIO_KEEPALIVE_VALS"):  # Windows: idle/interval only
                sock.ioctl(
                    socket.SIO_KEEPALIVE_VALS,
                    (1, KEEPALIVE_IDLE_SECS * 1000, KEEPALIVE_INTERVAL_SECS * 1000),
                )
            elif hasattr(socket, "TCP_KEEPALIVE"):  # macOS spells idle this way
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPALIVE, KEEPALIVE_IDLE_SECS)
        except OSError as e:
            logger.debug("Could not arm TCP keepalive on the node socket: %s", e)
            return
        self._keepalive_socket_id = id(sock)
        logger.info(
            "TCP keepalive armed on node socket (idle=%ds, interval=%ds, probes=%d)",
            KEEPALIVE_IDLE_SECS,
            KEEPALIVE_INTERVAL_SECS,
            KEEPALIVE_FAIL_COUNT,
        )

    def _note_link_drop(self, target: str) -> None:
        """Timestamp a drop so the recovery log can classify what happened."""
        self._link_down_since[target] = time.time()

    def _report_link_recovery(self, target: str) -> None:
        """Log how long *target* was gone and what that says about the node.

        A short outage means the node stayed up and only the socket died; a long
        one means the node itself was away. The ``SOCKET_RESET_MAX_OUTAGE_SECS``
        split is reconnect-latency dependent — heuristic by design.
        """
        dropped_at = self._link_down_since.pop(target, None)
        if dropped_at is None:
            return
        outage = time.time() - dropped_at
        kind = connection.classify_link_drop(outage)
        self._link_drop_counts[kind] += 1
        verdict = (
            "socket reset, node stayed up"
            if kind == connection.SOCKET_RESET
            else "node was unreachable (reboot, WiFi drop, or power loss)"
        )
        logger.info(
            "Meshtastic link to %s restored after %.1fs — %s "
            "(session totals: %d socket resets, %d node absences)",
            target,
            outage,
            verdict,
            self._link_drop_counts["socket_reset"],
            self._link_drop_counts["node_absent"],
        )

    def pause_link(self, minutes: float | None = None) -> dict[str, Any]:
        """Release the radio while leaving the rest of the gateway running.

        The node accepts a limited number of TCP clients, so working with it
        from a phone or the web UI means the gateway has to let go first —
        without dropping every platform and in-flight conversation. Pausing
        keeps the process, the queues and the other platforms up; only the
        interface is closed and the reconnect loop parked.
        """
        if minutes is not None and not math.isfinite(minutes):
            # NaN/Inf deadline never expires and crashes pause_state — reject it.
            raise ValueError("pause_link minutes must be a finite number")
        if minutes == 0:
            # A zero-minute pause is a no-op, not an indefinite one: 0 is falsy,
            # so the deadline branch below would otherwise set _pause_until=None
            # (an untimed pause that only an explicit resume_link() clears).
            return self.pause_state()
        with self._pause_lock:
            # Deadline first: the loop must never see paused=True with a stale one.
            self._pause_until = time.time() + minutes * 60 if minutes is not None else None
            self._paused = True
        logger.info(
            "Meshtastic link paused%s",
            f" for {minutes:g} minute(s)" if minutes else " until resumed",
        )
        return self.pause_state()

    def resume_link(self) -> dict[str, Any]:
        """Re-arm the reconnect loop; it reconnects on its own within ~1s."""
        with self._pause_lock:
            was_paused = self._paused
            self._paused, self._pause_until = False, None
        if was_paused:
            logger.info("Meshtastic link resumed")
        return self.pause_state()

    def pause_state(self) -> dict[str, Any]:
        """Current pause status, for tools to report instead of guessing."""
        with self._pause_lock:
            paused, pause_until = self._paused, self._pause_until
        return {
            "paused": paused,
            "resumes_at": connection.resumes_at_str(pause_until),
            "resumes_in_minutes": connection.resumes_in_minutes(pause_until, time.time()),
        }

    def _pause_expired(self) -> bool:
        """Auto-resume once a timed pause runs out.

        Polled from the reconnect loop rather than armed as a timer, so a timed
        pause cannot silently become "the mesh was down all night".
        """
        with self._pause_lock:
            paused, until = self._paused, self._pause_until
        if connection.pause_classify(paused, until, time.time()) == connection.PAUSE_EXPIRED:
            logger.info("Meshtastic pause expired — resuming link")
            # resume_link re-acquires _pause_lock, so release it before the call.
            self.resume_link()
            return True
        return False

    async def _reconnect_loop(self, target: str, lifecycle_id: int):
        """Exponential backoff reconnect loop for one connection target."""
        backoff = connection.INITIAL_BACKOFF
        while self._lifecycle_is_active(lifecycle_id):
            try:
                # A timed pause that ran out auto-resumes here; the step
                # decision below then takes the normal connect/poll branch.
                self._pause_expired()
                with self._pause_lock:
                    paused = self._paused
                with self._iface_lock:
                    has_iface = target in self._interfaces
                step = connection.reconnect_step(paused, has_iface)
                if step == connection.RELEASE:
                    # Paused: release the node's socket (via the serialized
                    # close) so another client can take its TCP slot.
                    if not await self._release_interface_if_paused(target, lifecycle_id):
                        break
                    await asyncio.sleep(connection.PAUSE_POLL_SECS)
                elif step == connection.WAIT:
                    # Paused without an interface: stay parked, don't reconnect.
                    await asyncio.sleep(connection.PAUSE_POLL_SECS)
                elif step == connection.CONNECT:
                    logger.info("Attempting to connect to Meshtastic target: %s...", target)
                    iface = await self._open_interface_for_lifecycle(target, lifecycle_id)
                    if iface is None:
                        # Open returned None (executor gone, stale lifecycle, or
                        # a pure failure). A concurrent timed-out open may still
                        # have registered the target — adopt it and poll rather
                        # than exiting permanently with a live, unmonitored iface.
                        with self._iface_lock:
                            iface = self._interfaces.get(target)
                        if iface is None:
                            break
                    backoff = connection.reset_backoff()  # Reset backoff on success
                    logger.info("Successfully connected to Meshtastic on %s", target)
                    self._apply_tcp_keepalive(iface)
                    self._report_link_recovery(target)
                    self._warn_missing_node_key(iface)
                else:
                    # Connected: poll until the link drops, then reconnect.
                    if await self._poll_interface_until_drop(target, lifecycle_id):
                        continue
            except transport.TransportBusyError:
                logger.warning("Meshtastic transport worker busy for %s; retrying", target)
                await asyncio.sleep(backoff)
                backoff = connection.next_backoff(backoff)
            except Exception as e:
                logger.error("Failed to connect to Meshtastic on %s: %s", target, e)
                active_lifecycle, dropped = self._pop_interface_for_lifecycle(target, lifecycle_id)
                if not active_lifecycle:
                    break
                if dropped is not None:
                    await self._close_interfaces([dropped])

                # Sleep with exponential backoff
                await asyncio.sleep(backoff)
                backoff = connection.next_backoff(backoff)

    async def _release_interface_if_paused(self, target: str, lifecycle_id: int) -> bool:
        """Drop ``target``'s interface while paused.

        Returns False when the lifecycle ended and the loop should exit;
        True otherwise (the caller sleeps one pause tick and re-checks).
        """
        active, dropped = self._pop_interface_for_lifecycle(target, lifecycle_id)
        if not active:
            return False
        if dropped is not None:
            logger.info("Releasing Meshtastic interface %s while paused", target)
            await self._close_interfaces([dropped])
        return True

    async def _open_interface_for_lifecycle(self, target: str, lifecycle_id: int) -> Any | None:
        """Submit one interface open to the transport worker and await it.

        Returns the live interface, or ``None`` when the executor is gone or a
        stale open was closed (lifecycle ended while the constructor ran) — in
        both cases the loop exits rather than backing off. Constructors can
        block, so the worker adopts the result only if this lifecycle still
        wants it; canceled/stale opens are closed before the worker returns.
        """
        with self._lifecycle_lock:
            executor = self._transport_executor
        if executor is None:
            return None
        try:
            open_cf = executor.submit(
                lambda t=target, lid=lifecycle_id: self._open_and_register_interface(t, lid)
            )
        except RuntimeError as exc:
            # Executor shutdown mid-teardown — not a connection failure, so no
            # backoff/retry: just exit the loop.
            if send_path.is_executor_shutdown_error(exc):
                return None
            raise
        open_timeout = self._open_timeout()
        try:
            # Bound the success-path open so a wedged constructor cannot pin
            # this await (and the reconnect loop) forever — expiry is a connect
            # failure that the loop backs off from.
            return await transport.await_concurrent_future(open_cf, open_timeout or None)
        except TimeoutError:
            logger.warning(
                "Meshtastic open for %s did not finish within %.1fs; backing off "
                "(constructor still runs on the daemon worker)",
                target,
                open_timeout,
            )
            raise
        except asyncio.CancelledError:
            # Constructor work cannot be canceled once running. Wait briefly
            # for stale-lifecycle cleanup; if the open is hung (or timeout is
            # 0), abandon the await so disconnect can finish. Daemon worker
            # still closes a late result via lifecycle_id.
            timeout = self._open_cancel_timeout()
            if timeout > 0:
                try:
                    await transport.await_concurrent_future(open_cf, timeout)
                except TimeoutError:
                    logger.warning(
                        "Meshtastic open for %s still running after %.1fs cancel "
                        "wait; disconnect continues (stale result will be closed)",
                        target,
                        timeout,
                    )
                except Exception:
                    # The constructor failed after we were cancelled. Preserve
                    # the CancelledError — that is the meaningful outcome for
                    # the caller; the worker already logged the open failure.
                    logger.debug(
                        "Cancelled Meshtastic open for %s also raised",
                        target,
                        exc_info=True,
                    )
            else:
                logger.warning(
                    "Meshtastic open for %s abandoned immediately on cancel "
                    "(MESHTASTIC_OPEN_CANCEL_TIMEOUT=0); stale result will be closed",
                    target,
                )
            raise

    def _warn_missing_node_key(self, iface: Any) -> None:
        """Log a security warning when the local node has no initialized key."""
        my_node = getattr(iface, "localNode", None)
        if my_node:
            # Try to read info dictionary
            nodes = getattr(iface, "nodes", {}) or {}
            my_id = self._get_interface_node_id(iface) or ""
            my_info = nodes.get(my_id, {})
            if not my_info.get("user", {}).get("publicKey"):
                logger.warning(
                    "!!! WARNING: Local node %s has no initialized public/private key. "
                    "DMs WILL FAIL. Please pair/connect the node to the official "
                    "Meshtastic mobile app at least once to complete encryption setup.",
                    my_id,
                )

    async def _poll_interface_until_drop(self, target: str, lifecycle_id: int) -> bool:
        """Poll a connected interface's liveness until it drops.

        Returns True when the link dropped (the outer loop reconnects with
        backoff); False when the poller should exit (lifecycle ended, executor
        gone, or the interface was replaced under us). A drop is timestamped
        here; the outer loop owns the serialized close (interfaces must not be
        closed on the loop thread).
        """
        while self._lifecycle_is_active(lifecycle_id):
            with self._iface_lock:
                if target not in self._interfaces:
                    return False
                iface = self._interfaces[target]
            with self._lifecycle_lock:
                executor = self._transport_executor
            if executor is None:
                return False
            # cast(Any): the dual-import binds ``transport`` to a union of two
            # module objects, so the nominal _DaemonTransportExecutor type
            # cannot cross this boundary; the value itself is unchanged.
            try:
                alive = await asyncio.wrap_future(
                    transport.submit_liveness_probe(
                        cast(Any, executor),
                        target,
                        iface,
                        interfaces=self._interfaces,
                        iface_lock=self._iface_lock,
                    )
                )
            except Exception as exc:
                # Executor shut down between grab and probe submit (concurrent
                # disconnect): exit the poller silently — not a "Failed to
                # connect" ERROR with re-run close logic.
                if send_path.is_executor_shutdown_error(exc):
                    return False
                raise
            outcome = connection.poll_outcome(alive)
            if outcome == connection.EXIT:
                return False
            if outcome == connection.DROP:
                logger.warning("Meshtastic target %s dropped connection!", target)
                self._note_link_drop(target)
                # The liveness probe confirmed the drop (the same poll that owns
                # interface teardown). A solicited reply can only return over the
                # connection the request went out on, so fail the waiters now
                # instead of sitting out the full timeout — but only here, never
                # on the library's transient connection.lost event, which a
                # self-healing TCP link outlives.
                self._abandon_response_waiters("connection lost")
                return True

            # The library swaps the socket out from under us when it
            # self-heals a failed read/write, so re-arm on the new one.
            self._apply_tcp_keepalive(iface)
            await asyncio.sleep(connection.LIVENESS_POLL_SECS)
        return False

    def _interface_is_alive(self, iface: Any) -> bool:
        """Test-compat delegate — body moved to transport.interface_is_alive."""
        return transport.interface_is_alive(iface)

    async def _drain_queue_loop(self, lifecycle_id: int):
        """Monitor and drain the outbound messages queue when connections are active."""
        while self._lifecycle_is_active(lifecycle_id):
            if self._has_interfaces() and self._outbound_queue:
                with self._queue_lock:
                    item = self._outbound_queue.pop(0)

                try:
                    logger.info("Draining queued message to %s", item["chat_id"])
                    # Shield the executor-backed transport call so disconnect
                    # can await its real result: requeue only when it definitely
                    # did not send, avoiding loss or a duplicate after teardown.
                    send_task = asyncio.create_task(
                        self._send_immediate(item["chat_id"], item["content"])
                    )
                    try:
                        res = await asyncio.shield(send_task)
                    except asyncio.CancelledError:
                        timeout = self._executor_shutdown_timeout()
                        done, _ = await asyncio.wait({send_task}, timeout=timeout)
                        if done:
                            try:
                                res = send_task.result()
                            except Exception:
                                with self._queue_lock:
                                    self._outbound_queue.insert(0, item)
                            else:
                                if not res.success:
                                    with self._queue_lock:
                                        self._outbound_queue.insert(0, item)
                        else:
                            # Delivery is indeterminate: do not requeue and risk
                            # a duplicate. The daemon worker may still complete.
                            logger.warning(
                                "Queued Meshtastic send still running after %.1fs during "
                                "disconnect; not requeueing (delivery indeterminate)",
                                timeout,
                            )
                        raise
                    if not res.success:
                        if self._requeue_or_drop(item, res.error):
                            await asyncio.sleep(5.0)
                        continue
                    # Never raises: a garbage MESHTASTIC_CHUNK_DELAY after the
                    # item was already delivered must not requeue a duplicate.
                    await asyncio.sleep(send_path.safe_chunk_pacing_delay())
                except Exception as e:
                    logger.error("Error draining queued message: %s", e, exc_info=True)
                    if self._requeue_or_drop(item, None):
                        await asyncio.sleep(5.0)
            else:
                await asyncio.sleep(1.0)

    def _requeue_or_drop(self, item: dict[str, Any], error: str | None) -> bool:
        """Requeue a retryable drained item, or drop it to keep the queue moving.

        Mirrors ``_send_chunk`` via ``send_path.drain_retry_decision``: only
        transient failures are re-queued; permanent failures are dropped after
        one attempt so they cannot block the items behind them. Returns True
        when the item was re-queued.
        """
        attempts = item.get("attempts", 0) + 1
        if send_path.drain_retry_decision(error, attempts, self.DRAIN_MAX_ATTEMPTS):
            item["attempts"] = attempts
            with self._queue_lock:
                self._outbound_queue.insert(0, item)
            logger.warning(
                "Queued Meshtastic message to %s retryable (attempt %d/%d): %s",
                item["chat_id"],
                attempts,
                self.DRAIN_MAX_ATTEMPTS,
                error or "send raised",
            )
            return True
        logger.error(
            "Dropping queued Meshtastic message to %s after %d attempts: %s",
            item["chat_id"],
            attempts,
            error or "send raised",
        )
        return False

    def _start_disconnect_task(self, completion: ConcurrentFuture) -> None:
        """Start teardown on the event loop that owns platform tasks."""
        current_loop = asyncio.get_running_loop()
        with self._lifecycle_lock:
            if completion.done():
                return
            owner_loop = self._disconnect_owner_loop
            task = self._disconnect_task
            if (
                owner_loop is not None
                and owner_loop is not current_loop
                and owner_loop.is_running()
            ):
                # task=None means a call_soon_threadsafe callback is reserved
                # but has not run yet. A live task likewise still owns teardown.
                if task is None or not task.done():
                    return
            needs_task = task is None or task.done() or not task.get_loop().is_running()
            if needs_task:
                # Check + assign under one cross-thread lock so concurrent
                # takeover callers cannot launch duplicate teardown tasks.
                if task is not None and not task.done():
                    self._cancel_task_threadsafe(task)
                self._disconnect_owner_loop = current_loop
                self._disconnect_task = current_loop.create_task(self._disconnect_impl(completion))

    def _reserve_disconnect_owner(self, loop: asyncio.AbstractEventLoop) -> None:
        """Claim the disconnect-owner loop under the lifecycle lock."""
        with self._lifecycle_lock:
            self._disconnect_owner_loop = loop

    async def disconnect(self) -> None:
        """Request platform-loop teardown and await its shared completion."""
        platform_loop: asyncio.AbstractEventLoop | None = None
        with self._lifecycle_lock:
            if self._disconnecting:
                completion = self._disconnect_future
                start_teardown = False
            else:
                completion = ConcurrentFuture()
                self._disconnect_future = completion
                self._disconnecting = True
                self._disconnect_done.clear()
                self._running = False
                self._lifecycle_id += 1
                start_teardown = True
                # Snapshot the platform loop under the lock alongside the
                # _running flip so teardown is dispatched to the loop that owns
                # the lifecycle tasks.
                consumer_task = self._incoming_consumer_task
                platform_loop = consumer_task.get_loop() if consumer_task is not None else self.loop

        if completion is None:
            return
        if start_teardown:
            current_loop = asyncio.get_running_loop()
            if platform_loop is current_loop:
                self._reserve_disconnect_owner(current_loop)
                self._start_disconnect_task(completion)
            elif platform_loop is not None and platform_loop.is_running():
                self._reserve_disconnect_owner(platform_loop)
                try:
                    platform_loop.call_soon_threadsafe(self._start_disconnect_task, completion)
                except RuntimeError:
                    self._reserve_disconnect_owner(current_loop)
                    self._start_disconnect_task(completion)
            else:
                # Platform loop already stopped: fallback cleanup can still
                # cancel (without awaiting) old-loop tasks and close transport.
                self._reserve_disconnect_owner(current_loop)
                self._start_disconnect_task(completion)
        else:
            logger.debug("Waiting for Meshtastic disconnect already in progress")

        # Polling does not tie completion to the caller loop and caller
        # cancellation cannot cancel the shared teardown. If the owner loop
        # stops after accepting the callback, take over cleanup here.
        try:
            while not completion.done():
                # Detect cancelled/done owner tasks even when their loop is still
                # running, plus tasks stranded on a stopped loop.
                self._start_disconnect_task(completion)
                await asyncio.sleep(0.05)
        except asyncio.CancelledError:
            # If this caller reserved a foreign-loop callback that never ran,
            # transfer the empty reservation locally before propagating caller
            # cancellation. The teardown task is independent of this waiter.
            current_loop = asyncio.get_running_loop()
            with self._lifecycle_lock:
                take_over = not completion.done() and self._disconnect_task is None
                if take_over:
                    self._disconnect_owner_loop = current_loop
            if take_over:
                self._start_disconnect_task(completion)
            raise
        completion.result()

    async def _disconnect_impl(self, completion: ConcurrentFuture) -> None:
        """Teardown implementation; always owned by the platform loop when live.

        Ownership-gate pattern: a follower caller can supersede this task at any
        time (``_start_disconnect_task``), so every stage boundary re-checks under
        one ``_lifecycle_lock`` hold that ``self._disconnecting`` is still set,
        ``self._disconnect_future is completion`` (same epoch), and
        ``self._disconnect_task is current_task`` (this task is the owner). Failing
        any gate means a newer owner exists — return ``superseded`` without mutating
        its state; the ``finally`` only settles ``completion`` when the same checks
        pass, so a stale task never advertises completion or wipes a successor.
        """
        failure: BaseException | None = None
        cancelled = False
        superseded = False
        current_task = asyncio.current_task()
        try:
            with self._lifecycle_lock:
                if not connection.teardown_owner_current(
                    self._disconnecting,
                    self._disconnect_future,
                    self._disconnect_task,
                    completion,
                    current_task,
                ):
                    superseded = True
                    return
                # Claim all lifecycle-owned state atomically while ownership is
                # valid. A stale task may later finish work on this snapshot,
                # but can never reach a newly connected lifecycle's state.
                self._set_tools_adapter(None)
                self._unsubscribe_pubsub()
                with self._iface_lock:
                    detached = list(self._interfaces.items())
                    self._interfaces.clear()
                if detached:
                    # Accumulate (do NOT clear on supersede): a follower task
                    # continuing this same teardown epoch must close what any
                    # superseded owner detached. The list is only cleared when
                    # an owner actually settles completion. Double-close cannot
                    # occur — _disconnect_close_started gates who runs close.
                    self._disconnect_interfaces.extend(detached)
                ports = list(self._disconnect_interfaces)
                self._fail_pending_acks(reason="DISCONNECTED")
                # Solicited waiters die with the link; abandon is idempotent.
                self._abandon_response_waiters("disconnect")
                lifecycle_tasks = connection.teardown_task_list(
                    self._reconnect_tasks, self._queue_drain_task, self._incoming_consumer_task
                )
                self._reconnect_tasks.clear()
                self._queue_drain_task = None
                self._incoming_consumer_task = None
                self._incoming_queue = None
            # Cancel on each task's owning loop. A takeover teardown may run on
            # a follower loop while the platform loop is still running; calling
            # Task.cancel() directly from another thread is not safe, so foreign-
            # loop tasks are marshalled via call_soon_threadsafe.
            await self._cancel_and_gather(lifecycle_tasks)
            with self._lifecycle_lock:
                if not connection.teardown_owner_current(
                    self._disconnecting,
                    self._disconnect_future,
                    self._disconnect_task,
                    completion,
                    current_task,
                ):
                    superseded = True
                    return
                message_tasks = list(self._message_tasks)
                self._message_tasks.clear()
            await self._cancel_and_gather(message_tasks)
            with self._lifecycle_lock:
                if not connection.teardown_owner_current(
                    self._disconnecting,
                    self._disconnect_future,
                    self._disconnect_task,
                    completion,
                    current_task,
                ):
                    superseded = True
                    return
                should_start_close = not self._disconnect_close_started
                if should_start_close:
                    self._disconnect_close_started = True
            if should_start_close:
                await self._close_interfaces([iface for _, iface in ports])
            with self._lifecycle_lock:
                if not connection.teardown_owner_current(
                    self._disconnecting,
                    self._disconnect_future,
                    self._disconnect_task,
                    completion,
                    current_task,
                ):
                    superseded = True
                    return
                executor = self._transport_executor
                self._transport_executor = None
            if executor is not None:
                # Bounded join on a daemon worker — safe to call from the loop
                # thread because shutdown(wait) only joins with a timeout.
                await self._shutdown_transport_executor(executor)
            logger.info("Disconnected Meshtastic Platform.")
        except asyncio.CancelledError:
            # Do not advertise completion. Cleanup is idempotent; a polling
            # caller will atomically start a takeover task on a live loop.
            cancelled = True
            raise
        except BaseException as exc:
            failure = exc
            logger.error("Error disconnecting Meshtastic platform: %s", exc, exc_info=True)
        finally:
            self._teardown_epilogue(completion, current_task, failure, cancelled, superseded)

    async def _cancel_and_gather(self, tasks: list[asyncio.Task]) -> None:
        """Cancel each task on its owning loop; await the local ones.

        Task.cancel() is only safe from the task's own loop thread, so
        foreign-loop tasks are marshalled via call_soon_threadsafe; the local
        subset is gathered (exceptions swallowed — teardown never fails a
        cancelled task's outcome).
        """
        current_loop = asyncio.get_running_loop()
        for task in tasks:
            self._cancel_task_threadsafe(task)
        local_tasks = connection.tasks_on_loop(tasks, current_loop)
        if local_tasks:
            await asyncio.gather(*local_tasks, return_exceptions=True)

    def _teardown_epilogue(
        self,
        completion: ConcurrentFuture,
        current_task: asyncio.Task | None,
        failure: BaseException | None,
        cancelled: bool,
        superseded: bool,
    ) -> None:
        """Settle teardown bookkeeping once ownership is released (finally block).

        Cancelled or superseded teardowns only clear the task/owner slots — a
        polling caller atomically starts a takeover task on a live loop. A
        settled owner additionally resolves the shared completion and resets
        the teardown flags; it never settles while the same three ownership
        checks fail, so a stale task cannot advertise completion or wipe a
        successor's bookkeeping.
        """
        if cancelled or superseded:
            with self._lifecycle_lock:
                if self._disconnect_task is current_task:
                    self._disconnect_task = None
                    self._disconnect_owner_loop = None
            return
        with self._lifecycle_lock:
            if connection.teardown_owner_current(
                self._disconnecting,
                self._disconnect_future,
                self._disconnect_task,
                completion,
                current_task,
            ):
                # Settle completion before releasing ownership so polling
                # callers cannot observe task=None with completion pending.
                try:
                    if failure is None:
                        completion.set_result(None)
                    else:
                        completion.set_exception(failure)
                except ConcurrentInvalidStateError:
                    pass
                self._disconnecting = False
                self._disconnect_done.set()
                self._disconnect_interfaces.clear()
                self._disconnect_close_started = False
                self._disconnect_task = None
                self._disconnect_owner_loop = None

    def _on_receive_pubsub(self, packet, interface=None):
        """Wrapper callback called by the pubsub framework (running on PySub background thread).

        Always marshals onto the *platform* loop (``self.loop``): that is the
        loop that owns ``_incoming_queue``. There is no running loop on the
        pubsub thread, and a send-loop queue would never be drained.
        """
        queue = self._incoming_queue
        if not self._running or queue is None:
            return
        if interface is not None:
            with self._iface_lock:
                if not any(active is interface for active in self._interfaces.values()):
                    logger.debug("Ignoring packet from detached Meshtastic interface")
                    return
        self._schedule_on_loop(
            self.loop,
            inbound.enqueue_incoming,
            queue,
            packet,
            interface,
            what="inbound packet enqueue",
        )

    def _on_connection_lost(self, interface=None):
        """Log Meshtastic-reported connection drops (pubsub background thread).

        The library fires ``meshtastic.connection.lost`` from ``_disconnected()``
        — e.g. on a reader-thread exit or a device reboot — cases the liveness
        poll can miss or lag. This is observability-only: the reconnect loop's
        ``_interface_is_alive`` poll still owns teardown AND waiter abandonment,
        so a transient blip ``TCPInterface._reconnect()`` self-heals without
        failing an in-flight solicited request whose reply may still return over
        the re-established link.
        """
        logger.warning("Meshtastic reported connection lost (interface=%s).", interface)

    def _abandon_response_waiters(self, reason: str) -> None:
        """Fail every in-flight solicited request when the link goes down."""
        self._solicited.abandon_all(reason)

    def _transport_executor_for_solicit(self) -> Any:
        """Read the lifecycle-scoped transport executor under the lifecycle lock."""
        with self._lifecycle_lock:
            return self._transport_executor

    def _on_connection_established(self, interface=None):
        """Log Meshtastic-reported connection establishment (pubsub background thread)."""
        logger.info("Meshtastic reported connection established (interface=%s).", interface)

    async def _consume_incoming_queue(self, lifecycle_id: int, incoming_queue: asyncio.Queue):
        """Consume incoming packets from the asyncio Queue."""
        while self._lifecycle_is_active(lifecycle_id):
            try:
                packet, interface = await incoming_queue.get()
                try:
                    # Validate generation under the lock, then dispatch unlocked.
                    # _on_receive → _maybe_record_pubsub_ack → _record_ack_response
                    # re-acquires _lifecycle_lock when upgrading IMPLICIT→real ACK;
                    # holding a non-reentrant Lock across _on_receive deadlocks the
                    # platform loop on that multi-hop path. Stale packets after
                    # disconnect are still filtered by lifecycle stamps inside ACK
                    # recording and by _running / generation checks elsewhere.
                    with self._lifecycle_lock:
                        if lifecycle_id != self._lifecycle_id or not self._running:
                            break
                    self._on_receive(packet, interface)
                finally:
                    incoming_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in incoming queue consumer: %s", e, exc_info=True)

    def _handle_message_done(self, task: asyncio.Task):
        """Callback to discard finished task and log exceptions."""
        self._message_tasks.discard(task)
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error("Error in handle_message task: %s", e, exc_info=True)

    @staticmethod
    def _channel_field(ch: Any, key: str) -> Any:
        """Read a channel field from a dict (mock) or a protobuf Channel (hardware).

        Delegates to ``inbound.channel_field``; kept for callers that still
        reach the adapter (``_send_immediate``, tests).
        """
        return inbound.channel_field(ch, key)

    def _on_receive(self, packet: dict, interface: Any = None):
        """Processes incoming packet in the main loop thread."""
        try:
            # Meshtastic response callbacks are one-shot. A relay can consume
            # onAckNak with an implicit ACK before the destination's real ACK,
            # so also feed matching routing packets from pubsub into existing
            # outbound ACK records. This precedes conversation authorization:
            # the request id must already be one of ours.
            self._maybe_record_pubsub_ack(packet)

            my_node_id = self._get_interface_node_id(interface) if interface else None

            # Receive-stage pipeline: packet normalization, freshness overlay,
            # self-echo filter, signal/telemetry/position routing and the authz
            # pre-check all run in inbound.InboundProcessor — synchronous on
            # this loop, with SQLite writes delegated via its writer callbacks
            # to _run_db_write.
            result = self._inbound.process(packet, my_node_id=my_node_id)
            sender = result.sender
            if sender is None:
                return

            # Resolve any solicited-request waiter for this node (before the auth
            # gate — a reply is protocol data addressed to us). rxTime lets the
            # tracker reject a packet that predates the request (a pre-armed
            # periodic broadcast, not a reply).
            rx_time = (
                packet.get("rxTime")
                if isinstance(packet, dict)
                else getattr(packet, "rxTime", None)
            )
            self._maybe_resolve_solicited(sender, result.decoded, rx_time=rx_time)

            if result.dropped:
                return

            if not result.authorized:
                self._warn_unauthorized_node(sender)
                return

            text = result.text
            if text is None:  # unreachable: the pipeline drops packets without text
                return

            # RF path of this inbound text, logged next to the message itself so
            # the forward route (mesh -> us) can be correlated with the ACK path
            # of our reply without cross-referencing SQLite. hops=0 means direct;
            # >0 means it came via that many relays.
            logger.info(
                "Meshtastic inbound text: from=%s hops=%s snr=%s rssi=%s bytes=%d text=%r",
                sender,
                result.hop_count,
                result.snr,
                result.rssi,
                len(text.encode("utf-8")),
                text[:80],
            )

            # By default the agent only answers direct messages — never a shared
            # channel/broadcast (avoids spamming a public channel's airtime).
            if result.is_broadcast and not self.allow_channels:
                logger.info(
                    "Ignoring channel/broadcast message from %s "
                    "(set MESHTASTIC_ALLOW_CHANNELS=true to answer channels)",
                    sender,
                )
                return

            if result.is_broadcast:
                # Scoped channel group chat session
                channel_name = inbound.resolve_channel_name(interface, result.channel_index)
                chat_id = f"meshtastic:channel:{channel_name}"
                chat_type = "group"
            else:
                # Private direct message session
                chat_id = f"meshtastic:{sender}"
                chat_type = "dm"

            # Fetch sender display names
            sender_name = inbound.resolve_sender_name(interface, sender)

            # Build packet context for the agent. Keep this compact but include
            # the LoRa metadata that matters for decisions/debugging.
            packet_context = inbound.build_packet_context(
                packet,
                sender=sender,
                sender_name=sender_name,
                to_id=result.to_id,
                chat_id=chat_id,
                chat_type=chat_type,
                channel_index=result.channel_index,
                snr=result.snr,
                rssi=result.rssi,
                hop_count=result.hop_count,
                hop_limit=result.hop_limit,
                hop_start=result.hop_start,
            )

            # Build Hermes MessageEvent
            source = self.build_source(
                chat_id=chat_id,
                user_id=sender,
                user_name=sender_name,
                chat_type=chat_type,
            )

            # Prefer the radio's receive time so session history reflects when
            # the packet actually arrived over the air, not when the loop
            # drained it (packets can sit in the incoming queue across
            # reconnects). A skewed or garbage rxTime must never drop the
            # message — fall back to now().
            event_ts = inbound.event_timestamp(packet)

            # If the phone app sent this as a reply, surface the replied-to
            # packet id so the agent/gateway has reply context.
            reply_id = result.decoded.get("replyId")

            event = MessageEvent(
                text=text,
                message_type=MessageType.TEXT,
                source=source,
                raw_message=packet,
                message_id=inbound.resolve_packet_id(packet),
                channel_context=packet_context,
                timestamp=event_ts,
                reply_to_message_id=str(reply_id) if reply_id is not None else None,
            )

            # Cap in-flight gateway tasks so an authorized-node text flood cannot
            # grow _message_tasks without bound (drop under extreme pressure).
            if len(self._message_tasks) >= self.MESSAGE_TASK_LIMIT:
                logger.warning(
                    "Dropping inbound Meshtastic message from %s: too many in-flight gateway tasks",
                    sender,
                )
                return

            # Bridge to Hermes Gateway
            task = asyncio.create_task(self.handle_message(event))
            self._message_tasks.add(task)
            task.add_done_callback(self._handle_message_done)

        except Exception as e:
            logger.error("Error handling inbound Meshtastic packet: %s", e, exc_info=True)

    # ------------------------------------------------------------------
    # Solicited requests (agent actively asks a node for data)
    #
    # Unlike the read-only tools that serve already-heard data, these put a
    # packet on the shared LoRa channel. Each is addressed to ONE node and is
    # never retried — a silent node just reports "no response". Waiters use the
    # same ConcurrentFuture model as ACK waiters; the transmit goes through the
    # lifecycle transport executor so it can't race close.
    # ------------------------------------------------------------------

    def _maybe_resolve_solicited(self, from_id: str, decoded: dict, *, rx_time: Any = None) -> None:
        """Feed a telemetry/position/traceroute packet to any matching waiter."""
        self._solicited.maybe_resolve(from_id, decoded, rx_time=rx_time)

    def _post_request(
        self,
        iface: Any,
        dest: str,
        payload: Any,
        portnum: Any,
        hop_limit: int | None = None,
    ) -> None:
        """Transmit a ``want_response`` packet WITHOUT the library's blocking wait.

        ``sendPosition`` / ``sendTelemetry`` / ``sendTraceRoute`` each call their
        own ``waitForX()`` helper when ``wantResponse=True``, which busy-waits on
        the interface ``Timeout`` — 300s for TCP — inside our transport-executor
        thread and raises on expiry. That stalled the whole tool call for five
        minutes on a silent/unreachable node and surfaced as "failed to send",
        while ``solicit``'s own timeout never applied. We already resolve replies
        on the pubsub receive path, so post via ``sendData`` (serialize + send
        only, ``onResponse=None``) and let ``solicit``'s timeout be the single
        authority on the wait.
        """
        iface.sendData(
            payload,
            destinationId=dest,
            portNum=portnum,
            wantResponse=True,
            onResponse=None,
            hopLimit=hop_limit,
        )

    @staticmethod
    def _telemetry_request(iface: Any) -> Any:
        """Build the telemetry request the stock client sends — our own metrics.

        Firmware answers any want_response telemetry packet, but the official
        client fills the request with its OWN device metrics so the peer hears
        our battery state too; mirroring that keeps us a well-behaved citizen
        rather than a bare poller. An empty node DB just sends an empty one.
        """
        request = telemetry_pb2.Telemetry()
        try:
            metrics = (iface.getMyNodeInfo() or {}).get("deviceMetrics") or {}
        except Exception:
            metrics = {}
        for field, key in (
            ("battery_level", "batteryLevel"),
            ("voltage", "voltage"),
            ("channel_utilization", "channelUtilization"),
            ("air_util_tx", "airUtilTx"),
            ("uptime_seconds", "uptimeSeconds"),
        ):
            value = metrics.get(key)
            if value is not None:
                setattr(request.device_metrics, field, value)
        return request

    async def request_telemetry(self, node_id: str, timeout: float = 45.0) -> dict[str, Any]:
        """Ask a node for fresh device metrics (battery, voltage, uptime)."""
        dest = self._normalize_node_id(node_id) or node_id
        return await self._solicited.solicit(
            "telemetry",
            node_id,
            lambda iface: self._post_request(
                iface, dest, self._telemetry_request(iface), portnums_pb2.PortNum.TELEMETRY_APP
            ),
            timeout,
        )

    async def request_position(self, node_id: str, timeout: float = 45.0) -> dict[str, Any]:
        """Ask a node for its current position."""
        dest = self._normalize_node_id(node_id) or node_id
        return await self._solicited.solicit(
            "position",
            node_id,
            # Empty Position — what the stock client sends when asking (not
            # reporting); every field is optional.
            lambda iface: self._post_request(
                iface, dest, mesh_pb2.Position(), portnums_pb2.PortNum.POSITION_APP
            ),
            timeout,
        )

    async def request_traceroute(
        self, node_id: str, hop_limit: int = 5, timeout: float = 60.0
    ) -> dict[str, Any]:
        """Discover the actual route to a node, with per-hop SNR."""
        dest = self._normalize_node_id(node_id) or node_id
        return await self._solicited.solicit(
            "traceroute",
            node_id,
            # Empty RouteDiscovery — each relay appends itself en route; the
            # reply carries the assembled path.
            lambda iface: self._post_request(
                iface,
                dest,
                mesh_pb2.RouteDiscovery(),
                portnums_pb2.PortNum.TRACEROUTE_APP,
                hop_limit=hop_limit,
            ),
            timeout,
        )

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: str | None = None,
        metadata: dict[str, Any] | None = None,
        allow_queueing: bool = True,
    ) -> SendResult:
        """Send a message, queueing if not connected; oversized payloads are chunked."""
        # Gateway tool-progress path bypasses format_tool_event and builds long
        # verb+preview lines; compact those to a short emoji blurb before chunking.
        # Only rewrite single-line chrome that starts with a non-ASCII symbol
        # (emoji). ASCII punctuation (markdown bullets "-", "*", quotes, parens)
        # is never compacted — those are plausible single-line agent replies, and
        # _compact_tool_progress_line's " for " / ": " splits would mangle them.
        if content and "\n" not in content and content.strip():
            lead = content.strip()[0]
            if not lead.isascii():
                compacted = self._compact_tool_progress_line(content)
                if compacted != content and len(compacted) < len(content):
                    content = compacted
        # reply_to is a prior packet id; only valid integer ids become threaded replies.
        reply_id = self._parse_reply_id(reply_to)
        wait_for_ack, ack_timeout = self._ack_wait_config(metadata)
        retries = self._send_retries(metadata)

        # Retry applies to direct messages only: broadcasts have no per-recipient
        # ACK, so re-sending them would flood the shared channel.
        dest = send_path.dest_from_chat_id(chat_id)
        is_dm = send_path.is_node_dest(dest)

        # Retrying is only meaningful when we can observe delivery, so enabling
        # retries for a DM implies waiting for its ACK.
        wait_for_ack, ack_timeout = send_path.retry_implies_ack_wait(
            retries, is_dm, wait_for_ack, ack_timeout
        )

        max_attempts = send_path.max_send_attempts(retries, wait_for_ack, is_dm)
        retry_backoff = self._retry_backoff()

        chunks, chunk_error = send_path.chunk_send_result(content, self._chunk_message)
        if chunk_error is not None:
            return SendResult(success=False, error=chunk_error)

        logger.info(
            "Sending message to %s. Splitting into %d chunks (bytes=%d).",
            chat_id,
            len(chunks),
            len((content or "").encode("utf-8")),
        )

        last_msg_id = None
        sent_ids = []
        raw_chunks = []
        for idx, chunk in enumerate(chunks):
            # Multi-packet LoRa delivery needs real pacing; too-fast writes are
            # accepted by the local serial API but get dropped/overwritten on air.
            if idx > 0:
                delay = send_path.chunk_pacing_delay()
                logger.info(
                    "Waiting %.1fs before Meshtastic chunk %d/%d", delay, idx + 1, len(chunks)
                )
                await asyncio.sleep(delay)

            # Deliver this chunk, retrying transient failures up to max_attempts.
            attempt = 0
            while True:
                attempt += 1
                res = await self._send_chunk(
                    chat_id,
                    chunk,
                    allow_queueing=allow_queueing,
                    wait_for_ack=wait_for_ack,
                    ack_timeout=ack_timeout,
                    reply_id=reply_id,
                )
                if not send_path.should_retry_chunk(
                    res.success, attempt, max_attempts, self._is_retriable_failure(res)
                ):
                    break
                logger.warning(
                    "Meshtastic chunk %d/%d not delivered (attempt %d/%d): %s — retrying in %.1fs",
                    idx + 1,
                    len(chunks),
                    attempt,
                    max_attempts,
                    res.error,
                    retry_backoff,
                )
                await asyncio.sleep(retry_backoff)

            if res.raw_response is not None:
                res.raw_response["attempts"] = attempt
                raw_chunks.append(res.raw_response)
            if not res.success:
                logger.error(
                    "Meshtastic chunk %d/%d failed after %d attempt(s): %s",
                    idx + 1,
                    len(chunks),
                    attempt,
                    res.error,
                )
                return SendResult(
                    success=False,
                    message_id=last_msg_id,
                    error=f"message only partially delivered: chunk {idx + 1}/{len(chunks)} failed after {attempt} attempt(s): {res.error}",
                    raw_response={"chunks": raw_chunks, "ack_waited": wait_for_ack},
                    continuation_message_ids=tuple(sent_ids[:-1]) if len(sent_ids) > 1 else (),
                    retryable=self._is_retriable_failure(res),
                )
            if attempt > 1:
                logger.info(
                    "Meshtastic chunk %d/%d delivered on attempt %d/%d",
                    idx + 1,
                    len(chunks),
                    attempt,
                    max_attempts,
                )
            if res.message_id:
                sent_ids.append(res.message_id)
                last_msg_id = res.message_id

        return SendResult(
            success=True,
            message_id=last_msg_id,
            raw_response={"chunks": raw_chunks, "ack_waited": wait_for_ack},
            continuation_message_ids=tuple(sent_ids[:-1]) if len(sent_ids) > 1 else (),
        )

    def _chunk_message(self, content: str) -> list[str]:
        return chunking.chunk_message(content)

    def _extract_packet_id(self, pkt: Any) -> str | None:
        """Return a Meshtastic packet ID from object or dict packet shapes."""
        pkt_id = getattr(pkt, "id", None)
        if pkt_id is None and isinstance(pkt, dict):
            pkt_id = pkt.get("id")
        return str(pkt_id) if pkt_id is not None else None

    @staticmethod
    def _parse_reply_id(reply_to: str | None) -> int | None:
        """Coerce a Hermes reply_to (prior packet id string) to a Meshtastic int replyId.

        Returns None for absent/non-integer ids (e.g. synthetic "queued" markers),
        so sendText is only threaded onto a genuine prior packet.
        """
        if not reply_to:
            return None
        try:
            return int(reply_to)
        except (TypeError, ValueError):
            return None

    def _queue_outbound_chunk(self, chat_id: str, chunk: str) -> SendResult:
        """Enqueue a chunk while disconnected (bounded, oldest-first eviction)."""
        with self._queue_lock:
            if len(self._outbound_queue) >= self.OUTBOUND_QUEUE_MAXSIZE:
                self._outbound_queue.pop(0)
            self._outbound_queue.append(
                {"chat_id": chat_id, "content": chunk, "timestamp": time.time()}
            )
        logger.info("Outbound connection down. Message successfully queued.")
        return SendResult(success=True, message_id="queued")

    def _send_text_serialized(
        self,
        *,
        lifecycle_id: int,
        dest: str,
        content: str,
        parts: list[str],
        reply_id: int | None,
        ack_callback: Callable[..., Any],
    ) -> tuple[str | None, Any, str]:
        """Select an interface and call sendText on the single transport worker.

        Returns ``(error, packet, dest)``. ``error`` is a short machine token:
        ``no_iface``, ``no_pubkey``, or None on success. The worker serializes
        concurrent agent-session sends and
        platform reconnect/close against Meshtastic's unsynchronized packet-id /
        response-handler / TX-queue state. DM node / channel resolution and the
        pubkey readiness decision are delegated to send_path.
        """
        with self._iface_lock:
            if lifecycle_id != self._lifecycle_id or not self._running:
                return "no_iface", None, dest
            ifaces = list(self._interfaces.values())
            if not ifaces:
                return "no_iface", None, dest

        if send_path.is_node_dest(dest):
            iface, dest, dm_ready = send_path.dm_send_target(dest, ifaces)
            if not dm_ready:
                return "no_pubkey", None, dest
            pkt = iface.sendText(
                text=content,
                destinationId=dest,
                wantAck=True,
                onResponse=ack_callback,
                replyId=reply_id,
            )
            return None, pkt, dest

        channel_index, iface = send_path.channel_send_target(parts, ifaces, self._channel_field)
        if iface is None:  # numeric spec no interface exposes
            return "no_channel", None, dest
        pkt = iface.sendText(
            text=content,
            channelIndex=channel_index,
            wantAck=True,
            onResponse=ack_callback,
            replyId=reply_id,
        )
        return None, pkt, dest

    async def _send_chunk(
        self,
        chat_id: str,
        chunk: str,
        allow_queueing: bool = True,
        *,
        wait_for_ack: bool = False,
        ack_timeout: float = 0.0,
        reply_id: int | None = None,
    ) -> SendResult:
        """Helper to send a single wrapped chunk, queueing it on failure/disconnect."""
        # Fast path under _iface_lock (map presence only); the worker re-checks,
        # so a mid-send disconnect surfaces no_iface for queueing.
        if not self._has_interfaces():
            if wait_for_ack:
                # ack_state treats this as retriable (queueing is ACK-unsafe).
                return SendResult(
                    success=False,
                    error=f"{send_path.NO_INTERFACES_ERROR}; cannot wait for ACK",
                )
            if not allow_queueing:
                return SendResult(
                    success=False,
                    error=f"{send_path.NO_INTERFACES_ERROR} and queueing disabled",
                )
            return self._queue_outbound_chunk(chat_id, chunk)

        res = await self._send_immediate(
            chat_id,
            chunk,
            wait_for_ack=wait_for_ack,
            ack_timeout=ack_timeout,
            reply_id=reply_id,
        )
        # Race: interface dropped after the fast-path check but before the
        # locked send — nothing went out, so re-queue is safe (no gap risk).
        # Only the no-interface token qualifies: a generic "Meshtastic send
        # failed" from a mid-sequence sendText raise must NOT be requeued here,
        # because earlier chunks in this send() may already be on the mesh and
        # requeueing a lone later chunk would deliver them out of order. That
        # failure is instead surfaced to send(), which aborts the sequence with
        # a "partially delivered" error (and retries within the DM retry budget
        # via TRANSIENT_TRANSPORT_ERRORS; broadcasts abort — no ACK to observe).
        if (
            not res.success
            and res.error == send_path.NO_INTERFACES_ERROR
            and not wait_for_ack
            and allow_queueing
        ):
            return self._queue_outbound_chunk(chat_id, chunk)
        return res

    def _drop_send_token(self, send_token: object) -> None:
        """Pop a send generation's provisional ACK staging under the ack lock."""
        with self._ack_lock:
            self._ack_inflight_tokens.pop(send_token, None)
            self._early_ack_packets.pop(send_token, None)

    async def _send_immediate(
        self,
        chat_id: str,
        content: str,
        *,
        wait_for_ack: bool = False,
        ack_timeout: float = 0.0,
        reply_id: int | None = None,
    ) -> SendResult:
        """Dispatch one text chunk immediately to the interface.

        Orchestrates the send: transport submission on the daemon worker,
        AckTracker bookkeeping and the optional ACK wait. The decisions —
        transport-error mapping, ACK-outcome classification, raw_response /
        ack-record shapes — are delegated to send_path.
        """
        send_token = object()
        try:
            parts = chat_id.split(":", 2)
            if len(parts) < 2:
                return SendResult(success=False, error="Invalid chat_id format")

            dest = send_path.normalize_dm_dest(parts[1], self._normalize_node_id)

            with self._lifecycle_lock:
                executor = self._transport_executor
                lifecycle_id = self._lifecycle_id
            if executor is None:
                return SendResult(success=False, error=send_path.NO_INTERFACES_ERROR)
            with self._ack_lock:
                self._ack_inflight_tokens[send_token] = lifecycle_id
            ack_callback = self._make_ack_callback_for_send(dest, content, send_token, lifecycle_id)
            try:
                err, pkt, dest = await asyncio.wrap_future(
                    executor.submit(
                        lambda: self._send_text_serialized(
                            lifecycle_id=lifecycle_id,
                            dest=dest,
                            content=content,
                            parts=parts,
                            reply_id=reply_id,
                            ack_callback=ack_callback,
                        )
                    )
                )
            except RuntimeError as exc:
                if send_path.is_executor_shutdown_error(exc) or isinstance(
                    exc, transport.TransportBusyError
                ):
                    # Shutdown, or the worker's queue is full (wedged blocking
                    # call): surface the queueing trigger so _send_chunk re-queues
                    # instead of buffering in the executor without limit.
                    self._drop_send_token(send_token)
                    return SendResult(success=False, error=send_path.NO_INTERFACES_ERROR)
                raise
            with self._lifecycle_lock:
                stale_lifecycle = lifecycle_id != self._lifecycle_id or not self._running
            # Inspect definitive pre-send failures before lifecycle turnover.
            # In particular, a stale-generation worker returns no_iface before
            # sendText, which lets _send_chunk safely queue a non-ACK message.
            pre_send_error = send_path.map_transport_error(err, dest)
            if pre_send_error is not None:
                self._drop_send_token(send_token)
                return SendResult(success=False, error=pre_send_error)
            if stale_lifecycle:
                self._drop_send_token(send_token)
                pkt_id = self._extract_packet_id(pkt)
                # Note: deliberately NOT stored in _pending_acks/_ack_responses.
                # A stale-lifecycle send must not pollute the new lifecycle's ACK
                # bookkeeping (an old worker returning after reconnect cannot
                # enter new ACK state). The outcome is surfaced only via this
                # SendResult's raw_response.
                ack_record = send_path.disconnect_ack_record(dest, content)
                return SendResult(
                    success=False,
                    message_id=pkt_id,
                    error=send_path.disconnect_error(wait_for_ack, pkt_id),
                    raw_response=send_path.stale_send_raw_response(
                        pkt_id, dest, wait_for_ack, ack_timeout, ack_record
                    ),
                )
            pkt_id = self._extract_packet_id(pkt)
            ack_future = self._track_pending_ack(
                pkt_id,
                dest,
                content,
                create_future=wait_for_ack,
                send_token=send_token,
            )
            with self._ack_lock:
                self._ack_inflight_tokens.pop(send_token, None)
                early_ack = self._early_ack_packets.pop(send_token, None)
            if early_ack is not None:
                early_packet, early_dest, early_content, early_lifecycle = early_ack
                self._record_ack_response(
                    early_packet,
                    early_dest,
                    early_content,
                    send_token=send_token,
                    lifecycle_id=early_lifecycle,
                )
            logger.info(
                "Meshtastic chunk queued: dest=%s packet_id=%s bytes=%d text=%r",
                dest,
                pkt_id,
                len(content.encode("utf-8")),
                content[:80],
            )
            raw_response = send_path.outbound_raw_response(
                pkt_id,
                dest,
                wait_for_ack,
                ack_timeout,
                self.get_ack_status(pkt_id) if pkt_id else None,
            )

            if wait_for_ack:
                waitable = send_path.waitable_ack_wait(pkt_id, ack_future)
                if waitable is None:
                    return SendResult(
                        success=False,
                        message_id=pkt_id,
                        error="Cannot wait for ACK without a packet id",
                        raw_response=raw_response,
                    )
                pkt_id, waitable_future = waitable
                ack_record = await self._wait_for_ack(pkt_id, waitable_future, ack_timeout)
                raw_response["ack"] = ack_record
                success, error = send_path.classify_ack_outcome(ack_record, pkt_id)
                return SendResult(
                    success=success, message_id=pkt_id, error=error, raw_response=raw_response
                )

            return SendResult(success=err is None, message_id=pkt_id, raw_response=raw_response)

        except Exception as e:
            # Map to a stable token: raw internal exception text (executor
            # messages, library stack fragments) must not leak into the
            # user-facing SendResult.error. Log the real cause separately.
            #
            # Design note: an unexpected exception here is a permanent drop for
            # the *live* send. _send_chunk requeues only the no-interface token
            # (nothing went out → safe); this generic token is surfaced to
            # send(), which aborts a multi-chunk sequence as "partially
            # delivered" (requeueing a lone later chunk would gap the order).
            # It is still retriable within the DM retry budget — it is a member
            # of ack_state.TRANSIENT_TRANSPORT_ERRORS, so is_retriable_failure
            # returns True and send()'s chunk-retry loop re-sends when a budget
            # is configured (DMs with MESHTASTIC_SEND_RETRIES). Broadcasts have
            # no ACK to observe and no retry budget, so they abort — by design.
            logger.error("Failed to deliver message immediately: %s", e, exc_info=True)
            return SendResult(success=False, error="Meshtastic send failed")
        finally:
            self._drop_send_token(send_token)

    async def edit_message(
        self,
        chat_id: str,
        message_id: str,
        content: str,
        *,
        finalize: bool = False,
        metadata: dict[str, Any] | None = None,
        **kwargs,
    ) -> SendResult:
        """Meshtastic has no edit primitive — pretend success, do not re-send.

        Hermes' progress loop treats a *custom* ``edit_message`` as "platform
        can edit", then on edit failure falls back to a **new permanent send
        per tool step**. That flooded LoRa with full progress lines.

        Returning success with the same ``message_id`` (no radio traffic)
        keeps the progress bubble "editable" so later steps are silent.
        Only the first short blurb (see ``_compact_tool_progress_line``) hits
        the mesh; the final answer still goes through ``send()``.
        """
        del chat_id, content, finalize, metadata, kwargs
        return SendResult(success=True, message_id=message_id or "mesh-progress")

    async def get_chat_info(self, chat_id: str) -> dict[str, Any]:
        """Fetch chat details."""
        parts = chat_id.split(":", 2)
        dest = parts[1] if len(parts) > 1 else ""

        if dest.startswith("!"):
            # DM
            name = dest
            ifaces = self.get_interfaces()
            for iface in ifaces:
                if hasattr(iface, "nodes") and dest in iface.nodes:
                    user = iface.nodes[dest].get("user", {})
                    name = user.get("longName") or user.get("shortName") or dest
                    break
            return {"name": name, "type": "dm"}
        else:
            # Channel
            channel_name = parts[2] if len(parts) > 2 else "0"
            return {"name": f"LoRa Channel {channel_name}", "type": "group"}


def _env_enablement() -> dict | None:
    """Helper to register and seed config extra from environment."""
    port = os.getenv("MESHTASTIC_SERIAL_PORT")
    tcp_host = os.getenv("MESHTASTIC_TCP_HOST")
    # Enable the platform when either transport is configured.
    if not port and not tcp_host:
        return None

    return {
        "serial_port": port,
        # ``or`` (not the getenv default) so a blank ``VAR=`` in .env still
        # falls back to the default instead of raising on ``int("")``.
        "baud_rate": int(os.getenv("MESHTASTIC_BAUD_RATE") or 115200),
        "tcp_host": tcp_host or "",
        "tcp_port": int(os.getenv("MESHTASTIC_TCP_PORT") or DEFAULT_TCP_PORT),
        "allowed_nodes": os.getenv("MESHTASTIC_ALLOWED_NODES")
        or os.getenv("MESHTASTIC_ALLOWED_USERS", ""),
        "allow_all_users": os.getenv("MESHTASTIC_ALLOW_ALL_USERS", "").lower()
        in ("1", "true", "yes"),
        "home_channel": os.getenv("MESHTASTIC_HOME_CHANNEL", ""),
    }


async def _standalone_send(
    pconfig,
    chat_id: str,
    message: str,
    *,
    thread_id: str | None = None,
    media_files: list[str] | None = None,
    force_document: bool = False,
) -> dict[str, Any]:
    """Standalone cron ephemeral delivery sender support."""
    try:
        # Create an instance of MeshtasticAdapter
        adapter = MeshtasticAdapter(pconfig)

        success = False
        error: str | None = None
        try:
            # Connect to establish the interface(s). connect() can raise after
            # flipping _running and spawning the transport-executor thread /
            # consumer+reconnect tasks, so disconnect() must run even on that
            # path — otherwise a repeatedly-invoked cron sender leaks threads
            # and tasks per failed run. disconnect() is idempotent against a
            # partially-connected adapter.
            await adapter.connect()

            # Wait for the reconnect task to open an interface. connect() returns
            # before the daemon transport worker finishes opening (SerialInterface/
            # TCPInterface constructors block until node info arrives, seconds), so
            # poll up to _STANDALONE_OPEN_TIMEOUT_SECS instead of racing the worker
            # with a fixed 2s budget — cron delivery used to fail intermittently.
            deadline = time.monotonic() + _STANDALONE_OPEN_TIMEOUT_SECS
            while not adapter.get_interfaces():
                if time.monotonic() >= deadline:
                    break
                await asyncio.sleep(0.1)

            res = await adapter.send(chat_id=chat_id, content=message, allow_queueing=False)
            success = res.success
            error = res.error
        finally:
            await adapter.disconnect()

        if success:
            return {"success": True}
        else:
            return {"error": error or "Failed to send message"}
    except Exception as e:
        logger.error("Standalone send failure: %s", e)
        return {"error": str(e)}


def register(ctx):
    """Entry point: called by the Hermes plugin loader."""
    ctx.register_platform(
        name="meshtastic",
        label="Meshtastic",
        adapter_factory=lambda cfg: MeshtasticAdapter(cfg),
        check_fn=lambda: True,  # Always load: connect() fails loudly with
        # install instructions when the meshtastic lib is missing, instead of
        # silently falling back to the mock interface.
        # No strictly-required env var: the adapter connects over serial (auto
        # discovery) OR TCP (MESHTASTIC_TCP_HOST). required_env only drives setup
        # UI display, and listing one transport's var would mislabel the other as
        # "not configured".
        required_env=[],
        env_enablement_fn=_env_enablement,
        # Declare the allowlist env vars so the gateway's own _is_user_authorized
        # layer integrates with them (defense-in-depth + setup-UI visibility).
        # The legacy MESHTASTIC_ALLOWED_USERS alias is still read adapter-locally.
        allowed_users_env="MESHTASTIC_ALLOWED_NODES",
        allow_all_env="MESHTASTIC_ALLOW_ALL_USERS",
        cron_deliver_env_var="MESHTASTIC_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        max_message_length=233,
        emoji="📡",
        pii_safe=True,
        platform_hint=(
            "You are chatting with the user over the Meshtastic LoRa mesh network. "
            "Only the message TRANSPORT is constrained: replies are split into ~170-byte "
            "LoRa-safe chunks, so keep answers concise and avoid filler. Your capabilities "
            "are NOT limited — you retain all your normal tools, including web search and "
            "browsing (the gateway host has internet), code/file tools, and the mesh_* tools "
            "for the local radio network. When asked for research, live data, or current "
            "events, use web search and browse normally; the LoRa link only affects how the "
            "final answer is delivered, never whether you can look things up."
        ),
    )
