"""
trace-viewer/reconstruction.py

Pure-Python, database-independent trace reconstruction model.

Takes an unordered flat collection of SpanRecord objects and reconstructs
parent-child relationships using span_id, parent_span_id, and start_time.
No database access, no FastAPI, no I/O of any kind.

SpanRecord covers all promoted business columns from
analytics.stg_telemetry_spans. Ingestion-envelope columns
(schema_version, event_type, kafka_*, ingested_at), the raw attribute bag
(attributes), and derived boolean flags (is_*_span) are intentionally
excluded — they carry no value for the trace viewer UI.

Public API:
    SpanRecord          — input: one span from any source
    TraceNode           — output: a span positioned within the tree
    TraceReconstruction — output: the full reconstruction result
    build_trace_tree()  — entry point
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class SpanRecord:
    """One span record as received from any source.

    Required fields (7) are sufficient for reconstruction.
    Optional fields mirror the promoted business columns in
    analytics.stg_telemetry_spans and are available for the UI display panel.
    """

    # Required — always provided
    span_id: str
    parent_span_id: Optional[str]
    span_name: str
    start_time: datetime
    end_time: datetime
    duration_ms: float
    status_code: str

    # Optional — populated when the source provides them
    trace_id: Optional[str] = None
    service_name: Optional[str] = None
    span_kind: Optional[str] = None
    status_message: Optional[str] = None
    request_id: Optional[str] = None
    session_id: Optional[str] = None
    gen_ai_operation: Optional[str] = None
    agent_name: Optional[str] = None
    agent_operation: Optional[str] = None
    tool_name: Optional[str] = None
    tool_status: Optional[str] = None
    retrieval_result_count: Optional[int] = None
    retrieval_top_relevance_score: Optional[float] = None
    error_type: Optional[str] = None
    error_message: Optional[str] = None


@dataclass
class TraceNode:
    """A span positioned within the reconstructed tree.

    is_cycle_member is True ONLY for spans that actually participate in a
    cycle — i.e., a back-edge was detected from one of their descendants
    back to them (or to one of their ancestors) during DFS.

    Spans that are merely connected to a cycle component but whose own
    parent link does not form or extend a cycle are NOT flagged.
    """

    span: SpanRecord
    children: list[TraceNode] = field(default_factory=list)
    is_cycle_member: bool = False


@dataclass
class TraceReconstruction:
    """Full result of build_trace_tree().

    Every input span appears exactly once across roots, orphans, and
    cycle_members.

    has_cycle reflects whether any back-edge was detected during DFS in
    any phase — not merely whether Phase 3 produced output.
    """

    # Subtrees rooted at spans whose parent_span_id is None.
    roots: list[TraceNode]

    # Subtrees rooted at spans whose parent_span_id is not in the input.
    orphans: list[TraceNode]

    # Synthetic subtrees for spans unreachable from any natural root.
    # Each entry is the lexicographically-smallest span in its disconnected
    # component, chosen as a deterministic synthetic root.
    cycle_members: list[TraceNode]

    # True if any span in the full reconstruction is part of a detected
    # cycle (back-edge found during DFS in any phase).
    has_cycle: bool
    span_count: int  # equals len(input spans)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _sort_key(span: SpanRecord) -> tuple[datetime, str]:
    """Deterministic ordering: start_time ASC, span_id ASC as tie-breaker."""
    return (span.start_time, span.span_id)


def _build_children_map(spans: list[SpanRecord]) -> dict[str, list[SpanRecord]]:
    """Map parent_span_id → list of child SpanRecord objects."""
    result: dict[str, list[SpanRecord]] = {}
    for span in spans:
        if span.parent_span_id is not None:
            result.setdefault(span.parent_span_id, []).append(span)
    return result


# 3-color DFS state constants
_WHITE = "white"   # not yet entered
_GRAY  = "gray"    # on the active recursion path
_BLACK = "black"   # fully processed


def _dfs(
    span_id: str,
    children_map: dict[str, list[SpanRecord]],
    span_by_id: dict[str, SpanRecord],
    color: dict[str, str],
    path: list[str],
    cycle_ids: set[str],
) -> TraceNode:
    """
    Build a TraceNode for span_id and all its unvisited descendants using
    3-color DFS for precise cycle-member identification.

    Color transitions:
        WHITE → GRAY  when a span is entered (added to active path)
        GRAY  → BLACK when a span is exited  (removed from active path)

    Child dispatch:
        WHITE child  → not yet visited; recurse normally
        GRAY  child  → back-edge detected; the child is an ancestor still
                       on the active path. Every span from that child's
                       position in `path` through the current end is a
                       cycle member and is added to `cycle_ids`. The
                       back-edge is NOT rendered as a tree child (cycle
                       is broken at this point).
        BLACK child  → cross/forward edge; already claimed by another
                       subtree; skip to avoid duplicates.

    is_cycle_member on the returned node is set AFTER all children are
    processed so that back-edges discovered deeper in the tree propagate
    back up correctly.

    `path` is shared and mutated within a single top-level DFS call; each
    new top-level call receives a fresh empty list.
    `cycle_ids` is shared across all phases so that cycles anywhere in
    the reconstruction are captured.
    """
    color[span_id] = _GRAY
    path.append(span_id)

    child_nodes: list[TraceNode] = []
    for child_span in sorted(children_map.get(span_id, []), key=_sort_key):
        cid = child_span.span_id
        state = color[cid]
        if state == _WHITE:
            child_nodes.append(
                _dfs(cid, children_map, span_by_id, color, path, cycle_ids)
            )
        elif state == _GRAY:
            # Back-edge: cid is an ancestor still on the active path.
            # Mark every span from cid's position onward as cycle members.
            idx = path.index(cid)
            cycle_ids.update(path[idx:])
        # _BLACK: already fully processed (cross/forward edge) — skip.

    path.pop()
    color[span_id] = _BLACK

    return TraceNode(
        span=span_by_id[span_id],
        children=child_nodes,
        is_cycle_member=(span_id in cycle_ids),
    )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def build_trace_tree(spans: list[SpanRecord]) -> TraceReconstruction:
    """
    Reconstruct parent-child relationships from an unordered flat list of spans.

    The result covers every input span exactly once across roots, orphans,
    and cycle_members. Input order is irrelevant; sibling order is
    deterministic (start_time ASC, span_id ASC).

    Raises:
        ValueError  if any span_id appears more than once in the input.
        ValueError  if spans carry trace_id values from more than one trace.

    Returns an empty TraceReconstruction for an empty input.
    """
    if not spans:
        return TraceReconstruction(
            roots=[], orphans=[], cycle_members=[],
            has_cycle=False, span_count=0,
        )

    # Validate: no duplicate span_ids
    seen_ids: set[str] = set()
    for s in spans:
        if s.span_id in seen_ids:
            raise ValueError(f"Duplicate span_id in input: {s.span_id!r}")
        seen_ids.add(s.span_id)

    # Validate: single trace (only enforced when trace_id is provided)
    trace_ids = {s.trace_id for s in spans if s.trace_id is not None}
    if len(trace_ids) > 1:
        raise ValueError(
            f"Spans from multiple traces in a single reconstruction call: "
            f"{sorted(trace_ids)}"
        )

    span_by_id: dict[str, SpanRecord] = {s.span_id: s for s in spans}
    children_map = _build_children_map(spans)

    # Shared 3-color state across all phases.
    color: dict[str, str] = {s.span_id: _WHITE for s in spans}
    # Populated only when a back-edge is detected; drives is_cycle_member
    # and has_cycle for the full result.
    cycle_ids: set[str] = set()

    # --- Phase 1: natural roots (parent_span_id is None) ---
    natural_roots = sorted(
        [s for s in spans if s.parent_span_id is None],
        key=_sort_key,
    )
    root_nodes = [
        _dfs(r.span_id, children_map, span_by_id, color, [], cycle_ids)
        for r in natural_roots
    ]

    # --- Phase 2: orphans (parent referenced but absent from input) ---
    orphan_roots = sorted(
        [
            s for s in spans
            if color[s.span_id] == _WHITE
            and s.parent_span_id is not None
            and s.parent_span_id not in span_by_id
        ],
        key=_sort_key,
    )
    orphan_nodes = [
        _dfs(o.span_id, children_map, span_by_id, color, [], cycle_ids)
        for o in orphan_roots
    ]

    # --- Phase 3: cycle members ---
    # Every still-WHITE span has a parent in span_by_id but no path to any
    # natural root — it is in a disconnected cycle component.
    # Synthetic root = lexicographically smallest span_id in the component.
    cycle_nodes: list[TraceNode] = []
    while True:
        remaining = [s for s in spans if color[s.span_id] == _WHITE]
        if not remaining:
            break
        synthetic_root = min(remaining, key=lambda s: s.span_id)
        node = _dfs(synthetic_root.span_id, children_map, span_by_id, color, [], cycle_ids)
        cycle_nodes.append(node)

    return TraceReconstruction(
        roots=root_nodes,
        orphans=orphan_nodes,
        cycle_members=cycle_nodes,
        has_cycle=bool(cycle_ids),
        span_count=len(spans),
    )
