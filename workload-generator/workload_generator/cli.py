"""
workload_generator/cli.py

Command-line entry point of the workload generator.

Phase 14.3B: PLAN ONLY.  The command builds a plan from its arguments,
prints a summary and, when asked, writes the plan report as JSON.  It
contacts nothing: no collector, no Kafka, no database, no Docker, and it
reads no environment variable and no configuration file.

    python -m workload_generator --scenario mixed-production --traces 100 \\
        --rate 5 --seed 42

--send is refused: no execution driver exists in this phase.  --mode is
accepted and recorded, and changes nothing yet.

Safety limits are enforced here even though nothing is executed, so that
they are already in place when execution is added:

    traces        at least 1; more than 10,000 needs --allow-large; never
                  more than 1,000,000 (a plan is held in memory)
    rate          more than 0, at most 10,000 requests per second
    concurrency   1 to 64
    max duration  the plan must fit in it (default 15 minutes)

Exit codes:
    0  planned
    2  invalid arguments or a refused request (nothing was planned)

Standard library only.

Public API:
    main()
"""

from __future__ import annotations

import argparse
import re
import secrets
import sys
from pathlib import Path
from typing import Optional, Sequence, TextIO

from .personas import Persona
from .plan import MODES, PlanConfig, build_plan, validate_run_id
from .report import SAMPLE_SIZE, build_report, format_summary, write_report
from .scenarios import Scenario

DEFAULT_MODE: str = "real"
DEFAULT_SCENARIO: str = Scenario.NORMAL.value
DEFAULT_PERSONA: str = "mix"
DEFAULT_TRACES: int = 100
DEFAULT_RATE: float = 5.0
DEFAULT_CONCURRENCY: int = 4
DEFAULT_SEED: int = 0
DEFAULT_MAX_DURATION: str = "15m"

#: Above this, --allow-large is required.
LARGE_TRACES: int = 10_000
#: Never planned in one run, with or without --allow-large: every planned
#: request is an object in memory (roughly half a kilobyte), and a plan this
#: size already takes several hundred megabytes.
ABSOLUTE_MAX_TRACES: int = 1_000_000
MAX_RATE: float = 10_000.0
MAX_CONCURRENCY: int = 64
MAX_SEED: int = 2 ** 63 - 1

_DURATION_RE = re.compile(r"(\d+(?:\.\d+)?)([smh]?)")
_DURATION_UNITS = {"": 1.0, "s": 1.0, "m": 60.0, "h": 3600.0}

_SEND_REFUSAL = (
    "--send is not available: execution is not implemented in Phase 14.3B. "
    "This command only plans; nothing can be sent."
)


def _parse_duration(text: str) -> float:
    """Seconds from '90', '90s', '15m' or '2h'."""
    match = _DURATION_RE.fullmatch(text.strip().lower())
    if match is None:
        raise ValueError("expected a number optionally followed by s, m or h")
    return float(match.group(1)) * _DURATION_UNITS[match.group(2)]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="workload_generator",
        description=(
            "Plan a realistic workload for the AgentOps demo agent. "
            "Phase 14.3B plans only: nothing is executed and nothing is sent."
        ),
    )
    parser.add_argument(
        "--mode", choices=MODES, default=DEFAULT_MODE,
        help="how the plan would later be executed; recorded only (default: real)",
    )
    parser.add_argument(
        "--scenario", choices=[scenario.value for scenario in Scenario],
        default=DEFAULT_SCENARIO, help="default: normal",
    )
    parser.add_argument(
        "--persona", choices=[DEFAULT_PERSONA, *(persona.value for persona in Persona)],
        default=DEFAULT_PERSONA, help="one traffic profile, or the default mix",
    )
    parser.add_argument(
        "--traces", type=int, default=DEFAULT_TRACES,
        help=f"requests to plan (default: {DEFAULT_TRACES})",
    )
    parser.add_argument(
        "--rate", type=float, default=DEFAULT_RATE,
        help=f"average requests per second (default: {DEFAULT_RATE:g})",
    )
    parser.add_argument(
        "--concurrency", type=int, default=DEFAULT_CONCURRENCY,
        help=f"requests in flight at once, 1-{MAX_CONCURRENCY} "
             f"(default: {DEFAULT_CONCURRENCY}); does not change the plan",
    )
    parser.add_argument(
        "--seed", type=int, default=DEFAULT_SEED,
        help=f"decides the planned traffic (default: {DEFAULT_SEED})",
    )
    parser.add_argument(
        "--run-id", default=None,
        help="decides the identifiers; random when not given, always printed",
    )
    parser.add_argument(
        "--max-duration", default=DEFAULT_MAX_DURATION,
        help="longest plan accepted, e.g. 90s, 15m, 2h (default: 15m)",
    )
    parser.add_argument(
        "--allow-large", action="store_true",
        help=f"permit planning more than {LARGE_TRACES:,} requests",
    )
    parser.add_argument(
        "--send", action="store_true",
        help="reserved for execution; refused in this phase",
    )
    parser.add_argument(
        "--report", default=None, metavar="PATH",
        help="also write the plan report as JSON to a new file",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None, *, stdout: Optional[TextIO] = None) -> int:
    out = sys.stdout if stdout is None else stdout
    parser = _build_parser()
    args = parser.parse_args(argv)

    # Refused before anything else, so that it cannot be overlooked.
    if args.send:
        parser.error(_SEND_REFUSAL)

    if args.traces < 1:
        parser.error("--traces must be at least 1")
    if args.traces > ABSOLUTE_MAX_TRACES:
        parser.error(
            f"--traces must not exceed {ABSOLUTE_MAX_TRACES:,}: a plan is held in memory"
        )
    if args.traces > LARGE_TRACES and not args.allow_large:
        parser.error(
            f"--traces above {LARGE_TRACES:,} requires --allow-large"
        )
    # "not (0 < x <= limit)" also rejects nan.
    if not 0.0 < args.rate <= MAX_RATE:
        parser.error(f"--rate must be more than 0 and at most {MAX_RATE:g}")
    if not 1 <= args.concurrency <= MAX_CONCURRENCY:
        parser.error(f"--concurrency must be between 1 and {MAX_CONCURRENCY}")
    if not 0 <= args.seed <= MAX_SEED:
        parser.error("--seed must be a non-negative integer")

    try:
        max_duration = _parse_duration(args.max_duration)
    except ValueError as exc:
        parser.error(f"--max-duration: {exc}")
    if not 0.0 < max_duration < float("inf"):
        parser.error("--max-duration must be positive")

    planned_duration = (args.traces - 1) / args.rate
    if planned_duration > max_duration:
        parser.error(
            f"{args.traces} requests at {args.rate:g} per second take "
            f"{planned_duration:.0f} seconds, more than --max-duration "
            f"({max_duration:g} seconds); lower --traces, raise --rate or "
            "raise --max-duration"
        )

    run_id = args.run_id if args.run_id is not None else secrets.token_hex(6)
    try:
        validate_run_id(run_id)
    except ValueError as exc:
        parser.error(f"--run-id: {exc}")

    report_path: Optional[Path] = None
    if args.report is not None:
        report_path = Path(args.report)
        if report_path.exists():
            parser.error(f"--report: {report_path} already exists; nothing is overwritten")
        if not report_path.parent.is_dir():
            parser.error(f"--report: directory does not exist: {report_path.parent}")

    config = PlanConfig(
        seed=args.seed,
        run_id=run_id,
        scenario=Scenario(args.scenario),
        traces=args.traces,
        rate=args.rate,
        persona=None if args.persona == DEFAULT_PERSONA else Persona(args.persona),
        mode=args.mode,
    )
    plan = build_plan(config)
    report = build_report(
        plan, concurrency=args.concurrency, max_duration_seconds=max_duration,
    )

    out.write(format_summary(report, plan.requests[:SAMPLE_SIZE]))
    if report_path is not None:
        write_report(report, report_path)
        out.write(f"plan report written to {report_path}\n")
    return 0
