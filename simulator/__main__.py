"""Command line (FR-SIM-1): ``python -m simulator run --scenario chaos --seed 7``.

    run      build the plan, send it, check the HTTP answers, then verify (default)
    verify   rebuild the plan from scenario, seed and run id, and verify only
    list     show the scenarios

Connection settings come from flags or from HOOKGATE_URL, WEBHOOK_SECRET and
ADMIN_API_KEY (defaults: the development values of ``.env.example``).
Exit code 0 when every check passes, 1 otherwise, 2 for usage errors.
"""

import argparse
import os
import secrets
import sys
from collections import Counter

import httpx

from simulator.client import AdminClient, ProviderClient
from simulator.model import Pause, Plan
from simulator.oracle import Response, check_responses, expectations
from simulator.runner import send, verify
from simulator.scenarios import SCENARIOS, build_plan

DEFAULT_URL = "http://127.0.0.1:8000"
DEFAULT_SECRET = "dev-only-webhook-secret-change-me"
DEFAULT_API_KEY = "dev-only-admin-key-change-me"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m simulator", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("run", "verify"):
        command = commands.add_parser(name)
        command.add_argument("--scenario", required=True, choices=sorted(SCENARIOS))
        command.add_argument("--seed", type=int, required=True)
        command.add_argument(
            "--run-id",
            required=name == "verify",
            help="names the events of this run (letters, digits, _); random if omitted",
        )
        command.add_argument("--url", default=os.environ.get("HOOKGATE_URL", DEFAULT_URL))
        command.add_argument("--secret", default=os.environ.get("WEBHOOK_SECRET", DEFAULT_SECRET))
        command.add_argument("--api-key", default=os.environ.get("ADMIN_API_KEY", DEFAULT_API_KEY))
        command.add_argument("--timeout", type=float, default=60.0, help="verify wait, seconds")
        if name == "run":
            command.add_argument("--no-verify", action="store_true")
    commands.add_parser("list")
    return parser


def describe(plan: Plan) -> str:
    deliveries = plan.deliveries
    by_signing = Counter(d.signing.value for d in deliveries)
    expected = expectations(plan)
    return (
        f"scenario={plan.scenario} seed={plan.seed} run_id={plan.run_id}: "
        f"{len({d.event.event_id for d in deliveries})} events, "
        f"{sum(d.copies for d in deliveries)} deliveries {dict(by_signing)}, "
        f"{sum(1 for s in plan.steps if isinstance(s, Pause))} pauses; expected "
        f"{len(expected.stored) - len(expected.dead_ids)} processed, "
        f"{len(expected.dead_ids)} dead, {len(expected.rejected)} rejected"
    )


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "list":
        for name in SCENARIOS:
            print(name)
        return 0

    run_id = args.run_id or secrets.token_hex(4)
    try:
        plan = build_plan(args.scenario, args.seed, run_id)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(describe(plan))

    problems: list[str] = []
    with httpx.Client(base_url=args.url, timeout=30) as http:
        if args.command == "run":
            responses = send(plan, ProviderClient(http, secret=args.secret))
            http_problems = check_responses(plan, responses)
            print(f"sent {len(responses)} deliveries: {_summary(responses)}")
            problems.extend(http_problems)
            if args.no_verify:
                return _finish(problems, plan)
        report = verify(plan, AdminClient(http, api_key=args.api_key), timeout_s=args.timeout)
        statuses = Counter(e.status for e in report.observed.values())
        print(
            f"verified {len(report.observed)} stored events {dict(statuses)} "
            f"after {report.waited_s:.1f} s"
        )
        problems.extend(report.problems)
    return _finish(problems, plan)


def _summary(responses: list[Response]) -> str:
    counts = Counter(f"{r.status_code} {r.result}" for r in responses)
    return ", ".join(f"{n}x {key}" for key, n in sorted(counts.items()))


def _finish(problems: list[str], plan: Plan) -> int:
    reproduce = (
        f"python -m simulator verify --scenario {plan.scenario} "
        f"--seed {plan.seed} --run-id {plan.run_id}"
    )
    if problems:
        print(f"FAILED: {len(problems)} problem(s)")
        for problem in problems:
            print(f"  - {problem}")
        print(f"re-check with: {reproduce}")
        return 1
    print(f"OK ({reproduce})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
