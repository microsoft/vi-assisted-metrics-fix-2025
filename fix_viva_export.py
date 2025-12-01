#!/usr/bin/env python3
"""Usage:
    python fix_viva_export.py --input INPUT.csv [--output OUTPUT.csv] [options]

Summary:
  Applies Microsoft’s workaround for the September–October 2025 underreporting
  of “Total meeting hours summarized or recapped by Copilot” (and related
  assisted-hours metrics) in Viva Insights exports. The script keeps the original
  CSV structure intact—only the affected metric values are adjusted.

Defaults:
  Granularity    : weekly (Sunday–Saturday weeks)
  Source window  : 2025-07-27 to 2025-08-30 (reference weeks used to compute
                    the multiplier)
  Target window  : 2025-08-31 to 2025-11-01 (affected weeks to be corrected)
  Hourly rate    : 72 USD (used to recompute Copilot assisted value)

Options:
    --granularity {weekly,monthly,daily}
  --source-start YYYY-MM-DD
  --source-end   YYYY-MM-DD
  --target-start YYYY-MM-DD
  --target-end   YYYY-MM-DD
    --rate FLOAT
    --overwrite
    --quiet
    --accept-partial
    --test (requires --original, --corrected)
    --original PATH
    --corrected PATH
    --tolerance FLOAT
    --help

Important:
    “Copilot assisted hours” remains an estimate, because Microsoft’s official
    workaround extrapolates meeting hours from the August 2025 reference window
    (or whichever window you override via --source-start/--source-end). All
    non-impacted rows and columns flow through unchanged. Columns are never
    renamed, removed, or reordered.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Iterable, Optional

try:
    import colorama
except ImportError:  # pragma: no cover - optional dependency
    colorama = None

_ANSI_RESET = "\x1b[0m"
_COLOR_INFO = "\x1b[36m"  # cyan
_COLOR_HIGHLIGHT = "\x1b[33m"  # yellow
_COLOR_PATH = "\x1b[32m"  # green
_COLOR_SUCCESS = "\x1b[32m"
_COLOR_ERROR = "\x1b[31m"

_ANSI_PATTERN = re.compile(r"\x1b\[[0-9;]*m")


def _configure_console_colors() -> None:
    if os.name == "nt" and colorama is not None:
        try:
            colorama.just_fix_windows_console()
        except Exception:
            pass


_configure_console_colors()


def _strip_ansi(text: str) -> str:
    return _ANSI_PATTERN.sub("", text)


class RunLogger:
    def __init__(self, quiet: bool):
        self.quiet = quiet
        self._lines: list[str] = []

    def info(self, message: str, *, color: Optional[str] = None) -> None:
        plain = _strip_ansi(message)
        self._lines.append(plain)
        if not self.quiet:
            if color:
                print(f"{color}{message}{_ANSI_RESET}")
            else:
                print(message)

    def warn(self, message: str) -> None:
        formatted = f"Warning: {message}"
        self._lines.append(formatted)
        print(f"\x1b[35m{formatted}{_ANSI_RESET}", file=sys.stderr)  # magenta

    def error(self, message: str) -> None:
        formatted = f"Error: {message}"
        self._lines.append(formatted)
        print(formatted, file=sys.stderr)

    def prompt(self, message: str) -> str:
        self._lines.append(message)
        try:
            response = input(message)
        except EOFError:
            response = ""
        else:
            self._lines.append(f"> {response}")
            return response
        self._lines.append("> ")
        return ""

    def write_log(self, path: Path) -> None:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            text = "\n".join(self._lines)
            path.write_text(text + ("\n" if text else ""), encoding="utf-8")
        except Exception as exc:  # pragma: no cover - best-effort logging
            print(f"Warning: Unable to write log to {path}: {exc}", file=sys.stderr)


def _ensure_module(module: str, package: Optional[str] = None) -> None:
    """Import *module*, installing *package* via pip if necessary."""

    try:
        __import__(module)
    except ImportError:  # pragma: no cover - only executed when module missing
        pkg = package or module
        print(f"[bootstrap] Installing required module '{pkg}'...", file=sys.stderr)
        subprocess.run([sys.executable, "-m", "pip", "install", pkg], check=True)
        __import__(module)


_ensure_module("polars")
import polars as pl  # noqa: E402


MEETING_COL = "Total Meeting hours summarized or recapped by Copilot"
RECAP_COL = "Intelligent recap actions taken"
SUMMARIZE_COL = "Summarize meeting actions taken using Copilot in Teams"
ASSISTED_COL = "Copilot assisted hours"
VALUE_COL = "Copilot assisted value"
DATE_COL = "MetricDate"

DEFAULT_VALIDATION_TOLERANCE = 1e-6

DEFAULT_SOURCE_START = "2025-07-27"
DEFAULT_SOURCE_END = "2025-08-30"
DEFAULT_TARGET_START = "2025-08-31"
DEFAULT_TARGET_END = "2025-11-01"
DEFAULT_RATE = 72.0

_COLUMN_ALIASES = {
    MEETING_COL: [
        MEETING_COL,
        "Total meeting hours summarized or recapped by Copilot",
    ],
    RECAP_COL: [
        RECAP_COL,
        "Intelligent recap actions taken using Copilot",
        "Intelligent recap actions taken",
    ],
    SUMMARIZE_COL: [
        SUMMARIZE_COL,
        "Summarize meeting actions taken using Copilot",
        "Summarize meeting actions taken using Copilot in Teams",
    ],
    ASSISTED_COL: [
        ASSISTED_COL,
    ],
    VALUE_COL: [
        VALUE_COL,
    ],
}

_REQUIRED_CORRECTION_METRICS = (MEETING_COL, ASSISTED_COL)
_OPTIONAL_VALUE_METRIC = VALUE_COL


class ValidationError(Exception):
    """Raised when Viva Insights export validation fails."""


def _require_validation_columns(path: Path) -> tuple[dict[str, str | None], dict[str, str], list[str]]:
    schema_columns = _collect_schema(path.as_posix())
    column_map, missing_required, alias_used = _resolve_columns(
        schema_columns,
        required_keys={MEETING_COL, RECAP_COL, SUMMARIZE_COL, ASSISTED_COL},
    )
    if missing_required:
        raise ValidationError(
            "Missing required Copilot columns: " + ", ".join(sorted(missing_required))
        )
    return column_map, alias_used, schema_columns


def _load_validation_frame(
    path: Path,
    column_map: dict[str, str | None],
    schema_columns: list[str],
    granularity: str,
) -> pl.LazyFrame:
    columns = [
        DATE_COL,
        column_map[MEETING_COL],
        column_map[ASSISTED_COL],
        column_map[RECAP_COL],
        column_map[SUMMARIZE_COL],
    ]
    optional = column_map.get(VALUE_COL)
    if optional:
        columns.append(optional)
    if "PersonId" in schema_columns and "PersonId" not in columns:
        columns.insert(0, "PersonId")

    lf = pl.scan_csv(path.as_posix()).select(columns)
    return _prepare_dates(lf).with_columns(_period_expression(granularity))


def _collect_validation_target(
    lf: pl.LazyFrame,
    start: _dt.date,
    end: _dt.date,
    granularity: str,
) -> pl.DataFrame:
    start_aligned = _align_start(start, granularity)
    end_aligned = _align_end(end, granularity)
    mask = (
        (pl.col("__period_start") >= pl.lit(start_aligned))
        & (pl.col("__period_start") <= pl.lit(end_aligned))
    )
    return lf.filter(mask).drop("__period_start").collect()


def _join_validation_targets(
    original: pl.DataFrame,
    corrected: pl.DataFrame,
) -> pl.DataFrame:
    key_cols = [DATE_COL]
    if "PersonId" in original.columns and "PersonId" in corrected.columns:
        key_cols.insert(0, "PersonId")
    return original.join(corrected, on=key_cols, suffix="_corrected", how="full")


def _max_ratio_spread(values: pl.Series) -> float:
    if values.is_empty():
        return 0.0
    filtered = values.drop_nulls().drop_nans()
    if filtered.is_empty():
        return 0.0
    return float(filtered.max() - filtered.min())


def _validate_exports(
    original_path: Path,
    corrected_path: Path,
    *,
    source_start: _dt.date,
    source_end: _dt.date,
    target_start: _dt.date,
    target_end: _dt.date,
    granularity: str,
    tolerance: float,
) -> tuple[float, dict[str, float], dict[str, str]]:
    column_map, alias_used, schema_columns = _require_validation_columns(original_path)

    multiplier = _compute_multiplier(
        original_path.as_posix(),
        granularity,
        source_start,
        source_end,
        column_map,
    )

    lf_orig = _load_validation_frame(original_path, column_map, schema_columns, granularity)
    lf_fix = _load_validation_frame(corrected_path, column_map, schema_columns, granularity)

    orig_target = _collect_validation_target(lf_orig, target_start, target_end, granularity)
    fix_target = _collect_validation_target(lf_fix, target_start, target_end, granularity)

    if orig_target.height != fix_target.height:
        raise ValidationError(
            f"Row count mismatch in target window: original={orig_target.height:,} corrected={fix_target.height:,}"
        )

    joined = _join_validation_targets(orig_target, fix_target)

    meeting_name = column_map[MEETING_COL]
    assisted_name = column_map[ASSISTED_COL]
    recap_name = column_map[RECAP_COL]
    summarize_name = column_map[SUMMARIZE_COL]
    value_name = column_map.get(VALUE_COL)

    meeting_corrected = f"{meeting_name}_corrected"
    assisted_corrected = f"{assisted_name}_corrected"
    recap_corrected = f"{recap_name}_corrected"
    summarize_corrected = f"{summarize_name}_corrected"

    missing_original = joined.filter(pl.col(meeting_name).is_null()).height
    missing_corrected = joined.filter(pl.col(meeting_corrected).is_null()).height
    if missing_original or missing_corrected:
        raise ValidationError(
            "Mismatch detected between original and corrected datasets: "
            + f"original_missing={missing_original}, corrected_missing={missing_corrected}"
        )

    joined = joined.with_columns([
        (
            pl.col(recap_corrected).cast(pl.Float64, strict=False).fill_null(0)
            + pl.col(summarize_corrected).cast(pl.Float64, strict=False).fill_null(0)
        ).alias("actions_corrected"),
        (
            pl.col(recap_name).cast(pl.Float64, strict=False).fill_null(0)
            + pl.col(summarize_name).cast(pl.Float64, strict=False).fill_null(0)
        ).alias("actions_original"),
    ])

    joined = joined.with_columns([
        (
            pl.col(meeting_corrected).cast(pl.Float64, strict=False).fill_null(0)
            - pl.col(meeting_name).cast(pl.Float64, strict=False).fill_null(0)
        ).alias("meeting_delta"),
        (
            pl.col(assisted_corrected).cast(pl.Float64, strict=False).fill_null(0)
            - pl.col(assisted_name).cast(pl.Float64, strict=False).fill_null(0)
        ).alias("assisted_delta"),
    ])

    joined = joined.with_columns((pl.col("actions_corrected") * multiplier).alias("expected_meeting"))
    joined = joined.with_columns([
        (
            pl.col(meeting_corrected).cast(pl.Float64, strict=False).fill_null(0)
            - pl.col("expected_meeting")
        ).alias("formula_delta"),
        (pl.col("actions_corrected") - pl.col("actions_original")).alias("actions_delta"),
    ])

    stats: dict[str, float] = {}

    formula_max = float(joined["formula_delta"].abs().max())
    stats["formula_delta_max"] = formula_max
    if formula_max > tolerance:
        raise ValidationError(
            f"Meeting-hours deviation exceeds tolerance: {formula_max:.6g} > {tolerance}"
        )

    assisted_vs_meeting = float((joined["assisted_delta"] - joined["meeting_delta"]).abs().max())
    stats["assisted_vs_meeting_max"] = assisted_vs_meeting
    if assisted_vs_meeting > tolerance:
        raise ValidationError(
            "Assisted hours delta does not match meeting hours delta within tolerance."
        )

    actions_delta = float(joined["actions_delta"].abs().max())
    stats["actions_delta_max"] = actions_delta
    if actions_delta > tolerance:
        raise ValidationError("Copilot action counts changed in the corrected file.")

    if value_name:
        value_corrected = f"{value_name}_corrected"
        if value_corrected in joined.columns:
            joined = joined.with_columns(
                pl.when(pl.col(assisted_corrected).cast(pl.Float64, strict=False) != 0)
                .then(
                    pl.col(value_corrected).cast(pl.Float64, strict=False)
                    / pl.col(assisted_corrected).cast(pl.Float64, strict=False)
                )
                .otherwise(None)
                .alias("value_ratio")
            )
            stats["value_ratio_spread"] = _max_ratio_spread(joined["value_ratio"])
            if stats["value_ratio_spread"] > tolerance:
                raise ValidationError(
                    "Copilot assisted value is inconsistent relative to assisted hours."
                )

    stats["meeting_delta_total"] = float(joined["meeting_delta"].sum())
    stats["assisted_delta_total"] = float(joined["assisted_delta"].sum())

    return multiplier, stats, alias_used


def _render_stat_row(label: str, value: float, unit: str = "") -> str:
    unit_display = f" {unit}" if unit else ""
    return f"{_COLOR_INFO}{label:<26}:{_ANSI_RESET} {_COLOR_HIGHLIGHT}{value}{unit_display}{_ANSI_RESET}"


def _log_validation_report(
    *,
    logger: RunLogger,
    header: str,
    original_path: Path,
    corrected_path: Path,
    multiplier: float,
    stats: dict[str, float],
    alias_used: dict[str, str],
    tolerance: float,
    source_start: _dt.date,
    source_end: _dt.date,
    target_start: _dt.date,
    target_end: _dt.date,
    intro_lines: Iterable[str] | None = None,
    include_paths: bool = True,
    success_message: str | None = None,
) -> None:
    separator = f"{_COLOR_INFO}{'═' * 70}{_ANSI_RESET}"
    logger.info(separator)
    logger.info(header, color=_COLOR_HIGHLIGHT)
    if intro_lines:
        for line in intro_lines:
            logger.info(line)

    if alias_used:
        for canonical, alias in alias_used.items():
            logger.info(f"Using column '{alias}' for expected field '{canonical}'.", color=_COLOR_INFO)

    if include_paths:
        logger.info(f"Original : {_COLOR_PATH}{original_path}{_ANSI_RESET}")
        logger.info(f"Corrected: {_COLOR_PATH}{corrected_path}{_ANSI_RESET}")

    logger.info(_render_stat_row("Multiplier", f"{multiplier:.12f}"))
    logger.info(f"    Reference window: {source_start.isoformat()} to {source_end.isoformat()}")
    logger.info(f"    Target window: {target_start.isoformat()} to {target_end.isoformat()}")
    logger.info("    Inputs used: Intelligent recap actions + Summarize meeting actions")
    logger.info(_render_stat_row("Formula Δ max", f"{stats['formula_delta_max']:.6g}"))
    logger.info("    Quick check: meeting hours rebuilt using the multiplier; stays tiny when the fix is correct.")
    logger.info(_render_stat_row("Assisted Δ max", f"{stats['assisted_vs_meeting_max']:.6g}"))
    logger.info("    Quick check: assisted hours change matches meeting hours; stays tiny when both align.")
    logger.info(_render_stat_row("Actions Δ max", f"{stats['actions_delta_max']:.6g}"))
    logger.info("    Quick check: action counts stay zero; the fix never edits them.")
    logger.info(_render_stat_row("Meeting Δ total", f"{stats['meeting_delta_total']:.6f}"))
    logger.info(_render_stat_row("Assisted Δ total", f"{stats['assisted_delta_total']:.6f}"))
    logger.info("    These totals summarize how many hours were restored across the target window.")
    if "value_ratio_spread" in stats:
        logger.info(_render_stat_row("Assisted value spread", f"{stats['value_ratio_spread']:.6g}"))
        logger.info("    Spread shows how consistent \"Copilot assisted value\" is relative to assisted hours.")

    final_message = success_message or (
        f"{_COLOR_SUCCESS}All tests passed. Multiplier={multiplier:.12f}{_ANSI_RESET}"
    )
    logger.info(final_message)
    logger.info(
        "If any of the metrics above exceed the allowed tolerance, the report will call out the issue"
        " so you can inspect the corresponding rows in the corrected export."
    )
    logger.info(separator)


def _run_validation_mode(args: argparse.Namespace, logger: RunLogger) -> None:
    original_path = Path(args.original).resolve()
    corrected_path = Path(args.corrected).resolve()

    tolerance = args.tolerance if args.tolerance is not None else DEFAULT_VALIDATION_TOLERANCE

    source_start = _to_date(args.source_start or DEFAULT_SOURCE_START)
    source_end = _to_date(args.source_end or DEFAULT_SOURCE_END)
    target_start = _to_date(args.target_start or DEFAULT_TARGET_START)
    target_end = _to_date(args.target_end or DEFAULT_TARGET_END)

    try:
        multiplier, stats, alias_used = _validate_exports(
            original_path,
            corrected_path,
            source_start=source_start,
            source_end=source_end,
            target_start=target_start,
            target_end=target_end,
            granularity=args.granularity or DEFAULT_GRANULARITY,
            tolerance=tolerance,
        )
    except ValidationError as exc:
        logger.error(f"{_COLOR_ERROR}{exc}{_ANSI_RESET}")
        raise

    _log_validation_report(
        logger=logger,
        header="Validation mode",
        original_path=original_path,
        corrected_path=corrected_path,
        multiplier=multiplier,
        stats=stats,
        alias_used=alias_used,
        tolerance=tolerance,
        source_start=source_start,
        source_end=source_end,
        target_start=target_start,
        target_end=target_end,
        intro_lines=[
            "This check confirms the corrected export uses Microsoft’s Viva Insights assisted metrics fix (2025).",
            "The script recomputes the reference multiplier, then verifies every adjusted row in the corrected CSV.",
        ],
        include_paths=True,
        success_message=f"{_COLOR_SUCCESS}All tests passed. Multiplier={multiplier:.12f}{_ANSI_RESET}",
    )

def _parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        add_help=False,
    )
    parser.add_argument("--input", required=False, help="Path to Viva Insights CSV export")
    parser.add_argument(
        "--output",
        help="Destination CSV (defaults to <input>_corrected_<timestamp>.csv)",
    )
    parser.add_argument(
        "--granularity",
        choices=("weekly", "monthly", "daily"),
        default="weekly",
        help="Aggregate by weekly (default), monthly, or daily periods",
    )
    parser.add_argument(
        "--source-start",
        default=None,
        help=f"Reference window start date (default: {DEFAULT_SOURCE_START})",
    )
    parser.add_argument(
        "--source-end",
        default=None,
        help=f"Reference window end date (default: {DEFAULT_SOURCE_END})",
    )
    parser.add_argument(
        "--target-start",
        default=DEFAULT_TARGET_START,
        help=f"Affected window start date (default: {DEFAULT_TARGET_START})",
    )
    parser.add_argument(
        "--target-end",
        default=DEFAULT_TARGET_END,
        help=f"Affected window end date (default: {DEFAULT_TARGET_END})",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=DEFAULT_RATE,
        help=f"Hourly rate used to recompute Copilot assisted value (default: {DEFAULT_RATE:.0f})",
    )
    parser.add_argument("--overwrite", action="store_true", help="Overwrite output file if it exists")
    parser.add_argument("--quiet", action="store_true", help="Suppress summary output")
    parser.add_argument(
        "--accept-partial",
        dest="accept_partial",
        action="store_true",
        help="Automatically continue when correction metrics are missing",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Run validation mode instead of applying corrections (requires --original and --corrected).",
    )
    parser.add_argument(
        "-original",
        "--original",
        help="Path to the unmodified Viva Insights export when running --test.",
    )
    parser.add_argument(
        "-corrected",
        "--corrected",
        help="Path to the corrected Viva Insights export when running --test.",
    )
    parser.add_argument(
        "--tolerance",
        type=float,
        default=DEFAULT_VALIDATION_TOLERANCE,
        help=f"Floating-point tolerance for validation mode (default: {DEFAULT_VALIDATION_TOLERANCE}).",
    )
    parser.add_argument("--help", action="help", help="Show this help message and exit")
    return parser.parse_args(argv)


def _to_date(date_str: str) -> _dt.date:
    try:
        return _dt.date.fromisoformat(date_str)
    except ValueError as exc:  # pragma: no cover - input validation
        raise SystemExit(f"Invalid ISO date: {date_str}") from exc


def _align_start(date: _dt.date, granularity: str) -> _dt.date:
    if granularity == "weekly":
        offset = (date.weekday() + 1) % 7
        return date - _dt.timedelta(days=offset)
    if granularity == "monthly":
        return date.replace(day=1)
    return date


def _align_end(date: _dt.date, granularity: str) -> _dt.date:
    if granularity == "weekly":
        return _align_start(date, granularity)
    if granularity == "monthly":
        return date.replace(day=1)
    return date


def _period_expression(granularity: str) -> pl.Expr:
    metric_date = pl.col("__metric_date")
    if granularity == "weekly":
        # polars.dt.weekday() returns ISO-style indices (Monday=1 … Sunday=7).
        # Taking modulo 7 maps Sunday to 0 while preserving 1–6 for Monday–Saturday.
        # This yields a Sunday-aligned week start that matches _align_start.
        offset = metric_date.dt.weekday().mod(7) * pl.duration(days=1)
        return (metric_date - offset).alias("__period_start")
    if granularity == "monthly":
        return metric_date.dt.truncate("1mo").alias("__period_start")
    return metric_date.alias("__period_start")


def _prepare_dates(lf: pl.LazyFrame) -> pl.LazyFrame:
    return lf.with_columns(
        pl.col(DATE_COL)
        .str.strptime(pl.Date, strict=False)
        .alias("__metric_date")
    )


def _count_rows_in_range(
    path: str,
    granularity: str,
    start: _dt.date,
    end: _dt.date,
) -> int:
    lf = pl.scan_csv(path)
    lf = _prepare_dates(lf).with_columns(_period_expression(granularity))

    start_aligned = _align_start(start, granularity)
    end_aligned = _align_end(end, granularity)

    mask = (
        (pl.col("__period_start") >= pl.lit(start_aligned))
        & (pl.col("__period_start") <= pl.lit(end_aligned))
    )

    count = (
        lf.filter(mask)
        .select(pl.len().alias("count"))
        .collect()[0, "count"]
    )

    return int(count)


def _collect_schema(path: str) -> list[str]:
    return list(pl.read_csv(path, n_rows=0).columns)


def _resolve_columns(
    schema: list[str],
    required_keys: set[str],
) -> tuple[dict[str, str | None], list[str], dict[str, str]]:
    resolved: dict[str, str | None] = {}
    missing_required: list[str] = []
    alias_used: dict[str, str] = {}

    for canonical, candidates in _COLUMN_ALIASES.items():
        match = next((alias for alias in candidates if alias in schema), None)
        resolved[canonical] = match
        if match is None:
            if canonical in required_keys:
                missing_required.append(canonical)
        else:
            if match != canonical:
                alias_used[canonical] = match

    return resolved, missing_required, alias_used


def _compute_multiplier(
    path: str,
    granularity: str,
    source_start: _dt.date,
    source_end: _dt.date,
    column_map: dict[str, str | None],
) -> float:
    meeting_name = column_map[MEETING_COL]
    recap_name = column_map[RECAP_COL]
    summarize_name = column_map[SUMMARIZE_COL]

    assert meeting_name is not None and recap_name is not None and summarize_name is not None

    columns = [DATE_COL, meeting_name, recap_name, summarize_name]
    lf = pl.scan_csv(path).select(columns)
    lf = _prepare_dates(lf).with_columns(_period_expression(granularity))

    period_start = _align_start(source_start, granularity)
    period_end = _align_end(source_end, granularity)

    subset = (
        lf.filter(
            (pl.col("__period_start") >= pl.lit(period_start))
            & (pl.col("__period_start") <= pl.lit(period_end))
        )
        .with_columns(
            pl.col(meeting_name).cast(pl.Float64, strict=False).fill_null(0),
            pl.col(recap_name).cast(pl.Float64, strict=False).fill_null(0),
            pl.col(summarize_name).cast(pl.Float64, strict=False).fill_null(0),
        )
        .select(
            pl.sum(meeting_name).alias("total_hours"),
            (pl.sum(recap_name) + pl.sum(summarize_name)).alias("total_actions"),
        )
        .collect()
    )

    total_hours = subset["total_hours"][0]
    total_actions = subset["total_actions"][0]

    if total_actions == 0:  # pragma: no cover - sanity guard
        raise SystemExit("Reference window produced zero Copilot actions; cannot compute multiplier.")

    return float(total_hours / total_actions)


def _apply_corrections(
    path: str,
    output: Path,
    granularity: str,
    multiplier: float,
    target_start: _dt.date,
    target_end: _dt.date,
    hourly_rate: float,
    column_map: dict[str, str | None],
    logger: RunLogger,
) -> int:
    meeting_name = column_map[MEETING_COL]
    recap_name = column_map[RECAP_COL]
    summarize_name = column_map[SUMMARIZE_COL]
    assisted_name = column_map[ASSISTED_COL]
    value_name = column_map.get(VALUE_COL)

    assert meeting_name is not None and recap_name is not None and summarize_name is not None and assisted_name is not None

    original_columns = list(pl.read_csv(path, n_rows=0).columns)

    lf = pl.scan_csv(path)
    lf = _prepare_dates(lf).with_columns(_period_expression(granularity))

    target_start_aligned = _align_start(target_start, granularity)
    target_end_aligned = _align_end(target_end, granularity)

    period_mask = (
        (pl.col("__period_start") >= pl.lit(target_start_aligned))
        & (pl.col("__period_start") <= pl.lit(target_end_aligned))
    )

    rows_updated = (
        lf.filter(period_mask)
        .select(pl.len().alias("count"))
        .collect()[0, "count"]
    )

    meeting_col = pl.col(meeting_name).cast(pl.Float64, strict=False).fill_null(0)
    recap_expr = pl.col(recap_name).cast(pl.Float64, strict=False).fill_null(0)
    summarize_expr = pl.col(summarize_name).cast(pl.Float64, strict=False).fill_null(0)
    assisted_col = (
        pl.col(assisted_name).cast(pl.Float64, strict=False).fill_null(0)
        if assisted_name is not None
        else None
    )
    value_col = (
        pl.col(value_name).cast(pl.Float64, strict=False).fill_null(0)
        if value_name is not None and assisted_col is not None
        else None
    )

    other_component = (
        assisted_col - meeting_col
        if assisted_col is not None
        else None
    )
    corrected_meeting = (recap_expr + summarize_expr) * multiplier

    updates = [
        pl.when(period_mask)
        .then(corrected_meeting)
        .otherwise(meeting_col)
        .alias(meeting_name),
    ]

    if assisted_col is not None and assisted_name is not None and other_component is not None:
        updates.append(
            pl.when(period_mask)
            .then(other_component + corrected_meeting)
            .otherwise(assisted_col)
            .alias(assisted_name)
        )

    if (
        value_col is not None
        and value_name is not None
        and assisted_col is not None
        and other_component is not None
    ):
        updates.append(
            pl.when(period_mask)
            .then((other_component + corrected_meeting) * hourly_rate)
            .otherwise(value_col)
            .alias(value_name)
        )

    if value_name is not None and assisted_col is None:
        logger.warn("Skipping 'Copilot assisted value' because 'Copilot assisted hours' is absent.")

    lf = lf.with_columns(*updates)

    lf = lf.drop(["__metric_date", "__period_start"]).select(original_columns)

    temp_path = output.with_suffix(output.suffix + ".tmp")
    temp_path.parent.mkdir(parents=True, exist_ok=True)

    lf.sink_csv(temp_path.as_posix())
    os.replace(temp_path, output)

    # logging performed by caller

    return int(rows_updated)


def main(argv: Iterable[str] | None = None) -> None:
    args = _parse_args(argv)
    logger = RunLogger(args.quiet)
    log_path: Optional[Path] = None
    try:
        if args.test:
            if not args.original or not args.corrected:
                logger.error("The --test option requires both --original and --corrected paths.")
                raise SystemExit(1)
            if args.input:
                logger.warn("--input is ignored when running in --test mode.")
            try:
                _run_validation_mode(args, logger)
            except ValidationError:
                raise SystemExit(2)
            return

        if args.original or args.corrected:
            logger.error("--original/--corrected are only valid when --test is supplied.")
            raise SystemExit(1)

        if not args.input:
            logger.error("--input must be provided when not running --test.")
            raise SystemExit(1)

        if (args.source_start is None) ^ (args.source_end is None):
            logger.error("--source-start and --source-end must be supplied together.")
            raise SystemExit(1)

        source_start_str = args.source_start or DEFAULT_SOURCE_START
        source_end_str = args.source_end or DEFAULT_SOURCE_END

        source_start = _to_date(source_start_str)
        source_end = _to_date(source_end_str)

        if source_end < source_start:
            logger.error("Source window end date cannot be earlier than start date.")
            raise SystemExit(1)

        target_start = _to_date(args.target_start)
        target_end = _to_date(args.target_end)

        input_path = Path(args.input).resolve()
        if not input_path.exists():  # pragma: no cover
            logger.error(f"Input file not found: {input_path}")
            raise SystemExit(1)

        if args.output:
            output_path = Path(args.output).resolve()
        else:
            timestamp = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = input_path.with_name(f"{input_path.stem}_corrected_{timestamp}.csv")

        log_path = output_path.with_suffix(".log")

        try:
            schema_columns = _collect_schema(input_path.as_posix())
        except Exception as exc:  # pragma: no cover - unexpected IO errors
            logger.error(f"Unable to read CSV header: {exc}")
            raise SystemExit(1) from exc

        column_map, missing_required, alias_used = _resolve_columns(
            schema_columns,
            required_keys={RECAP_COL, SUMMARIZE_COL},
        )

        if missing_required:
            logger.error(
                "The CSV is missing required Copilot columns: "
                + ", ".join(missing_required)
            )
            raise SystemExit(1)

        for canonical, alias in alias_used.items():
            logger.info(f"Using column '{alias}' for expected field '{canonical}'.")

        value_column_name = column_map.get(_OPTIONAL_VALUE_METRIC)

        metric_presence = {
            metric: column_map.get(metric) is not None for metric in _REQUIRED_CORRECTION_METRICS
        }
        present_metrics = [metric for metric, present in metric_presence.items() if present]
        missing_metrics = [metric for metric, present in metric_presence.items() if not present]

        if not present_metrics:
            logger.info(
                "No Copilot correction metrics (Meeting hours recapped by Copilot, "
                "Copilot assisted hours, Copilot assisted value) were found in the export. Nothing to adjust."
            )
            return

        if missing_metrics:
            logger.info(
                "Detected correction metrics:\n  Present: "
                + ", ".join(present_metrics)
                + "\n  Missing: "
                + ", ".join(missing_metrics)
            )

            if column_map.get(MEETING_COL) is None:
                logger.error(
                    "Unable to proceed: the meeting-hours metric is missing, so no corrections can be computed."
                )
                return

            if args.accept_partial:
                logger.info("Auto-continue enabled (--accept-partial); proceeding with available metrics.")
            else:
                if args.quiet:
                    logger.info("Quiet mode enabled; aborting without changes by default.")
                    return

                response = logger.prompt("Continue using available metrics? [y/N]: ").strip().lower()

                if response not in {"y", "yes"}:
                    logger.info("Aborting without changes.")
                    return

        meeting_name_resolved = column_map.get(MEETING_COL)
        assisted_name_resolved = column_map.get(ASSISTED_COL)
        updated_metric_names = [name for name in [meeting_name_resolved, assisted_name_resolved, value_column_name] if name]
        missing_required_display = [metric for metric in _REQUIRED_CORRECTION_METRICS if column_map.get(metric) is None]

        if output_path.exists() and not args.overwrite:
            response = logger.prompt(
                f"Output file already exists:\n  {output_path}\nOverwrite? [y/N]: "
            ).strip().lower()
            if response not in {"y", "yes"}:
                logger.info("Aborting without changes.")
                return

        source_row_total = _count_rows_in_range(
            input_path.as_posix(),
            args.granularity,
            source_start,
            source_end,
        )
        target_row_total = _count_rows_in_range(
            input_path.as_posix(),
            args.granularity,
            target_start,
            target_end,
        )

        multiplier = _compute_multiplier(
            input_path.as_posix(),
            args.granularity,
            source_start,
            source_end,
            column_map,
        )

        rows_updated = _apply_corrections(
            input_path.as_posix(),
            output_path,
            args.granularity,
            multiplier,
            target_start,
            target_end,
            args.rate,
            column_map,
            logger,
        )
        validation_tolerance = args.tolerance if args.tolerance is not None else DEFAULT_VALIDATION_TOLERANCE
        try:
            validation_multiplier, validation_stats, validation_alias = _validate_exports(
                input_path,
                output_path,
                source_start=source_start,
                source_end=source_end,
                target_start=target_start,
                target_end=target_end,
                granularity=args.granularity,
                tolerance=validation_tolerance,
            )
        except ValidationError as exc:
            logger.error(f"{_COLOR_ERROR}{exc}{_ANSI_RESET}")
            raise SystemExit(2)

        separator = f"{_COLOR_INFO}{'=' * 60}{_ANSI_RESET}"
        logger.info(separator)
        logger.info("Summary", color=_COLOR_HIGHLIGHT)
        logger.info(f"{_COLOR_INFO}Multiplier applied:{_ANSI_RESET} {_COLOR_HIGHLIGHT}{multiplier:.6f}{_ANSI_RESET}")
        logger.info(
            f"{_COLOR_INFO}Processed metrics:{_ANSI_RESET} {_COLOR_HIGHLIGHT}{', '.join(updated_metric_names) if updated_metric_names else 'None'}{_ANSI_RESET}"
        )
        skipped_metrics = missing_required_display.copy()
        if value_column_name is None:
            skipped_metrics.append(f"{_OPTIONAL_VALUE_METRIC} (deprecated)")
        logger.info(
            f"{_COLOR_INFO}Metrics skipped:{_ANSI_RESET} {_COLOR_HIGHLIGHT}{', '.join(skipped_metrics) if skipped_metrics else 'None'}{_ANSI_RESET}"
        )
        logger.info(
            f"{_COLOR_INFO}Source range rows ({source_start.isoformat()} to {source_end.isoformat()}):{_ANSI_RESET} {_COLOR_HIGHLIGHT}{source_row_total}{_ANSI_RESET}"
        )
        logger.info(
            f"{_COLOR_INFO}Target range rows ({target_start.isoformat()} to {target_end.isoformat()}):{_ANSI_RESET} {_COLOR_HIGHLIGHT}{target_row_total}{_ANSI_RESET}"
        )
        logger.info(
            f"{_COLOR_INFO}Rows updated ({target_start.isoformat()} to {target_end.isoformat()}):{_ANSI_RESET} {_COLOR_HIGHLIGHT}{rows_updated}{_ANSI_RESET}"
        )
        logger.info(f"{_COLOR_INFO}Input file:{_ANSI_RESET} {_COLOR_PATH}{input_path.as_posix()}{_ANSI_RESET}")
        logger.info(f"{_COLOR_INFO}Log file:{_ANSI_RESET} {_COLOR_PATH}{log_path.as_posix()}{_ANSI_RESET}")
        logger.info(f"{_COLOR_INFO}Corrected file:{_ANSI_RESET} {_COLOR_PATH}{output_path.as_posix()}{_ANSI_RESET}")
        _log_validation_report(
            logger=logger,
            header="Validation results",
            original_path=input_path,
            corrected_path=output_path,
            multiplier=validation_multiplier,
            stats=validation_stats,
            alias_used=validation_alias,
            tolerance=validation_tolerance,
            source_start=source_start,
            source_end=source_end,
            target_start=target_start,
            target_end=target_end,
            intro_lines=None,
            include_paths=False,
            success_message=(
                f"{_COLOR_SUCCESS}Auto-validation complete at tolerance {validation_tolerance:.6g}. All checks passed.{_ANSI_RESET}"
            ),
        )
    finally:
        if args.test:
            return
        if log_path is None:
            base_candidate = args.output or args.input
            if base_candidate is None:
                return
            try:
                base = Path(base_candidate).resolve()
            except FileNotFoundError:
                base = Path(base_candidate)
            log_path = base.with_suffix(".log")
        logger.write_log(log_path)


if __name__ == "__main__":  # pragma: no cover
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        sys.exit(1)
