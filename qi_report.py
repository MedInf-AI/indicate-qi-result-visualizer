#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb>=1.1", "pyyaml>=6"]
# ///
"""Serve a QI result report — initial population and adherence per QI and period.

Reads materialized QI results (and the QI debug values written
alongside them) for one or more datasets, aggregates them by calendar
period, and serves the report over HTTP. The clinical source data is
never consulted.

Data sources (``--source``):

``csv``
    CSV files as written by the CQL engine from ``Main.cql``: no header, the
    columns of the OMOP ``observation`` table in ``CSV_COLUMNS`` order.
``db``
    An OMOP CDM-formatted database (PostgreSQL; one schema per dataset)
    whose ``observation`` table holds the same rows. ``person_id`` may be
    NULL — it is never read.

Only rows whose ``observation_concept_id`` lies in the OMOP range for
locally-defined concepts (>= 2,000,000,000) are loaded. Which of those
concepts are QI results and which are debug quantities — and their names —
comes from two files: the result concepts file (``qi-result-concepts.csv``:
``qi``, ``concept_id``, ``concept_name``) and the debug concepts file
(``qi-debug-concepts.csv``: the same columns and any further ones, which are
shown next to the debug value's chart). QI names and titles come from
``qi-definitions.csv``. All are re-read whenever they change, without
reloading the data.

The QI result rows carry no patient identity, so every number is a count of
*patient-days* — one result row per patient and 24-hour period in which the
QI's initial population criterion held — and ``value_as_number`` is that
day's adherence ratio (numerator / denominator).

Data is loaded once at startup (in the background; the server answers right
away) and on demand via the "Reload data" button. Run with ``--help`` for
usage; see README.md for the configuration file.

"""

from __future__ import annotations

import argparse
import contextlib
import csv
import html
import logging
import math
import os
import re
import sys
import threading
import time
import traceback
import xml.etree.ElementTree as ET
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import quote, unquote, urlsplit

import duckdb
import yaml

log = logging.getLogger("qi_report")

#: column order the CQL engine writes into the results CSV files (no header)
CSV_COLUMNS = [
    "obs_event_field_concept_id",
    "observation_concept_id",
    "observation_date",
    "observation_datetime",
    "observation_id",
    "observation_event_id",
    "observation_source_concept_id",
    "observation_source_value",
    "observation_type_concept_id",
    "person_id",
    "provider_id",
    "qualifier_concept_id",
    "qualifier_source_value",
    "unit_concept_id",
    "unit_source_value",
    "value_as_concept_id",
    "value_as_number",
    "value_as_string",
    "value_source_value",
    "visit_detail_id",
    "visit_occurrence_id",
]

#: OMOP reserves concept ids above two billion for locally-defined concepts;
#: the QI result and debug concepts live there.
LOCAL_CONCEPT_MIN = 2_000_000_000

DATA_DIR = Path(__file__).resolve().parent / "data"
DEFAULT_RESULT_CONCEPTS = DATA_DIR / "qi-result-concepts.csv"
DEFAULT_DEBUG_CONCEPTS = DATA_DIR / "qi-debug-concepts.csv"
DEFAULT_QI_DEFINITIONS = DATA_DIR / "qi-definitions.csv"


#: required columns of the result and debug concepts files
CONCEPT_COLUMNS = ("qi", "concept_id", "concept_name")
#: optional column of the debug concepts file: the unit of the values
DEBUG_UNIT_COLUMN = "unit"
#: all other columns are free-form information shown with the debug value
#: required column of the QI definitions file; "name", "short_name" and "title" are optional
QI_DEFINITION_COLUMNS = ("qi",)

#: beyond this many days of span, switch from week- to month-sized buckets
MONTH_BIN_THRESHOLD_DAYS = 3 * 365
#: beyond this many days of span, switch from month- to year-sized buckets
YEAR_BIN_THRESHOLD_DAYS = 15 * 365


# ==========================================================================
# Configuration


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    path: Path | None = None  # source "csv"
    dsn: str | None = None  # source "db"
    schema: str | None = None  # source "db"
    log: Path | None = None  # run log of the CQL engine, if known
    profile: Path | None = None  # flamegraph (SVG) of the run, if known

    def describe(self) -> str:
        if self.path is not None:
            return str(self.path)
        return f"schema {self.schema} @ {_redact_dsn(self.dsn or '')}"


@dataclass
class Settings:
    source: str
    result_concepts: Path
    debug_concepts: Path | None  # None: the default file, which may be missing
    qi_definitions: Path | None  # None: the default file, which may be missing
    datasets: list[DatasetSpec]
    host: str
    port: int


def _redact_dsn(dsn: str) -> str:
    """Hide passwords in a DSN for display (URL and key=value forms)."""
    if "://" in dsn:
        parts = urlsplit(dsn)
        if parts.password:
            netloc = parts.netloc.replace(f":{parts.password}@", ":***@")
            return parts._replace(netloc=netloc).geturl()
        return dsn
    return " ".join("password=***" if tok.lower().startswith("password=") else tok
                    for tok in dsn.split())


def _dataset_name_from_path(path: Path) -> str:
    stem = path.name.removesuffix(".gz").removesuffix(".csv")
    return stem.removeprefix("results-") or stem


def _default_log_path(path: Path) -> Path | None:
    """``results-<dataset>.csv[.gz]`` → ``log-<dataset>.txt`` in the same directory."""
    m = re.fullmatch(r"results-(.+?)\.csv(?:\.gz)?", path.name)
    return path.with_name(f"log-{m[1]}.txt") if m else None


def _default_profile_path(path: Path) -> Path | None:
    """``results-<dataset>.csv[.gz]`` → ``profile-<dataset>.svg`` in the same directory."""
    m = re.fullmatch(r"results-(.+?)\.csv(?:\.gz)?", path.name)
    return path.with_name(f"profile-{m[1]}.svg") if m else None


def _csv_spec(name: str, path: Path, log_path: Path | None = None,
              profile_path: Path | None = None) -> DatasetSpec:
    path = path.resolve()
    return DatasetSpec(name, path=path,
                       log=log_path.resolve() if log_path else _default_log_path(path),
                       profile=(profile_path.resolve() if profile_path
                                else _default_profile_path(path)))


def _split_named(arg: str) -> tuple[str | None, str]:
    """``NAME=VALUE`` → (NAME, VALUE); ``VALUE`` → (None, VALUE)."""
    name, sep, value = arg.partition("=")
    if sep and name and "/" not in name and os.sep not in name:
        return name, value
    return None, arg


def _csv_specs_from_args(inputs: list[str]) -> list[DatasetSpec]:
    specs = []
    for arg in inputs:
        name, value = _split_named(arg)
        path = Path(value)
        if path.is_dir():
            if name:
                raise SystemExit(f"error: {arg}: a directory cannot be given a dataset name")
            found = sorted([*path.glob("results-*.csv"), *path.glob("results-*.csv.gz")])
            if not found:
                raise SystemExit(f"error: no results-*.csv[.gz] files in directory {path}")
            specs.extend(_csv_spec(_dataset_name_from_path(p), p) for p in found)
        else:
            specs.append(_csv_spec(name or _dataset_name_from_path(path), path))
    return specs


def _db_specs_from_args(inputs: list[str], dsn: str | None) -> list[DatasetSpec]:
    if not dsn:
        raise SystemExit("error: --source db needs a connection string (--dsn, or db.dsn "
                         "in the configuration file)")
    specs = []
    for arg in inputs:
        name, schema = _split_named(arg)
        specs.append(DatasetSpec(name or schema, dsn=dsn, schema=schema))
    return specs


def _read_config(path: Path) -> dict:
    """The YAML configuration file, with its sections checked to be mappings."""
    try:
        cfg = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as e:
        raise SystemExit(f"error: cannot read configuration file {path}: {e}") from e
    cfg = cfg if cfg is not None else {}  # empty file
    if not isinstance(cfg, dict):
        raise SystemExit(f"error: configuration file {path}: expected a mapping of settings")
    for key in ("server", "csv", "db"):
        if cfg.get(key) is None:
            cfg[key] = {}
        elif not isinstance(cfg[key], dict):
            raise SystemExit(f"error: configuration file {path}: {key} must be a mapping")
    for key in ("result_concepts", "debug_concepts", "qi_definitions"):
        if key in cfg and not isinstance(cfg[key], str):
            raise SystemExit(f"error: configuration file {path}: {key} must be a file path")
    return cfg


def _config_str(value, where: str, what: str) -> str:
    # YAML turns unquoted values like 2024, yes or null into numbers, booleans or None
    if not isinstance(value, str):
        raise SystemExit(f"error: {where}: {what} must be a string, got {value!r} "
                         "(put it in quotes)")
    return value


def _specs_from_config(cfg: dict, source: str, base: Path, dsn_override: str | None
                       ) -> list[DatasetSpec]:
    section = cfg.get(source) or {}
    entries = section.get("datasets") or {}
    if not isinstance(entries, dict):
        raise SystemExit(f"error: {source}.datasets must be a mapping of NAME: ... entries")
    specs = []
    for name, value in entries.items():
        name = str(name)
        where = f"{source}.datasets.{name}"
        log_path = profile_path = None
        if isinstance(value, dict) and value.get("log") is not None:
            log_path = (base / _config_str(value["log"], where, "log")).resolve()
        if isinstance(value, dict) and value.get("profile") is not None:
            profile_path = (base / _config_str(value["profile"], where, "profile")).resolve()
        if source == "csv":
            if isinstance(value, dict):
                if "path" not in value:
                    raise SystemExit(f"error: {where}: expected a file path or "
                                     "{path: ..., log: ..., profile: ...}")
                value = value["path"]
            specs.append(_csv_spec(name, base / _config_str(value, where, "path"), log_path,
                                   profile_path))
        else:
            if not isinstance(value, dict):
                value = {"schema": value}
            if "schema" not in value:
                raise SystemExit(f"error: {where}: expected a schema name or "
                                 "{schema: ..., dsn: ..., log: ..., profile: ...}")
            schema = _config_str(value["schema"], where, "schema")
            dsn = value.get("dsn") or dsn_override or section.get("dsn")
            if not dsn:
                raise SystemExit(f"error: {where}: no connection string "
                                 "(set db.dsn, the dataset's dsn, or --dsn)")
            dsn = _config_str(dsn, where, "dsn")
            specs.append(DatasetSpec(name, dsn=os.path.expandvars(dsn), schema=schema,
                                     log=log_path, profile=profile_path))
    return specs


def parse_args(argv: list[str] | None = None) -> Settings:
    p = argparse.ArgumentParser(
        description="Serve a report of QI results (initial population and adherence "
                    "per period) and QI debug values over HTTP.",
        epilog="examples:\n"
               "  %(prog)s results-mimic.csv\n"
               "  %(prog)s results/                      (all results-*.csv[.gz] in a directory)\n"
               "  %(prog)s --source db --dsn postgresql://user@host/qi mimic eicu\n"
               "  %(prog)s --config qi-report.yaml --source db",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("inputs", nargs="*", metavar="INPUT",
                   help="csv: results files (may be gzipped) or directories (results-*.csv[.gz]); db: schema "
                        "names. Either may be written NAME=VALUE to set the dataset name. "
                        "Overrides the datasets of the configuration file.")
    p.add_argument("-c", "--config", type=Path,
                   help="YAML configuration file listing datasets (see README.md)")
    p.add_argument("-s", "--source", choices=["csv", "db"],
                   help="read QI results from CSV files or from an OMOP CDM database "
                        "(default: the only source configured, else csv)")
    p.add_argument("--dsn", help="database connection string (PostgreSQL URL or libpq "
                                 "key=value string; 'duckdb:PATH' for a DuckDB file); "
                                 "environment variables like ${PGPASS} are expanded")
    p.add_argument("--result-concepts", type=Path,
                   help=f"QI result concepts file (default: config 'result_concepts', else "
                        f"{DEFAULT_RESULT_CONCEPTS.name} next to this script)")
    p.add_argument("--debug-concepts", type=Path,
                   help="debug concepts file (default: config 'debug_concepts', else "
                        f"{DEFAULT_DEBUG_CONCEPTS.name} next to this script)")
    p.add_argument("--qi-definitions", type=Path,
                   help="QI names and titles file (default: config 'qi_definitions', else "
                        f"{DEFAULT_QI_DEFINITIONS.name} next to this script)")
    p.add_argument("--host", default=None, help="address to listen on (default 127.0.0.1)")
    p.add_argument("--port", type=int, default=None, help="port to listen on (default 8000)")
    p.add_argument("-v", "--verbose", action="store_true", help="log every HTTP request")
    args = p.parse_args(argv)

    cfg: dict = {}
    base = Path.cwd()
    if args.config:
        cfg = _read_config(args.config)
        base = args.config.resolve().parent

    source = args.source
    if source is None:
        configured = [s for s in ("csv", "db") if cfg.get(s, {}).get("datasets")]
        source = configured[0] if len(configured) == 1 else "csv"

    dsn = os.path.expandvars(args.dsn) if args.dsn else None
    if args.inputs:
        if source == "csv":
            specs = _csv_specs_from_args(args.inputs)
        else:
            cfg_dsn = cfg.get("db", {}).get("dsn")
            specs = _db_specs_from_args(args.inputs,
                                        dsn or (os.path.expandvars(cfg_dsn) if cfg_dsn else None))
    else:
        specs = _specs_from_config(cfg, source, base, dsn)
    if not specs:
        other = "db" if source == "csv" else "csv"
        hint = (f" (the configuration file has {other}.datasets — use --source {other}?)"
                if cfg.get(other, {}).get("datasets") else "")
        raise SystemExit(f"error: no {source} datasets given{hint}; see --help")
    names = [s.name for s in specs]
    if dupes := sorted({n for n in names if names.count(n) > 1}):
        raise SystemExit(f"error: duplicate dataset names: {', '.join(dupes)} "
                         "(use NAME=VALUE to rename)")

    if args.result_concepts:
        result_concepts = args.result_concepts.resolve()
    elif "result_concepts" in cfg:
        result_concepts = (base / cfg["result_concepts"]).resolve()
    else:
        result_concepts = DEFAULT_RESULT_CONCEPTS
    if args.debug_concepts:
        debug_concepts: Path | None = args.debug_concepts.resolve()
    elif "debug_concepts" in cfg:
        debug_concepts = (base / cfg["debug_concepts"]).resolve()
    else:
        debug_concepts = None
    if args.qi_definitions:
        qi_definitions: Path | None = args.qi_definitions.resolve()
    elif "qi_definitions" in cfg:
        qi_definitions = (base / cfg["qi_definitions"]).resolve()
    else:
        qi_definitions = None
    server = cfg.get("server", {})
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
    return Settings(source=source, result_concepts=result_concepts, debug_concepts=debug_concepts,
                    qi_definitions=qi_definitions, datasets=specs,
                    host=args.host or server.get("host", "127.0.0.1"),
                    port=args.port or int(server.get("port", 8000)))


# ==========================================================================
# Concepts file


@dataclass(frozen=True)
class Concept:
    concept_id: int
    qi: str
    name: str
    debug: bool
    #: further (column, value) pairs of the debug concepts file, non-empty only
    extra: tuple[tuple[str, str], ...] = ()
    #: unit of the values (debug concepts file), "" if unknown or dimensionless
    unit: str = ""

    @property
    def short_name(self) -> str:
        """Debug concept name without the "QI03 debug:" prefix."""
        return self.name.split(":", 1)[-1].strip() if self.debug else self.name


@dataclass(frozen=True)
class QIInfo:
    """Names of a QI from the QI definitions file (all optional)."""

    qi: str
    name: str = ""  # e.g. "ventilation", shown next to the QI id in headings
    short_name: str = ""  # e.g. "VTE prevention", for the sidebar; defaults to name
    title: str = ""  # the full title from the QI definition

    @property
    def nav_name(self) -> str:
        return self.short_name or self.name


class _WatchedFile:
    """A file that is re-parsed whenever its modification time changes."""

    def __init__(self, path: Path, what: str, parse, required: bool):
        self.path, self.what, self.parse, self.required = path, what, parse, required
        self.mtime: float | None = None
        self.value: dict = {}
        self.error: str | None = None

    def refresh(self) -> None:
        try:
            mtime = self.path.stat().st_mtime
        except FileNotFoundError:
            self.value, self.mtime = {}, None
            self.error = (f"{self.what} file {self.path} does not exist" if self.required
                          else None)
            return
        except OSError as e:
            self.error = f"cannot read {self.what} file {self.path}: {e}"
            return
        if mtime == self.mtime:
            return
        try:
            self.value = self.parse(self.path)
            self.mtime, self.error = mtime, None
            log.info("read %d %s from %s", len(self.value), self.what, self.path)
        except (OSError, ValueError, KeyError, csv.Error) as e:
            # e.g. caught mid-write: keep the previous version, retry next time
            self.error = f"cannot parse {self.what} file {self.path}: {e!r}"
            log.warning(self.error)


def _read_result_concepts(path: Path) -> dict[int, Concept]:
    """QI result concepts: every row of the result concepts file."""
    out: dict[int, Concept] = {}
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        missing = [c for c in CONCEPT_COLUMNS if c not in (reader.fieldnames or [])]
        if missing:
            raise ValueError(f"missing column(s): {', '.join(missing)}")
        for row in reader:
            if not (row.get("concept_id") or "").strip():
                continue  # blank line
            cid = int(row["concept_id"])
            out.setdefault(cid, Concept(cid, row["qi"].strip(), row["concept_name"].strip(),
                                        False))
    return out


def _read_debug_concepts(path: Path) -> dict[int, Concept]:
    """Debug concepts: every row of the debug concepts file, extra columns included."""
    out: dict[int, Concept] = {}
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        missing = [c for c in CONCEPT_COLUMNS if c not in (reader.fieldnames or [])]
        if missing:
            raise ValueError(f"missing column(s): {', '.join(missing)}")
        for row in reader:
            if not (row.get("concept_id") or "").strip():
                continue  # blank line
            cid = int(row["concept_id"])
            known = (*CONCEPT_COLUMNS, DEBUG_UNIT_COLUMN)
            extra = tuple((k.strip(), v.strip()) for k, v in row.items()
                          if k and k not in known and v and v.strip())
            unit = (row.get(DEBUG_UNIT_COLUMN) or "").strip()
            out.setdefault(cid, Concept(cid, row["qi"].strip(), row["concept_name"].strip(),
                                        True, extra, unit))
    return out


def _read_qi_definitions(path: Path) -> dict[str, QIInfo]:
    """QI names and titles, one row per QI."""
    out: dict[str, QIInfo] = {}
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        missing = [c for c in QI_DEFINITION_COLUMNS if c not in (reader.fieldnames or [])]
        if missing:
            raise ValueError(f"missing column(s): {', '.join(missing)}")
        for row in reader:
            qi = (row.get("qi") or "").strip()
            if qi:
                out.setdefault(qi, QIInfo(qi, *((row.get(k) or "").strip()
                                                for k in ("name", "short_name", "title"))))
    return out


class ConceptCatalog:
    """QI result and debug concepts, re-read whenever one of their files changes."""

    def __init__(self, concepts: Path, debug_concepts: Path | None,
                 qi_definitions: Path | None = None):
        self._qi_file = _WatchedFile(qi_definitions or DEFAULT_QI_DEFINITIONS, "QI definitions",
                                     _read_qi_definitions, qi_definitions is not None)
        self._files = [
            _WatchedFile(concepts, "result concepts", _read_result_concepts, True),
            _WatchedFile(debug_concepts or DEFAULT_DEBUG_CONCEPTS, "debug concepts",
                         _read_debug_concepts, debug_concepts is not None),
        ]
        self._lock = threading.Lock()

    @property
    def error(self) -> str | None:
        return "; ".join(f.error for f in (*self._files, self._qi_file) if f.error) or None

    def get(self) -> dict[int, Concept]:
        with self._lock:
            merged: dict[int, Concept] = {}
            for f in self._files:  # debug concepts file last: it wins
                f.refresh()
                merged.update(f.value)
            return merged

    def qi_info(self) -> dict[str, QIInfo]:
        with self._lock:
            self._qi_file.refresh()
            return self._qi_file.value


# ==========================================================================
# Loading and aggregation


@dataclass
class Bucket:
    start: date
    n: int  # number of non-null values (= patient-days, for QI results)
    mean: float | None
    q25: float | None
    q75: float | None
    min: float | None
    max: float | None


@dataclass
class Series:
    """All values of one concept in one dataset, aggregated by period."""

    buckets: list[Bucket]
    n: int
    mean: float | None


@dataclass
class DatasetData:
    name: str
    source: str
    error: str | None = None
    unit: str = "week"
    first: date | None = None
    last: date | None = None
    rows: int = 0
    series: dict[int, Series] = field(default_factory=dict)
    seconds: float = 0.0
    log_path: Path | None = None  # where the run log is expected (None: unknown)
    log: RunLog | None = None  # None: no run log file at log_path
    profile_path: Path | None = None  # where the flamegraph is expected (None: unknown)
    profile: Profile | None = None  # None: no flamegraph file at profile_path


# ==========================================================================
# Run logs of the CQL engine


@dataclass
class LogWarning:
    count: int
    level: str  # e.g. "WARNING"
    message: str  # e.g. "401: Calorie target is null"


@dataclass
class RunLog:
    """Summary of a CQL engine run, read from its console log."""

    path: Path
    started: datetime | None = None  # timestamp of the first progress line
    seconds: float | None = None
    successes: int | None = None
    failures: int | None = None
    warnings_total: int | None = None
    warnings: list[LogWarning] = field(default_factory=list)
    progress: tuple[int, int] | None = None  # last progress line: (done, total)
    error: str | None = None

    @property
    def patients(self) -> int | None:
        if self.successes is None or self.failures is None:
            return None
        return self.successes + self.failures


#: 2026-11-18T18:34:22 [    1 s]       100/  139,293 [  0.07 %]   60.45 evals/s; ETA: 2303 s
_LOG_PROGRESS = re.compile(
    r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)\s+\[\s*\d+ s\]\s+([\d,]+)/\s*([\d,]+)\s")
#: 431.929 s, 139294 successes, 0 failures, 875870 warnings
_LOG_SUMMARY = re.compile(r"([\d.,]+) s, ([\d,]+) successes?, ([\d,]+) failures?, "
                          r"([\d,]+) warnings?\s*")
#:   4,486 time(s): WARNING '401: Calorie target is null'
_LOG_MESSAGE = re.compile(r"\s*([\d,]+) time\(s\): (\w+) '(.*)'\s*")


def _log_int(s: str) -> int:
    return int(s.replace(",", ""))


def read_run_log(path: Path) -> RunLog:
    """Parse a CQL engine console log: run time, successes/failures and message counts."""
    out = RunLog(path)
    summary_seen = False
    with path.open(encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.rstrip("\n")
            if not summary_seen:
                if m := _LOG_PROGRESS.match(line):
                    if out.started is None:
                        out.started = datetime.fromisoformat(m[1])
                    out.progress = (_log_int(m[2]), _log_int(m[3]))
                elif m := _LOG_SUMMARY.fullmatch(line):
                    out.seconds = float(m[1].replace(",", ""))
                    out.successes, out.failures, out.warnings_total = (
                        _log_int(m[2]), _log_int(m[3]), _log_int(m[4]))
                    summary_seen = True
            elif m := _LOG_MESSAGE.fullmatch(line):
                out.warnings.append(LogWarning(_log_int(m[1]), m[2], m[3]))
            elif line.strip():
                break  # end of the message counts; the per-message details follow
    out.warnings.sort(key=lambda w: -w.count)
    if not summary_seen:
        out.error = "the log has no summary line — the run may not have finished"
        if out.progress:
            out.error += (f" (last progress: {out.progress[0]:,} of {out.progress[1]:,} "
                          "patients)")
    return out


# ==========================================================================
# Profiles (flamegraphs) of the CQL engine


@dataclass
class Profile:
    """A flamegraph of a CQL engine run, rebuilt as a clean inline SVG."""

    path: Path
    svg: str | None = None  # the cleaned SVG markup, served on demand
    frames: int = 0
    total_ms: int | None = None  # time of the root frame
    error: str | None = None


_PROFILE_STYLE = re.compile(r"[\w#:;.,%\s-]*")  # colours and opacities only, no url(...)
_PROFILE_MS = re.compile(r"([\d,]+) ms")


def read_profile(path: Path, id_prefix: str) -> Profile:
    """Read a flamegraph SVG made of ``rect`` frames, each followed by its clip path and
    label lines, and rebuild it from those parts only (no scripts, no foreign markup).

    Every frame becomes a ``<g data-tip=...>`` for the report's tooltip; clip path ids
    get ``id_prefix`` so that they cannot clash with other ids on the page.
    """
    root = ET.parse(path).getroot()

    def tag(el: ET.Element) -> str:
        return el.tag.rpartition("}")[2]

    frames: list[dict] = []
    for el in root:
        t = tag(el)
        if t == "rect":
            frames.append({"rect": el, "clips": [], "texts": []})
        elif frames and t == "defs":
            frames[-1]["clips"] += [c for c in el if tag(c) == "clipPath"]
        elif frames and t == "text":
            frames[-1]["texts"].append(el)
    if not frames:
        return Profile(path, error="no flamegraph frames (rect elements) in the file")

    def num(el: ET.Element, a: str) -> float:
        try:
            return float(el.get(a, "0"))
        except ValueError:
            return 0.0

    def rect(src: ET.Element, parent: ET.Element) -> None:
        r = ET.SubElement(parent, "rect", {a: f"{num(src, a):.10g}"
                                           for a in ("x", "y", "width", "height")})
        style = src.get("style", "")
        if _PROFILE_STYLE.fullmatch(style):
            r.set("style", style)

    width = max(num(f["rect"], "x") + num(f["rect"], "width") for f in frames)
    height = max(num(f["rect"], "y") + num(f["rect"], "height") for f in frames)
    labels = [[" ".join("".join(t.itertext()).split()) for t in f["texts"]] for f in frames]
    m = _PROFILE_MS.fullmatch(labels[0][1]) if len(labels[0]) > 1 else None
    total_ms = _log_int(m[1]) if m else None

    svg = ET.Element("svg", {"xmlns": "http://www.w3.org/2000/svg", "class": "flame",
                             "width": f"{width:.10g}", "height": f"{height:.10g}"})
    for f, label in zip(frames, labels):
        tip = " · ".join(x for x in label if x)
        if total_ms and len(label) > 1 and (m := _PROFILE_MS.fullmatch(label[1])):
            share = _log_int(m[1]) * 100 / total_ms
            tip += f" · {share:.1f} % of the total" if share >= 1 else (
                f" · {share:.2g} % of the total" if share >= 0.001 else " · < 0.001 % of the total")
        g = ET.SubElement(svg, "g", {"data-tip": tip})
        ids = {}
        for c in f["clips"]:
            ids[c.get("id", "")] = cid = f"{id_prefix}{len(ids)}-{len(svg)}"
            defs = ET.SubElement(g, "defs")
            cp = ET.SubElement(defs, "clipPath", {"id": cid})
            for r in c:
                if tag(r) == "rect":
                    rect(r, cp)
        rect(f["rect"], g)
        for t, text in zip(f["texts"], label):
            el = ET.SubElement(g, "text", {"x": f"{num(t, 'x'):.10g}", "y": f"{num(t, 'y'):.10g}"})
            cm = re.fullmatch(r"url\(#(.*)\)", t.get("clip-path", ""))
            if cm and cm[1] in ids:
                el.set("clip-path", f"url(#{ids[cm[1]]})")
            el.text = text
    return Profile(path, ET.tostring(svg, encoding="unicode"), len(frames), total_ms)


def choose_bin_unit(first: date | None, last: date | None) -> str:
    """Bucket size from the span of dates covered by a dataset.

    Most datasets span at most a few years, where weeks read well. Datasets
    like MIMIC-IV anchor each patient's dates to an arbitrary, widely
    scattered year, so their apparent span is many decades — weekly buckets
    there would mean thousands of mostly-empty rows, so they get month- or
    year-sized buckets instead.
    """
    if first is None or last is None:
        return "week"
    span_days = (last - first).days
    if span_days > YEAR_BIN_THRESHOLD_DAYS:
        return "year"
    if span_days > MONTH_BIN_THRESHOLD_DAYS:
        return "month"
    return "week"


def bucket_end(start: date, unit: str) -> date:
    """First day after the bucket starting at ``start``."""
    if unit == "week":
        return date.fromordinal(start.toordinal() + 7)
    if unit == "month":
        return date(start.year + start.month // 12, start.month % 12 + 1, 1)
    return date(start.year + 1, 1, 1)


def _sql_str(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def _sql_ident(s: str) -> str:
    return '"' + s.replace('"', '""') + '"'


def _csv_source_sql(path: Path) -> str:
    names = "[" + ", ".join(_sql_str(c) for c in CSV_COLUMNS) + "]"
    return f"""SELECT TRY_CAST(observation_concept_id AS BIGINT) AS cid,
                      TRY_CAST(observation_date AS DATE) AS d,
                      TRY_CAST(value_as_number AS DOUBLE) AS v
               FROM read_csv({_sql_str(str(path))}, header = false, names = {names},
                             all_varchar = true)"""


def _db_source_sql(alias: str, schema: str) -> str:
    # The filter sits directly on the scanned table so that it is pushed down
    # to PostgreSQL; only these three columns are transferred.
    return f"""SELECT CAST(observation_concept_id AS BIGINT) AS cid,
                      CAST(observation_date AS DATE) AS d,
                      CAST(value_as_number AS DOUBLE) AS v
               FROM {alias}.{_sql_ident(schema)}.observation
               WHERE observation_concept_id >= {LOCAL_CONCEPT_MIN}"""


def _num(v) -> float | None:
    return None if v is None else float(v)


def load_dataset(con: duckdb.DuckDBPyConnection, spec: DatasetSpec, source_sql: str
                 ) -> DatasetData:
    out = DatasetData(spec.name, spec.describe())
    t0 = time.monotonic()
    con.execute(f"""CREATE OR REPLACE TEMP TABLE r AS
                    SELECT cid, d, v FROM ({source_sql})
                    WHERE cid >= {LOCAL_CONCEPT_MIN} AND d IS NOT NULL""")
    try:
        out.rows, out.first, out.last = con.execute(
            "SELECT count(*), min(d), max(d) FROM r").fetchone()
        out.unit = choose_bin_unit(out.first, out.last)
        rows = con.execute(
            f"""SELECT cid, CAST(date_trunc('{out.unit}', d) AS DATE) AS bucket,
                       count(v), avg(v), quantile_cont(v, 0.25), quantile_cont(v, 0.75),
                       min(v), max(v)
                FROM r GROUP BY ALL ORDER BY cid, bucket""").fetchall()
        overall = {cid: (n, mean) for cid, n, mean in con.execute(
            "SELECT cid, count(v), avg(v) FROM r GROUP BY cid").fetchall()}
    finally:
        con.execute("DROP TABLE IF EXISTS r")
    for cid, bucket, n, mean, q25, q75, lo, hi in rows:
        if cid not in out.series:
            n_total, mean_total = overall[cid]
            out.series[cid] = Series([], int(n_total), _num(mean_total))
        out.series[cid].buckets.append(
            Bucket(bucket, int(n), _num(mean), _num(q25), _num(q75), _num(lo), _num(hi)))
    if out.rows == 0:
        out.error = (f"no rows with a QI output concept id (>= {LOCAL_CONCEPT_MIN:,}) "
                     "and a date")
    out.seconds = time.monotonic() - t0
    return out


class _Attachments:
    """Database connections attached to one DuckDB session, one per DSN."""

    def __init__(self, con: duckdb.DuckDBPyConnection):
        self.con = con
        self.aliases: dict[str, str] = {}
        self._postgres_loaded = False

    def alias_for(self, dsn: str) -> str:
        if dsn in self.aliases:
            return self.aliases[dsn]
        alias = f"src{len(self.aliases)}"
        if dsn.startswith("duckdb:"):
            self.con.execute(f"ATTACH {_sql_str(dsn.removeprefix('duckdb:'))} AS {alias} "
                             "(READ_ONLY)")
        else:
            if not self._postgres_loaded:
                self.con.execute("INSTALL postgres; LOAD postgres;")
                # renamed or gone in later versions: pushdown is the default then
                with contextlib.suppress(duckdb.Error):
                    self.con.execute("SET pg_experimental_filter_pushdown = true")
                self._postgres_loaded = True
            self.con.execute(f"ATTACH {_sql_str(dsn)} AS {alias} (TYPE postgres, READ_ONLY)")
        self.aliases[dsn] = alias
        return alias


class DataStore:
    """Holds the aggregated data of all datasets; (re)loads it in a background thread."""

    def __init__(self, specs: list[DatasetSpec]):
        self.specs = specs
        self.datasets: list[DatasetData] | None = None
        self.loaded_at: datetime | None = None
        self.progress: str | None = None  # set while a load is running
        self._lock = threading.Lock()

    @property
    def loading(self) -> bool:
        return self.progress is not None

    def reload(self) -> bool:
        """Start loading in the background; False if a load is already running."""
        with self._lock:
            if self.progress is not None:
                return False
            self.progress = "starting"
        threading.Thread(target=self._load_all, name="loader", daemon=True).start()
        return True

    def _load_all(self) -> None:
        t0 = time.monotonic()
        results: list[DatasetData] = []
        con = duckdb.connect(":memory:")
        try:
            con.execute("SET preserve_insertion_order = false")
            attachments = _Attachments(con)
            for i, spec in enumerate(self.specs, 1):
                self.progress = f"dataset {i} of {len(self.specs)}: {spec.name}"
                log.info("loading %s from %s", spec.name, spec.describe())
                try:
                    if spec.path is not None:
                        if not spec.path.exists():
                            raise FileNotFoundError(f"no QI results file at {spec.path}")
                        sql = _csv_source_sql(spec.path)
                    else:
                        sql = _db_source_sql(attachments.alias_for(spec.dsn), spec.schema)
                    data = load_dataset(con, spec, sql)
                except Exception as e:  # noqa: BLE001 — shown in the report, not a crash
                    msg = str(e)
                    if spec.dsn:  # driver errors may echo the DSN, password included
                        msg = msg.replace(spec.dsn, _redact_dsn(spec.dsn))
                    log.warning("cannot load %s: %s", spec.name, msg)
                    data = DatasetData(spec.name, spec.describe(), error=msg)
                else:
                    log.info("loaded %s: %d rows, %d concepts, %s buckets, %.1fs", spec.name,
                             data.rows, len(data.series), data.unit, data.seconds)
                data.log_path = spec.log
                if spec.log is not None and spec.log.exists():
                    try:
                        data.log = read_run_log(spec.log)
                    except (OSError, ValueError) as e:
                        data.log = RunLog(spec.log, error=f"cannot read run log: {e}")
                    if data.log.error:
                        log.warning("run log of %s: %s", spec.name, data.log.error)
                data.profile_path = spec.profile
                if spec.profile is not None and spec.profile.exists():
                    try:
                        data.profile = read_profile(spec.profile, f"pf{i}-")
                    except (OSError, ET.ParseError) as e:
                        data.profile = Profile(spec.profile, error=f"cannot read profile: {e}")
                    if data.profile.error:
                        log.warning("profile of %s: %s", spec.name, data.profile.error)
                results.append(data)
        finally:
            con.close()
            with self._lock:
                self.datasets = results
                self.loaded_at = datetime.now().astimezone()
                self.progress = None
        log.info("all datasets loaded in %.1fs", time.monotonic() - t0)


# ==========================================================================
# Report model: the loaded data joined with the current concepts file


@dataclass
class QIView:
    """One QI in one dataset: its result series and its debug series."""

    qi: str
    result: tuple[Concept, Series] | None
    debug: list[tuple[Concept, Series]]


def qi_views(d: DatasetData, concepts: dict[int, Concept]) -> dict[str, QIView]:
    views: dict[str, QIView] = {}
    for cid, series in sorted(d.series.items()):
        c = concepts.get(cid)
        if c is None:
            continue
        view = views.setdefault(c.qi, QIView(c.qi, None, []))
        if c.debug:
            view.debug.append((c, series))
        elif view.result is None:
            view.result = (c, series)
    return dict(sorted(views.items()))


def unknown_concepts(d: DatasetData, concepts: dict[int, Concept]) -> list[tuple[int, int]]:
    return [(cid, s.n) for cid, s in sorted(d.series.items()) if cid not in concepts]


# ==========================================================================
# Formatting


def esc(text) -> str:
    return html.escape(str(text)) if text is not None else ""


def fmt_int(n: int | None) -> str:
    return "—" if n is None else f"{n:,}"


def fmt_num(v: float | None) -> str:
    if v is None:
        return "—"
    if v == 0 or 1e-3 <= abs(v) < 1e7:
        return f"{v:,.4g}" if abs(v) < 1000 else f"{v:,.0f}"
    return f"{v:.3g}"


def fmt_pct(v: float | None) -> str:
    return "—" if v is None else f"{v:.1%}"


def fmt_duration(s: float | None) -> str:
    if s is None:
        return "—"
    if s < 60:
        return f"{s:.1f} s"
    m, sec = divmod(round(s), 60)
    if m < 60:
        return f"{m} min {sec:02d} s"
    h, m = divmod(m, 60)
    return f"{h} h {m:02d} min"


UNIT_LABEL = {"week": "week", "month": "month", "year": "year"}
PERIOD_COLUMN = {"week": "Week (from Monday)", "month": "Month", "year": "Year"}


def fmt_period(d: date, unit: str) -> str:
    if unit == "year":
        return f"{d.year}"
    if unit == "month":
        return f"{d.year}-{d.month:02d}"
    return d.isoformat()


def anchor(text: str) -> str:
    return "s-" + "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in text)


# ==========================================================================
# Charts: inline SVG panels sharing a time axis

_W = 1000
_PAD_L, _PAD_R, _PAD_T, _PAD_B = 64, 16, 8, 6
_PLOT_H = 96  # height of the plot area of one panel
_AXIS_H = 22  # extra height of the panel carrying the x-axis labels
_INNER_W = _W - _PAD_L - _PAD_R


class TimeAxis:
    def __init__(self, unit: str, starts: list[date]):
        self.unit = unit
        self.first = min(starts)
        self.last = max(starts)
        self.x0 = self.first.toordinal()
        self.x1 = bucket_end(self.last, unit).toordinal()

    def x(self, d: date) -> float:
        return _PAD_L + (d.toordinal() - self.x0) / (self.x1 - self.x0) * _INNER_W

    def span(self, start: date) -> tuple[float, float]:
        return self.x(start), self.x(bucket_end(start, self.unit))

    def mid(self, start: date) -> float:
        a, b = self.span(start)
        return (a + b) / 2

    def all_starts(self) -> list[date]:
        out, d = [], self.first
        while d <= self.last:
            out.append(d)
            d = bucket_end(d, self.unit)
        return out


def nice_ticks(lo: float, hi: float, target: int = 4) -> list[float]:
    """Round tick values (1/2/5 × 10^k steps) spanning [lo, hi]."""
    if lo == hi:
        pad = abs(lo) * 0.1 or 1.0
        lo, hi = lo - pad, hi + pad
    raw = (hi - lo) / (target - 1)
    magnitude = 10 ** math.floor(math.log10(raw))
    step = next(m * magnitude for m in (1, 2, 5, 10) if m * magnitude >= raw)
    ticks = [math.floor(lo / step) * step]
    while ticks[-1] < hi - 1e-9 * step:
        ticks.append(ticks[-1] + step)
    return [round(t, 12) for t in ticks]


def _axis_label(v: float, pct: bool) -> str:
    if pct:
        return f"{v:.0%}"
    if v == 0 or 1e-3 <= abs(v) < 1e7:
        return f"{v:,.6g}"
    return f"{v:.2g}"


def _runs(axis: TimeAxis, buckets: list[Bucket]) -> list[list[Bucket]]:
    """Split buckets into runs of consecutive periods (lines break at gaps)."""
    runs: list[list[Bucket]] = []
    for b in buckets:
        if runs and bucket_end(runs[-1][-1].start, axis.unit) == b.start:
            runs[-1].append(b)
        else:
            runs.append([b])
    return runs


def _panel(axis: TimeAxis, label: str, note: str, y_ticks: list[float], pct: bool,
           marks: list[str], hits: list[str], show_x: bool, info: str = "",
           unit: str = "") -> str:
    height = _PAD_T + _PLOT_H + _PAD_B + (_AXIS_H if show_x else 0)
    parts = []
    for v in y_ticks:
        yy = _y(v, y_ticks)
        parts.append(f'<line class="grid" x1="{_PAD_L}" x2="{_W - _PAD_R}" '
                     f'y1="{yy:.1f}" y2="{yy:.1f}"/>'
                     f'<text class="axis" x="{_PAD_L - 6}" y="{yy + 4:.1f}" '
                     f'text-anchor="end">{_axis_label(v, pct)}</text>')
    base_y = _PAD_T + _PLOT_H
    if show_x:
        starts = axis.all_starts()
        stride = max(1, math.ceil(len(starts) / 7))  # constant spacing, at most 7 labels
        for i in range(0, len(starts), stride):
            xx = axis.mid(starts[i])
            parts.append(f'<line class="tick" x1="{xx:.1f}" x2="{xx:.1f}" y1="{base_y}" '
                         f'y2="{base_y + 4}"/>'
                         f'<text class="axis" x="{xx:.1f}" y="{base_y + 17}" '
                         f'text-anchor="middle">{fmt_period(starts[i], axis.unit)}</text>')
    parts.append(f'<line class="baseline" x1="{_PAD_L}" x2="{_W - _PAD_R}" '
                 f'y1="{base_y}" y2="{base_y}"/>')
    return f"""<figure class="panel">
  <figcaption>{esc(label)}{unit_html(unit)} <span class="dim">{note}</span>{info}</figcaption>
  <svg viewBox="0 0 {_W} {height}" role="img" aria-label="{esc(label)} over time">
    {"".join(parts)}
    {"".join(marks)}
    {"".join(hits)}
  </svg>
</figure>"""


def _y(v: float, ticks: list[float]) -> float:
    lo, hi = ticks[0], ticks[-1]
    return _PAD_T + (1 - (v - lo) / (hi - lo)) * _PLOT_H


def _hit(axis: TimeAxis, b: Bucket, tip: str) -> str:
    a, z = axis.span(b.start)
    return (f'<rect class="hit" x="{a:.1f}" y="{_PAD_T}" width="{max(z - a, 1):.1f}" '
            f'height="{_PLOT_H}" data-b="{b.start.isoformat()}" data-tip="{esc(tip)}"/>')


def bars_panel(axis: TimeAxis, label: str, note: str, buckets: list[Bucket],
               show_x: bool) -> str:
    ticks = nice_ticks(0, max([b.n for b in buckets] + [1]))
    marks, hits = [], []
    for b in buckets:
        a, z = axis.span(b.start)
        w = min(24.0, max(1.0, z - a - 2))  # 2px gap between neighbours, <= 24px
        x = (a + z) / 2 - w / 2
        top, bottom = _y(b.n, ticks), _y(0, ticks)
        h = bottom - top
        r = 4 if w >= 8 and h >= 4 else 0
        if r:
            marks.append(f'<path class="bar" d="M{x:.1f},{bottom:.1f} V{top + r:.1f} '
                         f'Q{x:.1f},{top:.1f} {x + r:.1f},{top:.1f} H{x + w - r:.1f} '
                         f'Q{x + w:.1f},{top:.1f} {x + w:.1f},{top + r:.1f} V{bottom:.1f} Z"/>')
        elif h > 0:
            marks.append(f'<rect class="bar" x="{x:.1f}" y="{top:.1f}" width="{w:.1f}" '
                         f'height="{h:.1f}"/>')
        hits.append(_hit(axis, b, f"{fmt_period(b.start, axis.unit)}: "
                                  f"{b.n:,} patient-days"))
    return _panel(axis, label, note, ticks, False, marks, hits, show_x)


def line_panel(axis: TimeAxis, label: str, note: str, buckets: list[Bucket], band: bool,
               pct: bool, show_x: bool, info: str = "", unit: str = "") -> str:
    pts = [b for b in buckets if b.mean is not None]
    if not pts:
        return ""
    lows = [b.q25 if band and b.q25 is not None else b.mean for b in pts]
    highs = [b.q75 if band and b.q75 is not None else b.mean for b in pts]
    lo, hi = min(lows), max(highs)
    if pct:
        lo, hi = min(0.0, lo), max(1.0, hi)
    ticks = nice_ticks(lo, hi)
    marks, hits = [], []
    for run in _runs(axis, pts):
        if band:
            upper = " ".join(f"{axis.mid(b.start):.1f},{_y(_hi(b), ticks):.1f}" for b in run)
            lower = " ".join(f"{axis.mid(b.start):.1f},{_y(_lo(b), ticks):.1f}"
                             for b in reversed(run))
            marks.append(f'<polygon class="band" points="{upper} {lower}"/>')
        if len(run) > 1:
            line = " ".join(f"{axis.mid(b.start):.1f},{_y(b.mean, ticks):.1f}" for b in run)
            marks.append(f'<polyline class="line" points="{line}"/>')
    small = len(pts) <= 60
    # isolated points (no neighbour to draw a line to) always get a marker
    isolated = {run[0].start for run in _runs(axis, pts) if len(run) == 1}
    for b in pts:
        if small or b.start in isolated:
            marks.append(f'<circle class="dot" cx="{axis.mid(b.start):.1f}" '
                         f'cy="{_y(b.mean, ticks):.1f}" r="{4 if small else 2.5}"/>')
        period = fmt_period(b.start, axis.unit)
        if pct:
            tip = f"{period}: mean adherence {fmt_pct(b.mean)} (n={b.n:,})"
        else:
            u = f" {unit}" if unit else ""
            tip = (f"{period}: mean {fmt_num(b.mean)}{u} · IQR {fmt_num(b.q25)}–"
                   f"{fmt_num(b.q75)}{u} · range {fmt_num(b.min)}–{fmt_num(b.max)}{u} · "
                   f"n={b.n:,}")
        hits.append(_hit(axis, b, tip))
    return _panel(axis, label, note, ticks, pct, marks, hits, show_x, info, unit)


def _lo(b: Bucket) -> float:
    return b.q25 if b.q25 is not None else b.mean


def _hi(b: Bucket) -> float:
    return b.q75 if b.q75 is not None else b.mean


# ==========================================================================
# HTML: blocks


def result_problem(view: QIView, unit: str) -> str | None:
    """Why the QI result looks broken in this dataset, or None if it looks plausible."""
    if view.result is None or view.result[1].n == 0:
        return "no QI result rows"
    means = [b.mean for b in view.result[1].buckets if b.mean is not None]
    if not means:
        return "no adherence values"
    if all(m == 0 for m in means):
        return f"adherence is 0 in every {unit}"
    return None


def qi_block(d: DatasetData, view: QIView, heading: str, block_id: str,
             subtitle: str = "") -> str:
    """Charts and tables of one QI in one dataset.

    The debug panels sit in a collapsible block that starts expanded when the
    QI result looks broken (see ``result_problem``). Result and debug panels
    share one time axis, and each group carries its own x-axis labels so that
    either reads on its own.
    """
    all_series = ([view.result[1]] if view.result else []) + [s for _, s in view.debug]
    starts = [b.start for s in all_series for b in s.buckets]
    axis = TimeAxis(d.unit, starts)
    unit = UNIT_LABEL[d.unit]
    problem = result_problem(view, unit)
    meta = []
    # panel makers take show_x; it is set on the last panel of each group
    result_items: list[Callable[[bool], str]] = []
    if view.result:
        concept, series = view.result
        periods = len(series.buckets)
        meta.append(f"<b>{fmt_int(series.n)}</b> patient-days in the initial population "
                    f"over {periods} {unit}{'s' if periods != 1 else ''}")
        meta.append(f"mean adherence <b>{fmt_pct(series.mean)}</b>")
        result_items.append(lambda x, s=series: bars_panel(
            axis, "Initial population", f"patient-days per {unit}", s.buckets, x))
        if _has_means(series):
            result_items.append(lambda x, s=series: line_panel(
                axis, "Adherence", f"mean per {unit}", s.buckets, False, True, x))
    debug = [(c, s) for c, s in view.debug if _has_means(s)]
    debug_items: list[Callable[[bool], str]] = [
        lambda x, c=c, s=s: line_panel(
            axis, c.short_name, f"OMOP:{c.concept_id} · n={s.n:,}", s.buckets,
            True, False, x, concept_info(c), c.unit)
        for c, s in debug]

    def render(items: list[Callable[[bool], str]]) -> str:
        return "".join(make(i == len(items) - 1) for i, make in enumerate(items))

    parts = [render(result_items)]
    if debug_items:
        why = " · expanded because the QI result looks broken" if problem else ""
        parts.append(f"""<details class="debug"{" open" if problem else ""}>
  <summary>Debug values <span class="dim">— {len(debug_items)} quantit{"y" if len(debug_items) == 1 else "ies"},
    mean per {unit}, band = interquartile range, each with its own scale{why}</span></summary>
  {render(debug_items)}
</details>""")
    tables = []
    if view.result:
        tables.append(_result_table(d.unit, view.result[0], view.result[1]))
    tables.extend(_debug_table(d.unit, c, s) for c, s in view.debug)
    warning = f'<p class="banner warn">⚠️ {esc(problem)}</p>' if problem else ""
    return f"""<section class="block" id="{block_id}">
  <h2>{heading}</h2>
  {subtitle}
  <p class="meta">{" · ".join(meta)}</p>
  {warning}
  <div class="fig">{"".join(parts)}</div>
  <details><summary>Table view</summary>{"".join(tables)}</details>
</section>"""


def empty_qi_block(qi: str, heading: str, block_id: str, subtitle: str = "") -> str:
    return f"""<section class="block" id="{block_id}">
  <h2>{heading}</h2>
  {subtitle}
  <p class="banner warn">⚠️ no data at all: neither QI result rows nor debug values for {esc(qi)}</p>
</section>"""


def qi_label(qi: str, info: dict[str, QIInfo], short: bool = False) -> str:
    """Plain-text QI label such as "QI01 ventilation" (the short name if ``short``)."""
    i = info.get(qi)
    name = (i.nav_name if short else i.name or i.short_name) if i else ""
    return f"{qi} {name}" if name else qi


def qi_heading(qi: str, info: dict[str, QIInfo]) -> str:
    """HTML QI label for headings and tables, with the full title as tooltip."""
    i = info.get(qi)
    name = (i.name or i.short_name) if i else ""
    tip = f' title="{esc(i.title)}"' if i and i.title else ""
    return (f'<span{tip}>{esc(qi)} <span class="qname">{esc(name)}</span></span>' if name
            else f"<span{tip}>{esc(qi)}</span>")


def qi_subtitle(qi: str, info: dict[str, QIInfo]) -> str:
    i = info.get(qi)
    return f'<p class="qtitle">{esc(i.title)}</p>' if i and i.title else ""


def unit_html(unit: str) -> str:
    return f' <span class="unit" title="unit">{esc(unit)}</span>' if unit else ""


def concept_info(c: Concept) -> str:
    """The free-form columns of a debug concept, as a line below its panel title."""
    if not c.extra:
        return ""
    items = " · ".join(esc(v) if k.lower() == "description"
                       else f'<span class="k">{esc(k)}:</span> {esc(v)}' for k, v in c.extra)
    return f'<span class="info">{items}</span>'


def _has_means(s: Series) -> bool:
    return any(b.mean is not None for b in s.buckets)


def _result_table(unit: str, c: Concept, s: Series) -> str:
    rows = "".join(f"<tr><td>{fmt_period(b.start, unit)}</td><td>{fmt_int(b.n)}</td>"
                   f"<td>{fmt_pct(b.mean)}</td></tr>" for b in s.buckets)
    return f"""<p class="tcap">{esc(c.name)} <span class="chip">OMOP:{c.concept_id}</span></p>
<div class="tablewrap"><table class="stats num">
  <thead><tr><th>{PERIOD_COLUMN[unit]}</th><th>Patient-days in initial population</th>
    <th>Mean adherence</th></tr></thead>
  <tbody>{rows}</tbody>
</table></div>"""


def _debug_table(unit: str, c: Concept, s: Series) -> str:
    rows = "".join(
        f"<tr><td>{fmt_period(b.start, unit)}</td><td>{fmt_int(b.n)}</td>"
        f"<td>{fmt_num(b.mean)}</td><td>{fmt_num(b.q25)}</td><td>{fmt_num(b.q75)}</td>"
        f"<td>{fmt_num(b.min)}</td><td>{fmt_num(b.max)}</td></tr>" for b in s.buckets)
    return f"""<p class="tcap">{esc(c.name)}{unit_html(c.unit)}
  <span class="chip">OMOP:{c.concept_id}</span>{concept_info(c)}</p>
<div class="tablewrap"><table class="stats num">
  <thead><tr><th>{PERIOD_COLUMN[unit]}</th><th>n</th><th>Mean</th><th>25th pct.</th>
    <th>75th pct.</th><th>Min</th><th>Max</th></tr></thead>
  <tbody>{rows}</tbody>
</table></div>"""


def dataset_meta(d: DatasetData) -> str:
    if d.error:
        return f'<p class="banner problem">⛔ {esc(d.error)}</p>'
    span = (f"{d.first.isoformat()} – {d.last.isoformat()}" if d.first and d.last else "—")
    return (f'<p class="meta">{esc(d.source)} · {fmt_int(d.rows)} rows loaded in '
            f'{d.seconds:.1f}s · dates {span} · binned by <b>{d.unit}</b></p>')


#: marks problems reported by a run log (failures, incomplete run); ⚠️ is kept for
#: QI results without plausible adherence information
RUN_SIGN = "❗"


def fmt_rate(count: int, patients: int | None) -> str:
    """``count`` per 1,000 patients."""
    if not patients:
        return "—"
    r = count * 1000 / patients
    return f"{r:,.1f}" if r < 100 else f"{r:,.0f}"


def run_problem(d: DatasetData) -> bool:
    """Whether the run log reports a problem (worth a RUN_SIGN in the sidebar)."""
    return d.log is not None and bool(d.log.error or d.log.failures)


def run_section(d: DatasetData) -> str:
    """Run time, patient counts and message frequencies from the dataset's run log."""
    head = '<section class="block" id="s-run"><h2>Evaluation run</h2>'
    if d.log is None:
        where = (f"no run log at {esc(d.log_path)}" if d.log_path
                 else "no run log configured for this dataset")
        return f'{head}<p class="meta">{where}.</p></section>'
    lg = d.log
    started = f" · started {lg.started:%Y-%m-%d %H:%M}" if lg.started else ""
    parts = [head, f'<p class="meta">{esc(lg.path)}{started}</p>']
    if lg.error:
        parts.append(f'<p class="banner warn">{RUN_SIGN} {esc(lg.error)}</p>')
    if lg.seconds is not None:
        fail_cls = " problem-text" if lg.failures else ""
        fail_v = f"{RUN_SIGN} {fmt_int(lg.failures)}" if lg.failures else fmt_int(lg.failures)
        tiles = [("Time taken", fmt_duration(lg.seconds), f"{lg.seconds:,.3f} s", ""),
                 ("Patients processed", fmt_int(lg.patients), "successes + failures", ""),
                 ("Successes", fmt_int(lg.successes), "", ""),
                 ("Failures", fail_v, "", fail_cls),
                 ("Warnings", fmt_int(lg.warnings_total),
                  f"{fmt_rate(lg.warnings_total or 0, lg.patients)} per 1,000 patients · "
                  f"{len(lg.warnings)} kind(s)", "")]
        parts.append('<div class="tiles">' + "".join(
            f'<div class="tile"><div class="k">{k}</div><div class="v{cls}">{v}</div>'
            f'<div class="sub">{esc(sub)}</div></div>' for k, v, sub, cls in tiles) + "</div>")
    if lg.warnings:
        total = lg.warnings_total or sum(w.count for w in lg.warnings)
        rows = "".join(
            f'<tr><td>{fmt_int(w.count)}</td><td>{fmt_rate(w.count, lg.patients)}</td>'
            f'<td>{fmt_pct(w.count / total if total else None)}</td><td class="lvl">{esc(w.level)}</td><td class="msg">{esc(w.message)}</td></tr>'
            for w in lg.warnings)
        parts.append(f"""<div class="tablewrap"><table class="stats num msgs">
  <thead><tr><th>Count</th><th>Per 1,000 patients</th><th>Share</th><th>Level</th>
    <th>Message (example)</th></tr></thead>
  <tbody>{rows}</tbody></table></div>
<p class="meta">Each row is one kind of warning; the engine shows the first message of that
  kind as an example, so ids and values in it (e.g. a body height or a record id) belong to
  that one case only. The count covers every warning of the kind. A warning can occur several
  times per patient, so the rate per 1,000 patients may exceed 1,000.</p>""")
        listed = sum(w.count for w in lg.warnings)
        if lg.warnings_total is not None and listed != lg.warnings_total:
            parts.append(f'<p class="meta">The messages listed add up to {listed:,}, not '
                         f"{lg.warnings_total:,} warnings.</p>")
    elif lg.seconds is not None:
        parts.append('<p class="meta">No warnings.</p>')
    parts.append("</section>")
    return "\n".join(parts)


def profile_section(d: DatasetData) -> str:
    """The run's flamegraph, collapsed; the SVG is fetched when the section is opened."""
    head = '<section class="block" id="s-profile"><h2>Profile</h2>'
    pf = d.profile
    if pf is None:
        where = (f"no profile at {esc(d.profile_path)}" if d.profile_path
                 else "no profile configured for this dataset")
        return f'{head}<p class="meta">{where}.</p></section>'
    if pf.error:
        return (f'{head}<p class="meta">{esc(pf.path)}</p>'
                f'<p class="banner warn">{esc(pf.error)}</p></section>')
    total = (f" · {fmt_duration(pf.total_ms / 1000)} of evaluation time (summed over "
             "patients)" if pf.total_ms else "")
    return f"""{head}<p class="meta">{esc(pf.path)} · {pf.frames:,} frames{total}</p>
<details class="profile"><summary>Flamegraph</summary>
  <div class="flamebar">
    <button type="button" data-zoom="fit">Fit width</button>
    <button type="button" data-zoom="1">100 %</button>
    <button type="button" data-zoom="out">Zoom out</button>
    <button type="button" data-zoom="in">Zoom in</button>
    <span class="meta">Hover a frame for its name and time; click it to zoom to it.
      Times are summed over all patients, so they may exceed the run's time taken.</span>
  </div>
  <div class="flamewrap" data-src="/profile/{quote(d.name, safe='')}">Loading…</div>
</details></section>"""


def unknown_note(d: DatasetData, concepts: dict[int, Concept]) -> str:
    unknown = unknown_concepts(d, concepts)
    if not unknown:
        return ""
    items = ", ".join(f"{cid} ({n:,} values)" for cid, n in unknown)
    return (f'<p class="banner warn">⚠️ {len(unknown)} concept id(s) in the data are not '
            f'listed in the concepts files and are not shown: {esc(items)}</p>')


# ==========================================================================
# HTML: pages


@dataclass
class Page:
    title: str
    body: str
    toc: list[tuple[str, str]]  # (anchor, label) of the page's sections, for the sidebar
    current: str  # path of this page, to highlight it in the sidebar


class Report:
    def __init__(self, store: DataStore, catalog: ConceptCatalog, settings: Settings):
        self.store = store
        self.catalog = catalog
        self.settings = settings

    @property
    def multi(self) -> bool:
        return len(self.settings.datasets) > 1

    # -- routing --------------------------------------------------------------

    def profile_svg(self, name: str) -> tuple[HTTPStatus, str]:
        """The cleaned flamegraph SVG of dataset ``name``, fetched by the profile section."""
        d = next((d for d in self.store.datasets or [] if d.name == name), None)
        if d is None or d.profile is None or d.profile.svg is None:
            return HTTPStatus.NOT_FOUND, "no profile for this dataset"
        return HTTPStatus.OK, d.profile.svg

    def render(self, path: str) -> tuple[HTTPStatus, str]:
        datasets = self.store.datasets
        if datasets is None:
            return HTTPStatus.OK, self._loading_page()
        concepts = self.catalog.get()
        info = self.catalog.qi_info()
        parts = [unquote(p) for p in path.strip("/").split("/") if p]
        page: Page | None = None
        if not parts:
            page = (self._overview(datasets, concepts, info) if self.multi
                    else self._dataset_page(datasets[0], concepts, info, "/"))
        elif len(parts) == 2 and parts[0] == "dataset":
            d = next((d for d in datasets if d.name == parts[1]), None)
            if d is not None:
                page = self._dataset_page(d, concepts, info, path)
        elif len(parts) == 2 and parts[0] == "qi":
            page = self._qi_page(parts[1], datasets, concepts, info, path)
        if page is None:
            return HTTPStatus.NOT_FOUND, self._layout(
                Page("Not found", '<h1>Not found</h1><p><a href="/">Back to the report</a></p>',
                     [], ""), datasets, concepts, info)
        return HTTPStatus.OK, self._layout(page, datasets, concepts, info)

    # -- pages ------------------------------------------------------------------

    def _overview(self, datasets: list[DatasetData], concepts: dict[int, Concept],
                  info: dict[str, QIInfo]) -> Page:
        views = {d.name: qi_views(d, concepts) for d in datasets if not d.error}
        qis = sorted({qi for v in views.values() for qi in v})
        ok = [d for d in datasets if not d.error]
        head1 = "".join(f'<th colspan="2" class="grp"><a href="{_ds_url(d.name)}">'
                        f"{esc(d.name)}</a></th>" for d in ok)
        head2 = "".join("<th>Patient-days</th><th>Adherence</th>" for _ in ok)
        rows = []
        for qi in qis:
            cells = []
            for d in ok:
                v = views[d.name].get(qi)
                if v and v.result:
                    s = v.result[1]
                    cells.append(f'<td class="grp">{fmt_int(s.n)}</td><td>{fmt_pct(s.mean)}</td>')
                else:
                    cells.append('<td class="grp dim">—</td><td class="dim">—</td>')
            rows.append(f'<tr><th><a href="{_qi_url(qi)}">{qi_heading(qi, info)}</a></th>'
                        f'{"".join(cells)}</tr>')
        comparison = (f"""<div class="tablewrap"><table class="stats num">
  <thead><tr><th></th>{head1}</tr><tr><th>QI</th>{head2}</tr></thead>
  <tbody>{"".join(rows)}</tbody></table></div>""" if qis and ok
                      else '<p class="meta">No QI results in any dataset.</p>')
        def run_cells(d: DatasetData) -> str:
            lg = d.log
            if lg is None or lg.seconds is None:
                why = ("no run log" if lg is None else lg.error or "")
                if lg is None:
                    return f'<td colspan="5" class="grp dim" title="{esc(why)}">—</td>'
                return (f'<td colspan="5" class="grp problem-text" title="{esc(why)}">'
                        f"{RUN_SIGN} run incomplete</td>")
            fail = (f'<td class="problem-text">{RUN_SIGN} {fmt_int(lg.failures)}</td>'
                    if lg.failures else f"<td>{fmt_int(lg.failures)}</td>")
            return (f'<td class="grp" title="{lg.seconds:,.3f} s">{fmt_duration(lg.seconds)}</td>'
                    f"<td>{fmt_int(lg.patients)}</td>{fail}<td>{fmt_int(lg.warnings_total)}</td>"
                    f"<td>{fmt_rate(lg.warnings_total or 0, lg.patients)}</td>")

        ds_rows = "".join(
            f'<tr><th><a href="{_ds_url(d.name)}">{esc(d.name)}</a></th>'
            + (f'<td colspan="4" class="problem-text">⛔ {esc(d.error)}</td>' if d.error else
               f"<td>{fmt_int(d.rows)}</td><td>{d.first} – {d.last}</td><td>{d.unit}</td>"
               f'<td class="src">{esc(d.source)}</td>')
            + run_cells(d) + "</tr>" for d in datasets)
        body = f"""<h1>QI results — overview</h1>
{self._intro()}
<section class="block" id="s-comparison">
  <h2>Cross-dataset comparison</h2>
  <p class="meta">Per QI and dataset: patient-days in the initial population and mean
    adherence, each over the dataset's whole time range.</p>
  {comparison}
</section>
<section class="block" id="s-datasets">
  <h2>Datasets</h2>
  <div class="tablewrap"><table class="stats">
    <thead><tr><th></th><th colspan="4">QI results</th>
      <th colspan="5" class="grp">Evaluation run (log)</th></tr>
      <tr><th>Dataset</th><th>Rows</th><th>Dates</th><th>Binned by</th><th>Source</th>
      <th class="grp">Time taken</th><th>Patients</th><th>Failures</th><th>Warnings</th>
      <th>Warnings / 1,000 patients</th></tr>
    </thead>
    <tbody>{ds_rows}</tbody></table></div>
</section>"""
        return Page("Overview", body,
                    [("s-comparison", "Cross-dataset comparison"), ("s-datasets", "Datasets")],
                    "/")

    def _dataset_page(self, d: DatasetData, concepts: dict[int, Concept],
                      info: dict[str, QIInfo], path: str) -> Page:
        parts = [f"<h1>{esc(d.name)}</h1>", dataset_meta(d)]
        toc = []
        if not self.multi:
            parts.append(self._intro())
        parts.append(run_section(d))
        toc.append(("s-run", f"Evaluation run {RUN_SIGN}" if run_problem(d) else "Evaluation run"))
        parts.append(profile_section(d))
        toc.append(("s-profile", "Profile"))
        if not d.error:
            parts.append(unknown_note(d, concepts))
            views = qi_views(d, concepts)
            # every QI of the concepts files, so that QIs without any data show up too
            known = {c.qi for c in concepts.values() if not c.debug}
            for qi in sorted(known | set(views)):
                a = anchor(qi)
                link = (f' <a class="xref" href="{_qi_url(qi)}">compare across datasets →</a>'
                        if self.multi and qi in views else "")
                view = views.get(qi)
                heading, subtitle = qi_heading(qi, info), qi_subtitle(qi, info)
                if view is None:
                    parts.append(empty_qi_block(qi, heading, a, subtitle))
                    bad = True
                else:
                    parts.append(qi_block(d, view, f"{heading}{link}", a, subtitle))
                    bad = result_problem(view, d.unit) is not None
                label = qi_label(qi, info, short=True)
                toc.append((a, f"{label} ⚠️" if bad else label))
            if not views:
                parts.append('<p class="meta">No rows for any QI result or debug concept of '
                             "the concepts files.</p>")
        return Page(d.name, "\n".join(parts), toc, path)

    def _qi_page(self, qi: str, datasets: list[DatasetData], concepts: dict[int, Concept],
                 info: dict[str, QIInfo], path: str) -> Page | None:
        found = [(d, qi_views(d, concepts).get(qi)) for d in datasets if not d.error]
        found = [(d, v) for d, v in found if v is not None]
        known = any(c.qi == qi for c in concepts.values())
        if not found and not known:
            return None
        rows = "".join(
            f'<tr><th><a href="#{anchor(d.name)}">{esc(d.name)}</a></th>'
            + (f"<td>{fmt_int(v.result[1].n)}</td><td>{len(v.result[1].buckets)} {d.unit}s</td>"
               f"<td>{fmt_pct(v.result[1].mean)}</td>" if v.result
               else '<td colspan="3" class="dim">debug values only</td>')
            + "</tr>" for d, v in found)
        missing = [d.name for d in datasets if d.name not in {d2.name for d2, _ in found}]
        parts = [f"<h1>{qi_heading(qi, info)} across datasets</h1>", qi_subtitle(qi, info)]
        toc = []
        if found:
            parts.append(f"""<section class="block" id="s-summary"><h2>Summary</h2>
<div class="tablewrap"><table class="stats num">
  <thead><tr><th>Dataset</th><th>Patient-days</th><th>Periods</th><th>Mean adherence</th></tr></thead>
  <tbody>{rows}</tbody></table></div>
{f'<p class="meta">No data for {esc(qi)} in: {esc(", ".join(missing))}</p>' if missing else ""}
<p class="meta">Each dataset is binned by its own period and time range, so the charts below
  do not share a time axis.</p></section>""")
            toc.append(("s-summary", "Summary"))
        else:
            parts.append(f'<p class="meta">No dataset has data for {esc(qi)}.</p>')
        for d, v in found:
            a = anchor(d.name)
            heading = (f'{esc(d.name)} <a class="xref" href="{_ds_url(d.name)}#{anchor(qi)}">'
                       "dataset page →</a>")
            parts.append(qi_block(d, v, heading, a))
            toc.append((a, d.name))
        return Page(f"{qi_label(qi, info)} across datasets", "\n".join(parts), toc, path)

    def _intro(self) -> str:
        return """<details class="banner info"><summary>How to read this report</summary>
  <p>The QI results carry no patient identity, so every count is of
  <b>patient-days</b>: one result row per patient and 24-hour period in which the QI's
  initial population criterion held — a patient with several qualifying days counts
  once per day. Adherence is the mean of the per-day ratios (numerator / denominator).
  Values are aggregated by calendar period; the period size (week / month / year) is
  chosen per dataset from the span of its dates, so datasets whose dates are shifted to
  arbitrary, widely scattered years (e.g. MIMIC-IV) get coarser periods and may show
  artefacts at year boundaries.</p>
  <p>Debug values are intermediate quantities written by the QI calculation alongside
  each 24-hour result; they have no common range, so each gets its own panel and scale.
  Hover a chart for the values of a period; “Table view” lists them all.</p>
  <p>Signs: ⚠️ marks a QI without plausible adherence information (no result rows, no
  adherence values, or adherence 0 throughout); ❗ marks an evaluation run whose log reports
  failures or has no final summary; ⛔ marks a dataset that could not be loaded.</p>
</details>"""

    def _loading_page(self) -> str:
        progress = esc(self.store.progress or "starting")
        return f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta http-equiv="refresh" content="2"><title>Loading — QI report</title>
<style>{CSS}</style></head>
<body class="loading"><main><h1>Loading QI results…</h1>
<p class="meta">Loading {progress}. This page refreshes automatically.</p></main></body></html>"""

    # -- layout -----------------------------------------------------------------

    def _layout(self, page: Page, datasets: list[DatasetData],
                concepts: dict[int, Concept], info: dict[str, QIInfo]) -> str:
        nav = self._sidebar(page, datasets, concepts, info)
        banners = []
        if self.store.loading:
            banners.append(f'<p class="banner info">⏳ Reloading data ({esc(self.store.progress)}) '
                           "— showing the previous data until it is done.</p>")
        if self.catalog.error:
            banners.append(f'<p class="banner warn">⚠️ {esc(self.catalog.error)} — using the '
                           "last version that could be read.</p>")
        return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{esc(page.title)} — QI report</title>
<style>{CSS}</style>
</head>
<body>
{nav}
<main>
{"".join(banners)}
{page.body}
</main>
<div id="tip" role="tooltip" hidden></div>
<script>{JS}</script>
</body>
</html>"""

    def _sidebar(self, page: Page, datasets: list[DatasetData],
                 concepts: dict[int, Concept], info: dict[str, QIInfo]) -> str:
        def toc_html() -> str:
            if not page.toc:
                return ""
            items = "".join(f'<li><a href="#{a}">{esc(label)}</a></li>' for a, label in page.toc)
            return f'<ul class="toc">{items}</ul>'

        def entry(href: str, label: str, extra: str = "") -> str:
            current = href == page.current or (href == "/" and page.current == "")
            cls = ' class="current"' if current else ""
            return (f'<li><a href="{href}"{cls}>{label}</a>{extra}'
                    f'{toc_html() if current else ""}</li>')

        sections = []
        if self.multi:
            sections.append(f'<ul>{entry("/", "Overview")}</ul>')
            ds_items = "".join(
                entry(_ds_url(d.name), esc(d.name),
                      ' <span class="problem-text" title="could not be loaded">⛔</span>'
                      if d.error else
                      f' <span title="the run log reports failures or is incomplete">{RUN_SIGN}</span>'
                      if run_problem(d) else "")
                for d in datasets)
            sections.append(f"<h3>Datasets</h3><ul>{ds_items}</ul>")
            qis = sorted({qi for d in datasets if not d.error for qi in qi_views(d, concepts)})
            qi_items = "".join(entry(_qi_url(qi), esc(qi_label(qi, info, short=True)))
                               for qi in qis)
            if qi_items:
                sections.append(f"<h3>QIs across datasets</h3><ul>{qi_items}</ul>")
        else:
            d = datasets[0]
            sections.append(f'<h3>{esc(d.name)}</h3>{toc_html()}')
        loaded = self.store.loaded_at.strftime("%Y-%m-%d %H:%M") if self.store.loaded_at else "—"
        button = ('<button type="submit" disabled>Reloading…</button>' if self.store.loading
                  else '<button type="submit">Reload data</button>')
        return f"""<nav class="sidebar">
  <div class="brand"><a href="/">QI report</a></div>
  {"".join(sections)}
  <form class="reload" method="post" action="/reload">
    <span class="meta">data loaded {loaded}</span>{button}
  </form>
</nav>"""


def _ds_url(name: str) -> str:
    return "/dataset/" + quote(name, safe="")


def _qi_url(qi: str) -> str:
    return "/qi/" + quote(qi, safe="")


# ==========================================================================
# Style and behaviour

CSS = """
:root {
  color-scheme: light;
  --surface: #fcfcfb; --surface-2: #f3f2ef; --border: rgba(0,0,0,0.12);
  --text: #0b0b0b; --text-2: #52514e; --text-3: #8a8984; --link: #1f63b5;
  --series: #2a78d6; --band: rgba(42,120,214,0.22); --grid: rgba(0,0,0,0.08);
  --axis: rgba(0,0,0,0.35); --hover: rgba(0,0,0,0.06);
  --problem-bg: #fdecea; --problem-border: #f5c6c0; --problem-text: #a8261b;
  --warn-bg: #fff7e0; --warn-border: #ecd9a0; --info-bg: #eef4fc; --info-border: #c9dbf3;
}
@media (prefers-color-scheme: dark) {
  :root {
    color-scheme: dark;
    --surface: #1a1a19; --surface-2: #232321; --border: rgba(255,255,255,0.14);
    --text: #f0efec; --text-2: #c3c2b7; --text-3: #8c8b83; --link: #7fb1ef;
    --series: #3987e5; --band: rgba(57,135,229,0.30); --grid: rgba(255,255,255,0.09);
    --axis: rgba(255,255,255,0.35); --hover: rgba(255,255,255,0.08);
    --problem-bg: #3a1f1c; --problem-border: #6b3029; --problem-text: #f19a90;
    --warn-bg: #352d14; --warn-border: #64552a; --info-bg: #1c2a3c; --info-border: #2f4866;
  }
}
* { box-sizing: border-box; }
html { scroll-padding-top: 12px; }
body { margin: 0; font: 15px/1.5 system-ui, -apple-system, "Segoe UI", sans-serif;
       color: var(--text); background: var(--surface);
       display: grid; grid-template-columns: 250px minmax(0, 1fr); min-height: 100vh; }
body.loading { display: block; }
a { color: var(--link); text-decoration: none; }
a:hover { text-decoration: underline; }
main { padding: 24px 32px 64px; max-width: 1180px; width: 100%; }
h1 { font-size: 24px; margin: 0 0 4px; }
h2 { font-size: 18px; margin: 0 0 2px; }
.meta { color: var(--text-2); font-size: 13px; margin: 2px 0 10px; }
.dim { color: var(--text-3); font-weight: normal; }
.problem-text { color: var(--problem-text); }
.banner { padding: 8px 12px; border-radius: 8px; font-size: 14px; margin: 8px 0 14px;
          border: 1px solid; }
.banner.problem { background: var(--problem-bg); border-color: var(--problem-border); }
.banner.warn { background: var(--warn-bg); border-color: var(--warn-border); }
.banner.info { background: var(--info-bg); border-color: var(--info-border); }
.banner p { margin: 6px 0; }
details > summary { cursor: pointer; }
.banner > summary { font-weight: 600; }

nav.sidebar { position: sticky; top: 0; height: 100vh; overflow-y: auto;
              border-right: 1px solid var(--border); background: var(--surface-2);
              padding: 18px 12px; font-size: 14px; display: flex; flex-direction: column; }
nav .brand { font-weight: 700; font-size: 16px; margin: 0 8px 10px; }
nav .brand a { color: var(--text); }
nav h3 { font-size: 11.5px; text-transform: uppercase; letter-spacing: .05em;
         color: var(--text-3); margin: 16px 8px 4px; }
nav ul { list-style: none; margin: 0; padding: 0; }
nav li > a { display: block; padding: 3px 8px; border-radius: 6px; color: var(--text); }
nav li > a:hover { background: var(--hover); text-decoration: none; }
nav li > a.current { font-weight: 600; background: var(--hover); }
nav li > span { margin-left: 4px; }
nav li:has(> span) > a { display: inline-block; }
nav ul.toc { margin: 2px 0 6px 10px; border-left: 2px solid var(--border); }
nav ul.toc a { font-size: 13px; color: var(--text-2); padding: 2px 8px; border-radius: 0 6px 6px 0;
               margin-left: -2px; border-left: 2px solid transparent; }
nav ul.toc a.active { color: var(--series); border-left-color: var(--series);
                      font-weight: 600; }
nav form.reload { margin-top: auto; padding: 16px 8px 0; display: flex; flex-direction: column;
                  gap: 6px; }
nav form.reload .meta { margin: 0; }
nav button { font: inherit; font-size: 13px; padding: 5px 10px; border-radius: 6px;
             border: 1px solid var(--border); background: var(--surface); color: var(--text);
             cursor: pointer; }
nav button:disabled { cursor: default; color: var(--text-3); }

section.block { border-top: 1px solid var(--border); margin-top: 26px; padding-top: 14px; }
h2 .xref { font-size: 13px; font-weight: normal; margin-left: 10px; }
.qname { font-weight: normal; color: var(--text-2); }
.qtitle { font-size: 14px; color: var(--text-2); margin: 0 0 4px; }
.fig { margin: 4px 0 6px; }
figure.panel { margin: 6px 0 0; }
figure.panel figcaption { font-size: 13px; font-weight: 600; }
.info { display: block; font-weight: normal; font-size: 12.5px; color: var(--text-2);
        max-width: 80ch; }
.info .k { color: var(--text-3); }
.unit { font-weight: normal; font-size: 12px; color: var(--text-2); margin-left: 4px;
        padding: 0 6px; border: 1px solid var(--border); border-radius: 4px; }
figure.panel svg { display: block; width: 100%; height: auto; overflow: visible; }
details.debug { margin-top: 16px; }
details.profile > summary { font-weight: 600; }
.flamebar { display: flex; flex-wrap: wrap; gap: 6px; align-items: center; margin: 8px 0; }
.flamebar button { font: inherit; font-size: 13px; padding: 3px 10px; border-radius: 6px;
  border: 1px solid var(--border); background: var(--surface, transparent); color: inherit;
  cursor: pointer; }
.flamebar .meta { margin: 0 0 0 6px; }
/* the flamegraph's colours are made for a light background, in either theme */
.flamewrap { overflow: auto; max-height: 75vh; background: #fff; color: #111;
  border: 1px solid var(--border); border-radius: 6px; }
svg.flame { display: block; }
svg.flame text { font: 12px system-ui, sans-serif; fill: #111; pointer-events: none; }
svg.flame g:hover > rect { stroke: #111 !important; stroke-width: 1.5px; }
svg.flame g { cursor: zoom-in; }
details.debug > summary { font-size: 13px; font-weight: 600; color: var(--text-2); }
svg .axis { font-size: 11px; fill: var(--text-2); font-variant-numeric: tabular-nums; }
svg .grid { stroke: var(--grid); }
svg .baseline, svg .tick { stroke: var(--axis); }
svg .bar { fill: var(--series); }
svg .line { fill: none; stroke: var(--series); stroke-width: 2; stroke-linejoin: round; }
svg .band { fill: var(--band); }
svg .dot { fill: var(--series); stroke: var(--surface); stroke-width: 1.5; }
svg .hit { fill: transparent; }
svg .hit.on { fill: var(--hover); }
#tip { position: fixed; z-index: 10; pointer-events: none; max-width: 420px;
       background: var(--text); color: var(--surface); font-size: 12.5px;
       padding: 5px 9px; border-radius: 6px; font-variant-numeric: tabular-nums; }

.tablewrap { overflow-x: auto; }
table.stats { border-collapse: collapse; font-size: 13.5px; margin: 4px 0 12px; }
table.stats th, table.stats td { text-align: left; padding: 4px 10px;
       border-bottom: 1px solid var(--border); white-space: nowrap; }
table.stats thead th { color: var(--text-2); font-weight: 600; font-size: 12.5px; }
table.stats tbody th { font-weight: 600; }
table.num td { text-align: right; font-variant-numeric: tabular-nums; }
table.stats .grp { border-left: 1px solid var(--border); }
table.stats thead th.grp { text-align: center; }
table.stats td.src { font-size: 12.5px; color: var(--text-2); }
.tiles { display: flex; flex-wrap: wrap; gap: 10px; margin: 8px 0 12px; }
.tile { border: 1px solid var(--border); border-radius: 8px; padding: 8px 14px; min-width: 140px; }
.tile .k { font-size: 12px; color: var(--text-2); }
.tile .v { font-size: 20px; font-weight: 600; font-variant-numeric: tabular-nums; }
.tile .sub { font-size: 11.5px; color: var(--text-3); min-height: 1em; }
table.msgs td.lvl { text-align: left; font-size: 12px; color: var(--text-2); }
table.msgs td.msg { text-align: left; white-space: normal; min-width: 40ch; }
.tcap { font-size: 13px; font-weight: 600; margin: 12px 0 0; }
.chip { display: inline-block; font-size: 11px; font-weight: 600; color: var(--text-2);
        background: var(--hover); border-radius: 10px; padding: 0 7px; margin-left: 4px; }

@media (max-width: 820px) {
  body { display: block; }
  nav.sidebar { position: static; height: auto; border-right: 0;
                border-bottom: 1px solid var(--border); }
  main { padding: 16px; }
}
"""

JS = """
(() => {
  // tooltip + highlight of the same period in every panel of a figure
  const tip = document.getElementById('tip');
  let lit = [];
  document.addEventListener('mouseover', e => {
    const h = e.target.closest && e.target.closest('rect.hit');
    lit.forEach(r => r.classList.remove('on'));
    lit = [];
    const fr = !h && e.target.closest && e.target.closest('svg.flame g[data-tip]');
    if (fr) { tip.textContent = fr.dataset.tip; tip.hidden = false; return; }
    if (!h) { tip.hidden = true; return; }
    const fig = h.closest('.fig') || document;
    lit = [...fig.querySelectorAll(`rect.hit[data-b="${h.dataset.b}"]`)];
    lit.forEach(r => r.classList.add('on'));
    tip.textContent = h.dataset.tip;
    tip.hidden = false;
  });
  document.addEventListener('mousemove', e => {
    if (tip.hidden) return;
    const r = tip.getBoundingClientRect();
    let x = e.clientX + 14, y = e.clientY + 14;
    if (x + r.width > innerWidth - 8) x = e.clientX - r.width - 14;
    if (y + r.height > innerHeight - 8) y = e.clientY - r.height - 14;
    tip.style.left = x + 'px'; tip.style.top = y + 'px';
  });

  // flamegraphs: fetched when their section is first opened; zooming stretches the
  // x axis only, so that labels keep their size
  function flame(wrap) {
    const svg = wrap.querySelector('svg.flame');
    const W = +svg.getAttribute('width');
    const els = [...svg.querySelectorAll('rect, text')].map(el =>
      [el, +el.getAttribute('x'), el.tagName === 'rect' ? +el.getAttribute('width') : null]);
    let scale = 1;
    const MAX = 2e6 / W;  // browsers cannot lay out arbitrarily wide boxes
    function zoom(s, x0) {  // x0: unscaled x to bring to the left edge (default: keep centre)
      const centre = (wrap.scrollLeft + wrap.clientWidth / 2) / scale;
      scale = Math.min(Math.max(s, wrap.clientWidth / W), MAX);
      for (const [el, x, w] of els) {
        el.setAttribute('x', x * scale);
        if (w !== null) el.setAttribute('width', w * scale);
      }
      svg.setAttribute('width', W * scale);
      wrap.scrollLeft = x0 !== undefined ? x0 * scale : centre * scale - wrap.clientWidth / 2;
    }
    wrap.parentElement.querySelector('.flamebar').addEventListener('click', e => {
      const z = e.target.dataset && e.target.dataset.zoom;
      if (!z) return;
      zoom(z === 'fit' ? 0 : z === 'in' ? scale * 2 : z === 'out' ? scale / 2 : +z);
    });
    svg.addEventListener('click', e => {
      const g = e.target.closest('g[data-tip]');
      const r = g && g.querySelector(':scope > rect');
      if (!r) return;
      const i = els.findIndex(([el]) => el === r);
      zoom(wrap.clientWidth / Math.max(els[i][2], 1e-9), els[i][1]);
    });
  }
  document.addEventListener('toggle', async e => {
    const d = e.target;
    if (!d.matches || !d.matches('details.profile') || !d.open) return;
    const wrap = d.querySelector('.flamewrap');
    if (wrap.dataset.loaded) return;
    wrap.dataset.loaded = '1';
    try {
      const r = await fetch(wrap.dataset.src);
      if (!r.ok) throw new Error(await r.text());
      wrap.innerHTML = await r.text();
      flame(wrap);
    } catch (err) {
      wrap.textContent = 'Cannot load the flamegraph: ' + err.message;
      delete wrap.dataset.loaded;
    }
  }, true);

  // sidebar table of contents follows the scroll position
  const links = [...document.querySelectorAll('nav ul.toc a[href^="#"]')];
  const targets = links.map(a => document.getElementById(decodeURIComponent(a.hash.slice(1))));
  let pending = false;
  function spy() {
    pending = false;
    let active = 0;
    targets.forEach((t, i) => { if (t && t.getBoundingClientRect().top <= 120) active = i; });
    if (innerHeight + scrollY >= document.documentElement.scrollHeight - 4)
      active = targets.length - 1;
    links.forEach((a, i) => a.classList.toggle('active', i === active));
    const a = links[active];
    if (a) {
      const nav = a.closest('nav'), ar = a.getBoundingClientRect(), nr = nav.getBoundingClientRect();
      if (ar.top < nr.top || ar.bottom > nr.bottom) a.scrollIntoView({block: 'nearest'});
    }
  }
  if (links.length) {
    addEventListener('scroll', () => { if (!pending) { pending = true; requestAnimationFrame(spy); } },
                     {passive: true});
    spy();
  }
})();
"""


# ==========================================================================
# HTTP server


class Handler(BaseHTTPRequestHandler):
    report: Report  # set on the subclass created in main()
    server_version = "qi-report"

    def do_GET(self) -> None:  # noqa: N802
        path = urlsplit(self.path).path
        if path == "/favicon.ico":
            self._send(HTTPStatus.NO_CONTENT, b"", "text/plain")
            return
        if path.startswith("/profile/"):
            status, svg = self.report.profile_svg(unquote(path.removeprefix("/profile/")))
            self._send(status, svg.encode("utf-8"),
                       "image/svg+xml" if status == HTTPStatus.OK else "text/plain")
            return
        try:
            status, body = self.report.render(path)
        except Exception:  # noqa: BLE001 — local tool: show the traceback
            log.exception("error rendering %s", path)
            status = HTTPStatus.INTERNAL_SERVER_ERROR
            body = f"<pre>{esc(traceback.format_exc())}</pre>"
        self._send(status, body.encode("utf-8"), "text/html; charset=utf-8")

    def do_POST(self) -> None:  # noqa: N802
        if urlsplit(self.path).path != "/reload":
            self._send(HTTPStatus.NOT_FOUND, b"not found", "text/plain")
            return
        if self.report.store.reload():
            log.info("reload requested")
        referer = urlsplit(self.headers.get("Referer") or "/")
        self.send_response(HTTPStatus.SEE_OTHER)
        self.send_header("Location", referer.path or "/")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _send(self, status: HTTPStatus, body: bytes, ctype: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args) -> None:  # noqa: A002
        log.debug("%s %s", self.address_string(), format % args)


def main(argv: list[str] | None = None) -> None:
    settings = parse_args(argv)
    catalog = ConceptCatalog(settings.result_concepts, settings.debug_concepts, settings.qi_definitions)
    catalog.get()
    if catalog.error:
        raise SystemExit(f"error: {catalog.error}")
    store = DataStore(settings.datasets)
    store.reload()
    handler = type("BoundHandler", (Handler,), {"report": Report(store, catalog, settings)})
    try:
        server = ThreadingHTTPServer((settings.host, settings.port), handler)
    except OSError as e:
        raise SystemExit(f"error: cannot listen on {settings.host}:{settings.port}: {e}") from e
    server.daemon_threads = True
    host = f"[{settings.host}]" if ":" in settings.host else settings.host
    log.info("serving the QI report on http://%s:%d/ (%s source, %d dataset(s))",
             host, settings.port, settings.source, len(settings.datasets))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    sys.exit(main())
