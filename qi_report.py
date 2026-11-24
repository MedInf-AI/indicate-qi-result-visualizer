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

Without datasets on the command line or a configuration file, the report shows
the *result bundles* of a directory (``results/``): one ``<YYYY-MM-DD>/``
directory of results, run logs and profiles per evaluation, which can be uploaded
as ``results-bundle-<YYYY-MM-DD>.tar.gz`` archives. Each bundle is shown with
its changes since an older one: the preceding one, or any other that is chosen.

Data is loaded once at startup (in the background; the server answers right
away) and on demand via the "Reload data" button. Run with ``--help`` for
usage; see README.md for the configuration file.

"""

from __future__ import annotations

import argparse
import contextlib
import csv
import html
import json
import logging
import math
import os
import re
import shutil
import sys
import tarfile
import tempfile
import threading
import time
import traceback
import xml.etree.ElementTree as ET
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path, PurePosixPath
from urllib.parse import parse_qs, quote, unquote, urlsplit

import duckdb
import yaml

__version__ = '0.1.0'

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

#: where result bundles are kept when no datasets are given (relative to the working directory)
DEFAULT_RESULTS_DIR = Path("results")
#: file name of an uploaded result bundle; its date names the bundle's directory
BUNDLE_ARCHIVE = re.compile(r"results-bundle-(\d{4}-\d\d-\d\d)\.(?:tar\.gz|tgz|tar)")
#: name of a bundle directory below the results directory
BUNDLE_DIR = re.compile(r"\d{4}-\d\d-\d\d")
#: files taken from an uploaded bundle (by base name); all others are skipped
BUNDLE_MEMBER = re.compile(r"(?:results-[\w.-]+\.csv(?:\.gz)?|log-[\w.-]+\.txt|profile-[\w.-]+\.svg)")

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
    datasets: list[DatasetSpec]  # empty in bundle mode
    host: str
    port: int
    #: bundle mode: the directory holding one ``<YYYY-MM-DD>/`` directory per result bundle
    results_dir: Path | None = None


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


def _bundle_specs(directory: Path) -> list[DatasetSpec]:
    """The datasets of a result bundle: every results file in its directory."""
    found = sorted([*directory.glob("results-*.csv"), *directory.glob("results-*.csv.gz")])
    return [_csv_spec(_dataset_name_from_path(p), p) for p in found]


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
               "  %(prog)s                               (result bundles in ./results/)\n"
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
    p.add_argument("-r", "--results-dir", type=Path,
                   help="directory of result bundles, one <YYYY-MM-DD>/ directory each, to which "
                        "bundles can be uploaded (default, and used only, when neither INPUT "
                        f"nor --config is given: {DEFAULT_RESULTS_DIR}/)")
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
                        f"{DEFAULT_RESULT_CONCEPTS.name} in data/ next to this script)")
    p.add_argument("--debug-concepts", type=Path,
                   help="debug concepts file (default: config 'debug_concepts', else "
                        f"{DEFAULT_DEBUG_CONCEPTS.name} in data/ next to this script)")
    p.add_argument("--qi-definitions", type=Path,
                   help="QI names and titles file (default: config 'qi_definitions', else "
                        f"{DEFAULT_QI_DEFINITIONS.name} in data/ next to this script)")
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
    results_dir = None
    if args.results_dir or not (args.inputs or args.config):
        if args.inputs or args.config:
            p.error("--results-dir cannot be combined with INPUT or --config")
        if args.source == "db":
            p.error("result bundles hold CSV files; --source db needs INPUT or --config")
        results_dir = (args.results_dir or DEFAULT_RESULTS_DIR).resolve()
        if results_dir.exists() and not results_dir.is_dir():
            raise SystemExit(f"error: {results_dir} is not a directory")
        specs = []
    elif args.inputs:
        if source == "csv":
            specs = _csv_specs_from_args(args.inputs)
        else:
            cfg_dsn = cfg.get("db", {}).get("dsn")
            specs = _db_specs_from_args(args.inputs,
                                        dsn or (os.path.expandvars(cfg_dsn) if cfg_dsn else None))
    else:
        specs = _specs_from_config(cfg, source, base, dsn)
    if not specs and results_dir is None:
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
                    port=args.port or int(server.get("port", 8000)), results_dir=results_dir)


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


#: one load at a time, so that loading several bundles does not multiply memory use
_LOAD_LOCK = threading.Lock()


class DataStore:
    """Holds the aggregated data of all datasets; (re)loads it in a background thread."""

    def __init__(self, specs: list[DatasetSpec],
                 rescan: Callable[[], list[DatasetSpec]] | None = None):
        self.specs = specs
        self._rescan = rescan  # called on every (re)load to find the datasets anew
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
        results: list[DatasetData] = []
        if not _LOAD_LOCK.acquire(blocking=False):
            self.progress = "waiting for another load to finish"
            _LOAD_LOCK.acquire()
        t0 = time.monotonic()
        con = None
        try:
            if self._rescan is not None:
                self.specs = self._rescan()
            con = duckdb.connect(":memory:")
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
            if con is not None:
                con.close()
            _LOAD_LOCK.release()
            with self._lock:
                self.datasets = results
                self.loaded_at = datetime.now().astimezone()
                self.progress = None
        log.info("all datasets loaded in %.1fs", time.monotonic() - t0)


# ==========================================================================
# Result bundles: one directory of results, logs and profiles per date


class BundleError(Exception):
    """An uploaded result bundle cannot be used; the message is shown to the uploader."""

    def __init__(self, message: str, status: HTTPStatus = HTTPStatus.BAD_REQUEST):
        super().__init__(message)
        self.status = status


def extract_bundle(archive: Path, dest: Path) -> list[str]:
    """Copy the results, logs and profiles of a bundle archive into ``dest``.

    Files are taken by base name only (so no member can be written outside
    ``dest``); directories are ignored and anything else is skipped. Returns the
    names of the skipped members.
    """
    skipped: list[str] = []
    seen: set[str] = set()
    try:
        with tarfile.open(archive, "r:*") as tf:
            for member in tf:
                if member.isdir():
                    continue
                name = PurePosixPath(member.name).name
                if not member.isfile() or not BUNDLE_MEMBER.fullmatch(name):
                    skipped.append(member.name)
                    continue
                if name in seen:
                    raise BundleError(f"the archive holds {name} more than once")
                seen.add(name)
                src = tf.extractfile(member)
                if src is None:
                    raise BundleError(f"cannot read {member.name} from the archive")
                with src, (dest / name).open("xb") as out:
                    shutil.copyfileobj(src, out, 1 << 20)
                os.utime(dest / name, (member.mtime, member.mtime))
    except tarfile.ReadError as e:
        raise BundleError("not a tar archive (optionally gzipped)") from e
    except (tarfile.TarError, EOFError, OSError) as e:
        raise BundleError(f"cannot read the archive: {e}") from e
    if not any(n.startswith("results-") for n in seen):
        raise BundleError("the archive holds no results-*.csv[.gz] files")
    return skipped


class Library:
    """The result bundles in a results directory (``<YYYY-MM-DD>/`` each), loaded on demand.

    Loaded bundles stay in memory: their aggregates are small.
    """

    def __init__(self, root: Path):
        self.root = root
        self._stores: dict[str, DataStore] = {}
        self._lock = threading.Lock()

    def dates(self) -> list[str]:
        """The bundles present, oldest first (re-read every time: bundles may be copied in)."""
        try:
            entries = list(self.root.iterdir())
        except FileNotFoundError:
            return []
        out = []
        for e in entries:
            if BUNDLE_DIR.fullmatch(e.name) and e.is_dir():
                try:
                    date.fromisoformat(e.name)
                except ValueError:
                    continue
                out.append(e.name)
        return sorted(out)

    @staticmethod
    def previous(key: str, dates: list[str]) -> str | None:
        """The bundle preceding ``key`` by date, if any."""
        older = [d for d in dates if d < key]
        return older[-1] if older else None

    def store(self, key: str) -> DataStore:
        """The data of bundle ``key``; starts loading it on first use."""
        with self._lock:
            st = self._stores.get(key)
            if st is None:
                directory = self.root / key
                st = self._stores[key] = DataStore([], lambda: _bundle_specs(directory))
                st.reload()
            return st

    def add(self, filename: str, body, length: int, replace: bool) -> tuple[str, list[str]]:
        """Store an uploaded bundle archive (``length`` bytes read from ``body``) as
        ``<results dir>/<date>/``; returns the date and the skipped archive members."""
        self.root.mkdir(parents=True, exist_ok=True)
        archive = extracted = replaced = None
        try:
            # read the whole upload first, so that the client gets to see an error reply
            with tempfile.NamedTemporaryFile(dir=self.root, prefix=".upload-",
                                             delete=False) as f:
                archive = Path(f.name)
                remaining = length
                while remaining > 0:
                    chunk = body.read(min(1 << 20, remaining))
                    if not chunk:
                        raise BundleError("the upload ended prematurely")
                    f.write(chunk)
                    remaining -= len(chunk)
            m = BUNDLE_ARCHIVE.fullmatch(filename)
            if not m:
                raise BundleError(f"{filename!r} is not named results-bundle-YYYY-MM-DD.tar.gz")
            key = m[1]
            try:
                date.fromisoformat(key)
            except ValueError:
                raise BundleError(f"{key} in {filename!r} is not a valid date") from None
            target = self.root / key
            if target.exists() and not replace:
                raise BundleError(f"there is a bundle for {key} already", HTTPStatus.CONFLICT)
            extracted = Path(tempfile.mkdtemp(dir=self.root, prefix=".extract-"))
            extracted.chmod(0o755)
            skipped = extract_bundle(archive, extracted)
            with self._lock:
                if target.exists():
                    if not replace:
                        raise BundleError(f"there is a bundle for {key} already",
                                          HTTPStatus.CONFLICT)
                    replaced = target.rename(extracted.with_name(f".replaced-{key}-{time.time_ns()}"))
                extracted.rename(target)
                extracted = None
                self._stores.pop(key, None)  # loaded again when next shown
        finally:
            if archive is not None:
                archive.unlink(missing_ok=True)
            for d in (extracted, replaced):
                if d is not None:
                    shutil.rmtree(d, ignore_errors=True)
        log.info("stored result bundle %s from %s (%d member(s) skipped)", key, filename,
                 len(skipped))
        return key, skipped


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
               show_x: bool, prev: list[Bucket] | None = None, prev_key: str = "") -> str:
    """A bar chart of the counts per period.

    ``prev`` (the same series in the bundle of ``prev_key``, on the same kind of
    periods) is drawn as faint outlined bars in the colour of changes, over the
    current ones so that it shows whether it is higher or lower.
    """
    ticks = nice_ticks(0, max([b.n for b in buckets + (prev or [])] + [1]))
    marks, hits = [], []

    def bar_box(b: Bucket) -> tuple[float, float, float, float]:
        a, z = axis.span(b.start)
        w = min(24.0, max(1.0, z - a - 2))  # 2px gap between neighbours, <= 24px
        return (a + z) / 2 - w / 2, w, _y(b.n, ticks), _y(0, ticks)

    before = {b.start: b for b in prev or []}
    for b in buckets:
        x, w, top, bottom = bar_box(b)
        h = bottom - top
        r = 4 if w >= 8 and h >= 4 else 0
        if r:
            marks.append(f'<path class="bar" d="M{x:.1f},{bottom:.1f} V{top + r:.1f} '
                         f'Q{x:.1f},{top:.1f} {x + r:.1f},{top:.1f} H{x + w - r:.1f} '
                         f'Q{x + w:.1f},{top:.1f} {x + w:.1f},{top + r:.1f} V{bottom:.1f} Z"/>')
        elif h > 0:
            marks.append(f'<rect class="bar" x="{x:.1f}" y="{top:.1f}" width="{w:.1f}" '
                         f'height="{h:.1f}"/>')
        tip = f"{fmt_period(b.start, axis.unit)}: {b.n:,} patient-days"
        if prev is not None:
            pb = before.pop(b.start, None)
            tip += f"\n{prev_key}: " + (f"{pb.n:,} patient-days" if pb else "none")
        hits.append(_hit(axis, b, tip))
    for pb in before.values():  # periods in the preceding bundle only
        hits.append(_hit(axis, pb, f"{fmt_period(pb.start, axis.unit)}: none\n"
                                   f"{prev_key}: {pb.n:,} patient-days"))
    for pb in prev or []:
        x, w, top, bottom = bar_box(pb)
        if bottom - top > 0:
            marks.append(f'<rect class="bar prev" x="{x:.1f}" y="{top:.1f}" '
                         f'width="{w:.1f}" height="{bottom - top:.1f}"/>')
    return _panel(axis, label, note, ticks, False, marks, hits, show_x)


def _line_marks(axis: TimeAxis, pts: list[Bucket], ticks: list[float], band: bool,
                cls: str = "", dot_r: float | None = None) -> list[str]:
    """Band (interquartile range), line and dots of a series; ``cls`` is added to each
    mark's class. Dots get radius ``dot_r`` (default: by the number of points)."""
    marks = []
    runs = _runs(axis, pts)
    for run in runs:
        if band:
            upper = " ".join(f"{axis.mid(b.start):.1f},{_y(_hi(b), ticks):.1f}" for b in run)
            lower = " ".join(f"{axis.mid(b.start):.1f},{_y(_lo(b), ticks):.1f}"
                             for b in reversed(run))
            marks.append(f'<polygon class="band{cls}" points="{upper} {lower}"/>')
        if len(run) > 1:
            line = " ".join(f"{axis.mid(b.start):.1f},{_y(b.mean, ticks):.1f}" for b in run)
            marks.append(f'<polyline class="line{cls}" points="{line}"/>')
    small = len(pts) <= 60
    # isolated points (no neighbour to draw a line to) always get a marker
    isolated = {run[0].start for run in runs if len(run) == 1}
    r = dot_r if dot_r is not None else 4 if small else 2.5
    marks.extend(f'<circle class="dot{cls}" cx="{axis.mid(b.start):.1f}" '
                 f'cy="{_y(b.mean, ticks):.1f}" r="{r}"/>'
                 for b in pts if small or b.start in isolated)
    return marks


def line_panel(axis: TimeAxis, label: str, note: str, buckets: list[Bucket], band: bool,
               pct: bool, show_x: bool, info: str = "", unit: str = "",
               prev: list[Bucket] | None = None, prev_key: str = "") -> str:
    """A line chart of the period means (with their interquartile range if ``band``).

    ``prev`` (the same series in the bundle of ``prev_key``, on the same kind of
    periods) is drawn behind it, de-emphasized in the colour of changes.
    """
    pts = [b for b in buckets if b.mean is not None]
    if not pts:
        return ""
    ppts = [b for b in prev or [] if b.mean is not None]
    lows = [b.q25 if band and b.q25 is not None else b.mean for b in pts + ppts]
    highs = [b.q75 if band and b.q75 is not None else b.mean for b in pts + ppts]
    lo, hi = min(lows), max(highs)
    if pct:
        lo, hi = min(0.0, lo), max(1.0, hi)
    ticks = nice_ticks(lo, hi)
    marks = _line_marks(axis, ppts, ticks, band, " prev", 2) if ppts else []
    marks += _line_marks(axis, pts, ticks, band)
    u = f" {unit}" if unit else ""

    def describe(b: Bucket) -> str:
        if pct:
            return f"mean adherence {fmt_pct(b.mean)} (n={b.n:,})"
        return (f"mean {fmt_num(b.mean)}{u} · IQR {fmt_num(b.q25)}–{fmt_num(b.q75)}{u} · "
                f"range {fmt_num(b.min)}–{fmt_num(b.max)}{u} · n={b.n:,}")

    before = {b.start: b for b in ppts}
    hits = []
    for b in pts:
        tip = f"{fmt_period(b.start, axis.unit)}: {describe(b)}"
        if prev is not None:
            pb = before.pop(b.start, None)
            tip += f"\n{prev_key}: " + (describe(pb) if pb else "no values")
        hits.append(_hit(axis, b, tip))
    for pb in before.values():  # periods with values in the preceding bundle only
        hits.append(_hit(axis, pb, f"{fmt_period(pb.start, axis.unit)}: no values\n"
                                   f"{prev_key}: {describe(pb)}"))
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
             subtitle: str = "", prev_key: str | None = None,
             prev_view: QIView | None = None, prev_unit: str | None = None) -> str:
    """Charts and tables of one QI in one dataset.

    The debug panels sit in a collapsible block that starts expanded when the
    QI result looks broken (see ``result_problem``). Result and debug panels
    share one time axis, and each group carries its own x-axis labels so that
    either reads on its own.

    With ``prev_key`` (the dataset is in the preceding bundle), the totals show how
    they changed since that bundle, where ``prev_view`` is the QI (None: it had none).
    If that bundle's dataset has the same kind of periods (``prev_unit``), its debug
    values are drawn behind the current ones.
    """
    # debug series of the preceding bundle, by concept, to draw behind the current ones
    prev_debug: dict[int, Series] = {}
    if prev_key and prev_view and prev_unit == d.unit:
        prev_debug = {c.concept_id: s for c, s in prev_view.debug
                      if any(c2.concept_id == c.concept_id for c2, _ in view.debug)}
    prev_result = (prev_view.result[1] if prev_key and prev_view and prev_view.result
                   and view.result and prev_unit == d.unit else None)
    all_series = (([view.result[1]] if view.result else []) + [s for _, s in view.debug]
                  + list(prev_debug.values()) + ([prev_result] if prev_result else []))
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
        prev = prev_view.result[1] if prev_view and prev_view.result else None
        dn = dm = ""
        if prev_key:
            dn = delta_html(series.n, prev and prev.n, "int", prev_key)
            dm = delta_html(series.mean, prev and prev.mean, "pct", prev_key)
        meta.append(f"<b>{fmt_int(series.n)}</b>{dn} patient-days in the initial population "
                    f"over {periods} {unit}{'s' if periods != 1 else ''}")
        meta.append(f"mean adherence <b>{fmt_pct(series.mean)}</b>{dm}")
        pb = prev_result.buckets if prev_result else None
        result_items.append(lambda x, s=series: bars_panel(
            axis, "Initial population", f"patient-days per {unit}", s.buckets, x,
            pb, prev_key or ""))
        if _has_means(series):
            result_items.append(lambda x, s=series: line_panel(
                axis, "Adherence", f"mean per {unit}", s.buckets, False, True, x, "", "",
                pb, prev_key or ""))
        if prev_result:
            meta.append(f'<span class="prev-key">{esc(prev_key)}</span> drawn faintly in '
                        "the charts")
    debug = [(c, s) for c, s in view.debug if _has_means(s)]
    debug_items: list[Callable[[bool], str]] = [
        lambda x, c=c, s=s: line_panel(
            axis, c.short_name, f"OMOP:{c.concept_id} · n={s.n:,}", s.buckets,
            True, False, x, concept_info(c), c.unit,
            prev_debug[c.concept_id].buckets if c.concept_id in prev_debug else None,
            prev_key or "")
        for c, s in debug]

    def render(items: list[Callable[[bool], str]]) -> str:
        return "".join(make(i == len(items) - 1) for i, make in enumerate(items))

    parts = [render(result_items)]
    if debug_items:
        why = " · expanded because the QI result looks broken" if problem else ""
        if prev_debug:
            why += (f' · <span class="prev-key">{esc(prev_key)}</span> drawn behind, where '
                    "it had values")
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


def run_section(d: DatasetData, prev_key: str | None = None,
                p: DatasetData | None = None) -> str:
    """Run time, patient counts and message frequencies from the dataset's run log; with
    the changes since dataset ``p`` of the bundle of ``prev_key``, if it has a run log."""
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
    plg = p.log if p is not None and p.log is not None and p.log.seconds is not None else None

    def chg(attr: str, kind: str, value=None) -> str:
        if plg is None:
            return ""
        new = getattr(lg, attr) if value is None else value
        return delta_html(new, getattr(plg, attr), kind, prev_key)

    if lg.seconds is not None:
        fail_cls = " problem-text" if lg.failures else ""
        fail_v = f"{RUN_SIGN} {fmt_int(lg.failures)}" if lg.failures else fmt_int(lg.failures)
        tiles = [("Time taken", fmt_duration(lg.seconds) + chg("seconds", "dur"),
                  f"{lg.seconds:,.3f} s", ""),
                 ("Patients processed", fmt_int(lg.patients) + chg("patients", "int"),
                  "successes + failures", ""),
                 ("Successes", fmt_int(lg.successes) + chg("successes", "int"), "", ""),
                 ("Failures", fail_v + chg("failures", "int"), "", fail_cls),
                 ("Warnings", fmt_int(lg.warnings_total) + chg("warnings_total", "int"),
                  f"{fmt_rate(lg.warnings_total or 0, lg.patients)} per 1,000 patients · "
                  f"{len(lg.warnings)} kind(s)", "")]
        parts.append('<div class="tiles">' + "".join(
            f'<div class="tile"><div class="k">{k}</div><div class="v{cls}">{v}</div>'
            f'<div class="sub">{esc(sub)}</div></div>' for k, v, sub, cls in tiles) + "</div>")
    if lg.warnings:
        total = lg.warnings_total or sum(w.count for w in lg.warnings)
        before = {warning_kind(w): w.count for w in plg.warnings} if plg else {}

        def change(w: LogWarning) -> str:
            if plg is None:
                return ""
            dt = delta_html(w.count, before.get(warning_kind(w)), "int", prev_key)
            return f'<td class="chg">{dt}</td>'

        rows = "".join(
            f'<tr><td>{fmt_int(w.count)}</td>{change(w)}<td>{fmt_rate(w.count, lg.patients)}</td>'
            f'<td>{fmt_pct(w.count / total if total else None)}</td><td class="lvl">{esc(w.level)}</td><td class="msg">{esc(w.message)}</td></tr>'
            for w in lg.warnings)
        change_th = f'<th title="since {esc(prev_key)}">Change</th>' if plg else ""
        parts.append(f"""<div class="tablewrap"><table class="stats num msgs">
  <thead><tr><th>Count</th>{change_th}<th>Per 1,000 patients</th><th>Share</th><th>Level</th>
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


def profile_section(d: DatasetData, base: str = "") -> str:
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
  <div class="flamewrap" data-src="{base}/profile/{quote(d.name, safe='')}">Loading…</div>
</details></section>"""


def unknown_note(d: DatasetData, concepts: dict[int, Concept]) -> str:
    unknown = unknown_concepts(d, concepts)
    if not unknown:
        return ""
    items = ", ".join(f"{cid} ({n:,} values)" for cid, n in unknown)
    return (f'<p class="banner warn">⚠️ {len(unknown)} concept id(s) in the data are not '
            f'listed in the concepts files and are not shown: {esc(items)}</p>')


# ==========================================================================
# Differences from the preceding result bundle

#: how values of each kind are shown; two values differ if they are shown differently
_DELTA_FMT: dict[str, Callable] = {"int": fmt_int, "pct": fmt_pct, "dur": fmt_duration,
                                   "num": fmt_num, "text": lambda v: "—" if v is None else str(v)}


def differs(new, old, kind: str) -> bool:
    return _DELTA_FMT[kind](new) != _DELTA_FMT[kind](old)


def delta_text(new, old, kind: str) -> str:
    """The change from ``old`` to ``new`` in words, e.g. "+1,234 (+3.2 %)" or "-1.5 pp"."""
    if old is None:
        return "new"
    if new is None:
        return "gone"
    if kind == "text":
        return "changed"
    d = new - old
    if kind == "pct":
        return f"{d * 100:+.1f} pp"
    rel = f" ({d / old:+.1%})" if old else ""
    sign = "+" if d >= 0 else "-"
    if kind == "int":
        return f"{d:+,}{rel}"
    if kind == "dur":
        return f"{sign}{fmt_duration(abs(d))}{rel}"
    return f"{sign}{fmt_num(abs(d))}{rel}"


def delta_html(new, old, kind: str, prev_key: str) -> str:
    """A small change marker for ``new``, or "" if it is shown as before."""
    if not differs(new, old, kind):
        return ""
    tip = f"{prev_key}: {_DELTA_FMT[kind](old)}"
    return f'<span class="delta" title="{esc(tip)}">{esc(delta_text(new, old, kind))}</span>'


#: digits (and the numbers, dates and times they make up) in a warning message: the
#: engine prints an example message per kind, so these differ from run to run
_MESSAGE_NUMBERS = re.compile(r"\d[\d.,:T+-]*")


def warning_kind(w: LogWarning) -> tuple[str, str]:
    """What identifies a kind of warning across runs: its level and its message with the
    numbers blanked out."""
    return w.level, _MESSAGE_NUMBERS.sub("#", w.message)


@dataclass
class ChangeRow:
    """One changed quantity of a dataset, compared with the preceding bundle."""

    group: str  # "data", "run", "warnings" or a QI id
    label: str  # HTML
    old: str
    new: str
    delta: str  # e.g. "+3.2 %", "new", "gone"
    #: debug values only: (old, new, delta) of the mean, while old/new/delta are of n
    mean: tuple[str, str, str] | None = None


def dataset_changes(d: DatasetData, p: DatasetData, concepts: dict[int, Concept]
                    ) -> list[ChangeRow]:
    """What differs between dataset ``d`` and the same dataset ``p`` of the preceding bundle."""
    rows: list[ChangeRow] = []

    def add(group: str, label: str, new, old, kind: str) -> None:
        if differs(new, old, kind):
            f = _DELTA_FMT[kind]
            rows.append(ChangeRow(group, label, f(old), f(new), delta_text(new, old, kind)))

    add("data", "Could not be loaded", d.error, p.error, "text")
    if not (d.error or p.error):
        add("data", "Rows", d.rows, p.rows, "int")
        add("data", "First date", d.first, p.first, "text")
        add("data", "Last date", d.last, p.last, "text")
        add("data", "Binned by", d.unit, p.unit, "text")

    lg, plg = d.log, p.log
    add("run", "Run log", "present" if lg else None, "present" if plg else None, "text")
    if lg and plg:
        add("run", "Log problem", lg.error, plg.error, "text")
    # an unfinished run (no summary) has no totals: the log problem says enough then
    if lg and plg and lg.seconds is not None and plg.seconds is not None:
        add("run", "Time taken", lg.seconds, plg.seconds, "dur")
        add("run", "Patients processed", lg.patients, plg.patients, "int")
        add("run", "Failures", lg.failures, plg.failures, "int")
        add("run", "Warnings", lg.warnings_total, plg.warnings_total, "int")
        old = {warning_kind(w): w for w in plg.warnings}
        new = {warning_kind(w): w for w in lg.warnings}
        for k in sorted(new.keys() | old.keys(),
                        key=lambda k: -(new[k].count if k in new else old[k].count)):
            w = new.get(k) or old[k]
            label = f'<span class="lvl">{esc(w.level)}</span> {esc(w.message)}'
            add("warnings", label, new[k].count if k in new else None,
                old[k].count if k in old else None, "int")
    pf, ppf = d.profile, p.profile
    if pf and ppf and pf.total_ms and ppf.total_ms:
        add("run", "Profile: evaluation time", pf.total_ms / 1000, ppf.total_ms / 1000, "dur")

    if d.error or p.error:
        return rows
    views, pviews = qi_views(d, concepts), qi_views(p, concepts)
    for qi in sorted(views.keys() | pviews.keys()):
        v, pv = views.get(qi), pviews.get(qi)
        r = v.result[1] if v and v.result else None
        pr = pv.result[1] if pv and pv.result else None
        add(qi, "Patient-days", r.n if r else None, pr.n if pr else None, "int")
        add(qi, "Mean adherence", r.mean if r else None, pr.mean if pr else None, "pct")
        add(qi, "Periods", len(r.buckets) if r else None, len(pr.buckets) if pr else None,
            "int")
        debug = {c.concept_id: (c, s) for c, s in (v.debug if v else [])}
        pdebug = {c.concept_id: (c, s) for c, s in (pv.debug if pv else [])}
        for cid in sorted(debug.keys() | pdebug.keys()):
            c, s = debug.get(cid) or (pdebug[cid][0], None)
            ps = pdebug[cid][1] if cid in pdebug else None
            n, pn = (s.n if s else None), (ps.n if ps else None)
            mean, pmean = (s.mean if s else None), (ps.mean if ps else None)
            if differs(n, pn, "int") or differs(mean, pmean, "num"):
                # one row for both: an unchanged aspect is shown without a change
                def triple(new, old, kind: str) -> tuple[str, str, str]:
                    return (_DELTA_FMT[kind](old), _DELTA_FMT[kind](new),
                            delta_text(new, old, kind) if differs(new, old, kind) else "")
                rows.append(ChangeRow(qi, f"{esc(c.short_name)}{unit_html(c.unit)}",
                                      *triple(n, pn, "int"), mean=triple(mean, pmean, "num")))
    return rows


def changes_table(rows: list[ChangeRow], prev_key: str, key: str,
                  heading: Callable[[str], str]) -> str:
    """The changes of one dataset, grouped under ``heading(group)``.

    Debug values take one row each, with columns for both their count (n) and mean,
    under a sub-heading of their own in their QI's group.
    """
    wide = any(r.mean is not None for r in rows)
    cols = 7 if wide else 4
    pad = '<td colspan="3"></td>' if wide else ""

    def cells(old: str, new: str, delta: str, cls: str = "") -> str:
        return (f"<td{cls}>{esc(old)}</td><td>{esc(new)}</td>"
                f'<td class="chg">{esc(delta)}</td>')

    GRP = ' class="grp"'  # noqa: N806
    out, group, in_debug = [], None, False
    for r in rows:
        if r.group != group:
            group, in_debug = r.group, False
            out.append(f'<tr class="group"><th colspan="{cols}">{heading(group)}</th></tr>')
        if r.mean is None:
            out.append(f'<tr><td class="what">{r.label}</td>{cells(r.old, r.new, r.delta)}'
                       f"{pad}</tr>")
            continue
        if not in_debug:
            in_debug = True
            out.append('<tr class="sub"><td class="what">Debug values</td>'
                       '<th colspan="3">n</th><th colspan="3" class="grp">Mean</th></tr>')
        out.append(f'<tr><td class="what">{r.label}</td>{cells(r.old, r.new, r.delta)}'
                   f"{cells(*r.mean, cls=GRP)}</tr>")
    dates = f"<th>{esc(prev_key)}</th><th>{esc(key)}</th><th>Change</th>"
    more = (f'<th class="grp">{esc(prev_key)}</th><th>{esc(key)}</th><th>Change</th>'
            if wide else "")
    return f"""<div class="tablewrap"><table class="stats num changes">
  <thead><tr><th></th>{dates}{more}</tr></thead>
  <tbody>{"".join(out)}</tbody></table></div>"""


def changes_summary(rows: list[ChangeRow], qi_link: Callable[[str], str]) -> str:
    """One line per kind of change, for the overview."""
    by_group: dict[str, list[ChangeRow]] = {}
    for r in rows:
        by_group.setdefault(r.group, []).append(r)
    parts = []
    for group, label in (("data", "Data"), ("run", "Run")):
        if group in by_group:
            items = ", ".join(f"{r.label} {esc(r.delta)}" for r in by_group[group])
            parts.append(f'<span class="k">{label}:</span> {items}')
    if ws := by_group.get("warnings"):
        counts: dict[str, int] = {}
        for r in ws:
            k = r.delta if r.delta in ("new", "gone") else "changed"
            counts[k] = counts.get(k, 0) + 1
        items = ", ".join(f"{n} {k}" for k, n in counts.items() if n)
        parts.append(f'<span class="k">Warning kinds:</span> {items}')
    qis = [g for g in by_group if g not in ("data", "run", "warnings")]
    if qis:
        parts.append('<span class="k">QIs:</span> ' + ", ".join(qi_link(qi) for qi in qis))
    return "<br>".join(parts)


# ==========================================================================
# HTML: pages


@dataclass
class Page:
    title: str
    body: str
    toc: list[tuple[str, str]]  # (anchor, label) of the page's sections, for the sidebar
    current: str  # path of this page, to highlight it in the sidebar


@dataclass
class BundleContext:
    """Which result bundle a report shows, among which, and the one it is compared with."""

    key: str  # date of the bundle shown
    dates: list[str]  # all bundles, oldest first
    previous: str | None = None  # date of the bundle compared with
    previous_store: DataStore | None = None
    chosen: str | None = None  # the comparison chosen (a date or "none"); None: the preceding

    @property
    def preceding(self) -> str | None:
        """The bundle preceding the one shown by date (the default comparison)."""
        return Library.previous(self.key, self.dates)


class Report:
    """The report of one set of datasets, made per request.

    ``base`` prefixes every link (``/bundle/<date>`` in bundle mode, else empty; with a
    chosen comparison ``/bundle/<date>/vs/<older date or none>``).
    """

    def __init__(self, store: DataStore | None, catalog: ConceptCatalog, settings: Settings,
                 base: str = "", bundle: BundleContext | None = None):
        self.store = store
        self.catalog = catalog
        self.settings = settings
        self.base = base
        self.bundle = bundle

    @property
    def multi(self) -> bool:
        return len(self.store.datasets or []) > 1

    def url(self, path: str) -> str:
        """Link to ``path`` (absolute within the report) in this report."""
        return self.base + path

    def _ds_url(self, name: str) -> str:
        return self.url("/dataset/" + quote(name, safe=""))

    def _qi_url(self, qi: str) -> str:
        return self.url("/qi/" + quote(qi, safe=""))

    # -- the bundle compared with -------------------------------------------------

    @property
    def prev_key(self) -> str | None:
        """Date of the bundle compared with, if there is one (and it is loaded)."""
        b = self.bundle
        return b.previous if b and b.previous_store and b.previous_store.datasets is not None else None

    def _prev(self, name: str) -> DatasetData | None:
        """Dataset ``name`` of the bundle compared with."""
        if not self.prev_key:
            return None
        return next((p for p in self.bundle.previous_store.datasets if p.name == name), None)

    def _prev_views(self, name: str, concepts: dict[int, Concept]) -> dict[str, QIView] | None:
        """The QIs of dataset ``name`` in the bundle compared with; None if it is not there
        (or could not be loaded), so that there is nothing to compare with."""
        p = self._prev(name)
        return qi_views(p, concepts) if p is not None and not p.error else None

    def _prev_url(self, path: str) -> str:
        return f"/bundle/{self.prev_key}{path}"

    def _changes_note(self) -> str:
        which = ("the preceding bundle, of" if self.prev_key == self.bundle.preceding
                 else "the bundle of")
        return (f'<p class="meta">Compared with {which} '
                f'<a href="{self._prev_url("/")}">{esc(self.prev_key)}</a> (choose another '
                'in the sidebar). Values that are shown '
                "the same in both are not listed; hover a change for the value before.</p>")

    def _overview_changes(self, datasets: list[DatasetData], concepts: dict[int, Concept]
                          ) -> str:
        pk = self.prev_key
        names = {d.name for d in datasets}
        rows = []
        for d in datasets:
            link = f'<a href="{self._ds_url(d.name)}#s-changes">{esc(d.name)}</a>'
            p = self._prev(d.name)
            if p is None:
                rows.append(f'<tr><th>{link}</th><td class="chg">new in this bundle</td></tr>')
                continue
            changes = dataset_changes(d, p, concepts)
            summary = (changes_summary(changes, lambda qi, d=d: (
                f'<a href="{self._ds_url(d.name)}#{anchor(qi)}">{esc(qi)}</a>'))
                       if changes else '<span class="dim">no changes</span>')
            rows.append(f"<tr><th>{link}</th><td>{summary}</td></tr>")
        for p in self.bundle.previous_store.datasets:
            if p.name not in names:
                rows.append(f'<tr><th><a href="{self._prev_url("/dataset/" + quote(p.name, safe=""))}">'
                            f'{esc(p.name)}</a></th><td class="chg">gone: not in this bundle'
                            "</td></tr>")
        return f"""<section class="block" id="s-changes">
  <h2>Changes since {esc(pk)}</h2>
  {self._changes_note()}
  <div class="tablewrap"><table class="stats changes-summary"><tbody>{"".join(rows)}</tbody>
  </table></div>
</section>"""

    def _dataset_changes(self, d: DatasetData, concepts: dict[int, Concept],
                         info: dict[str, QIInfo]) -> str:
        pk = self.prev_key
        head = f'<section class="block" id="s-changes"><h2>Changes since {esc(pk)}</h2>'
        p = self._prev(d.name)
        if p is None:
            return (f'{head}<p class="meta">New in this bundle: the bundle of '
                    f'<a href="{self._prev_url("/")}">{esc(pk)}</a> has no {esc(d.name)}.'
                    "</p></section>")
        changes = dataset_changes(d, p, concepts)
        if not changes:
            return (f'{head}<p class="meta">No changes since the bundle of '
                    f'<a href="{self._prev_url(self._ds_url(d.name).removeprefix(self.base))}">'
                    f"{esc(pk)}</a>.</p></section>")
        groups = {"data": "Data", "run": "Evaluation run",
                  "warnings": "Warning kinds (count)"}

        def heading(group: str) -> str:
            if group in groups:
                return groups[group]
            return f'<a href="#{anchor(group)}">{qi_heading(group, info)}</a>'

        n = len(changes)
        return f"""<section class="block" id="s-changes">
<details class="changes">
  <summary><h2>Changes since {esc(pk)}</h2>
    <span class="dim">— {n} changed value{"s" if n != 1 else ""}</span></summary>
  {self._changes_note()}
  {changes_table(changes, pk, self.bundle.key, heading)}
</details></section>"""

    # -- routing --------------------------------------------------------------

    def profile_svg(self, name: str) -> tuple[HTTPStatus, str]:
        """The cleaned flamegraph SVG of dataset ``name``, fetched by the profile section."""
        d = next((d for d in self.store.datasets or [] if d.name == name), None)
        if d is None or d.profile is None or d.profile.svg is None:
            return HTTPStatus.NOT_FOUND, "no profile for this dataset"
        return HTTPStatus.OK, d.profile.svg

    def render(self, path: str) -> tuple[HTTPStatus, str]:
        """The page at ``path`` (below ``base``)."""
        datasets = self.store.datasets
        prev = self.bundle.previous_store if self.bundle else None
        if datasets is None or (prev is not None and prev.datasets is None):
            return HTTPStatus.OK, self._loading_page()
        concepts = self.catalog.get()
        info = self.catalog.qi_info()
        parts = [unquote(p) for p in path.strip("/").split("/") if p]
        current = self.url(path)
        page: Page | None = None
        if not parts:
            if not datasets:
                where = self.bundle and esc(self.settings.results_dir / self.bundle.key)
                page = Page("No results", f"""<h1>{self._title()}</h1>
<p class="banner warn">⚠️ There are no results-*.csv[.gz] files in {where}.</p>""",
                            [], current)
            elif self.multi:
                page = self._overview(datasets, concepts, info, current)
            else:
                page = self._dataset_page(datasets[0], concepts, info, current)
        elif len(parts) == 2 and parts[0] == "dataset":
            d = next((d for d in datasets if d.name == parts[1]), None)
            if d is not None:
                page = self._dataset_page(d, concepts, info, current)
        elif len(parts) == 2 and parts[0] == "qi":
            page = self._qi_page(parts[1], datasets, concepts, info, current)
        if page is None:
            where = f" in the bundle of {esc(self.bundle.key)}" if self.bundle else ""
            return HTTPStatus.NOT_FOUND, self._layout(
                Page("Not found", f'<h1>Not found{where}</h1><p><a href="{self.url("/")}">'
                                  "Back to the report</a></p>", [], ""),
                datasets, concepts, info)
        return HTTPStatus.OK, self._layout(page, datasets, concepts, info)

    def no_bundles_page(self) -> str:
        """Bundle mode with an empty results directory: just the upload form."""
        body = f"""<h1>QI results</h1>
<p class="banner info">There are no result bundles in {esc(self.settings.results_dir)} yet.
  Upload one (<code>results-bundle-YYYY-MM-DD.tar.gz</code>) to get started.</p>
{self._upload_form()}"""
        return f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>QI report</title><style>{CSS}</style></head>
<body class="loading"><main>{body}</main><div id="tip" role="tooltip" hidden></div>
<script>{JS}</script></body></html>"""

    def _title(self) -> str:
        return f"QI results of {esc(self.bundle.key)}" if self.bundle else "QI results"

    # -- pages ------------------------------------------------------------------

    def _overview(self, datasets: list[DatasetData], concepts: dict[int, Concept],
                  info: dict[str, QIInfo], path: str) -> Page:
        views = {d.name: qi_views(d, concepts) for d in datasets if not d.error}
        qis = sorted({qi for v in views.values() for qi in v})
        ok = [d for d in datasets if not d.error]
        pk = self.prev_key
        pviews = {d.name: self._prev_views(d.name, concepts) for d in ok}
        head1 = "".join(f'<th colspan="2" class="grp"><a href="{self._ds_url(d.name)}">'
                        f"{esc(d.name)}</a></th>" for d in ok)
        head2 = "".join("<th>Patient-days</th><th>Adherence</th>" for _ in ok)
        rows = []
        for qi in qis:
            cells = []
            for d in ok:
                v = views[d.name].get(qi)
                if v and v.result:
                    s = v.result[1]
                    dn = dm = ""
                    if pviews[d.name] is not None:
                        pv = pviews[d.name].get(qi)
                        ps = pv.result[1] if pv and pv.result else None
                        dn = delta_html(s.n, ps and ps.n, "int", pk)
                        dm = delta_html(s.mean, ps and ps.mean, "pct", pk)
                    cells.append(f'<td class="grp">{fmt_int(s.n)}{dn}</td>'
                                 f'<td>{fmt_pct(s.mean)}{dm}</td>')
                else:
                    cells.append('<td class="grp dim">—</td><td class="dim">—</td>')
            rows.append(f'<tr><th><a href="{self._qi_url(qi)}">{qi_heading(qi, info)}</a></th>'
                        f'{"".join(cells)}</tr>')
        comparison = (f"""<div class="tablewrap"><table class="stats num">
  <thead><tr><th></th>{head1}</tr><tr><th>QI</th>{head2}</tr></thead>
  <tbody>{"".join(rows)}</tbody></table></div>""" if qis and ok
                      else '<p class="meta">No QI results in any dataset.</p>')
        def run_cells(d: DatasetData) -> str:
            lg = d.log
            p = self._prev(d.name)
            plg = p.log if p and p.log and p.log.seconds is not None else None

            def chg(attr: str, kind: str) -> str:
                return delta_html(getattr(lg, attr), getattr(plg, attr), kind, pk) if plg else ""

            if lg is None or lg.seconds is None:
                why = ("no run log" if lg is None else lg.error or "")
                if lg is None:
                    return f'<td colspan="5" class="grp dim" title="{esc(why)}">—</td>'
                return (f'<td colspan="5" class="grp problem-text" title="{esc(why)}">'
                        f"{RUN_SIGN} run incomplete</td>")
            fail = (f'<td class="problem-text">{RUN_SIGN} {fmt_int(lg.failures)}</td>'
                    if lg.failures else f"<td>{fmt_int(lg.failures)}</td>")
            fail = fail.removesuffix("</td>") + chg("failures", "int") + "</td>"
            return (f'<td class="grp" title="{lg.seconds:,.3f} s">{fmt_duration(lg.seconds)}'
                    f'{chg("seconds", "dur")}</td>'
                    f'<td>{fmt_int(lg.patients)}{chg("patients", "int")}</td>{fail}'
                    f'<td>{fmt_int(lg.warnings_total)}{chg("warnings_total", "int")}</td>'
                    f"<td>{fmt_rate(lg.warnings_total or 0, lg.patients)}</td>")

        def rows_delta(d: DatasetData) -> str:
            p = self._prev(d.name)
            return delta_html(d.rows, p.rows, "int", pk) if p and not p.error else ""

        ds_rows = "".join(
            f'<tr><th><a href="{self._ds_url(d.name)}">{esc(d.name)}</a></th>'
            + (f'<td colspan="3" class="problem-text">⛔ {esc(d.error)}</td>' if d.error else
               f"<td>{fmt_int(d.rows)}{rows_delta(d)}</td><td>{d.first} – {d.last}</td>"
               f"<td>{d.unit}</td>")
            # the source last: a long path would push the other columns out of view
            + run_cells(d) + f'<td class="grp src">{esc(d.source)}</td></tr>'
            for d in datasets)
        changes = self._overview_changes(datasets, concepts) if pk else ""
        body = f"""<h1>{self._title()} — overview</h1>
{self._intro()}
{changes}
<section class="block" id="s-comparison">
  <h2>Cross-dataset comparison</h2>
  <p class="meta">Per QI and dataset: patient-days in the initial population and mean
    adherence, each over the dataset's whole time range.</p>
  {comparison}
</section>
<section class="block" id="s-datasets">
  <h2>Datasets</h2>
  <div class="tablewrap"><table class="stats">
    <thead><tr><th></th><th colspan="3">QI results</th>
      <th colspan="5" class="grp">Evaluation run (log)</th><th class="grp"></th></tr>
      <tr><th>Dataset</th><th>Rows</th><th>Dates</th><th>Binned by</th>
      <th class="grp">Time taken</th><th>Patients</th><th>Failures</th><th>Warnings</th>
      <th>Warnings / 1,000 patients</th><th class="grp">Source</th></tr>
    </thead>
    <tbody>{ds_rows}</tbody></table></div>
</section>"""
        toc = [("s-comparison", "Cross-dataset comparison"), ("s-datasets", "Datasets")]
        if pk:
            toc.insert(0, ("s-changes", f"Changes since {pk}"))
        return Page("Overview", body, toc, path)

    def _dataset_page(self, d: DatasetData, concepts: dict[int, Concept],
                      info: dict[str, QIInfo], path: str) -> Page:
        title = f"{esc(d.name)} <span class=\"dim\">· {esc(self.bundle.key)}</span>" if self.bundle else esc(d.name)
        parts = [f"<h1>{title}</h1>", dataset_meta(d)]
        toc = []
        if not self.multi:
            parts.append(self._intro())
        pk = self.prev_key
        p = self._prev(d.name)
        if pk:
            parts.append(self._dataset_changes(d, concepts, info))
            toc.append(("s-changes", f"Changes since {pk}"))
        parts.append(run_section(d, pk, p))
        toc.append(("s-run", f"Evaluation run {RUN_SIGN}" if run_problem(d) else "Evaluation run"))
        parts.append(profile_section(d, self.base))
        toc.append(("s-profile", "Profile"))
        if not d.error:
            parts.append(unknown_note(d, concepts))
            views = qi_views(d, concepts)
            pviews = self._prev_views(d.name, concepts)
            # every QI of the concepts files, so that QIs without any data show up too
            known = {c.qi for c in concepts.values() if not c.debug}
            for qi in sorted(known | set(views)):
                a = anchor(qi)
                link = (f' <a class="xref" href="{self._qi_url(qi)}">compare across datasets →</a>'
                        if self.multi and qi in views else "")
                view = views.get(qi)
                heading, subtitle = qi_heading(qi, info), qi_subtitle(qi, info)
                if view is None:
                    parts.append(empty_qi_block(qi, heading, a, subtitle))
                    bad = True
                else:
                    parts.append(qi_block(d, view, f"{heading}{link}", a, subtitle,
                                          pk if pviews is not None else None,
                                          pviews.get(qi) if pviews is not None else None,
                                          p.unit if p is not None else None))
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
        pk = self.prev_key
        # per dataset: whether the preceding bundle has it, and its view of this QI
        prev: dict[str, tuple[bool, QIView | None]] = {}
        for d, _ in found:
            pviews = self._prev_views(d.name, concepts)
            prev[d.name] = (pviews is not None, pviews.get(qi) if pviews is not None else None)

        def summary_cells(d: DatasetData, v: QIView) -> str:
            if not v.result:
                return '<td colspan="3" class="dim">debug values only</td>'
            s = v.result[1]
            dn = dm = ""
            had, pv = prev[d.name]
            if had:
                ps = pv.result[1] if pv and pv.result else None
                dn = delta_html(s.n, ps and ps.n, "int", pk)
                dm = delta_html(s.mean, ps and ps.mean, "pct", pk)
            return (f"<td>{fmt_int(s.n)}{dn}</td><td>{len(s.buckets)} {d.unit}s</td>"
                    f"<td>{fmt_pct(s.mean)}{dm}</td>")

        rows = "".join(f'<tr><th><a href="#{anchor(d.name)}">{esc(d.name)}</a></th>'
                       f"{summary_cells(d, v)}</tr>" for d, v in found)
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
            heading = (f'{esc(d.name)} <a class="xref" href="{self._ds_url(d.name)}#{anchor(qi)}">'
                       "dataset page →</a>")
            had, pv = prev[d.name]
            p = self._prev(d.name) if had else None
            parts.append(qi_block(d, v, heading, a, "", pk if had else None, pv,
                                  p.unit if p is not None else None))
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
        lines = []
        if self.store.datasets is None:
            what = f" of {esc(self.bundle.key)}" if self.bundle else ""
            lines.append(f"Loading the QI results{what}: {esc(self.store.progress or 'starting')}.")
        prev = self.bundle.previous_store if self.bundle else None
        if prev is not None and prev.datasets is None:
            lines.append(f"Loading the bundle of {esc(self.bundle.previous)} for the "
                         f"comparison: {esc(prev.progress or 'waiting')}.")
        text = "".join(f'<p class="meta">{line}</p>' for line in lines)
        return f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta http-equiv="refresh" content="2"><title>Loading — QI report</title>
<style>{CSS}</style></head>
<body class="loading"><main><h1>Loading QI results…</h1>
{text}<p class="meta">This page refreshes automatically.</p></main></body></html>"""

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
            current = href == page.current
            cls = ' class="current"' if current else ""
            return (f'<li><a href="{href}"{cls}>{label}</a>{extra}'
                    f'{toc_html() if current else ""}</li>')

        sections = []
        if self.multi:
            sections.append(f'<ul>{entry(self.url("/"), "Overview")}</ul>')
            ds_items = "".join(
                entry(self._ds_url(d.name), esc(d.name),
                      ' <span class="problem-text" title="could not be loaded">⛔</span>'
                      if d.error else
                      f' <span title="the run log reports failures or is incomplete">{RUN_SIGN}</span>'
                      if run_problem(d) else "")
                for d in datasets)
            sections.append(f"<h3>Datasets</h3><ul>{ds_items}</ul>")
            qis = sorted({qi for d in datasets if not d.error for qi in qi_views(d, concepts)})
            qi_items = "".join(entry(self._qi_url(qi), esc(qi_label(qi, info, short=True)))
                               for qi in qis)
            if qi_items:
                sections.append(f"<h3>QIs across datasets</h3><ul>{qi_items}</ul>")
        elif datasets:
            d = datasets[0]
            sections.append(f'<h3>{esc(d.name)}</h3>{toc_html()}')
        loaded = self.store.loaded_at.strftime("%Y-%m-%d %H:%M") if self.store.loaded_at else "—"
        button = ('<button type="submit" disabled>Reloading…</button>' if self.store.loading
                  else '<button type="submit">Reload data</button>')
        return f"""<nav class="sidebar">
  <div class="brand"><a href="{self.url("/")}">QI report</a></div>
  {self._bundle_picker(page)}
  {"".join(sections)}
  <form class="reload" method="post" action="{self.url("/reload")}">
    <span class="meta">data loaded {loaded}</span>{button}
  </form>
  {self._upload_form() if self.bundle else ""}
</nav>"""

    def _bundle_picker(self, page: Page) -> str:
        """Selects the bundle to show, staying on the same page if it exists there."""
        b = self.bundle
        if b is None:
            return ""
        options = "".join(
            f'<option value="{esc(k)}"{" selected" if k == b.key else ""}>{esc(k)}'
            f'{" (newest)" if k == b.dates[-1] else ""}</option>' for k in reversed(b.dates))
        rest = page.current.removeprefix(self.base) or "/"
        older = [k for k in reversed(b.dates) if k < b.key]
        if older:
            chosen = b.chosen or ""
            vs_options = [("", f"preceding ({b.preceding})")]
            vs_options += [(k, k) for k in older]
            vs_options.append(("none", "nothing"))
            vs = "".join(f'<option value="{esc(v)}"{" selected" if v == chosen else ""}>'
                         f"{esc(label)}</option>" for v, label in vs_options)
            link = (f'<span class="meta">show <a href="/bundle/{esc(b.previous)}{esc(rest)}">'
                    f"{esc(b.previous)}</a></span>" if b.previous else "")
            compare = f"""<label for="bundle-vs">Compared with</label>
    <select id="bundle-vs" name="vs">{vs}</select>
    {link}"""
        else:
            compare = '<span class="meta">the oldest bundle: nothing to compare with</span>'
        return f"""<form class="bundle" method="get" action="/bundle">
    <label for="bundle-date">Result bundle</label>
    <select id="bundle-date" name="date">{options}</select>
    {compare}
    <input type="hidden" name="page" value="{esc(rest)}">
    <noscript><button type="submit">Show</button></noscript>
  </form>"""

    def _upload_form(self) -> str:
        dates = esc(json.dumps(self.bundle.dates if self.bundle else []))
        return f"""<form class="upload" data-dates="{dates}">
    <label for="bundle-file">Upload a result bundle</label>
    <input id="bundle-file" type="file" accept=".tar.gz,.tgz,.tar" required>
    <button type="submit">Upload</button>
    <span class="meta" aria-live="polite"></span>
  </form>"""


# ==========================================================================
# Style and behaviour

CSS = (Path(__file__).resolve().parent / 'style.css').read_text()

JS = (Path(__file__).resolve().parent / 'behavior.js').read_text()


# ==========================================================================
# HTTP server


class App:
    """What the server shows: fixed datasets, or the result bundles of a directory."""

    def __init__(self, catalog: ConceptCatalog, settings: Settings):
        self.catalog = catalog
        self.settings = settings
        self.library = Library(settings.results_dir) if settings.results_dir else None
        self.store = None if self.library else DataStore(settings.datasets)

    def start(self) -> None:
        """Start loading what is shown first: the datasets, or the newest bundle (and the
        one before it, for the comparison)."""
        if self.store is not None:
            self.store.reload()
        elif dates := self.library.dates():
            self.library.store(dates[-1])
            if prev := Library.previous(dates[-1], dates):
                self.library.store(prev)

    def report(self, path: str) -> tuple[Report, str] | str | None:
        """The report and the path within it for a request path; a path to redirect to;
        or None for an unknown bundle."""
        if self.store is not None:
            return Report(self.store, self.catalog, self.settings), path
        dates = self.library.dates()
        if path in ("", "/"):
            if not dates:
                return Report(None, self.catalog, self.settings), ""
            return f"/bundle/{dates[-1]}/"
        m = re.fullmatch(r"/bundle/([^/]+)(?:/vs/([^/]+))?(/.*)?", path)
        if not m or m[1] not in dates:
            return None
        key, chosen = m[1], m[2]
        if chosen is not None and chosen != "none" and not (chosen in dates and chosen < key):
            return None  # only older bundles can be compared with
        base = f"/bundle/{key}" + (f"/vs/{chosen}" if chosen else "")
        if m[3] is None:
            return base + "/"
        prev = (None if chosen == "none" else chosen) if chosen else Library.previous(key, dates)
        ctx = BundleContext(key, dates, prev, self.library.store(prev) if prev else None, chosen)
        return Report(self.library.store(key), self.catalog, self.settings, base, ctx), m[3]


class Handler(BaseHTTPRequestHandler):
    app: App  # set on the subclass created in main()
    server_version = "qi-report"

    def do_GET(self) -> None:  # noqa: N802
        url = urlsplit(self.path)
        path = url.path
        if path == "/favicon.ico":
            self._send(HTTPStatus.NO_CONTENT, b"", "text/plain")
            return
        if path == "/bundle" and self.app.library:  # the bundle picker's form
            query = parse_qs(url.query)
            key = (query.get("date") or [""])[0]
            vs = (query.get("vs") or [""])[0]
            page = (query.get("page") or ["/"])[0]
            if not page.startswith("/") or page.startswith("//"):
                page = "/"
            # a comparison is kept only while it is older than the bundle shown
            if vs and vs != "none" and not vs < key:
                vs = ""
            via = f"/vs/{quote(vs, safe='')}" if vs else ""
            self._redirect(f"/bundle/{quote(key, safe='')}{via}{quote(page, safe='/')}")
            return
        try:
            found = self.app.report(path)
            if isinstance(found, str):
                self._redirect(found)
                return
            if found is None:
                status = HTTPStatus.NOT_FOUND
                body = ('<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">'
                        f'<title>Not found — QI report</title><style>{CSS}</style></head>'
                        '<body class="loading"><main><h1>Not found</h1>'
                        '<p>There is no such result bundle. <a href="/">Show the newest '
                        'one</a></p></main></body></html>')
            else:
                report, rest = found
                if report.store is None:
                    status, body = HTTPStatus.OK, report.no_bundles_page()
                elif rest.startswith("/profile/"):
                    status, svg = report.profile_svg(unquote(rest.removeprefix("/profile/")))
                    self._send(status, svg.encode("utf-8"),
                               "image/svg+xml" if status == HTTPStatus.OK else "text/plain")
                    return
                else:
                    status, body = report.render(rest)
        except Exception:  # noqa: BLE001 — local tool: show the traceback
            log.exception("error rendering %s", path)
            status = HTTPStatus.INTERNAL_SERVER_ERROR
            body = f"<pre>{esc(traceback.format_exc())}</pre>"
        self._send(status, body.encode("utf-8"), "text/html; charset=utf-8")

    def do_POST(self) -> None:  # noqa: N802
        url = urlsplit(self.path)
        if url.path == "/upload" and self.app.library:
            self._upload(parse_qs(url.query))
            return
        found = (self.app.report(url.path.removesuffix("reload"))
                 if url.path.endswith("/reload") else None)
        if not isinstance(found, tuple) or found[0].store is None:
            self._send(HTTPStatus.NOT_FOUND, b"not found", "text/plain")
            return
        report = found[0]
        stores = [report.store]
        if report.bundle and report.bundle.previous_store:
            stores.append(report.bundle.previous_store)
        if any([st.reload() for st in stores]):
            log.info("reload requested")
        referer = urlsplit(self.headers.get("Referer") or "/")
        self._redirect(referer.path or "/")

    def _upload(self, query: dict[str, list[str]]) -> None:
        """Store an uploaded bundle: the raw archive is the request body, its file name
        the ``name`` parameter; ``replace=1`` replaces a bundle of the same date."""
        def reply(status: HTTPStatus, **data) -> None:
            self._send(status, json.dumps(data).encode("utf-8"), "application/json")

        name = (query.get("name") or [""])[0]
        replace = (query.get("replace") or [""])[0] == "1"
        try:
            length = int(self.headers.get("Content-Length") or "")
        except ValueError:
            reply(HTTPStatus.LENGTH_REQUIRED, error="the upload has no Content-Length")
            return
        try:
            key, skipped = self.app.library.add(name, self.rfile, length, replace)
        except BundleError as e:
            reply(e.status, error=str(e))
            return
        except OSError as e:
            log.exception("cannot store the uploaded bundle %s", name)
            self.close_connection = True
            reply(HTTPStatus.INTERNAL_SERVER_ERROR, error=f"cannot store the bundle: {e}")
            return
        reply(HTTPStatus.OK, url=f"/bundle/{key}/", skipped=skipped)

    def _redirect(self, location: str) -> None:
        self.send_response(HTTPStatus.SEE_OTHER)
        self.send_header("Location", location)
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
    app = App(catalog, settings)
    app.start()
    handler = type("BoundHandler", (Handler,), {"app": app})
    try:
        server = ThreadingHTTPServer((settings.host, settings.port), handler)
    except OSError as e:
        raise SystemExit(f"error: cannot listen on {settings.host}:{settings.port}: {e}") from e
    server.daemon_threads = True
    host = f"[{settings.host}]" if ":" in settings.host else settings.host
    if app.library:
        what = (f"result bundles in {settings.results_dir} "
                f"({len(app.library.dates())} so far)")
    else:
        what = f"{settings.source} source, {len(settings.datasets)} dataset(s)"
    log.info("serving the QI report on http://%s:%d/ (%s)", host, settings.port, what)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    sys.exit(main())
