# QI result visualizer

`qi_report.py` serves a report of quality indicator (QI) results over HTTP:
per QI and calendar period, the initial population (patient-days) and the mean
adherence, plus the QI debug values written alongside the results. It is a
standalone successor of `icu_omop_harmonizer.reports.qi_indicators` that never
looks at the clinical source data (so there are no patient/visit totals or
visit-day ratios any more).

## Running

The dependencies are [DuckDB](https://duckdb.org/) and
[PyYAML](https://pyyaml.org/) (Python ≥ 3.11). The script declares them inline, so with [uv](https://docs.astral.sh/uv/) nothing needs to be
installed:

```sh
uv run qi_report.py                               # the result bundles in results/ (see below)
uv run qi_report.py results-mimic.csv            # a single set of results
uv run qi_report.py results/                      # every results-*.csv[.gz] in a directory
uv run qi_report.py --source db --dsn postgresql://user@host/qi my_schema
uv run qi_report.py --config qi-report.yaml --source db
```

(Or `pip install duckdb pyyaml` and run `python qi_report.py …`.) Then open
<http://127.0.0.1:8000/>. Options: `--host`, `--port`, `--results-dir DIR`,
`--result-concepts FILE`, `--debug-concepts FILE`, `--qi-definitions FILE`, `-v` (log
requests); see `--help`.

Dataset names default to the file name without `results-` and `.csv`, or to the
schema name; write `NAME=VALUE` to choose one, e.g. `site-a=export.csv`.

## Result bundles

Without `INPUT` arguments or `--config`, the report shows **result bundles**: the
results, run logs and profiles of one evaluation of all datasets, kept in one
directory per date below `results/` (relative to the working directory;
`--results-dir DIR` picks another):

```
results/
  2026-11-10/  results-mimic.csv.gz  log-mimic.txt  profile-mimic.svg  …
  2026-11-18/  results-mimic.csv.gz  log-mimic.txt  profile-mimic.svg  …
```

The newest bundle is shown at `/`; the selector at the top of the sidebar shows
another one (at `/bundle/<date>/`, on the same page if that bundle has it). Bundles
are loaded when first shown and then kept in memory; the directory is re-read on
every request, so bundle directories copied in by hand show up too.

**Uploading:** the sidebar's upload form takes an archive named
`results-bundle-YYYY-MM-DD.tar.gz` (or `.tgz`, `.tar`) and unpacks it into
`results/YYYY-MM-DD/`. Only `results-*.csv[.gz]`, `log-*.txt` and `profile-*.svg`
files are taken, by their base name (directories in the archive are ignored);
anything else is skipped and listed. Uploading a bundle for a date that is there
already asks before replacing it. The same can be done from the command line:

```sh
curl --data-binary @results-bundle-2026-11-18.tar.gz \
  'http://127.0.0.1:8000/upload?name=results-bundle-2026-11-18.tar.gz'   # &replace=1
```

**Changes since an older bundle:** each bundle is compared with the one before
it by date, unless another is chosen in the sidebar's **Compared with** selector:
any older bundle, or nothing. The choice is part of the address
(`/bundle/<date>/vs/<older date>/`, or `/vs/none/`) and is kept while moving
between pages, and while switching to another bundle as long as it is still
older. The bundle compared with is loaded as well. Changes are marked with a small
purple marker next to the value (e.g. `+1,234 (+3.2%)`, `-1.5 pp` for adherence,
`new`); hover it for the value before. Two values count as different when they are
shown differently, so rounding noise is not a change. Shown are:

* on the overview, a **Changes since …** section summarizing every dataset (new,
  gone, unchanged, or what changed), and markers in the comparison and dataset
  tables;
* on a dataset page, a collapsed **Changes since …** section (its heading says
  how many values changed; links to it open it) with a table of everything that
  changed — rows and dates, run time, patients, failures and warnings, each kind
  of warning, and per QI the patient-days, mean adherence, periods and debug
  values (one row each, with columns for n and the mean) — plus markers on the
  run tiles, the warnings table and each QI's totals;
* in the charts (on dataset and QI pages), the preceding bundle's values drawn
  faintly in the colour of changes: adherence and debug values (mean and
  interquartile range) behind the current ones, the initial population as
  outlined bars over the current ones (so that both higher and lower counts show);
  hovering a period shows both. This needs the same kind of periods
  (week/month/year) in both bundles;
* on a QI page, markers in the summary table and each dataset's totals.

Kinds of warnings are matched across runs by level and message with the numbers
blanked out, since the message is an example whose ids and values vary. Run
totals are only compared when both run logs are complete.

## Data sources

* **`--source csv`** (default): CSV files as written by the CQL engine from
  `Main.cql` — no header, the OMOP `observation` columns in the order of
  `CSV_COLUMNS` in the script.
* **`--source db`**: an OMOP CDM-formatted PostgreSQL database with one schema
  per dataset whose `observation` table holds the same rows (`person_id` may be
  NULL; it is never read). Only `observation_concept_id`, `observation_date`
  and `value_as_number` of rows with concept ids ≥ 2,000,000,000 are
  transferred. The DuckDB `postgres` extension is downloaded on first use.
  `--dsn duckdb:PATH` reads a DuckDB file with the same layout instead, which
  is handy for testing.

For several datasets, list them in a YAML configuration file, see
[`qi-report.example.yaml`](qi-report.example.yaml). It can hold both a CSV and
a database list; `--source` picks one. Datasets given on the command line
replace those of the file.

## Run logs and profiles

Next to the QI results, the report shows what the CQL engine's console log says
about the evaluation run of each dataset: the time taken, the number of
successes and failures and their sum (the patients processed), and every
kind of warning with its count, its rate per 1,000 patients (comparable across
datasets of different size) and its share of all warnings. The engine groups
warnings by kind and prints the first message of each kind, so the message shown
is an example: ids and values in it belong to that one case.

Run log problems (failures, or no final summary line) are marked ❗; ⚠️ is
reserved for QIs without plausible adherence information.

If the run was profiled, its flamegraph (an SVG) is shown below, in a collapsed
**Profile** section; it is only fetched when that section is opened. Hover a
frame for its name, time, calls and share of the total; click it to zoom to it,
or use the zoom buttons (zooming widens the graph but keeps labels legible).
The SVG is rebuilt from its frames and labels only, so nothing else in the
file (e.g. scripts) reaches the page.

For a results file `results-<dataset>.csv[.gz]`, the log is looked up at
`log-<dataset>.txt` and the profile at `profile-<dataset>.svg`, both in the same
directory. In the configuration file, a dataset may name them explicitly —
which is the only way for `--source db`:

```yaml
csv:
  datasets:
    mimic:
      path: results-mimic.csv.gz
      log: logs/mimic-run.txt
      profile: profiles/mimic.svg
db:
  datasets:
    eicu:
      schema: eicu
      log: logs/log-eicu.txt
      profile: profiles/profile-eicu.svg
```

A missing log or profile is noted on the dataset page; a log without the final
summary line (e.g. a run that is still going or crashed) is flagged with the
last progress reported. Logs and profiles are re-read with **Reload data**.

## Concepts files

Two files decide which concept ids are shown and supply their names (by default,
they and the QI definitions file below are read from `data/` next to `qi_report.py`):

* **`qi-result-concepts.csv`**: the QI result concepts, one row each, with the
  columns `qi`, `concept_id` and `concept_name`.
* **`qi-debug-concepts.csv`**: the QI debug quantities, one row each. Required
  columns are `qi`, `concept_id` and `concept_name`. The optional `unit` column
  (UCUM-style, e.g. `cm[H2O]`, `kcal/d`) is shown next to the chart and table
  titles and in the hover values. Every other column is free-form: non-empty
  values are shown below the debug value's chart and table, so you can add
  columns (e.g. `formula`, `expected_range`) without changing the script. `description` is shown as plain text, the others
  as `column: value`.

A third, optional file names the QIs themselves:

* **`qi-definitions.csv`**: one row per QI with the columns `qi` (e.g. `QI01`),
  `name` (e.g. `ventilation`, shown next to the id in headings and the overview
  table), `short_name` (a shorter variant for the sidebar; defaults to `name`)
  and `title` (the full title, shown below the heading and as a tooltip). The
  names and titles come from
  [indicate-qi-definitions](https://github.com/umg-minai/indicate-qi-definitions).

All of these files are re-read automatically whenever they change, with no restart or
data reload needed. If a file can't be parsed (e.g. while it is being saved),
a warning appears and the last good version stays in use. Concept ids found in
the data but in neither file are listed in a warning on the dataset page.
`--result-concepts`/`--debug-concepts`/`--qi-definitions` (or `result_concepts`/
`debug_concepts`/`qi_definitions` in the configuration file) point elsewhere.

If a concept is in both files, the debug concepts file wins.

## The report

* **Several datasets:** an overview with a cross-dataset comparison table, a
  page per dataset (all QIs), and a page per QI (all datasets).
* **One dataset:** just the dataset page.
* **Result bundles:** the same, for the bundle selected, with its changes since
  the preceding bundle (see [Result bundles](#result-bundles)).

The sidebar lists the pages and the sections of the current one, highlighting
the section in view. Each QI shows the initial population and the adherence per
period, then one panel per debug quantity (mean and interquartile range), all
on a shared time axis; hovering shows a period's values in every panel, and
"Table view" lists them.

Periods are weeks, months or years, chosen per dataset from the span of its
dates (MIMIC-style shifted dates spanning decades get coarser periods).

Data is loaded once at startup (the page shows progress until done) and kept
in memory as aggregates; **Reload data** in the sidebar re-reads it in the
background while the previous data stays visible (for a result bundle, the
bundle shown and the one it is compared with). A 430 MB results CSV loads
in about a second.
