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
uv run qi_report.py results-mimic.csv            # a single set of results
uv run qi_report.py results/                      # every results-*.csv[.gz] in a directory
uv run qi_report.py --source db --dsn postgresql://user@host/qi my_schema
uv run qi_report.py --config qi-report.yaml --source db
```

(Or `pip install duckdb pyyaml` and run `python qi_report.py …`.) Then open
<http://127.0.0.1:8000/>. Options: `--host`, `--port`, `--result-concepts FILE`,
`--debug-concepts FILE`, `--qi-definitions FILE`, `-v` (log requests); see `--help`.

Dataset names default to the file name without `results-` and `.csv`, or to the
schema name; write `NAME=VALUE` to choose one, e.g. `site-a=export.csv`.

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

Two files decide which concept ids are shown and supply their names:

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

The sidebar lists the pages and the sections of the current one, highlighting
the section in view. Each QI shows the initial population and the adherence per
period, then one panel per debug quantity (mean and interquartile range), all
on a shared time axis; hovering shows a period's values in every panel, and
"Table view" lists them.

Periods are weeks, months or years, chosen per dataset from the span of its
dates (MIMIC-style shifted dates spanning decades get coarser periods).

Data is loaded once at startup (the page shows progress until done) and kept
in memory as aggregates; **Reload data** in the sidebar re-reads it in the
background while the previous data stays visible. A 430 MB results CSV loads
in about a second.
