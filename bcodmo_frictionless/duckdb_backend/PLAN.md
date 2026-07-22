# DuckDB Execution Backend — Implementation Plan (single-source edition)

Location: `bcodmo_frictionless/bcodmo_frictionless/duckdb_backend/`
(importable as `bcodmo_frictionless.duckdb_backend`).

Status: proposed. Prereq validated — the DSL→SQL prototype proves all 4 BATS
`boolean_add_computed_field` targets (incl. the 23-branch `Vessel`) compile to
**byte-identical output** vs the current processor, at **~365×** (52.45s→0.14s /
500k rows). Artifacts in `scratchpad/` to be moved into this package.

---

## 1. Goal & constraints

Run the _existing_ pipeline-spec on DuckDB (vectorized, multi-threaded,
out-of-core) for large jobs, keeping dataflows as the always-works fallback.

Non-negotiable:

1. **Same pipeline-spec IR.** UI and spec format unchanged.
2. **Never OOM.** Any size completes; DuckDB spills to local disk under a
   `memory_limit`. Validated larger-than-RAM before rollout.
3. **Identical output.** Byte-for-byte equal dumped CSVs vs the dataflows engine.
4. **Single source of truth (this document's central theme).** Each processor's
   logic is written and maintained in **exactly one place**. The two engines are
   thin adapters over it. Divergence is made _impossible to ship_ by mandatory,
   automated differential tests (see §3, §11).
5. **Incremental & reversible.** Behind a router; unsupported ⇒ dataflows lane.

---

## 2. The single-source principle (core idea — read first)

The apparent duplication ("29 processors × 2 engines") is mostly not logic
duplication. We eliminate the rest with one contract.

### 2.1 What is (and isn't) duplicated

- **Relational / structural ops** (`join`, `sort`, `boolean_filter_rows` body as
  `WHERE`, `concatenate`, `unpivot`, `dedup`, `duplicate`, `delete_fields`):
  these are library/SQL primitives, not bcodmo logic. Using `JOIN`/`ORDER BY`/
  `WHERE` _removes_ code; it does not fork it. (The standard dataflows processors
  have **no bcodmo per-row code at all**, so nothing to unify.)
- **DSL logic** (boolean/math): **one grammar** (`boolean_processor_helper`).
  `parse_boolean` interprets it; `dsl_sql.py` compiles it. One source, two
  readers — not two implementations.
- **Custom per-row value transforms** (`convert_date`, `split_column`,
  `convert_units`, `find_replace`, `convert_to_decimal_degrees`,
  `extract_nonnumeric`, `string_format`, `set_types` casting): the **only** real
  duplication surface — and the contract below reduces it to zero-by-default.
- **Schema/metadata manipulation** (nearly every processor): shared once via
  `update_schema` (a good refactor regardless of DuckDB).

### 2.2 The Processor contract

One class per processor family. The **existing bcodmo Python is the authority**.

```python
class Processor:
    name: str                                   # matches the spec `run:` value

    def update_schema(self, schema, params) -> Schema:
        """Field add/remove/rename/reorder/type/metadata changes.
        SHARED: called by BOTH engines. The one place schema logic lives."""

    def process_rows(self, rows, params) -> Iterator[row]:
        """The per-row transform. THE single source of truth for row logic.
        Reused verbatim by the dataflows lane AND (as a UDF) by the DuckDB lane."""

    def to_sql(self, rel: Relation, params) -> Relation | None:
        """OPTIONAL native fast path. Return None ⇒ the DuckDB engine wraps
        `process_rows` as an Arrow UDF. Present ⇒ native SQL, and a differential
        test against `process_rows` is REQUIRED and auto-enforced (§3)."""
```

### 2.3 Both engines are thin adapters over the contract

- **dataflows lane** (`flow(params)`): `update_schema` on the datapackage, then
  `process_rows` per resource. (This is essentially what the processors already
  do — refactor to call the contract methods, don't rewrite them.)
- **DuckDB lane** (`engine.py`): `update_schema` on the tracked schema, then
  `to_sql(rel, params)` if defined, else `rel.map(udf(process_rows))`.

So a processor is authored **once** (contract methods). Neither engine contains
processor logic — they only _dispatch_ to it.

### 2.4 UDF-by-default ⇒ zero duplication baseline

Ship Phase 1 with **`to_sql = None` for every custom processor.** Then:

- One implementation (`process_rows`), reused verbatim. **No forked logic.**
- **Near-100% pipeline coverage** immediately (any row-map processor wraps as a
  UDF), so almost nothing falls back to the dataflows lane.
- Whole classes of divergence risk vanish because the **exact Python code runs**:
  casting (wrap the tableschema caster), regex dialect, date parsing — nothing to
  diverge.
- You already get the _architectural_ wins (native join, tee elimination, native
  sort, native scan/IO, disk spill) with **not one line of reimplemented logic.**

### 2.5 `to_sql` is an opt-in, auto-verified performance override

Add `to_sql` **only** where the profiler says a per-column Python UDF is a
bottleneck. The first natural candidates need **no bespoke code**:

- **DSL family** (`boolean_filter_rows`, `boolean_add_computed_field`): `to_sql`
  comes from the shared grammar compiler (`dsl_sql.py`) — same source as the
  interpreter. This is the 365× Vessel win, single-source.
- **Relational primitives** (`join`, `sort`, `concatenate`, `unpivot`): native
  SQL, no bcodmo logic exists to duplicate.

A custom per-row step gets a hand-written `to_sql` later, per step, only if proven
hot — and it **cannot merge without passing the differential test** (§3).

---

## 3. Anti-divergence discipline (documentation + testing) — MANDATORY

This section is the contract that makes "single source" real. It is enforced in
CI; a processor that violates it does not merge.

### 3.1 Documentation requirements (per processor)

Every processor module carries a `SEMANTICS` docstring block that is the **human
source of truth**, covering:

1. **Behavior** — what it does to schema and to rows, in prose.
2. **Parameters** — every param, type, default, and effect.
3. **Edge cases & invariants** — null/missing handling, ordering effect
   (`keep`/`reset`, see §7), type coercions, error conditions.
4. **Engine notes** — `process_rows` is the source of truth; whether a `to_sql`
   fast path exists and, if so, exactly which behaviors it must mirror and any
   deliberately-accepted differences (there should be none; if unavoidable, they
   are listed and tested).

A processor with a `to_sql` MUST document the mirrored-behavior list. Missing or
stale docs fail review (§3.4 lints the presence of the block).

### 3.2 The testing contract (per processor)

Three layers, all in CI:

- **(A) Differential CSV test — applies to EVERY processor, both engines.**
  A shared fixture set (crafted edge cases + real prod specs) runs through the
  dataflows lane and the DuckDB lane; the dumped CSV bytes must be **identical**.
  This is the definition of "supported" — the router will not enable a processor
  in the DuckDB lane until its differential tests are green.
- **(B) `to_sql` equivalence + fuzz — REQUIRED whenever `to_sql` is not None.**
  Directly compares `process_rows` output vs `to_sql` output over (i) curated
  edge cases and (ii) randomly generated valid inputs/params. This is what
  guarantees the optional fast path can never silently diverge from the source of
  truth. For the DSL family this is a grammar fuzzer (random valid expressions →
  assert interpreter == SQL).
- **(C) Out-of-core test — for full-table ops** (`join`/`sort`/`concatenate`/
  `unpivot`): a fixture exceeding `memory_limit` must complete and match (proves
  the never-OOM guarantee). ✅ **DONE** in `tests/test_never_oom.py` for
  load→transform→dump AND the structural ops. Ingest appends in bounded batches
  (fast multi-row `INSERT VALUES`); egress streams via `Engine.typed_rows_iter`
  (lazy chunked `fetchmany` + on-the-fly cast); dump feeds that stream to
  `S3Dumper`. **join/sort/unpivot/concatenate now STREAM**: they feed the live
  funcs (join_aux/\_sorter/unpivot/concatenator — all KVFile-backed / out-of-core)
  LAZY row iterators and drain the output straight into a fresh table via
  `Engine.reingest_stream`, so no input or output materializes in Python. The
  read-while-write hazard (reading a resource while writing its replacement) is
  solved by a dedicated write cursor (`Engine._wc`): reads stay on the main
  connection (relations + their `.query()` aliases are connection-bound), writes
  go on the cursor (shared catalog, independent result). **Leaf UDF processors also
  STREAM** now: `Engine.udf_map` fetches input in chunks, runs `process_rows` as one
  lazy pass, and streams the 1:1 output (rownum carried via a deque) into a fresh
  table — closing the last hole, so EVERY supported pipeline (load → leaf UDFs →
  structural → dump) is never-OOM. Proven: streaming peak ≪ full-materialization
  peak (RSS-sampled) for egress, large join, large sort, and a large leaf UDF;
  lazy==eager at scale; completion under a `memory_limit` below the dataset.

### 3.3 The invariant, stated plainly

> There is exactly one implementation of each processor's logic
> (`process_rows` + `update_schema`). Any `to_sql` is a **provably-verified
> mirror** of `process_rows`, enforced by test (B) on every CI run — never an
> independent implementation.

### 3.4 CI gates (mechanical enforcement)

- A registry lint asserts: every processor has `process_rows`+`update_schema`, a
  `SEMANTICS` docstring block, and ≥1 differential fixture.
- A test-collection lint asserts: **every processor defining `to_sql` has a
  corresponding equivalence+fuzz test** (B). No `to_sql` ships untested — this is
  a hard collection-time failure, not a convention.
- Coverage lint: every processor appears in ≥1 differential spec (A).
- The router reads a generated capability manifest; a processor is DuckDB-enabled
  **only** if A (and B if applicable, C if full-table) are green.

---

## 4. Architecture & package layout

All backend code lives in `bcodmo_frictionless.duckdb_backend`. The processor
_contract methods_ live with the processors in
`bcodmo_frictionless.bcodmo_pipeline_processors` (that is where the single source
of truth belongs). laminar_server gets only a thin routing seam (§9).

```
bcodmo_frictionless/bcodmo_frictionless/
  bcodmo_pipeline_processors/        # processors gain contract methods here (source of truth)
      convert_date.py                #   update_schema / process_rows [/ to_sql]
      ...
  duckdb_backend/
      PLAN.md                        # this doc
      __init__.py
      processor.py                   # Processor base class + registry
      engine.py                      # Relation model, connection/config, materialization policy, UDF wrap
      compiler.py                    # spec steps -> relation DAG (dispatch to processors)
      dsl_sql.py                     # boolean + math DSL -> SQL (from scratchpad prototype)
      types.py                       # frictionless schema <-> DuckDB types, casting helpers
      ingest.py                      # load  -> relations (read_csv / regex-CSV bridge / S3)
      egress.py                      # dump_to_s3 / dump_to_path -> COPY + orchestration glue
      progress.py                    # redis progress + error tagging (mirror run.py UX)
      router.py                      # capability manifest + "can this spec run here?"
      equivalence/                   # the never-diverge harness (§11)
          harness.py                 #   differential CSV runner (both lanes)
          fuzz_dsl.py                #   grammar fuzzer
          fixtures/                  #   per-processor edge-case + real specs
      tests/                         # (A) differential, (B) to_sql equivalence, (C) out-of-core
```

### 4.1 Execution model

- Each **resource** = a `DuckDBPyRelation` + a tracked **frictionless schema**
  (fields, types, `outputFormat`, `bcodmo:` metadata). We track schema ourselves
  (via `update_schema`) because dump/datapackage need it and many steps are
  schema-only.
- Pipeline state = `{resource_name: (relation, schema)}`. Steps rewrite entries.
- **Lazy chaining, bounded materialization**: projections/filters/computed fields
  chain lazily; materialize a temp table only at dump, `sort`, `join`,
  `checkpoint`, or a fan-out point (a relation consumed by >1 resource, so it
  isn't recomputed). DuckDB spills these — out-of-core preserved.

### 4.2 Storage model & the casting boundary (DECIDED)

- **All data is stored as VARCHAR** in DuckDB. Ingest is uniform (matches bcodmo
  load `cast_strategy=strings`), and never-OOM is trivial (no type surprises).
  The tracked schema carries the _intended_ frictionless type per field.
- **Casting is deferred and materialized at exactly two boundaries**, both of
  which delegate to `casting.cast_rows` = frictionless `schema_validator` (the
  dataflows lane's own code — **zero reimplementation, cannot diverge**):
  1. the **UDF fallback** (`udf_map`): cast VARCHAR→typed before calling a
     processor's `process_rows`, so it sees the SAME typed values the dataflows
     lane feeds it, then format back to VARCHAR. **Cast-IN and format-OUT are
     both implemented** (`casting.cast_rows` / `casting.format_out_rows`):
     format-out serializes typed UDF outputs so `cast_rows(format_out_rows(v)) == v`
     — temporals via the field's **storage format** (`casting._storage_format`:
     the explicit cast `format` if set, else `outputFormat`), number/integer/string
     via `str`. The storage-format invariant is what lets `convert_date` round-trip:
     its output field carries only `outputFormat` (no cast `format`, mirroring the
     live schema), so both format-out and cast-in key off `outputFormat` and stay
     exact inverses. `cast_rows` injects `format = outputFormat` for such temporal
     fields before delegating to `schema_validator`. Proven the exact inverse by
     `tests/test_casting_roundtrip.py` and end-to-end by `tests/test_convert_date.py`
     (byte-identical CSV). Edge: a temporal with NO explicit format anywhere
     (`format` and `outputFormat` both pattern-less) falls back to `isoformat()` and
     may not re-cast under frictionless's strict default parser — real temporal
     processors always set an explicit output format.
  2. the **dump** (`typed_rows` → `CustomCSVFormat` serializers): cast with the
     final schema, then serialize with bcodmo's exact `CustomCSVFormat.SERIALIZERS`
     (`num_to_string`, scientific notation, temporal `outputFormat`).
- **`set_types` is therefore a schema/deferred-cast tier**, not a row processor:
  in the DuckDB lane it updates field _types_ only and leaves values as VARCHAR
  (`apply` override). It has **no `to_sql`** — casting is not applied at that step —
  so the per-step `to_sql == process_rows` gate does not apply to it. Its
  correctness is proven **end to end** (byte-identical dump) plus the
  deferred-cast unit test (`typed_rows == process_rows(input, schema)`).
- Native `to_sql` fast paths that need a number (DSL, round, convert) do their own
  `TRY_CAST` at use-site (already in the DSL compiler), so they work directly on
  VARCHAR storage regardless of where `set_types` sits.
- **`to_sql` is opt-in ONLY where it is provably byte-exact.** DuckDB `DOUBLE`/
  `DECIMAL` arithmetic cannot reproduce bcodmo's `Decimal` per-value string repr
  (`Decimal('180')*-1` → `'180'`, not `'180.0'`). So the boolean_add **math-value**
  path returns `None` from `to_sql` and defers to the exact UDF; string/regex
  values (incl. the hot 23-branch Vessel classification) keep the SQL `CASE`. The
  **byte-identical differential test is what catches** a non-exact `to_sql` — this
  is the governance model working as designed, not an exception to it.

---

## 5. Row order (cross-cutting — implement first)

dataflows is ordered streaming; SQL relations are unordered. For identical CSVs:

- Ingest assigns a hidden `__rownum__` (BIGINT, per resource).
- Order-preserving steps carry it untouched (`keep`).
- `join` orders by target `__rownum__` (half-outer/left), appends full-outer
  leftovers after (match current), then reassigns (`reset`).
- `sort` orders by keys with `__rownum__` tiebreak (stable, matches dataflows),
  then reassigns (`reset`).
- `concatenate`/`unpivot`: order = source order then row order (`reset`).
- `dump` does `ORDER BY __rownum__`; `__rownum__` never appears in schema/output.

Each processor's `SEMANTICS` block states its order effect; the differential test
enforces it.

---

## 6. Per-processor plan (every processor)

Fast-path source: **prim** = relational/SQL primitive (no bcodmo logic) · **dsl**
= shared grammar compiler · **udf** = `process_rows` wrapped (source of truth, add
native `to_sql` later only if hot) · **schema** = `update_schema` only · **bridge**
= I/O. "1st-class Phase 1" = needed for the BATS pipeline end-to-end.

| Processor                            | Fast-path source | `process_rows`?         | Native `to_sql` plan                   | Notes / semantics to preserve                                                                                                                                                                                                                                                                                                                                                                        |
| ------------------------------------ | ---------------- | ----------------------- | -------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `join`                               | prim             | ✅ DONE (bare/std)      | opt-in native `JOIN`+`GROUP BY` later  | drives LIVE `dataflows.join_aux` verbatim over a package shim (byte-exact schema + rows; KVFile => out-of-core for free). Cast-in/format-out via shared `casting`. inner/half-outer/full-outer + source.delete + `*` expansion tested. Array aggs (set/array/counters) RAISE (VARCHAR round-trip gap). Bare/standard run-name; bcodmo `join_aux` dead-code path avoided                              |
| `sort`                               | prim             | ✅ DONE (bare/std)      | opt-in `ORDER BY` later                | drives LIVE `sort_rows._sorter`+`KeyCalc` verbatim on cast-in typed rows (numeric bit-encode matches); re-ingests original VARCHAR in sorted order (no value round-trip); KVFile => spills. order_effect=reset                                                                                                                                                                                       |
| `concatenate` (std + bcodmo)         | prim             | n/a / thin              | **DONE (UDF concatenator)**            | field mapping; `include_source_name`                                                                                                                                                                                                                                                                                                                                                                 |
| `unpivot` (std)                      | prim             | ✅ DONE (bare/std)      | opt-in `UNPIVOT` later                 | drives LIVE `unpivot` func verbatim over a package shim (byte-exact new schema + reshaped rows; regex key substitution). VARCHAR rows fed/re-ingested directly (no arithmetic). order_effect=reset                                                                                                                                                                                                   |
| `delete_fields`                      | prim             | delegates (std)         | **DONE** `* EXCLUDE`                   | regex on names; carries `__rownum__`                                                                                                                                                                                                                                                                                                                                                                 |
| `duplicate`                          | prim             | ✅ DONE (bare/std)      | native alias later                     | multi-resource copy (apply override); source-order preserved via `__rownum__`, fresh copy schema; insert-after-source or `duplicate_to_end`. **kills the KVFile tee**                                                                                                                                                                                                                                |
| `boolean_filter_rows`                | dsl              | interpret               | **DONE** `WHERE` (dsl_sql)             | order keep                                                                                                                                                                                                                                                                                                                                                                                           |
| `boolean_add_computed_field`         | dsl              | interpret               | **DONE** reversed `CASE` (dsl_sql)     | **proven**; last-match-wins; math values                                                                                                                                                                                                                                                                                                                                                             |
| `add_computed_field` (std)           | dsl/prim         | ✅ DONE (bare/std)      | opt-in per-op SQL later                | leaf/1:1 default apply; reuses LIVE `get_new_fields`+`process_resource`; cast-in/out via UDF path. constant/sum/avg/min/max/multiply/format/join                                                                                                                                                                                                                                                     |
| `set_types` (bcodmo)                 | schema           | cast (schema_validator) | none — cast deferred to dump (§4.2)    | **DONE.** Schema/deferred-cast tier: updates types only, values stay VARCHAR; cast materializes at dump via `schema_validator` ⇒ **zero cast divergence**                                                                                                                                                                                                                                            |
| `set_types` (std, bare)              | schema           | cast (schema_validator) | none — cast deferred to dump           | ✅ **DONE.** Bare-name schema-only apply; per-name option→type update, None→delete field. Ref = dataflows `set_type` primitive (**prod `standard_flows.set_types` is BROKEN: undefined `_set_type`**)                                                                                                                                                                                                |
| `bcodmo…load`                        | source (0→N)     | n/a                     | drive live `load.flow` → `ingest_iter` | ✅ **DONE.** `apply`-only source; drives the LIVE `load.flow` over `datastream()` and streams each resource into `Engine.ingest_iter` (bounded-batch, disk-spilling → never-OOM). All parsers/loaders/sheets/S3 come free (same live code both lanes). 16 differential tests inc. moto S3 + bcodmo-aws loader. std bare `load` not built (UI emits only the bcodmo one)                              |
| `bcodmo…convert_date`                | udf              | date logic              | **DONE (UDF)**                         | python-strptime path natively translatable; excel/matlab/decimalYear stay UDF                                                                                                                                                                                                                                                                                                                        |
| `bcodmo…split_column`                | udf              | regex/delim             | **DONE (UDF)**                         | `regexp_extract`/`str_split` when promoted                                                                                                                                                                                                                                                                                                                                                           |
| `bcodmo…convert_units`               | udf              | ×const                  | **DONE (UDF)**                         | fixed factors (`*0.3048`…) — pure arithmetic                                                                                                                                                                                                                                                                                                                                                         |
| `bcodmo…find_replace`                | udf              | regex replace           | **DONE (UDF)**                         | `regexp_replace`; upper/lowercase-per-group stays UDF                                                                                                                                                                                                                                                                                                                                                |
| `bcodmo…round_fields`                | udf              | round                   | **DONE (UDF)**                         | rounding-mode parity check                                                                                                                                                                                                                                                                                                                                                                           |
| `bcodmo…convert_to_decimal_degrees`  | udf              | DMS parse               | **DONE (UDF)**                         | `regexp_extract`+arithmetic when promoted                                                                                                                                                                                                                                                                                                                                                            |
| `bcodmo…extract_nonnumeric`          | udf              | regex split             | **DONE (UDF)**                         |                                                                                                                                                                                                                                                                                                                                                                                                      |
| `bcodmo…string_format`               | udf              | template                | **DONE (UDF)**                         | `printf`/`concat_ws` when promoted                                                                                                                                                                                                                                                                                                                                                                   |
| `bcodmo…edit_cells`                  | udf              | positional              | **DONE (UDF)**                         | `row_number()`+`CASE` when promoted                                                                                                                                                                                                                                                                                                                                                                  |
| `bcodmo…reorder_fields`              | schema           | identity                | **DONE** identity (schema-only)        | projection order via schema                                                                                                                                                                                                                                                                                                                                                                          |
| `bcodmo…rename_fields`               | schema           | delegates               | **DONE** `* RENAME`                    | `AS`; dup-name guard                                                                                                                                                                                                                                                                                                                                                                                 |
| `bcodmo…rename_fields_regex`         | schema           | —                       | **DONE** `* RENAME`                    | regex on names                                                                                                                                                                                                                                                                                                                                                                                       |
| `bcodmo…rename_resource`             | schema           | —                       | **DONE** (rename key)                  | + redis progress rename                                                                                                                                                                                                                                                                                                                                                                              |
| `bcodmo…remove_resources`            | schema           | —                       | **DONE** (drop resource)               | drop relation                                                                                                                                                                                                                                                                                                                                                                                        |
| `update_package` / `update_resource` | schema           | —                       | n/a                                    | metadata only                                                                                                                                                                                                                                                                                                                                                                                        |
| `bcodmo…update_fields`               | schema           | —                       | **DONE** (schema-only)                 | field metadata (verified)                                                                                                                                                                                                                                                                                                                                                                            |
| `bcodmo…add_schema_metadata`         | schema           | —                       | **DONE** (schema no-op)                | schema metadata                                                                                                                                                                                                                                                                                                                                                                                      |
| `bcodmo…load`                        | source (0→N)     | —                       | drive live `load.flow` → `ingest_iter` | ✅ **DONE** — see the DONE-rows table above; carries the full load descriptor for dump                                                                                                                                                                                                                                                                                                               |
| `bcodmo…dump_to_s3`                  | sink (N→0)       | —                       | drive live `S3Dumper`                  | ✅ **DONE.** `apply`-only sink drives the LIVE `S3Dumper` over a `DataStream` built from engine resources (load descriptor re-stamped with live schema + typed rows) → byte-identical CSV **and** datapackage.json + pipeline-spec.yaml. Differential vs dataflows lane via `ThreadedMotoServer` (billiard async upload needs a real endpoint). `use_titles` is a prod NameError (untested). 4 tests |
| `bcodmo…dump_to_path`                | sink (N→0)       | —                       | drive live path dumper                 | 🚫 OUT OF SCOPE (user, 2026-07-21) — UI's "Dump final" is dump_to_s3; would be a trivial CRLF→LF variant of the same dumper base if ever needed                                                                                                                                                                                                                                                      |

The BATS pipeline is fully covered by the Phase-1 rows (load, join, boolean\_\*,
delete, duplicate, dump) plus UDF-default set_types/convert_date/split_column.

---

## 7. Ingest / egress

**Ingest (`ingest.py`).** `read_csv(all_varchar=true, …)` for standard CSV/S3.
Regex-delimiter / fixed-width / seabird: bridge the **existing** bcodmo parser →
Arrow → `con.from_arrow` (zero reimplementation), migrate hot formats to
`regexp_split_to_array` later. `remove_empty_rows` → `WHERE NOT (all cols null)`.
Assign `__rownum__`. Multi-source → multiple relations.

**Egress (`egress.py`).** Pre-format numeric/temporal columns to strings **in SQL**
to match `CustomCSVFormat` exactly (this is where byte-equivalence is won),
`COPY (… ORDER BY __rownum__) TO <local csv>`, then feed the file to the
**existing** multipart S3 uploader (keeps redis progress, part sizing,
datapackage.json, pipeline-spec.yaml, `dump_unique_lat_lon`). Local ephemeral disk.

---

## 8. Router & worker lanes (thin laminar_server seam) — ✅ DONE

Implemented as `duckdb_backend/runner.py` + a thin branch in
`laminar_server/app/pipeline/run.py::_run_pipeline`:

- `runner.is_supported(spec)` = every step in the processor REGISTRY and no
  `checkpoint` (a dataflows/EFS-only feature). Per-pipeline routing; no
  intra-pipeline engine mixing. Else the whole pipeline runs on the dataflows lane
  UNCHANGED.
- `_should_use_duckdb(steps, metadata)` enables the engine per-run via
  `metadata['engine']=='duckdb'` or env `LAMINAR_ENGINE=duckdb`, gated by
  `is_supported`. `_run_pipeline_duckdb` mirrors the dataflows step-prep (cache_id /
  pipeline_spec injection, dump-bucket check, summary dump, dump_location marker,
  lat/lon datapackage reuse, redis cleanup) and delegates execution to
  `runner.execute` (a memory_limit/temp_directory Engine → never-OOM) + the UI
  sample to `runner.build_sample`. Same `(error, cache_files, sample)` contract.
- All engine logic stays in `bcodmo_frictionless.duckdb_backend`; laminar_server
  gets only the gate + orchestration glue (defensive import → falls back if the
  backend is unavailable). Config env: `LAMINAR_ENGINE`,
  `LAMINAR_DUCKDB_MEMORY_LIMIT`, `TMP_DIRECTORY`.
- NOT YET: Celery-queue selection (CPython `duckdb` worker vs PyPy) — the seam
  currently runs inline in the existing worker; queue routing is a follow-up.
  End-to-end staging validation still pending (only `runner` is unit-tested).

---

## 9. Out-of-core / memory safety

`SET memory_limit=…; SET temp_directory=<local ephemeral>; SET
max_temp_directory_size=…; SET threads=<vCPU>;` — temp + COPY targets on ephemeral
SSD (same rationale as the KVFile/TMPDIR fix), never EFS. CI test (C) proves
spilling at > memory sizes.

---

## 10. Equivalence harness (`equivalence/`) — the never-diverge engine

- `harness.py`: given a spec + inputs, runs the **dataflows lane** and the
  **DuckDB lane** and diffs dumped CSVs byte-for-byte (+ datapackage.json modulo
  known-volatile fields). Powers test (A) and the shadow-mode diff logger.
- `fuzz_dsl.py`: random valid boolean/math expressions → assert interpreter ==
  SQL. Powers test (B) for the DSL family.
- `fixtures/`: per-processor edge cases (nulls, missing values, boundaries, regex,
  dates, scientific numbers) + real prod specs.
- Everything wired into CI as the gates in §3.4.

---

## 11. Progress, errors, cancellation (`progress.py`)

Redis progress per resource/rows (reuse `get_redis_progress_*`), emitted at COPY
time + coarse per-step markers. Error mapping reproduces `run.py`'s messages
(cast/missing-file/dup-headers…) tagged with step index like `_StepTagger`.
Cancellation → DuckDB `interrupt()`.

---

## 12. Phased rollout (UDF-first ⇒ single source from day one)

- **Phase 0 — contract & harness.** `Processor` base + registry; refactor the
  dataflows `flow()`s to call `update_schema`/`process_rows`; `engine.py` skeleton
  with UDF-wrapping; `__rownum__` policy; `dsl_sql.py` moved in + finished;
  equivalence harness + CI gates (§3.4). Nothing user-facing.
- **Phase 1 — native core + UDF-default, shadow mode.** Native `to_sql` for the
  relational prims + DSL family + projections + load(CSV)/dump(CSV); **everything
  else UDF-default**. Run DuckDB in shadow (execute both, serve dataflows, log CSV
  diffs) to zero diffs. **BATS runs end-to-end here.**
- **Phase 2 — serve + selective fast paths.** Flip router to serve DuckDB for
  covered pipelines. Add `to_sql` to custom steps **only** as the profiler
  demands, each with its mandatory (B) test.
- **Phase 3 — long tail.** sort/unpivot/concatenate, checkpoints (`CREATE TABLE`),
  parquet dump, hot regex-CSV formats native, byte-format hardening.
- **Phase 4 — default for large jobs.** Route by size/coverage; PyPy dataflows
  stays the permanent fallback for unsupported steps.

---

## 13. Risk register

| Risk                                             | Mitigation                                                                                                |
| ------------------------------------------------ | --------------------------------------------------------------------------------------------------------- |
| Byte-exact CSV (number/date formatting, quoting) | SQL pre-formatting to match `CustomCSVFormat`; differential CSV gate (A)                                  |
| Row order                                        | `__rownum__` discipline (§5); enforced by (A)                                                             |
| Cast semantics DuckDB vs tableschema             | **UDF-wrap the tableschema caster by default** (zero divergence); native `TRY_CAST` only later behind (B) |
| Regex dialect (Python `re` vs RE2)               | UDF-default runs Python `re`; native promotion audited + (B)-tested                                       |
| Decimal vs double                                | `DECIMAL` where the interpreter uses `Decimal`; (A) at dump precision                                     |
| Arrow UDF marshalling (nulls/types)              | covered by (A) for every processor                                                                        |
| Silent `to_sql` divergence                       | **impossible**: (B) is a hard CI collection gate (§3.4)                                                   |
| Never-OOM regression                             | out-of-core test (C); conservative `memory_limit`                                                         |

---

## 14. Definition of done (per processor)

1. Contract methods implemented on the processor (`update_schema`,
   `process_rows`; `to_sql` optional).
2. `SEMANTICS` docstring block complete (§3.1).
3. Registered; router capability + `supports(params)` predicate set.
4. Differential CSV test (A) green in ≥1 spec.
5. If `to_sql` present: equivalence+fuzz test (B) green.
6. If full-table op: out-of-core test (C) green.

The router treats a processor as DuckDB-supported **only** after 1–6.

---

## Appendix — validated prototype

`scratchpad/dsl_to_sql.py`, `test_vessel_compile.py`, `bench_vessel.py`: all 4
BATS `boolean_add_computed_field` targets → identical output; Vessel 52.45s→0.14s
(**365×**). Move into `duckdb_backend/dsl_sql.py` + `equivalence/` as the seed.
