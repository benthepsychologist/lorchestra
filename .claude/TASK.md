# Current Epic: e005b — Command-Plane Refactoring

**Epic:** `local-governor/epics/e005b-command-plane-and-performance/epic.yaml`

## Status

Specs e005b-01 through e005b-06 are **complete** and merged to main.

| Phase | Specs | Status |
|-------|-------|--------|
| Schema & Executor | 01, 02 | Done |
| Boundaries & E2E | 03a-d | Done |
| CLI Rewire & Migration | 04a-f | Done |
| Handler Collapse | 05 | Done |
| Pipeline Orchestration | 06 | Done |
| Storacle Method Mapping | 07 | **Next** |
| Performance | 08a, 08b | Planned |

## Next Up

- **e005b-07** — Storacle method mapping: plan.build batch wrapping for bq.upsert
  - **Spec:** `local-governor/projects/lorchestra/specs/e005b-command-plane-and-performance/e005b-07-storacle-method-mapping.md`
  - **Prereq:** ~~Commit 3 bug fixes from smoke testing~~ Done (88328e0)
  - **Summary:** All 35 ingest/canonize/formation YAMLs incorrectly use `method: wal.append`. Fix plan.build to support batch wrapping mode (payload_wrap, id_field, dataset/table/key_columns bundling). Update all YAML defs with correct method + sink metadata. Formation jobs (3) retire in favor of existing measurement_projector + observation_projector pairs.

## Bug Fixes (committed)

3 bugs found during V1 vs V2 smoke testing — committed as `88328e0`:
1. `lorchestra/storacle/client.py` — import path: `from storacle.rpc import execute_plan`
2. `lorchestra/storacle/client.py` — call signature: remove `correlation_id` kwarg
3. `lorchestra/plan_builder.py` — emit `storacle.plan/1.0.0` contract format (+ all test updates)
