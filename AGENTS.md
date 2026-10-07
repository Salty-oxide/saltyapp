# Salty — agent instructions

Single source of truth for AI coding agents working in this repo. Claude Code
(via an `@` import in `.claude/CLAUDE.md`), OpenAI Codex and GitHub Copilot all
read this file (`AGENTS.md` at the repo root). Do not duplicate these rules in
tool-specific skill or instruction files.

## Safe-change workflow

Mandatory for ANY code change (Tauri/Rust backend + React/TypeScript frontend):
plan tests first, TDD, modular code, keep CI green, compile once, bump the
version, never commit, never regress existing behaviour. Applies before any
feature, bugfix, refactor or dependency change, however small.

Highest rule: **do not introduce bugs into existing behaviour.** If a step conflicts with that, stop and ask.

### 0. Read first
Read `.claude/CLAUDE.md` (project overview; its "Conventions" section records deliberate decisions — e.g. no Download button, `kafkaoxide.` localStorage prefix, never edit `backend/db/migrations/*.sql`, `GridTabs` lazy imports, virtualized `JsonTreeView`). Do not undo them.

### 1. Plan before coding (write the plan down, short)
- **Behaviour**: what changes, and what must NOT change (list the existing behaviours nearby).
- **Test plan**, one line each, before any production code:
  - Unit — Rust: `backend/*` crate tests; frontend: Vitest, test file beside the component/store (`Foo.tsx` + `Foo.test.tsx`).
  - Integration — Rust: `backend/*/tests/`; frontend: component tests with real stores/hooks, mocked `tauri` boundary only.
  - End-to-end — `SALTY_E2E_*` gated tests against a real broker (`scripts/e2e-fixtures.sh`, `scripts/e2e-acl-fixtures.sh`) when `client.rs`/ACL/publish paths are touched.
- **Modularity**: put logic where it can be tested without a broker or a running Tauri app — pure logic in `salty_core` / `backend/*`; `src-tauri/src/commands/` stays a thin wrapper. Frontend: logic in hooks/`lib/`/stores, components stay presentational. One responsibility per module; reuse before adding.

### 2. TDD loop
1. Write a failing test for the new/changed behaviour (and a regression test for each bug fixed).
2. Run it; confirm it fails **for the right reason**.
3. Write the minimum code to pass.
4. Refactor with tests green. Repeat.
Never weaken, skip or delete an existing test to get green; if one must change, say why.

### 3. Verification gates (all must pass; run what applies to the touched areas)
```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings   # if clippy is available
cargo test                                             # backend
npm --prefix frontend test                             # Vitest (does NOT type-check)
npm --prefix frontend run build                        # tsc && vite build — the only type gate
npm --prefix frontend run test:coverage                # 80% thresholds must hold
```
- **CI must not fail.** Mirror `.github/workflows/release.yml` / `coverage.yml`: no new warnings-as-errors, toolchain stays at the version pinned in `rust-toolchain.toml` (keep the workflow ref in step), no new unpinned dependency surprises, no platform-specific code that breaks Windows/macOS/Linux.
- **Do one full compile round of the app** after the change (`npm --prefix frontend run build` plus the Rust build of the touched crates; `cargo build --release --manifest-path src-tauri/Cargo.toml` if the environment can build it). `src-tauri` may not build locally — if it can't, say so explicitly and keep that crate's change minimal and mirrored on existing patterns instead of claiming it compiled.
- Report real results. Never claim "passes" without having run the command; state what was not run.

### 4. No regressions
- Touch the smallest surface that works; no drive-by refactors, renames or dependency bumps.
- Before changing a shared function/type/command, search all callers and confirm each still behaves the same.
- Don't change DB migrations, the export file format, localStorage keys, or Tauri command signatures without an explicit backward-compat plan and a test.
- Re-read the diff (`git diff`) before finishing and look for unintended changes.

### 5. Version bump (every change), no commit
- Bump the version in all three places, to the same value (patch bump for fixes/small changes, minor for features):
  - `frontend/package.json` → `version`
  - `src-tauri/tauri.conf.json` → `version`
  - `sonar-project.properties` → `sonar.projectVersion`
- **The version bump must never make CI fail.** All three files must hold the identical, valid semver string (and be strictly higher than the previous one). Verify after bumping:
  ```bash
  grep -m1 '"version"' frontend/package.json src-tauri/tauri.conf.json; grep projectVersion sonar-project.properties
  ```
  Do not touch `Cargo.toml` crate versions or lockfiles for this; do not hand-edit anything the release workflow derives from the version.
- **Do not commit, stage, push or open PRs** unless the user explicitly asks. Leave all changes in the working tree.

### 6. Finish
Summarise: what changed, tests added (unit / integration / e2e), commands run with results, anything not verifiable here, and the new version number.
