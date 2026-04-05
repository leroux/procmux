# Decision Log: Procmux Public Release Preparation
**Date**: 2026-04-04
**Intensity**: strict

## Decision 1: Release Target

**Options considered:**
- GitHub only — Open-source on GitHub, users clone/install from source
- GitHub + PyPI — GitHub and PyPI publishing, Rust source-only
- GitHub + PyPI + crates.io — Full publishing across all registries
- Just GitHub-ready — Make repo presentable, no actual publishing

**Choice:** GitHub only
**Rationale:** User chose GitHub-only distribution; no package registry publishing needed.
**Reversibility:** reversible
**Assumptions confirmed:** Project will be open-sourced on GitHub
**Failure modes considered:** None significant — GitHub-only is the simplest distribution model

## Decision 2: Implementation Scope

**Options considered:**
- Both equally — Both Rust and Python are first-class, need full polish
- Python primary — Python is main, Rust secondary
- Rust primary — Rust is main, Python secondary
- Python only — Ignore/remove Rust

**Choice:** Both equally
**Rationale:** Both Rust and Python implementations are first-class citizens for this release.
**Reversibility:** reversible
**Assumptions confirmed:** Both implementations need examples, clean code, and documentation
**Failure modes considered:** Doubling the work surface — need to ensure consistency across both implementations

## Decision 3: Target Audience

**Options considered:**
- Infra/systems devs — Experienced, minimal hand-holding
- General developers — May not be familiar with Unix sockets, async IO, process management
- Internal/team use — Reference-oriented docs for collaborators

**Choice:** General developers
**Rationale:** User wants the project accessible to developers who may not be deeply familiar with the underlying concepts.
**Reversibility:** reversible
**Assumptions confirmed:** Documentation and examples should provide context, not assume domain expertise
**Failure modes considered:** Over-explaining could make docs verbose for experienced users; under-explaining defeats the purpose

## Decision 4: Gap Analysis Scope

**Options considered:**
- Looks complete — 12 identified gaps across files, code, and docs
- Missing items — Additional issues to add
- Too much scope — Trim the list

**Choice:** Looks complete
**Rationale:** User confirmed the 12-item gap analysis covers the right scope.
**Reversibility:** reversible
**Assumptions confirmed:** All 12 gaps are valid work items
**Failure modes considered:** May discover additional issues during execution; can add to scope if needed

## Decision 5: Rename `agents` field to `processes`

**Options considered:**
- Rename to `processes` — breaking wire protocol change, consistent with "zero agent knowledge" claim
- Keep `agents` — leave as-is
- Rename to something else — user-specified alternative

**Choice:** Rename to `processes`
**Rationale:** The project claims zero knowledge of agents; naming should reflect that.
**Reversibility:** reversible (pre-release, no existing consumers)
**Assumptions confirmed:** No external consumers of this wire protocol yet
**Failure modes considered:** Must update both Rust and Python implementations consistently

## Decision 6: Rename `BRIDGE_STDIO_LOG_DIR` env var

**Options considered:**
- `PROCMUX_LOG_DIR` — simple, matches project name
- `PROCMUX_STDIO_LOG_DIR` — more specific, distinguishes from server logs
- Remove the env var — always derive from socket path

**Choice:** `PROCMUX_STDIO_LOG_DIR`
**Rationale:** More specific naming that distinguishes stdio logs from server stderr logs.
**Reversibility:** reversible
**Assumptions confirmed:** Env var is useful to keep for deployments
**Failure modes considered:** Must update both implementations

## Decision 7: Rust `env_clear()` behavior

**Options considered:**
- Remove env_clear() — inherit parent environment, match Python behavior
- Keep env_clear() — more secure/isolated but surprising
- Add env_inherit flag — `env_inherit: bool` field on CmdMsg, default true

**Choice:** Add env_inherit flag
**Rationale:** Gives users control while defaulting to the less-surprising behavior (inherit).
**Reversibility:** reversible
**Assumptions confirmed:** Python and Rust should have consistent behavior by default
**Failure modes considered:** Protocol change required in both implementations; must default to true

## Decision 8: Internal reference cleanup

**Options considered:**
- Remove all internal refs — delete Claude, flowcoder, bridge references from comments/docstrings
- Replace with generic examples — swap internal refs for generic equivalents

**Choice:** Remove all internal refs
**Rationale:** Keep code generic, no traces of internal tooling.
**Reversibility:** reversible
**Assumptions confirmed:** All internal references are in comments/docstrings, not functional code
**Failure modes considered:** May miss some references; grep-based search needed

## Decision 9: Examples scope

**Options considered:**
- Basic: one per language — spawn + stdin/stdout roundtrip + kill
- Basic + advanced — core workflow + reconnection/buffering
- Comprehensive suite — multiple examples per language

**Choice:** Basic: one per language
**Rationale:** Minimal viable examples to demonstrate the core workflow.
**Reversibility:** reversible (can add more later)
**Assumptions confirmed:** examples/python/basic.py and examples/rust/basic.rs
**Failure modes considered:** May not be enough for general developers; can add more if feedback suggests it

## Decision 10: README structure

**Options considered:**
- Single README, dual sections — one file with Python/Rust side-by-side
- Root README + per-lang READMEs — architecture/protocol in root, language-specific in py/ and rs/

**Choice:** Root README + per-lang READMEs
**Rationale:** Cleaner separation; users can find language-specific docs where they expect them.
**Reversibility:** reversible
**Assumptions confirmed:** Root README covers shared concerns (architecture, protocol, features)
**Failure modes considered:** Information could get out of sync between READMEs

## Decision 11: Execution plan approval

**Options considered:**
- Approve plan — 5 work blocks in sequence: code fixes, LICENSE, examples, docs, pyproject fix
- Reorder or modify — change grouping or sequence

**Choice:** Approve plan
**Rationale:** User approved the proposed sequence.
**Reversibility:** reversible
**Assumptions confirmed:** Work blocks are correctly scoped and sequenced
**Failure modes considered:** Later blocks may need adjustments based on earlier work
