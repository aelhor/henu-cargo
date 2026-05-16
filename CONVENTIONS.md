# Commit Message Convention

We stick to Conventional Commits.

Format: `<type>(<scope>): <subject>`

## Message
- Max 50 characters.
- Use imperative mood (e.g., Add, not Added).

### Scopes
- `core` — main orchestration (ingest-stream, parallel-ingest)
- `sinks` — sink interface, fs-sink, slow-sink
- `workers` — worker pool, sequencer, worker entry
- `transforms` — transform types and interfaces
- `types` — IngestionOptions, IngestionResult
- `infra` — CI, build config, tsconfig, package.json
- `tests` — test files and test helpers
- `bench` — benchmarks
- `docs` — README, CHANGELOG, CONVENTIONS

### Types
- `feat` — new feature
- `fix` — bug fix
- `docs` — documentation only
- `refactor` — code restructuring without behavior change
- `perf` — performance improvement
- `test` — adding or updating tests
- `chore` — maintenance (deps, build, etc.)
- `infra` — CI/CD, tooling, configuration

### Examples
- `feat(core): add onProgress callback with throughput reporting`
- `fix(workers): validate worker path before spawning pool`
- `refactor(sinks): extract drain-based backpressure into fs-sink`
- `test(tests): auto-generate bin fixture in before hook`
- `infra(ci): add GitHub Actions workflow for Node 20`

## Body (optional)

Explain why and how for complex changes.

Wrap lines at 72 characters (Max 3 lines).

---

# Pull Request (PR) Standard

Every PR is a mini-design doc.

## Description
Why is this change happening? What does it solve?

## Engineering Decisions
Approach: Why this implementation?

## Trade-offs (if any)
What was sacrificed (e.g., complexity vs. throughput)?

## Performance Impact
- [ ] No regression — benchmark results unchanged.
- [ ] Improvement — benchmark results attached.
- [ ] N/A — no hot-path changes.

## Checklist
- [ ] Zero runtime dependencies preserved.
- [ ] `npm run build` passes with zero errors.
- [ ] `npm run test:all` — all tests pass.
- [ ] Types exported in `index.ts` barrel.
- [ ] README updated if public API changed.
- [ ] CHANGELOG.md updated.

## Testing
- [ ] Unit Tests (core logic)
- [ ] Integration Tests (end-to-end stream ingestion)
- [ ] Benchmarks (if performance-related)
