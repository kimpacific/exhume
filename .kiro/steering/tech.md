# Exhume: technology conventions

## Backend (apps/server, Go)
- One Go module, several binaries: cmd/api, cmd/worker, cmd/mcp. Shared code lives in internal/.
- Postgres on Neon via pgx. SQL is written in db/queries and compiled with sqlc. Migrations use goose in db/migrations.
- Background work runs as River jobs. Every job is idempotent, keyed by (run_id, stage, unit_id).
- Pass context.Context everywhere; cancellation must stop LLM spending.
- Parallelism uses errgroup with bounded worker pools. LLM calls go through a rate limiter with retry, backoff and jitter.
- Errors are wrapped with %w. No panics in request or job paths.
- Tests are table-driven. Parser and exporter output use golden files.
- Never log source code content; log hashes and metadata only.
- Version every prompt. Store prompt version, model and temperature with each extracted rule.

## Frontend (apps/web, Next.js)
- App Router, TypeScript strict, Tailwind, shadcn/ui.
- React Flow for graphs, Monaco for the code-evidence viewer, Mermaid for diagrams.
- API types come from the generated client in packages/api-client. Do not hand-write API types.

## Tooling
- pnpm workspaces and Turborepo. Run everything through `pnpm build|test|lint|dev` from the repo root.
