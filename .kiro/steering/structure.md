# Exhume: repository layout

- apps/web: Next.js frontend
- apps/server: Go backend (cmd/api, cmd/worker, cmd/mcp, internal/*)
- packages/api-client: TypeScript client generated from api/openapi.yaml
- api/openapi.yaml: the contract between web and server
- db/migrations, db/queries: goose migrations and sqlc queries
- docs: project plan, overview and investor brief
- demo-patient: the legacy sample app used as analysis input. It is data, not part of the build. Never edit it as if it were Exhume code.
- .kiro: specs, steering and hooks for building Exhume itself
