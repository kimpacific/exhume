# Exhume: product

Exhume ingests a legacy codebase and recovers the knowledge trapped inside it:
evidence-linked business rules, living documentation, characterization tests,
a risk-ranked migration plan, a Kiro export, and an MCP server.

## Principles
1. Evidence or it didn't happen: every claim cites file and line ranges.
2. Tests are the source of truth. If the AI's description and a test disagree, the test wins.
3. Static analysis first. Never execute uploaded code outside the sandbox.
4. Humans stay in the loop: rules go through review before export.
5. Incremental by design (strangler-fig migration, one slice at a time).
6. Depth in one language (MVP language is decided in the demo-patient step), not shallow support for many.
