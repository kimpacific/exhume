# The evidence rule

Exhume must never output a claim about analyzed code without a citation.

- A business rule needs at least one evidence record: file path, start line, end line, excerpt hash.
- A rule whose evidence fails the verification pass is flagged or dropped, never silently kept.
- Confidence combines the verification result, the citation count and test agreement.
- Generated docs and exports may only restate rules that have valid evidence.
- When unsure, say so and lower the confidence. Do not invent citations.
