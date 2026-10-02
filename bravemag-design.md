# bravemag design notes

bravemag is a thin, opinionated gateway in front of Browser Bridge (`bbx`).

## Shape

`MCP client -> bravemag mcp (stdio, one tool) -> bbx CLI -> native host -> extension -> browser`

The single MCP tool takes a `call` (one bbx method) or a `batch` (a list of steps run
one after another, `mode: drip` adds `step_delay_ms` between steps). Steps use bbx
method names; `bravemag.status` is a gateway helper.

## Decisions

- **Current tab, pinned per batch.** The target is whichever tab the human is on when
  a call starts. Tab-bound steps in that batch carry that tab id, so a mid-batch tab
  switch cannot redirect them. The next call re-resolves the active tab.
- **One batch at a time.** A file lock serialises complete batches across processes.
- **No tab lifecycle for agents.** Create / close / activate are refused; the human
  owns the tab strip. `navigation.navigate` works in the current tab.
- **Fail loudly.** A failing step stops the batch (by default) and the error is returned
  as-is; there are no silent fallbacks.
- **Audit.** Each call is recorded; large reads are stored as artifacts under `/tmp/bravemag/`.

## Not in scope

Opening its own browser, intercepting file choosers, per-agent tab ownership.
