# bravemag

A small Rust gateway that lets an AI agent drive **your real, logged-in browser**
through [Browser Bridge](https://github.com/koltyakov/browser-bridge) (`bbx`).
It runs as a one-tool MCP stdio server (or from the CLI) and adds what a bare
`bbx` call does not give you:

- **Paced batches**: send several steps in one request, with a delay between steps.
- **Current-tab routing**: every call targets the tab you are looking at right now,
  and all tab-bound steps in one batch stay pinned to that tab even if you switch.
- **One agent at a time**: a cross-process file lock (`/tmp/bravemag/shared-agent-workspace.lock`)
  serialises whole batches, so two agents never fight over the same browser.
- **Guard rails**: tab creation, closing and activation are refused at the gateway;
  `navigation.navigate` works in the current tab.
- **Audit trail**: every call is logged under `/tmp/bravemag/`.

```text
agent (MCP client) -> bravemag (stdio) -> bbx CLI -> native host -> Browser Bridge extension -> your browser
```

## What you need first

bravemag does not replace Browser Bridge, it sits on top of it. Install these
from the upstream project, following its own instructions:

1. A Chromium browser (Brave, Chrome, ...) with the **Browser Bridge extension**.
2. The **`bbx` CLI and native host** (`npm i -g @browserbridge/bbx`), then run
   `bbx doctor` until it reports healthy. After a browser restart you may need to
   re-enable access from the extension's toolbar popup.
3. A Rust toolchain to build bravemag.

Upstream: <https://github.com/koltyakov/browser-bridge> (MIT).
Read its `RESPONSIBLE_USE.md` too: this controls a browser that is signed in to
your accounts. A separate browser profile for agent work is a good idea.

## Build

```bash
cargo build --release          # binary: target/release/bravemag
cargo test
```

## Use

```bash
bravemag status                # list tabs, active one marked (current)
bravemag run ...               # run a batch (see `bravemag --help`)
bravemag mcp                   # run as a one-tool MCP stdio server
```

Add it to an MCP client as a stdio server whose command is the full path to the
`bravemag` binary with the argument `mcp`.

Calls use `bbx` method names (`page.get_state`, `page.get_text`, `dom.query`,
`input.click`, `navigation.navigate`, `capture.screenshot`, `tabs.list`, ...):

```json
{"call":{"tool":"page.get_state","args":{}}}
```

```json
{"batch":{"mode":"drip","step_delay_ms":250,"steps":[
  {"tool":"dom.query","args":{"selector":"button"}},
  {"tool":"input.click","args":{"target":{"elementRef":"el_xxx"}}}
]}}
```

`{"call":{"tool":"bravemag.status","args":{}}}` reports the active tab's title, URL
and id. Agents should reuse a page that already matches instead of navigating again.

## Configuration

All optional. When unset, bravemag changes nothing about the environment it was started in.

| Variable | Meaning |
| --- | --- |
| `BRAVEMAG_BBX` | Absolute path to the `bbx` binary. Default: `bbx` from `PATH`. |
| `BRAVEMAG_EXTRA_PATH` | Directories put in front of `PATH` for the `bbx` call (for installs outside the caller's `PATH`). |
| `BRAVEMAG_HOME` | A home directory to run `bbx` under (sets `HOME`, `XDG_CONFIG_HOME`, `XDG_DATA_HOME`). For setups where several OS users call bravemag but the browser bridge belongs to one of them. |
| `BRAVEMAG_AUDIT_PATH` | Where audit rows are written. |
| `BRAVEMAG_CAPTURE_LIMIT_BYTES` | Cap on captured text per read. |
| `BRAVEMAG_SHOT_MAXW` | Max width for screenshots. |
| `BRAVEMAG_CALLER` | Label for who made the call, recorded in the audit log. |

## File uploads

Agents click the visible upload control with `input.click`; the browser opens its
normal native file picker for the human. The gateway does not intercept or fill file choosers.

## Status

Personal project, shared as-is. It works on the author's Linux setup; other
platforms are untested. Issues and PRs welcome.

## License

MIT, see `LICENSE`. Browser Bridge is by koltyakov and licensed separately.
