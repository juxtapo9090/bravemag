use crate::executor::execute_batch;
use crate::mcp::serve_stdio;
use crate::render::render_batch_preview;
use crate::toon::{BlendBatchRequest, ToolRequest, parse_raw, parse_script};
use crate::upstream::{
    UpstreamClientConfig, UpstreamRuntimeConfig, UpstreamSession, run_upstream_probe,
};
use crate::{AppResult, SERVER_NAME, SERVER_VERSION};
use anyhow::{Context, anyhow};
use clap::{Parser, Subcommand};
use serde_json::{Value, json};
use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::{Command, Stdio};

const DEFAULT_ENDPOINT: &str = "http://localhost:8931/mcp";
const DEFAULT_DEBUG_BROWSER: &str = "/usr/bin/firefox-developer-edition";
const DEFAULT_DEBUG_PORT: u16 = 9230;
const DEFAULT_DEBUG_URL: &str = "about:debugging#/runtime/this-firefox";

#[derive(Parser, Debug)]
#[command(
    name = SERVER_NAME,
    version = SERVER_VERSION,
    about = "bravemag — paced batch gateway for Browser Bridge (bbx)"
)]
pub struct Cli {
    #[arg(long, help = "Print the design draft and exit")]
    pub design: bool,
    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    #[command(
        visible_alias = "tabs",
        about = "List Brave tabs; the active tab is marked (current)"
    )]
    Status {
        #[arg(long, default_value = DEFAULT_ENDPOINT)]
        endpoint: String,
    },
    #[command(about = "Select a Brave tab by index, title, or URL substring")]
    Jump {
        #[arg(help = "Tab index or case-insensitive title/URL substring")]
        target: String,
        #[arg(long, default_value = DEFAULT_ENDPOINT)]
        endpoint: String,
    },
    #[command(about = "Parse and preview a bravemag TOON/RAW request")]
    Preview {
        #[arg(long)]
        script: Option<String>,
        #[arg(long)]
        raw: Option<String>,
        #[arg(long)]
        file: Option<PathBuf>,
        #[arg(long)]
        session_id: Option<String>,
        #[arg(long, default_value_t = false)]
        close: bool,
    },
    #[command(
        about = "Connect to Browser Bridge (bbx) upstream, list tools, and call one tool"
    )]
    UpstreamProbe {
        #[arg(long, default_value = DEFAULT_ENDPOINT)]
        endpoint: String,
        #[arg(long, default_value = "navigation.navigate")]
        tool: String,
        #[arg(
            long,
            default_value = r#"{"url":"data:text/html,<title>bravemag-probe</title><h1>bravemag ok</h1>"}"#
        )]
        args_json: String,
    },
    #[command(about = "Execute a parsed batch sequentially through Browser Bridge (bbx)")]
    Run {
        #[arg(long)]
        script: Option<String>,
        #[arg(long)]
        raw: Option<String>,
        #[arg(long)]
        file: Option<PathBuf>,
        #[arg(long, default_value = DEFAULT_ENDPOINT)]
        endpoint: String,
    },
    #[command(about = "Run bravemag as a one-tool MCP stdio server")]
    Mcp {
        #[arg(long, default_value = DEFAULT_ENDPOINT)]
        endpoint: String,
    },
    #[command(about = "Launch isolated Firefox extension-debug lane")]
    Debug {
        #[arg(long, help = "Firefox binary to launch")]
        browser: Option<PathBuf>,
        #[arg(long, help = "Dedicated Firefox profile path")]
        profile: Option<PathBuf>,
        #[arg(long, default_value_t = DEFAULT_DEBUG_PORT)]
        port: u16,
        #[arg(long, default_value = DEFAULT_DEBUG_URL)]
        url: String,
        #[arg(long, default_value_t = false, help = "Open page DevTools on launch")]
        devtools: bool,
        #[arg(long, default_value_t = false, help = "Open Firefox Browser Toolbox")]
        browser_toolbox: bool,
        #[arg(
            long,
            default_value_t = false,
            help = "Wait for Firefox instead of detaching"
        )]
        foreground: bool,
    },
}

/// Parse CLI arguments and dispatch the requested preview/design command.
///
/// # Errors
///
/// Returns an error when an input file cannot be read or supplied batch syntax
/// is invalid.
pub fn dispatch() -> AppResult<()> {
    let cli = Cli::parse();
    if cli.design {
        print!("{}", include_str!("../bravemag-design.md"));
        return Ok(());
    }

    match cli.command {
        Some(Commands::Status { endpoint }) => status(&runtime_config(endpoint)),
        Some(Commands::Jump { target, endpoint }) => jump(&runtime_config(endpoint), &target),
        Some(Commands::Preview {
            script,
            raw,
            file,
            session_id,
            close,
        }) => preview(script, raw, file, session_id, close),
        Some(Commands::UpstreamProbe {
            endpoint,
            tool,
            args_json,
        }) => {
            let result = run_upstream_probe(UpstreamClientConfig {
                endpoint,
                tool,
                args_json,
            })?;
            println!("{}", result.render_slim());
            Ok(())
        }
        Some(Commands::Run {
            script,
            raw,
            file,
            endpoint,
        }) => run(
            script.as_ref(),
            raw.as_ref(),
            file,
            &runtime_config(endpoint),
        ),
        Some(Commands::Mcp { endpoint }) => serve_stdio(&runtime_config(endpoint)),
        Some(Commands::Debug {
            browser,
            profile,
            port,
            url,
            devtools,
            browser_toolbox,
            foreground,
        }) => launch_debug(&DebugLaunch {
            browser: browser.unwrap_or_else(default_debug_browser),
            profile: profile.unwrap_or_else(default_debug_profile),
            port,
            url,
            devtools,
            browser_toolbox,
            foreground,
        }),
        None => {
            eprintln!("bravemag ready. Use `bravemag --design` or `bravemag preview`.");
            Ok(())
        }
    }
}

/// Print the upstream tab list without opening a resident session tab or changing selection.
fn status(config: &UpstreamRuntimeConfig) -> AppResult<()> {
    let mut session = UpstreamSession::connect(config)?;
    println!("{}", tab_list(&mut session)?);
    Ok(())
}

/// Resolve a human-friendly tab target, select it, then print the confirmed active tab list.
fn jump(config: &UpstreamRuntimeConfig, target: &str) -> AppResult<()> {
    let mut session = UpstreamSession::connect(config)?;
    let index = target
        .parse::<u64>()
        .ok()
        .or_else(|| find_tab_index(&tab_list(&mut session).ok()?, target))
        .ok_or_else(|| anyhow!("no Brave tab matches `{target}`"))?;
    session.call_tool("browser_tabs", &json!({"action": "select", "index": index}))?;
    println!("selected tab {index}\n{}", tab_list(&mut session)?);
    Ok(())
}

fn tab_list(session: &mut UpstreamSession) -> AppResult<String> {
    let result = session.call_tool("browser_tabs", &json!({"action": "list"}))?;
    result
        .pointer("/content/0/text")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| anyhow!("Brave tab list returned no text"))
}

fn find_tab_index(list: &str, needle: &str) -> Option<u64> {
    let needle = needle.trim().to_lowercase();
    if needle.is_empty() {
        return None;
    }
    list.lines()
        .filter(|line| line.to_lowercase().contains(&needle))
        .find_map(|line| {
            let digits = line.trim_start().strip_prefix('-')?.trim_start();
            let end = digits
                .find(|ch: char| !ch.is_ascii_digit())
                .unwrap_or(digits.len());
            digits[..end].parse().ok()
        })
}

fn run(
    script_arg: Option<&String>,
    raw_arg: Option<&String>,
    file: Option<PathBuf>,
    config: &UpstreamRuntimeConfig,
) -> AppResult<()> {
    let batch = parse_batch_input(script_arg, raw_arg, file)?;
    let result = execute_batch(config, &batch)?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

fn runtime_config(endpoint: String) -> UpstreamRuntimeConfig {
    UpstreamRuntimeConfig {
        endpoint,
        caller: env::var("BRAVEMAG_CALLER")
            .or_else(|_| env::var("WATCH_CALLER"))
            .unwrap_or_else(|_| "unknown".to_string()),
        audit_path: env::var("BRAVEMAG_AUDIT_PATH").map_or_else(
            |_| PathBuf::from("/tmp/bravemag/audit.jsonl"),
            PathBuf::from,
        ),
    }
}

fn preview(
    script_arg: Option<String>,
    raw_arg: Option<String>,
    file: Option<PathBuf>,
    session_id: Option<String>,
    close: bool,
) -> AppResult<()> {
    let parsed = if close
        && session_id.is_some()
        && file.is_none()
        && script_arg.is_none()
        && raw_arg.is_none()
    {
        BlendBatchRequest::default()
    } else {
        parse_batch_input(script_arg.as_ref(), raw_arg.as_ref(), file)?
    };

    let request = ToolRequest {
        call: None,
        script: script_arg,
        raw: raw_arg,
        session_id,
        close: Some(close),
        resume: None,
        sessionless: None,
        batch: Some(parsed),
    };
    println!("{}", render_batch_preview(&request));
    Ok(())
}

fn parse_batch_input(
    script_arg: Option<&String>,
    raw_arg: Option<&String>,
    file: Option<PathBuf>,
) -> AppResult<BlendBatchRequest> {
    if let Some(path) = file {
        parse_script(&fs::read_to_string(path)?)
    } else if let Some(raw) = raw_arg {
        parse_raw(raw)
    } else if let Some(script) = script_arg {
        parse_script(script)
    } else {
        Err(anyhow!("provide --script, --raw, or --file"))
    }
}

#[derive(Debug, Clone)]
struct DebugLaunch {
    browser: PathBuf,
    profile: PathBuf,
    port: u16,
    url: String,
    devtools: bool,
    browser_toolbox: bool,
    foreground: bool,
}

fn default_debug_browser() -> PathBuf {
    env::var_os("BRAVEMAG_DEBUG_BROWSER")
        .map_or_else(|| PathBuf::from(DEFAULT_DEBUG_BROWSER), PathBuf::from)
}

fn default_debug_profile() -> PathBuf {
    env::var_os("BRAVEMAG_DEBUG_PROFILE").map_or_else(
        || home_path(".local/share/bravemag-debug-firefox-profile"),
        PathBuf::from,
    )
}

fn home_path(relative: &str) -> PathBuf {
    env::var_os("HOME")
        .map_or_else(|| PathBuf::from("."), PathBuf::from)
        .join(relative)
}

fn launch_debug(config: &DebugLaunch) -> AppResult<()> {
    fs::create_dir_all(&config.profile)
        .with_context(|| format!("failed to create profile {}", config.profile.display()))?;
    write_debug_user_js(&config.profile)?;

    let args = debug_args(config);
    let mut command = Command::new(&config.browser);
    command.args(&args);

    if config.foreground {
        let status = command
            .status()
            .with_context(|| format!("failed to launch {}", config.browser.display()))?;
        if !status.success() {
            return Err(anyhow!("debug Firefox exited with {status}"));
        }
    } else {
        command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .with_context(|| format!("failed to launch {}", config.browser.display()))?;
    }

    println!(
        "bravemag debug launched\nbrowser={}\nprofile={}\nurl={}\nremote_agent=http://localhost:{}\ndevtools_server=ws://localhost:{}",
        config.browser.display(),
        config.profile.display(),
        config.url,
        config.port,
        config.port.saturating_add(1),
    );
    Ok(())
}

fn write_debug_user_js(profile: &std::path::Path) -> AppResult<()> {
    let user_js = profile.join("user.js");
    fs::write(
        &user_js,
        r#"// bravemag debug profile for extension/DOM work.
user_pref("devtools.chrome.enabled", true);
user_pref("devtools.debugger.remote-enabled", true);
user_pref("devtools.debugger.prompt-connection", false);
user_pref("devtools.browsertoolbox.scope", "everything");
user_pref("xpinstall.signatures.required", false);
"#,
    )
    .with_context(|| format!("failed to write {}", user_js.display()))?;
    Ok(())
}

fn debug_args(config: &DebugLaunch) -> Vec<String> {
    let mut args = vec![
        "--new-instance".to_string(),
        "--profile".to_string(),
        config.profile.to_string_lossy().into_owned(),
        "--remote-debugging-port".to_string(),
        config.port.to_string(),
        "--start-debugger-server".to_string(),
        format!("ws:{}", config.port.saturating_add(1)),
        "--new-window".to_string(),
        config.url.clone(),
    ];
    if config.devtools {
        args.push("--devtools".to_string());
    }
    if config.browser_toolbox {
        args.push("--jsdebugger".to_string());
    }
    args
}

#[cfg(test)]
mod debug_tests {
    use super::*;

    #[test]
    fn debug_args_use_isolated_profile_and_ports() {
        let config = DebugLaunch {
            browser: PathBuf::from("/usr/bin/firefox-developer-edition"),
            profile: PathBuf::from("/tmp/bravemag-debug-profile"),
            port: 9230,
            url: "about:debugging#/runtime/this-firefox".to_string(),
            devtools: true,
            browser_toolbox: true,
            foreground: false,
        };

        let args = debug_args(&config);

        assert!(args.contains(&"--new-instance".to_string()));
        assert!(args.contains(&"/tmp/bravemag-debug-profile".to_string()));
        assert!(args.contains(&"--remote-debugging-port".to_string()));
        assert!(args.contains(&"9230".to_string()));
        assert!(args.contains(&"ws:9231".to_string()));
        assert!(args.contains(&"--devtools".to_string()));
        assert!(args.contains(&"--jsdebugger".to_string()));
    }
}
