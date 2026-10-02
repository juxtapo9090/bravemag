pub mod cli;
pub mod executor;
pub mod mcp;
pub mod render;
pub mod toon;
pub mod upstream;

pub const SERVER_NAME: &str = "bravemag";
pub const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");
pub const BUILD_STAMP: &str = env!("BRAVEMAG_BUILD_STAMP");

pub type AppResult<T> = anyhow::Result<T>;
