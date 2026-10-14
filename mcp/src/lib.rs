//! MCP 2024-11-05 over stdio and legacy HTTP/SSE.
mod budget;
mod protocol;
pub use budget::SessionBudget;
pub mod sse;
pub mod stdio;
mod tools;
pub use protocol::Connection;
pub const MAX_MESSAGE_BYTES: usize = 8 * 1024 * 1024;
