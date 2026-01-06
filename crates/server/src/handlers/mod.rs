//! HTTP request handlers.

pub mod admin;
pub mod capabilities;
pub mod nix;
pub mod uploads;

pub use admin::*;
pub use capabilities::*;
pub use nix::*;
pub use uploads::*;
