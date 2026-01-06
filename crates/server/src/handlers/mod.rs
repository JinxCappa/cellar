//! HTTP request handlers.

pub mod admin;
pub mod auth;
pub mod capabilities;
pub mod common;
pub mod nix;
pub mod uploads;

pub use admin::*;
pub use auth::*;
pub use capabilities::*;
pub use common::*;
pub use nix::*;
pub use uploads::*;
