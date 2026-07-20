pub mod adapters;
pub mod application;
pub mod core;
pub mod interfaces;

pub use adapters::{executor, gitlab, interaction, notification, store, worktree};
pub use application::{app, dashboard, matcher, planner, scheduler};
pub use core::{calendar, domain, error, event, ids, state};
pub use interfaces::{server, showcase, tui, worker};

pub use app::MambaApp;
pub use error::{MambaError, Result};
