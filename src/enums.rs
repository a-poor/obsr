//! Enumerations from the protocol spec.

use std::ops::{BitAnd, BitOr, BitOrAssign};

use serde::{Deserialize, Serialize};

mod generated;
pub use generated::*;

/// A bitmask of event categories to receive, combined with `|`.
///
/// The default is [`ALL`](Self::ALL), which excludes high-volume events such as
/// [`INPUT_VOLUME_METERS`](Self::INPUT_VOLUME_METERS). Those must be requested explicitly.
///
/// ```
/// use obsr::EventSubscription;
///
/// let mask = EventSubscription::ALL | EventSubscription::INPUT_VOLUME_METERS;
/// assert!(mask.contains(EventSubscription::SCENES));
/// ```
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EventSubscription(u64);

impl EventSubscription {
    /// The raw bitmask used on the wire.
    pub const fn bits(self) -> u64 {
        self.0
    }

    /// Creates a mask from raw bits. Bits this crate does not know are preserved.
    pub const fn from_bits(bits: u64) -> Self {
        Self(bits)
    }

    /// Returns `true` if every bit of `other` is set in `self`.
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }
}

impl Default for EventSubscription {
    fn default() -> Self {
        Self::ALL
    }
}

impl std::fmt::Debug for EventSubscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EventSubscription({:#x})", self.0)
    }
}

impl BitOr for EventSubscription {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

impl BitOrAssign for EventSubscription {
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

impl BitAnd for EventSubscription {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self {
        Self(self.0 & rhs.0)
    }
}
