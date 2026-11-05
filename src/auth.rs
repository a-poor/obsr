//! The `authentication` string sent in `Identify`.

use base64::{Engine, engine::general_purpose::STANDARD};
use sha2::{Digest, Sha256};

/// Computes the response to a `Hello` authentication challenge:
/// `base64(sha256(base64(sha256(password + salt)) + challenge))`.
pub(crate) fn authentication_string(password: &str, salt: &str, challenge: &str) -> String {
    let secret = base64_sha256(&[password, salt]);
    base64_sha256(&[&secret, challenge])
}

fn base64_sha256(parts: &[&str]) -> String {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part.as_bytes());
    }
    STANDARD.encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    // The password, salt, and challenge come from the protocol spec's guide. The expected
    // string was computed independently with Python's `hashlib`.
    #[test]
    fn matches_independently_computed_vector() {
        let auth = authentication_string(
            "supersecretpassword",
            "lM1GncleQOaCu9lT1yeUZhFYnqhsLLP1G5lAGo3ixaI=",
            "+IxH4CnCiqpX1rM9scsNynZzbOe4KhDeYcTNS3PDaeY=",
        );
        assert_eq!(auth, "1Ct943GAT+6YQUUX47Ia/ncufilbe6+oD6lY+5kaCu4=");
    }

    #[test]
    fn differs_when_any_input_changes() {
        let base = authentication_string("p", "s", "c");
        assert_ne!(base, authentication_string("q", "s", "c"));
        assert_ne!(base, authentication_string("p", "t", "c"));
        assert_ne!(base, authentication_string("p", "s", "d"));
    }
}
