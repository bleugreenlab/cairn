//! The account identity a Claude CLI home must not carry into a session.
//!
//! The CLI keeps the signed-in account's profile in `<home>/.claude.json`
//! under `oauthAccount`, and on every turn it attaches `emailAddress` to the
//! first user message as a `# userEmail` context block ("The user's email
//! address is …"). Models have written that address into requests they
//! compose, and an agent's `run` traffic is not something Cairn mediates. So
//! the email is removed from the home before every launch.
//!
//! Measured on 2.1.311 by the `account_identity_probe` contract cases: without
//! the field the CLI authenticates and completes the turn on its token alone,
//! sends no paragraph, never refetches the profile, and never writes the field
//! back. `displayName` and `organizationName` are not sent, so they stay; the
//! `account_email_never_reaches_the_model` contract case goes red if a CLI
//! starts sending any of the three.
//!
//! The file is the CLI's, so it is edited in place: the removed members are
//! cut out of the text and every other byte is left as the CLI wrote it.

use std::path::Path;

use serde_json::Value;

/// The CLI's own state file in its home.
pub const CLI_STATE_FILE: &str = ".claude.json";
/// Where in [`CLI_STATE_FILE`] the CLI keeps the signed-in account's profile.
pub const ACCOUNT_PROFILE_KEY: &str = "oauthAccount";
/// The profile fields removed before a launch.
pub const STRIPPED_ACCOUNT_FIELDS: [&str; 1] = ["emailAddress"];

/// Remove [`STRIPPED_ACCOUNT_FIELDS`] from `home`'s [`CLI_STATE_FILE`], and
/// answer whether the file changed. A home with no state file, or whose state
/// holds none of the fields, is left untouched. A state file that cannot be
/// read as JSON is an error, so a caller that goes on to launch never launches
/// on a home it could not clear.
pub fn strip_account_identity(home: &Path) -> Result<bool, String> {
    let path = home.join(CLI_STATE_FILE);
    let raw = match std::fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(format!("Failed to read {}: {err}", path.display())),
    };
    let Some(stripped) = without_account_identity(&raw).map_err(|err| {
        format!(
            "{} cannot be cleared of the account email: {err}",
            path.display()
        )
    })?
    else {
        return Ok(false);
    };
    // Staged beside the file and renamed over it, so a CLI starting on this
    // home reads either the old file or the new one, never a torn one.
    let staged = home.join(format!(
        "{CLI_STATE_FILE}.cairn-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos())
            .unwrap_or_default()
    ));
    let written = std::fs::write(&staged, stripped.as_bytes()).and_then(|()| {
        let permissions = std::fs::metadata(&path)?.permissions();
        std::fs::set_permissions(&staged, permissions)?;
        std::fs::rename(&staged, &path)
    });
    if let Err(err) = written {
        let _ = std::fs::remove_file(&staged);
        return Err(format!("Failed to rewrite {}: {err}", path.display()));
    }
    Ok(true)
}

/// `raw` with every [`STRIPPED_ACCOUNT_FIELDS`] member of its
/// [`ACCOUNT_PROFILE_KEY`] object cut out, or `None` when it holds none.
pub fn without_account_identity(raw: &str) -> Result<Option<String>, String> {
    let parsed: Value = serde_json::from_str(raw).map_err(|err| err.to_string())?;
    if !parsed.is_object() {
        return Err("the state is not a JSON object".to_string());
    }
    let mut text = raw.to_string();
    let mut changed = false;
    while let Some(cut) = next_cut(&text)? {
        text.replace_range(cut, "");
        changed = true;
    }
    if !changed {
        return Ok(None);
    }
    // The cut must be exactly the removal of those members: the result parses
    // to the original with them removed, and nothing else.
    let mut expected = parsed;
    if let Some(profile) = expected
        .get_mut(ACCOUNT_PROFILE_KEY)
        .and_then(Value::as_object_mut)
    {
        for field in STRIPPED_ACCOUNT_FIELDS {
            profile.remove(field);
        }
    }
    let result: Value = serde_json::from_str(&text).map_err(|err| err.to_string())?;
    if result != expected {
        return Err("removing the account email changed other state".to_string());
    }
    Ok(Some(text))
}

/// The byte range to cut for the first stripped member of the profile object.
fn next_cut(text: &str) -> Result<Option<std::ops::Range<usize>>, String> {
    let mut scan = Scan {
        bytes: text.as_bytes(),
        at: 0,
    };
    scan.space();
    let top = scan.object()?;
    let Some(profile) = top
        .members
        .iter()
        .rev()
        .find(|member| member.key == ACCOUNT_PROFILE_KEY)
    else {
        return Ok(None);
    };
    let mut inner = Scan {
        bytes: text.as_bytes(),
        at: profile.value_start,
    };
    if inner.peek() != Some(b'{') {
        return Ok(None);
    }
    let object = inner.object()?;
    let members = &object.members;
    let Some(index) = members
        .iter()
        .position(|member| STRIPPED_ACCOUNT_FIELDS.contains(&member.key.as_str()))
    else {
        return Ok(None);
    };
    Ok(Some(if members.len() == 1 {
        // The only member: the object's whole interior goes.
        object.open + 1..object.close
    } else if index + 1 < members.len() {
        // The member, its comma, and the space before the next key, which
        // keeps this member's indentation for the next one.
        members[index].start..members[index + 1].start
    } else {
        // The last member: from the end of the previous one, its comma
        // included.
        members[index - 1].end..members[index].end
    }))
}

struct Member {
    key: String,
    /// Where the key's opening quote is.
    start: usize,
    value_start: usize,
    /// Just past the value.
    end: usize,
}

struct Object {
    open: usize,
    close: usize,
    members: Vec<Member>,
}

/// Just enough of a JSON scanner to find member spans. The input has already
/// parsed, so it only has to be right about valid JSON.
struct Scan<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl Scan<'_> {
    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.at).copied()
    }

    fn space(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.at += 1;
        }
    }

    fn expect(&mut self, byte: u8) -> Result<(), String> {
        if self.peek() != Some(byte) {
            return Err(format!("expected `{}` at byte {}", byte as char, self.at));
        }
        self.at += 1;
        Ok(())
    }

    /// A string literal, answered as its span.
    fn string(&mut self) -> Result<std::ops::Range<usize>, String> {
        let start = self.at;
        self.expect(b'"')?;
        loop {
            match self.peek() {
                None => return Err("unterminated string".to_string()),
                Some(b'\\') => self.at += 2,
                Some(b'"') => {
                    self.at += 1;
                    return Ok(start..self.at);
                }
                Some(_) => self.at += 1,
            }
        }
    }

    fn value(&mut self) -> Result<(), String> {
        match self.peek() {
            Some(b'"') => self.string().map(|_| ()),
            Some(b'{') => self.object().map(|_| ()),
            Some(b'[') => {
                self.at += 1;
                self.space();
                if self.peek() == Some(b']') {
                    self.at += 1;
                    return Ok(());
                }
                loop {
                    self.space();
                    self.value()?;
                    self.space();
                    match self.peek() {
                        Some(b',') => self.at += 1,
                        Some(b']') => {
                            self.at += 1;
                            return Ok(());
                        }
                        _ => return Err(format!("malformed array at byte {}", self.at)),
                    }
                }
            }
            Some(_) => {
                while !matches!(
                    self.peek(),
                    None | Some(b',' | b'}' | b']' | b' ' | b'\t' | b'\n' | b'\r')
                ) {
                    self.at += 1;
                }
                Ok(())
            }
            None => Err("unexpected end of input".to_string()),
        }
    }

    fn object(&mut self) -> Result<Object, String> {
        let open = self.at;
        self.expect(b'{')?;
        let mut members = Vec::new();
        self.space();
        if self.peek() == Some(b'}') {
            self.at += 1;
            return Ok(Object {
                open,
                close: self.at - 1,
                members,
            });
        }
        loop {
            self.space();
            let literal = self.string()?;
            let key: String = serde_json::from_slice(&self.bytes[literal.clone()])
                .map_err(|err| err.to_string())?;
            self.space();
            self.expect(b':')?;
            self.space();
            let value_start = self.at;
            self.value()?;
            members.push(Member {
                key,
                start: literal.start,
                value_start,
                end: self.at,
            });
            self.space();
            match self.peek() {
                Some(b',') => self.at += 1,
                Some(b'}') => {
                    self.at += 1;
                    return Ok(Object {
                        open,
                        close: self.at - 1,
                        members,
                    });
                }
                _ => return Err(format!("malformed object at byte {}", self.at)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A home's state as the CLI writes it (2.1.311, pretty-printed), with the
    /// account profile in the middle of the file.
    const CLI_STATE: &str = r#"{
  "numStartups": 3,
  "hasCompletedOnboarding": true,
  "oauthAccount": {
    "accountUuid": "00000000-0000-4000-8000-00000000c0de",
    "displayName": "Ada",
    "emailAddress": "ada@example.com",
    "organizationName": "Ada's Org",
    "organizationUuid": "00000000-0000-4000-8000-0000000000a1",
    "profileFetchedAt": 1762000000000
  },
  "userID": "6c9da9892be2824133054486a18a5f58",
  "pluginUsage": {
    "cc-plugin-agents-md@builtin": {
      "usageCount": 0,
      "emailAddress": "not-the-profile@example.com"
    }
  },
  "tags": ["a", {"b": "c\"}"}, 1.5e3, null]
}"#;

    #[test]
    fn the_email_is_cut_and_every_other_byte_is_kept() {
        let stripped = without_account_identity(CLI_STATE).unwrap().unwrap();
        let expected = CLI_STATE.replace("    \"emailAddress\": \"ada@example.com\",\n", "");
        assert_ne!(expected, CLI_STATE);
        assert_eq!(stripped, expected);
    }

    #[test]
    fn a_trailing_or_only_email_member_leaves_valid_json() {
        let last = "{\"oauthAccount\": {\"displayName\": \"Ada\", \"emailAddress\": \"ada@example.com\"}, \"x\": 1}";
        assert_eq!(
            without_account_identity(last).unwrap().unwrap(),
            "{\"oauthAccount\": {\"displayName\": \"Ada\"}, \"x\": 1}"
        );
        let only = "{\"oauthAccount\": {\n  \"emailAddress\": \"ada@example.com\"\n}}";
        assert_eq!(
            without_account_identity(only).unwrap().unwrap(),
            "{\"oauthAccount\": {}}"
        );
    }

    #[test]
    fn a_repeated_email_member_is_cut_every_time() {
        let twice = r#"{"oauthAccount": {"emailAddress": "a@x", "emailAddress": "b@x", "k": 1}}"#;
        assert_eq!(
            without_account_identity(twice).unwrap().unwrap(),
            r#"{"oauthAccount": {"k": 1}}"#
        );
    }

    #[test]
    fn state_without_the_email_is_left_alone() {
        for raw in [
            r#"{"numStartups": 1}"#,
            r#"{"oauthAccount": null}"#,
            r#"{"oauthAccount": {"displayName": "Ada"}}"#,
        ] {
            assert_eq!(without_account_identity(raw).unwrap(), None, "{raw}");
        }
    }

    #[test]
    fn unreadable_state_is_an_error() {
        for raw in ["", "{\"oauthAccount\": ", "[]"] {
            assert!(without_account_identity(raw).is_err(), "{raw:?}");
        }
    }

    #[test]
    fn the_home_file_is_rewritten_once_and_keeps_its_permissions() {
        let home = tempfile::tempdir().unwrap();
        let path = home.path().join(CLI_STATE_FILE);
        assert!(
            !strip_account_identity(home.path()).unwrap(),
            "no file, no change"
        );
        std::fs::write(&path, CLI_STATE).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        assert!(strip_account_identity(home.path()).unwrap());
        let after = std::fs::read_to_string(&path).unwrap();
        assert!(!after.contains("ada@example.com"));
        assert!(after.contains("not-the-profile@example.com"));
        assert!(
            !strip_account_identity(home.path()).unwrap(),
            "already clear"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        let leftovers: Vec<_> = std::fs::read_dir(home.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert_eq!(leftovers, [std::ffi::OsString::from(CLI_STATE_FILE)]);
    }

    #[test]
    fn an_unreadable_home_file_refuses_and_is_left_as_it_was() {
        let home = tempfile::tempdir().unwrap();
        let path = home.path().join(CLI_STATE_FILE);
        std::fs::write(&path, "{not json").unwrap();
        assert!(strip_account_identity(home.path()).is_err());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "{not json");
    }
}
