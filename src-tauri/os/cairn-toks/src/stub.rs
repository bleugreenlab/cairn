//! The build for targets without a toks bundle: nothing loads, so no value of
//! [`Tokenizer`] can exist and every method is unreachable.

use std::path::Path;

use crate::{AddedTokens, Cut, CutStrategy, EncodeError, LoadError};

pub(crate) enum Tokenizer {}

/// The o200k file is compiled in only where toks can load it.
pub(crate) fn o200k_json() -> Result<Vec<u8>, LoadError> {
    Err(LoadError::UnsupportedTarget)
}

impl Tokenizer {
    pub(crate) fn from_json(_bytes: &[u8], _added: AddedTokens) -> Result<Self, LoadError> {
        Err(LoadError::UnsupportedTarget)
    }

    pub(crate) fn from_dir(_dir: &Path) -> Result<Self, LoadError> {
        Err(LoadError::UnsupportedTarget)
    }

    pub(crate) fn source_sha256(&self) -> [u8; 32] {
        match *self {}
    }

    pub(crate) fn name(&self) -> &str {
        match *self {}
    }

    pub(crate) fn has_certified_cuts(&self) -> bool {
        match *self {}
    }

    pub(crate) fn count(&self, _text: &str) -> Result<u32, EncodeError> {
        match *self {}
    }

    pub(crate) fn cut(
        &self,
        _text: &str,
        _budget: u32,
        _strategy: Option<CutStrategy>,
    ) -> Result<Cut, EncodeError> {
        match *self {}
    }

    pub(crate) fn encode(&self, _text: &str) -> Result<Vec<u32>, EncodeError> {
        match *self {}
    }

    pub(crate) fn encode_whole(&self, _text: &str) -> Result<Vec<u32>, EncodeError> {
        match *self {}
    }

    pub(crate) fn token_bytes(&self, _id: u32) -> Option<&[u8]> {
        match *self {}
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn loads_report_the_unsupported_target() {
        assert!(!crate::NATIVE);
        assert!(matches!(
            crate::Tokenizer::from_json(b"{}"),
            Err(crate::LoadError::UnsupportedTarget)
        ));
        assert!(matches!(
            crate::o200k(),
            Err(crate::LoadError::UnsupportedTarget)
        ));
    }
}
