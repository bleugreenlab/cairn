#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Cut {
    pub byte_offset: usize,
    pub tokens: u32,
}

pub trait TokenMeter: Send + Sync {
    fn count(&self, text: &str) -> u32;
    fn cut(&self, text: &str, budget: u32) -> Cut;
}
