//! Tiny fixture exercised by the gated `lsp_real_servers` integration test.
//! `build_widget` is referenced from `use_widget`, giving definition, a
//! multi-site reference set, and hover something concrete to resolve.

pub struct Widget {
    pub size: u32,
}

impl Widget {
    pub fn area(&self) -> u32 {
        self.size * self.size
    }
}

pub fn build_widget() -> Widget {
    Widget { size: 4 }
}

pub fn use_widget() -> u32 {
    let w = build_widget();
    w.area()
}

pub fn use_widget_again() -> u32 {
    build_widget().area()
}
