//! What an image is, read from its own bytes: for a reply that does not say
//! (the OpenAI Images API returns no media type), and for the size a reply
//! was actually served at.

/// The media type `bytes` begin with, when it is one Cairn recognises: PNG,
/// JPEG, WebP, GIF, or SVG.
pub fn media_type(bytes: &[u8]) -> Option<&'static str> {
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        return Some("image/png");
    }
    if bytes.starts_with(&[0xFF, 0xD8, 0xFF]) {
        return Some("image/jpeg");
    }
    if bytes.len() >= 12 && &bytes[..4] == b"RIFF" && &bytes[8..12] == b"WEBP" {
        return Some("image/webp");
    }
    if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        return Some("image/gif");
    }
    is_svg(bytes).then_some("image/svg+xml")
}

fn is_svg(bytes: &[u8]) -> bool {
    let head = &bytes[..bytes.len().min(1024)];
    let text = String::from_utf8_lossy(head);
    let text = text.trim_start_matches('\u{feff}').trim_start();
    (text.starts_with("<svg") || text.starts_with("<?xml") || text.starts_with("<!DOCTYPE svg"))
        && text.contains("<svg")
}

/// The pixel size a raster image's header declares. `None` for SVG, which
/// has no pixel size, and for anything unreadable.
pub fn dimensions(bytes: &[u8]) -> Option<(u32, u32)> {
    let size = imagesize::blob_size(bytes).ok()?;
    Some((
        u32::try_from(size.width).ok()?,
        u32::try_from(size.height).ok()?,
    ))
}

/// The file extension for a media type, for a multipart filename.
pub fn extension(media_type: &str) -> &'static str {
    match media_type {
        "image/jpeg" | "image/jpg" => "jpg",
        "image/webp" => "webp",
        "image/gif" => "gif",
        "image/svg+xml" => "svg",
        _ => "png",
    }
}

#[cfg(test)]
pub(crate) mod fixtures {
    //! Tiny real images, so sniffing and size reading run on actual bytes.

    pub const PNG_2X3: &str = "iVBORw0KGgoAAAANSUhEUgAAAAIAAAADCAIAAAA2iEnWAAAAFElEQVR4nGM8ISfHwMDAxMDAgKAAFEoBCqHkTQMAAAAASUVORK5CYII=";
    pub const WEBP_1X1: &str = "UklGRhwAAABXRUJQVlA4TA8AAAAvAAAAAAcQ0f/+ByKi/wEA";
    pub const GIF_1X1: &str = "R0lGODdhAQABAIAAAAAAAAAAACwAAAAAAQABAAAIBAABBAQAOw==";
    pub const SVG_4X4: &str = "PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSI0IiBoZWlnaHQ9IjQiPjxyZWN0IHdpZHRoPSI0IiBoZWlnaHQ9IjQiIGZpbGw9InJlZCIvPjwvc3ZnPg==";
    /// A 2x3 RGBA PNG whose top-left pixel is transparent.
    pub const MASK_2X3: &str = "iVBORw0KGgoAAAANSUhEUgAAAAIAAAADCAYAAAC56t6BAAAAGUlEQVR4nGNggID/TAwMDP8ZGBgYmKAiDAAhMAID+rl5HQAAAABJRU5ErkJggg==";
    /// A 4x3 JPEG.
    pub const JPEG_4X3: &str = "/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDABALDA4MChAODQ4SERATGCgaGBYWGDEjJR0oOjM9PDkzODdASFxOQERXRTc4UG1RV19iZ2hnPk1xeXBkeFxlZ2P/2wBDARESEhgVGC8aGi9jQjhCY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2P/wAARCAADAAQDASIAAhEBAxEB/8QAHwAAAQUBAQEBAQEAAAAAAAAAAAECAwQFBgcICQoL/8QAtRAAAgEDAwIEAwUFBAQAAAF9AQIDAAQRBRIhMUEGE1FhByJxFDKBkaEII0KxwRVS0fAkM2JyggkKFhcYGRolJicoKSo0NTY3ODk6Q0RFRkdISUpTVFVWV1hZWmNkZWZnaGlqc3R1dnd4eXqDhIWGh4iJipKTlJWWl5iZmqKjpKWmp6ipqrKztLW2t7i5usLDxMXGx8jJytLT1NXW19jZ2uHi4+Tl5ufo6erx8vP09fb3+Pn6/8QAHwEAAwEBAQEBAQEBAQAAAAAAAAECAwQFBgcICQoL/8QAtREAAgECBAQDBAcFBAQAAQJ3AAECAxEEBSExBhJBUQdhcRMiMoEIFEKRobHBCSMzUvAVYnLRChYkNOEl8RcYGRomJygpKjU2Nzg5OkNERUZHSElKU1RVVldYWVpjZGVmZ2hpanN0dXZ3eHl6goOEhYaHiImKkpOUlZaXmJmaoqOkpaanqKmqsrO0tba3uLm6wsPExcbHyMnK0tPU1dbX2Nna4uPk5ebn6Onq8vP09fb3+Pn6/9oADAMBAAIRAxEAPwDEoooryzxj/9k=";

    pub fn bytes(base64: &str) -> Vec<u8> {
        crate::decode_base64(base64).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::fixtures::*;
    use super::*;

    #[test]
    fn each_format_is_known_by_its_own_bytes() {
        assert_eq!(media_type(&bytes(PNG_2X3)), Some("image/png"));
        assert_eq!(media_type(&bytes(JPEG_4X3)), Some("image/jpeg"));
        assert_eq!(media_type(&bytes(WEBP_1X1)), Some("image/webp"));
        assert_eq!(media_type(&bytes(GIF_1X1)), Some("image/gif"));
        assert_eq!(media_type(&bytes(SVG_4X4)), Some("image/svg+xml"));
        assert_eq!(
            media_type(b"\xef\xbb\xbf<?xml version=\"1.0\"?>\n<svg/>"),
            Some("image/svg+xml")
        );
        assert_eq!(media_type(b"<html>no</html>"), None);
        assert_eq!(media_type(b""), None);
    }

    #[test]
    fn raster_sizes_come_from_the_header_and_svg_has_none() {
        assert_eq!(dimensions(&bytes(PNG_2X3)), Some((2, 3)));
        assert_eq!(dimensions(&bytes(JPEG_4X3)), Some((4, 3)));
        assert_eq!(dimensions(&bytes(WEBP_1X1)), Some((1, 1)));
        assert_eq!(dimensions(&bytes(GIF_1X1)), Some((1, 1)));
        assert_eq!(dimensions(&bytes(SVG_4X4)), None);
        assert_eq!(extension("image/jpeg"), "jpg");
        assert_eq!(extension("application/octet-stream"), "png");
    }
}
