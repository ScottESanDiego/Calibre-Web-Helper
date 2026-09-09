use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use image::codecs::jpeg::JpegEncoder;
use image::imageops::FilterType;
use image::{DynamicImage, GenericImageView, ImageFormat, Rgb, RgbImage};
use rbook::Epub;
use std::fs;
use std::path::{Path, PathBuf};

use crate::models::BookMetadata;
use crate::utils::{detect_book_format, get_valid_filename};

/// Maximum cover image size in bytes (200KB)
const MAX_COVER_SIZE: u64 = 200 * 1024;

fn composite_onto_white(image: DynamicImage) -> RgbImage {
    let rgba = image.to_rgba8();
    RgbImage::from_fn(rgba.width(), rgba.height(), |x, y| {
        let pixel = rgba.get_pixel(x, y).0;
        let alpha = u16::from(pixel[3]);
        Rgb([
            ((u16::from(pixel[0]) * alpha + 255 * (255 - alpha) + 127) / 255) as u8,
            ((u16::from(pixel[1]) * alpha + 255 * (255 - alpha) + 127) / 255) as u8,
            ((u16::from(pixel[2]) * alpha + 255 * (255 - alpha) + 127) / 255) as u8,
        ])
    })
}

fn encode_jpeg(image: &RgbImage, quality: u8) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    JpegEncoder::new_with_quality(&mut output, quality)
        .encode_image(image)
        .context("Failed to encode normalized JPEG cover")?;
    Ok(output)
}

/// Decode every supported input, flatten transparency onto white, and emit a
/// real JPEG that obeys Calibre-Web's 200 KiB cover limit.
fn normalize_cover(cover_data: &[u8]) -> Result<Vec<u8>> {
    let decoded = image::load_from_memory(cover_data).context("Failed to decode cover image")?;
    let (original_width, original_height) = decoded.dimensions();
    let mut image = composite_onto_white(decoded);

    for _ in 0..32 {
        for quality in [90, 80, 70, 60, 50, 40, 30, 20, 10] {
            let output = encode_jpeg(&image, quality)?;
            if output.len() as u64 <= MAX_COVER_SIZE {
                if image.dimensions() != (original_width, original_height)
                    || cover_data.len() as u64 > MAX_COVER_SIZE
                {
                    println!(
                        " -> Normalized cover from {}KB to {}KB ({}x{} -> {}x{})",
                        cover_data.len() / 1024,
                        output.len() / 1024,
                        original_width,
                        original_height,
                        image.width(),
                        image.height()
                    );
                }
                return Ok(output);
            }
        }

        if image.width() == 1 && image.height() == 1 {
            break;
        }
        let width = (image.width() * 4 / 5).max(1);
        let height = (image.height() * 4 / 5).max(1);
        image = image::imageops::resize(&image, width, height, FilterType::Lanczos3);
    }

    anyhow::bail!("Could not normalize cover below the 200 KiB limit")
}

pub(crate) fn destination_filename(metadata: &BookMetadata, source: &Path) -> Result<String> {
    let (_format, extension) = detect_book_format(source)?;
    let title = get_valid_filename(&metadata.title, 42);
    let author = get_valid_filename(&metadata.author, 42);
    if title.is_empty() || author.is_empty() {
        anyhow::bail!("Book title and author must remain non-empty after filename sanitization");
    }
    Ok(format!("{} - {}{}", title, author, extension))
}

pub(crate) fn is_valid_installed_jpeg(path: &Path) -> bool {
    let Ok(contents) = fs::read(path) else {
        return false;
    };
    matches!(image::guess_format(&contents), Ok(ImageFormat::Jpeg))
        && image::load_from_memory_with_format(&contents, ImageFormat::Jpeg).is_ok()
}

pub(crate) fn extract_cover_bytes(epub_file: &Path) -> Result<Option<Vec<u8>>> {
    if let Ok(epub) = Epub::open(epub_file)
        && let Some(resource) = epub.manifest().cover_image()
        && let Ok(cover_data) = resource.read_bytes()
        && let Ok(normalized) = normalize_cover(&cover_data)
    {
        return Ok(Some(normalized));
    }
    let external = epub_file
        .parent()
        .map(|parent| parent.join("cover.jpg"))
        .unwrap_or_else(|| PathBuf::from("cover.jpg"));
    if external.is_file() {
        return Ok(fs::read(&external)
            .ok()
            .and_then(|cover| normalize_cover(&cover).ok()));
    }
    Ok(None)
}

fn first_metadata(
    metadata: rbook::epub::metadata::EpubMetadata<'_>,
    property: &str,
) -> Option<String> {
    metadata
        .by_property(property)
        .next()
        .map(|entry| entry.value().to_owned())
}

/// Extracts full metadata from the EPUB file.
pub(crate) fn get_epub_metadata(path: &Path) -> Result<BookMetadata> {
    let doc = Epub::open(path)?;
    let raw = doc.metadata();
    let title = first_metadata(raw, "dc:title").context("EPUB has no title metadata")?;
    let author =
        first_metadata(raw, "dc:creator").context("EPUB has no author (creator) metadata")?;
    let description = first_metadata(raw, "dc:description");
    let rights = first_metadata(raw, "dc:rights");
    let subtitle = first_metadata(raw, "subtitle");

    // Handle language codes with proper normalization
    let language = first_metadata(raw, "dc:language").map(|lang| {
        let lang = lang.trim().to_lowercase();

        // Helper closure to normalize language codes
        let normalize_language = |code: &str| -> String {
            match code {
                // Common ISO 639-1 to ISO 639-2 mappings (using terminological codes)
                "en" => "eng".to_string(),
                "fr" => "fra".to_string(), // French: fra (not fre)
                "es" => "spa".to_string(),
                "de" => "deu".to_string(), // German: deu (not ger)
                "it" => "ita".to_string(),
                "ja" => "jpn".to_string(),
                "zh" => "zho".to_string(), // Chinese: zho (not chi)
                "ru" => "rus".to_string(),
                "ar" => "ara".to_string(),
                "hi" => "hin".to_string(),
                "pt" => "por".to_string(),
                "nl" => "nld".to_string(), // Dutch: nld (not dut)
                "pl" => "pol".to_string(),
                "ko" => "kor".to_string(),
                // Add more mappings as needed
                _ => code.to_string(),
            }
        };

        // Split on hyphens to handle extended tags (e.g., "en-US" -> "en")
        let base_lang = lang.split(['-', '_']).next().unwrap_or(&lang);

        // Normalize the language code
        let normalized = if base_lang.len() == 2 {
            normalize_language(base_lang)
        } else if base_lang.len() == 3 {
            // Assume it's already ISO 639-2
            base_lang.to_string()
        } else {
            // Unknown format, keep as is
            base_lang.to_string()
        };

        // Verify it's a known ISO 639-2 code and convert unknown codes to "und"
        match normalized.as_str() {
            "eng" | "fra" | "deu" | "spa" | "ita" | "jpn" | "zho" | "rus" | "ara" | "hin"
            | "por" | "ben" | "urd" | "nld" | "tur" | "vie" | "tel" | "mar" | "tam" | "kor"
            | "fas" | "tha" | "pol" | "ukr" | "ron" | "mal" | "hun" | "ces" | "ell" | "swe"
            | "bul" | "dan" | "fin" | "nor" | "slk" | "cat" | "hrv" | "heb" | "lit" | "slv"
            | "est" | "lav" | "fil" | "mkd" | "gle" | "hye" | "lat" | "cym" | "eus" | "kat"
            | "aze" | "swa" | "afr" | "glg" | "alb" | "bel" | "kan" | "yue" | "cmn" => normalized,
            _ => "und".to_string(),
        }
    });

    let isbn = raw.by_property("dc:identifier").find_map(|id| {
        let id = id.value().trim();
        if id.starts_with("urn:isbn:") {
            return Some(id.trim_start_matches("urn:isbn:").to_string());
        }
        let digits: String = id.chars().filter(|c| c.is_ascii_digit()).collect();
        if digits.len() == 10 || digits.len() == 13 {
            return Some(digits);
        }
        None
    });

    // Get publisher
    let publisher = first_metadata(raw, "dc:publisher");

    // Get publication date
    let pubdate = first_metadata(raw, "dc:date").and_then(|date_str| {
        // Try various date formats
        let date_str = date_str.trim();

        // Try ISO8601/RFC3339 with time (YYYY-MM-DDThh:mm:ssZ)
        if let Ok(dt) = DateTime::parse_from_rfc3339(date_str) {
            return Some(dt.with_timezone(&Utc));
        }

        // Try ISO format (YYYY-MM-DD)
        if let Ok(dt) = chrono::NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
            return Some(DateTime::<Utc>::from_naive_utc_and_offset(
                dt.and_hms_opt(0, 0, 0).unwrap(),
                Utc,
            ));
        }

        // Try format with month name (DD MMMM YYYY)
        if let Ok(dt) = chrono::NaiveDate::parse_from_str(date_str, "%d %B %Y")
            .or_else(|_| chrono::NaiveDate::parse_from_str(date_str, "%d %b %Y"))
        {
            return Some(DateTime::<Utc>::from_naive_utc_and_offset(
                dt.and_hms_opt(0, 0, 0).unwrap(),
                Utc,
            ));
        }

        // Try year-month format (YYYY-MM)
        if let Ok(dt) = chrono::NaiveDate::parse_from_str(&format!("{}-01", date_str), "%Y-%m-%d") {
            return Some(DateTime::<Utc>::from_naive_utc_and_offset(
                dt.and_hms_opt(0, 0, 0).unwrap(),
                Utc,
            ));
        }

        // Try year only
        if let Ok(year) = date_str.parse::<i32>()
            && let Some(date) = chrono::NaiveDate::from_ymd_opt(year, 1, 1)
        {
            return Some(DateTime::<Utc>::from_naive_utc_and_offset(
                date.and_hms_opt(0, 0, 0).expect("midnight is always valid"),
                Utc,
            ));
        }

        None
    });

    // Extract series information from metadata
    // Look for calibre:series and calibre:series_index first
    let series = first_metadata(raw, "calibre:series").or_else(|| {
        // Fallback to looking for series information in the title
        // Common format: Series Name #X - Book Title
        let title_str = title.trim();
        if let Some(hash_idx) = title_str.find('#') {
            if let Some(_dash_idx) = title_str[hash_idx..].find('-') {
                // Extract everything before the # as the series name
                let series_part = title_str[..hash_idx].trim();
                if !series_part.is_empty() {
                    Some(series_part.to_string())
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        }
    });

    let series_index = first_metadata(raw, "calibre:series_index")
        .and_then(|idx| idx.parse::<f64>().ok())
        .or_else(|| {
            // Try to extract series index from title if in #X format
            title.find('#').and_then(|i| {
                let rest = &title[i + 1..];
                let num_str: String = rest
                    .chars()
                    .take_while(|c| c.is_ascii_digit() || *c == '.')
                    .collect();
                num_str.parse::<f64>().ok()
            })
        });

    // Get the file size
    let file_size = fs::metadata(path)
        .with_context(|| format!("Failed to get file size for {:?}", path))?
        .len();

    Ok(BookMetadata {
        title,
        author,
        path: path.to_path_buf(),
        description,
        language,
        isbn,
        rights,
        subtitle,
        series,
        series_index,
        publisher,
        pubdate,
        file_size,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use image::{Rgba, RgbaImage};
    use std::fs::File;
    use std::io::{Cursor, Write};
    use zip::CompressionMethod;
    use zip::write::SimpleFileOptions;

    fn temp_root(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("cwh-epub-{label}-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&path).unwrap();
        path
    }

    fn encoded_cover(format: ImageFormat) -> Vec<u8> {
        let image = RgbaImage::from_fn(32, 32, |x, _| {
            if x < 16 {
                Rgba([0, 0, 0, 0])
            } else {
                Rgba([200, 20, 10, 255])
            }
        });
        let mut bytes = Vec::new();
        DynamicImage::ImageRgba8(image)
            .write_to(&mut Cursor::new(&mut bytes), format)
            .unwrap();
        bytes
    }

    fn write_epub_fixture(path: &Path, epub3: bool) {
        let legacy_metadata = r#"
            <meta name="subtitle" content="Fixture Subtitle"/>
            <meta name="calibre:series" content="Fixture Series"/>
            <meta name="calibre:series_index" content="2.5"/>
            <meta name="cover" content="cover-image"/>"#;
        let modern_metadata = r#"
            <meta property="subtitle">Fixture Subtitle</meta>
            <meta property="calibre:series">Fixture Series</meta>
            <meta property="calibre:series_index">2.5</meta>"#;
        let cover_property = if epub3 {
            " properties=\"cover-image\""
        } else {
            ""
        };
        let opf = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
            <package xmlns="http://www.idpf.org/2007/opf" version="{}" unique-identifier="BookId">
              <metadata xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:opf="http://www.idpf.org/2007/opf">
                <dc:title>Fixture Title</dc:title><dc:title>Ignored Title</dc:title>
                <dc:creator>Fixture Author</dc:creator><dc:creator>Ignored Author</dc:creator>
                <dc:identifier>secondary</dc:identifier>
                <dc:identifier id="BookId">urn:isbn:9780306406157</dc:identifier>
                <dc:language>en-US</dc:language><dc:publisher>Fixture Publisher</dc:publisher>
                <dc:date>2024-02-03</dc:date><dc:description>Fixture Description</dc:description>
                <dc:rights>Fixture Rights</dc:rights>{}
              </metadata>
              <manifest>
                <item id="cover-image" href="cover.png" media-type="image/png"{}/>
                <item id="chapter" href="chapter.xhtml" media-type="application/xhtml+xml"/>
              </manifest>
              <spine><itemref idref="chapter"/></spine>
            </package>"#,
            if epub3 { "3.0" } else { "2.0" },
            if epub3 {
                modern_metadata
            } else {
                legacy_metadata
            },
            cover_property,
        );
        let container = r#"<?xml version="1.0"?>
            <container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
              <rootfiles><rootfile full-path="OEBPS/content.opf" media-type="application/oebps-package+xml"/></rootfiles>
            </container>"#;
        let cover = encoded_cover(ImageFormat::Png);
        let mut archive = zip::ZipWriter::new(File::create(path).unwrap());
        let options = SimpleFileOptions::default().compression_method(CompressionMethod::Stored);
        for (name, contents) in [
            ("mimetype", b"application/epub+zip".as_slice()),
            ("META-INF/container.xml", container.as_bytes()),
            ("OEBPS/content.opf", opf.as_bytes()),
            (
                "OEBPS/chapter.xhtml",
                b"<html xmlns=\"http://www.w3.org/1999/xhtml\"><body/></html>".as_slice(),
            ),
            ("OEBPS/cover.png", cover.as_slice()),
        ] {
            archive.start_file(name, options).unwrap();
            archive.write_all(contents).unwrap();
        }
        archive.finish().unwrap();
    }

    #[test]
    fn rbook_raw_metadata_and_cover_mapping_handles_epub2_and_epub3() {
        let root = temp_root("rbook-fixtures");
        for epub3 in [false, true] {
            let path = root.join(if epub3 {
                "fixture3.epub"
            } else {
                "fixture2.epub"
            });
            write_epub_fixture(&path, epub3);
            let metadata = get_epub_metadata(&path).unwrap();
            assert_eq!(metadata.title, "Fixture Title");
            assert_eq!(metadata.author, "Fixture Author");
            assert_eq!(metadata.isbn.as_deref(), Some("9780306406157"));
            assert_eq!(metadata.language.as_deref(), Some("eng"));
            assert_eq!(metadata.publisher.as_deref(), Some("Fixture Publisher"));
            assert_eq!(
                metadata.pubdate.unwrap().format("%Y-%m-%d").to_string(),
                "2024-02-03"
            );
            assert_eq!(metadata.description.as_deref(), Some("Fixture Description"));
            assert_eq!(metadata.rights.as_deref(), Some("Fixture Rights"));
            assert_eq!(metadata.subtitle.as_deref(), Some("Fixture Subtitle"));
            assert_eq!(metadata.series.as_deref(), Some("Fixture Series"));
            assert_eq!(metadata.series_index, Some(2.5));
            let cover = extract_cover_bytes(&path).unwrap().unwrap();
            assert!(cover.len() as u64 <= MAX_COVER_SIZE);
            assert!(matches!(image::guess_format(&cover), Ok(ImageFormat::Jpeg)));
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn png_webp_and_jpeg_normalize_to_small_decodable_jpeg_on_white() {
        for format in [ImageFormat::Png, ImageFormat::WebP] {
            let normalized = normalize_cover(&encoded_cover(format)).unwrap();
            assert!(normalized.len() as u64 <= MAX_COVER_SIZE);
            assert!(matches!(
                image::guess_format(&normalized),
                Ok(ImageFormat::Jpeg)
            ));
            let decoded = image::load_from_memory_with_format(&normalized, ImageFormat::Jpeg)
                .unwrap()
                .to_rgb8();
            let white = decoded.get_pixel(4, 16).0;
            assert!(white.iter().all(|channel| *channel > 240));
        }

        let jpeg = normalize_cover(&encoded_cover(ImageFormat::Jpeg)).unwrap();
        assert!(jpeg.len() as u64 <= MAX_COVER_SIZE);
        assert!(matches!(image::guess_format(&jpeg), Ok(ImageFormat::Jpeg)));
        assert!(image::load_from_memory_with_format(&jpeg, ImageFormat::Jpeg).is_ok());
    }

    #[test]
    fn malformed_external_cover_is_omitted() {
        let root = temp_root("malformed-new");
        let input = root.join("input");
        fs::create_dir(&input).unwrap();
        let source = input.join("book.epub");
        fs::write(&source, b"not an epub").unwrap();
        fs::write(input.join("cover.jpg"), b"not an image").unwrap();

        assert!(extract_cover_bytes(&source).unwrap().is_none());
        fs::remove_dir_all(root).unwrap();
    }
}
