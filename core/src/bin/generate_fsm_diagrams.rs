use std::env;
use std::fs;
use std::io;
use std::path::Path;
use std::process::{Command, Stdio};

fn main() {
    // Require d2 upfront; no HTML or D2 fallback when missing
    if !d2_available() {
        eprintln!(
            "Error: d2 is not installed. SVG generation and index.html will not be produced.\nInstall d2: https://d2lang.com/tour/install"
        );
        std::process::exit(1);
    }

    // Resolve core crate dir and diagram dirs
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
    let diagram_dir = Path::new(&manifest_dir).join("src/worker/workflow/machines/diagrams");
    let svg_dir = diagram_dir.join("svg");

    // Ensure directory exists and clean prior outputs
    let _ = fs::create_dir_all(&diagram_dir);
    let _ = clean_dir(&diagram_dir, &["d2", "svg"]);
    let _ = fs::remove_dir_all(&svg_dir);

    // Trigger proc-macro generation via cargo check with env set on the child
    let _ = Command::new("cargo")
        .arg("check")
        .arg("--quiet")
        .env("TEMPORAL_GENERATE_FSM_DIAGRAMS", "1")
        .env("TEMPORAL_FSM_DIAGRAM_DIR", diagram_dir.as_os_str())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();

    // If no d2 files were generated (cargo no-op), force a rebuild via touching a source file
    if count_ext(&diagram_dir, "d2") == 0 {
        // Try touching core/src/lib.rs, else touch Cargo.toml
        let lib_rs = Path::new(&manifest_dir).join("src/lib.rs");
        if !touch_file(&lib_rs) {
            let ct = Path::new(&manifest_dir).join("Cargo.toml");
            let _ = touch_file(&ct);
        }
        let _ = Command::new("cargo")
            .arg("check")
            .arg("--quiet")
            .env("TEMPORAL_GENERATE_FSM_DIAGRAMS", "1")
            .env("TEMPORAL_FSM_DIAGRAM_DIR", diagram_dir.as_os_str())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }

    // Validate .d2 presence
    if count_ext(&diagram_dir, "d2") == 0 {
        eprintln!(
            "Error: No .d2 diagrams were generated. Ensure state machines compile and try again."
        );
        std::process::exit(2);
    }

    // Render SVGs
    let _ = fs::create_dir_all(&svg_dir);
    if let Ok(entries) = fs::read_dir(&diagram_dir) {
        for entry in entries.flatten() {
            let p = entry.path();
            if p.extension().and_then(|e| e.to_str()) == Some("d2") {
                let stem = p.file_stem().unwrap();
                let out = svg_dir.join(format!("{}.svg", stem.to_string_lossy()));
                let _ = Command::new("d2")
                    .arg(&p)
                    .arg(&out)
                    .arg("--theme")
                    .arg("200")
                    .arg("--layout")
                    .arg("elk")
                    .stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status();
            }
        }
    }

    // Create HTML only if we have SVGs
    if count_ext(&svg_dir, "svg") == 0 {
        eprintln!("Error: No SVGs generated (d2 run produced none).");
        std::process::exit(3);
    }
    let _ = write_index_html(&diagram_dir, true);
    println!("{}", svg_dir.display());
}

fn d2_available() -> bool {
    Command::new("d2")
        .arg("--version")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn clean_dir(dir: &Path, exts: &[&str]) -> io::Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(dir)? {
        if let Ok(entry) = entry {
            let p = entry.path();
            if p.is_file() {
                if let Some(ext) = p.extension().and_then(|e| e.to_str()) {
                    if exts.iter().any(|x| *x == ext) {
                        let _ = fs::remove_file(p);
                    }
                }
            }
        }
    }
    Ok(())
}

fn write_index_html(diagram_dir: &Path, _use_svg: bool) -> io::Result<()> {
    #[derive(Clone)]
    struct Item {
        stem: String,
        id: String,
        label: String,
    }
    let mut items: Vec<Item> = vec![];
    let svg_dir = diagram_dir.join("svg");
    if let Ok(entries) = fs::read_dir(&svg_dir) {
        for entry in entries.flatten() {
            let p = entry.path();
            if p.extension().and_then(|e| e.to_str()) == Some("svg") {
                if let Some(stem) = p.file_stem().and_then(|s| s.to_str()) {
                    let stem_s = stem.to_string();
                    let label =
                        display_label_from_d2(diagram_dir, stem).unwrap_or_else(|| stem_s.clone());
                    let id = label.clone();
                    items.push(Item {
                        stem: stem_s,
                        id,
                        label,
                    });
                }
            }
        }
    }
    items.sort_by(|a, b| a.label.cmp(&b.label));

    let mut html = String::new();
    html.push_str("<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n");
    html.push_str("<title>Temporal SDK Core FSM Diagrams</title>\n");
    html.push_str("<style>body{margin:0;font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;}\n");
    html.push_str(".sidebar{position:fixed;top:0;left:0;height:100vh;width:260px;overflow:auto;background:#fafafa;border-right:1px solid #e0e0e0;padding:16px;}\n");
    html.push_str(".sidebar h1{font-size:16px;margin:0 0 12px 0;} .sidebar a{display:block;color:#1565c0;text-decoration:none;margin:6px 0;word-break:break-word;} .sidebar a:hover{text-decoration:underline;}\n");
    html.push_str(".content{margin-left:260px;padding:16px;} h2{margin-top:48px;border-bottom:1px solid #eee;padding-bottom:10px;text-align:center;} .diagram{display:block;max-width:100%;height:auto;max-height:calc(100vh - 160px);margin:0 auto;border:1px solid #eee;background:#fff} pre{background:#f5f5f5;padding:12px;overflow:auto;border:1px solid #e0e0e0}\n");
    html.push_str("code{font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,\\\"Liberation Mono\\\",monospace;background:none;padding:0}\n");
    html.push_str("</style></head><body>\n");

    // Sidebar
    html.push_str("<nav class=\"sidebar\">\n<h1>FSM Diagrams</h1>\n");
    for it in &items {
        html.push_str(&format!(
            "<a href=\"#{}\"><code>{}</code></a>\n",
            it.id, it.label
        ));
    }
    html.push_str("</nav>\n");

    // Content
    html.push_str("<main class=\"content\">\n");
    for it in &items {
        html.push_str(&format!(
            "<section id=\"{}\">\n<h2><code>{}</code></h2>\n",
            it.id, it.label
        ));
        let src = format!("svg/{}.svg", it.stem);
        html.push_str(&format!(
            "<img class=\"diagram\" src=\"{}\" alt=\"{}\"/>\n",
            src, it.label
        ));
        html.push_str("</section>\n");
    }
    html.push_str("</main>\n</body></html>");

    fs::write(diagram_dir.join("index.html"), html)
}

fn display_label_from_d2(diagram_dir: &Path, stem: &str) -> Option<String> {
    let d2 = diagram_dir.join(format!("{}.d2", stem));
    let content = fs::read_to_string(d2).ok()?;
    for line in content.lines().take(5) {
        if let Some(name) = line
            .strip_prefix('#')
            .and_then(|rest| rest.trim().strip_suffix(" State Machine"))
        {
            let mut s = to_snake(name.trim());
            if let Some(stripped) = s.strip_suffix("_machine") {
                s = stripped.to_string();
            }
            return Some(s);
        }
    }
    None
}

fn to_snake(s: &str) -> String {
    let mut out = String::new();
    let mut prev_lower = false;
    for ch in s.chars() {
        if ch.is_ascii_uppercase() {
            if prev_lower && !out.is_empty() {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
            prev_lower = false;
        } else {
            let is_sep = ch == ' ' || ch == '-' || ch == '/';
            if is_sep {
                if !out.ends_with('_') && !out.is_empty() {
                    out.push('_');
                }
                prev_lower = false;
            } else {
                out.push(ch);
                prev_lower = ch.is_ascii_lowercase() || ch.is_ascii_digit();
            }
        }
    }
    while out.contains("__") {
        out = out.replace("__", "_");
    }
    out.trim_matches('_').to_string()
}

fn count_ext(dir: &Path, ext: &str) -> usize {
    let mut c = 0usize;
    if let Ok(entries) = fs::read_dir(dir) {
        for e in entries.flatten() {
            let p = e.path();
            if p.extension().and_then(|e| e.to_str()) == Some(ext) {
                c += 1;
            }
        }
    }
    c
}

fn touch_file(p: &Path) -> bool {
    if !p.exists() {
        return false;
    }
    match fs::read(p) {
        Ok(bytes) => fs::write(p, bytes).is_ok(),
        Err(_) => false,
    }
}
