fn main() {
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("find vendored protoc");
    // The build script sets PROTOC before launching any concurrent work.
    unsafe { std::env::set_var("PROTOC", protoc) };
    let descriptor_path = std::path::PathBuf::from(std::env::var_os("OUT_DIR").expect("OUT_DIR"))
        .join("biubin_descriptor.bin");
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(descriptor_path)
        .compile_protos(&["../../proto/biubin.proto"], &["../../proto"])
        .expect("compile biubin.proto");
    println!("cargo:rerun-if-changed=../../proto/biubin.proto");

    let manifest_dir = std::path::PathBuf::from(
        std::env::var_os("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"),
    );
    let web_dir = manifest_dir.join("../../web");
    let built_index = web_dir.join("dist/index.html");
    let built_openapi = web_dir.join("dist/openapi.html");
    let fallback_index = web_dir.join("fallback.html");
    let index = if built_index.is_file() {
        built_index
    } else {
        fallback_index
    };
    let openapi = if built_openapi.is_file() {
        built_openapi
    } else {
        web_dir.join("fallback.html")
    };
    let out_dir = std::path::PathBuf::from(std::env::var_os("OUT_DIR").expect("OUT_DIR"));
    for (source, name) in [(&index, "index.html"), (&openapi, "openapi.html")] {
        let embedded = std::fs::read_to_string(source)
            .unwrap_or_else(|error| panic!("read embedded web page {}: {error}", source.display()));
        std::fs::write(out_dir.join(name), embedded)
            .unwrap_or_else(|error| panic!("write embedded web page {name}: {error}"));
    }
    let assets_dir =
        std::path::PathBuf::from(std::env::var_os("OUT_DIR").expect("OUT_DIR")).join("web-assets");
    let _ = std::fs::remove_dir_all(&assets_dir);
    std::fs::create_dir_all(&assets_dir)
        .unwrap_or_else(|error| panic!("create embedded web assets directory: {error}"));
    let mut assets = Vec::new();
    if web_dir.join("dist").is_dir() {
        collect_assets(
            &web_dir.join("dist"),
            &web_dir.join("dist"),
            &assets_dir,
            &mut assets,
        );
    }
    assets.sort();
    let mut asset_source = String::from("pub static WEB_ASSETS: &[(&str, &[u8])] = &[\n");
    for (name, path) in assets {
        use std::fmt::Write as _;
        writeln!(asset_source, "    ({name:?}, include_bytes!({path:?})),")
            .expect("write embedded asset source");
    }
    asset_source.push_str("];\n");
    let asset_source_path = std::path::PathBuf::from(std::env::var_os("OUT_DIR").expect("OUT_DIR"))
        .join("web_assets.rs");
    std::fs::write(&asset_source_path, asset_source)
        .unwrap_or_else(|error| panic!("write embedded web assets source: {error}"));
    println!("cargo:rerun-if-changed=../../web/index.html");
    println!("cargo:rerun-if-changed=../../web/openapi.html");
    println!("cargo:rerun-if-changed=../../web/fallback.html");
    println!("cargo:rerun-if-changed=../../web/dist");
}

fn collect_assets(
    root: &std::path::Path,
    current: &std::path::Path,
    destination: &std::path::Path,
    assets: &mut Vec<(String, String)>,
) {
    let entries = std::fs::read_dir(current)
        .unwrap_or_else(|error| panic!("read web assets directory {}: {error}", current.display()));
    for entry in entries {
        let entry = entry.expect("read web asset entry");
        let path = entry.path();
        if path.is_dir() {
            collect_assets(root, &path, destination, assets);
            continue;
        }
        let relative = path
            .strip_prefix(root)
            .expect("web asset is inside dist")
            .to_string_lossy()
            .replace(std::path::MAIN_SEPARATOR, "/");
        let output = destination.join(relative.replace('/', std::path::MAIN_SEPARATOR_STR));
        if let Some(parent) = output.parent() {
            std::fs::create_dir_all(parent).expect("create copied web asset directory");
        }
        std::fs::copy(&path, &output)
            .unwrap_or_else(|error| panic!("copy web asset {}: {error}", path.display()));
        assets.push((relative, output.to_string_lossy().into_owned()));
    }
}
