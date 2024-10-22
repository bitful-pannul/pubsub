use anyhow::{anyhow, Result};
use kinode_process_lib::{
    vfs::{VfsAction, VfsRequest, VfsResponse},
    Address, ProcessId, Request,
};
use sha2_const::Sha256;
/// helper functions for binary files
// wasm binaries to allow avoiding the need to copy them into your project.
// at boot of a new process, we check if there's a wasm in the pkg folder,
// if it's sha256 matches the const, we use it.
// otherwise we write the embedded version to the pkg folder and use that.
use std::sync::OnceLock;

// actually this... could be a feature flag? include_wasm = true/false?
// then if you're already distributing an app, and you have it in you pkg, can keep the binary size of code down.
static PUB_WASM: &[u8] = include_bytes!("../processes/pkg/pub.wasm");
static SUB_WASM: &[u8] = include_bytes!("../processes/pkg/sub.wasm");

static PUB_WASM_VEC: OnceLock<Vec<u8>> = OnceLock::new();
static SUB_WASM_VEC: OnceLock<Vec<u8>> = OnceLock::new();

// Calculate SHA256 hashes at compile-time
const PUB_WASM_HASH: [u8; 32] = calc_sha256(PUB_WASM);
const SUB_WASM_HASH: [u8; 32] = calc_sha256(SUB_WASM);

// Const function to calculate SHA256 hash
const fn calc_sha256(data: &[u8]) -> [u8; 32] {
    Sha256::new().update(data).finalize()
}

// function helpers to check for binary.

pub enum WasmType {
    Pub,
    Sub,
}

pub fn populate_wasm(our: &Address, wasm_type: WasmType) -> Result<()> {
    let (wasm_name, get_wasm, get_wasm_hash): (
        &str,
        fn() -> &'static Vec<u8>,
        fn() -> &'static [u8; 32],
    ) = match wasm_type {
        WasmType::Pub => ("pub", get_pub_wasm, get_pub_wasm_hash),
        WasmType::Sub => ("sub", get_sub_wasm, get_sub_wasm_hash),
    };

    let wasm_path = format!("{}/pkg/{}.wasm", our.package_id(), wasm_name);

    if let Ok(current_hash) = get_file_hash(&wasm_path, our) {
        if current_hash == *get_wasm_hash() {
            return Ok(()); // hash matches, we're done
        }
    }

    // update WASM file
    update_wasm_file(&wasm_path, our, get_wasm())?;

    // verify hash after update
    let updated_hash = get_file_hash(&wasm_path, our)?;
    if updated_hash == *get_wasm_hash() {
        Ok(())
    } else {
        Err(anyhow!("Hash mismatch after updating {} WASM", wasm_name))
    }
}

fn get_file_hash(path: &str, our: &Address) -> Result<[u8; 32]> {
    let vfs_address = Address::new(
        our.node.clone(),
        ProcessId::new(Some("vfs"), "distro", "sys"),
    );

    let res = Request::to(&vfs_address)
        .body(serde_json::to_vec(&VfsRequest {
            path: path.to_string(),
            action: VfsAction::Hash,
        })?)
        .send_and_await_response(5)??;

    match serde_json::from_slice(&res.body())? {
        VfsResponse::Hash(hash) => Ok(hash),
        VfsResponse::Err(e) => Err(anyhow!("VFS error: {:?}", e)),
        _ => Err(anyhow!("Unexpected VFS response")),
    }
}

fn update_wasm_file(path: &str, our: &Address, wasm_bytes: &[u8]) -> Result<()> {
    let vfs_address = Address::new(
        our.node.clone(),
        ProcessId::new(Some("vfs"), "distro", "sys"),
    );

    let res = Request::to(&vfs_address)
        .body(serde_json::to_vec(&VfsRequest {
            path: path.to_string(),
            action: VfsAction::Write,
        })?)
        .blob_bytes(wasm_bytes)
        .send_and_await_response(5)??;

    match serde_json::from_slice(&res.body())? {
        VfsResponse::Ok => Ok(()),
        VfsResponse::Err(e) => Err(anyhow!("VFS error while writing WASM: {:?}", e)),
        _ => Err(anyhow!("Failed to write WASM file")),
    }
}

// get wasm binaries helper functions
fn get_pub_wasm() -> &'static Vec<u8> {
    PUB_WASM_VEC.get_or_init(|| PUB_WASM.to_vec())
}

fn get_sub_wasm() -> &'static Vec<u8> {
    SUB_WASM_VEC.get_or_init(|| SUB_WASM.to_vec())
}

fn get_pub_wasm_hash() -> &'static [u8; 32] {
    &PUB_WASM_HASH
}

fn get_sub_wasm_hash() -> &'static [u8; 32] {
    &SUB_WASM_HASH
}
