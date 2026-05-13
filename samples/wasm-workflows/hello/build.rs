use std::{borrow::Cow, env, fs, path::PathBuf};
use wasm_encoder::{
    CodeSection, CustomSection, Encode, Function, FunctionSection, ImportSection, Instruction,
    LinkingSection, Module, RefType, SymbolTable, TableType, TypeSection, ValType,
};

const R_WASM_TABLE_INDEX_SLEB: u8 = 1;
const R_WASM_TYPE_INDEX_LEB: u8 = 6;
const R_WASM_TABLE_NUMBER_LEB: u8 = 20;

fn main() {
    println!("cargo:rerun-if-changed=src/funcref_table_mutation.wat");
    wat::parse_file("src/funcref_table_mutation.wat")
        .expect("funcref table mutation WAT should parse");

    if env::var("CARGO_CFG_TARGET_ARCH").as_deref() != Ok("wasm32") {
        return;
    }

    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR should be set"));
    let object_path = out_dir.join("funcref_table_mutation.o");
    fs::write(&object_path, funcref_table_mutation_object())
        .expect("funcref table mutation object should be writable");
    println!("cargo:rustc-link-arg={}", object_path.display());
}

fn funcref_table_mutation_object() -> Vec<u8> {
    let mut module = Module::new();

    let mut types = TypeSection::new();
    types.ty().function([], [ValType::I32]);
    types.ty().function([], []);
    module.section(&types);

    let mut imports = ImportSection::new();
    imports.import(
        "env",
        "__indirect_function_table",
        TableType {
            element_type: RefType::FUNCREF,
            minimum: 1,
            maximum: None,
            table64: false,
            shared: false,
        },
    );
    module.section(&imports);

    let mut functions = FunctionSection::new();
    functions.function(0);
    functions.function(0);
    functions.function(1);
    functions.function(0);
    module.section(&functions);

    let mut code = CodeSection::new();
    let mut before = Function::new([]);
    before.instruction(&Instruction::I32Const(11));
    before.instruction(&Instruction::End);
    code.function(&before);

    let mut after = Function::new([]);
    after.instruction(&Instruction::I32Const(22));
    after.instruction(&Instruction::End);
    code.function(&after);

    let mut set_to_after = Vec::new();
    set_to_after.push(0);
    set_to_after.push(0x41);
    set_to_after.push(0);
    set_to_after.push(0x41);
    let set_source_table_index_offset = set_to_after.len() as u32;
    padded_u32(0, &mut set_to_after);
    set_to_after.push(0x25);
    let set_table_get_offset = set_to_after.len() as u32;
    padded_u32(0, &mut set_to_after);
    set_to_after.push(0x26);
    let set_table_offset = set_to_after.len() as u32;
    padded_u32(0, &mut set_to_after);
    set_to_after.push(0x0b);
    code.raw(&set_to_after);

    let mut call_slot = Vec::new();
    call_slot.push(0);
    call_slot.push(0x41);
    call_slot.push(0);
    call_slot.push(0x11);
    let call_type_offset = call_slot.len() as u32;
    padded_u32(0, &mut call_slot);
    let call_table_offset = call_slot.len() as u32;
    padded_u32(0, &mut call_slot);
    call_slot.push(0x0b);
    code.raw(&call_slot);
    module.section(&code);

    let mut linking = LinkingSection::new();
    let mut symbols = SymbolTable::new();
    let local = SymbolTable::WASM_SYM_BINDING_LOCAL;
    symbols.function(local, 0, Some("temporal_funcref_before"));
    symbols.function(
        SymbolTable::WASM_SYM_EXPORTED | SymbolTable::WASM_SYM_NO_STRIP,
        1,
        Some("temporal_funcref_after"),
    );
    symbols.function(
        SymbolTable::WASM_SYM_EXPORTED | SymbolTable::WASM_SYM_NO_STRIP,
        2,
        Some("temporal_funcref_table_set_to_after"),
    );
    symbols.function(
        SymbolTable::WASM_SYM_EXPORTED | SymbolTable::WASM_SYM_NO_STRIP,
        3,
        Some("temporal_funcref_table_call"),
    );
    symbols.table(
        SymbolTable::WASM_SYM_UNDEFINED | SymbolTable::WASM_SYM_NO_STRIP,
        0,
        None,
    );
    linking.symbol_table(&symbols);
    module.section(&linking);

    module.section(&CustomSection {
        name: "reloc.CODE".into(),
        data: Cow::Owned(reloc_code_section(
            set_source_table_index_offset,
            set_table_get_offset,
            set_table_offset,
            set_to_after.len() as u32,
            call_type_offset,
            call_table_offset,
        )),
    });

    module.section(&CustomSection {
        name: "target_features".into(),
        data: Cow::Owned(target_features_section()),
    });

    module.finish()
}

fn reloc_code_section(
    set_source_table_index_offset: u32,
    set_table_get_offset: u32,
    set_table_offset: u32,
    set_to_after_len: u32,
    call_type_offset: u32,
    call_table_offset: u32,
) -> Vec<u8> {
    let mut reloc = Vec::new();
    3u32.encode(&mut reloc);

    let before_function_len = 5;
    let after_function_len = 5;
    let set_body_start = 1 + before_function_len + after_function_len + 1;
    let call_body_start = set_body_start + set_to_after_len + 1;

    let mut entries = Vec::new();
    push_reloc(
        &mut entries,
        R_WASM_TABLE_INDEX_SLEB,
        set_body_start + set_source_table_index_offset,
        1,
    );
    push_reloc(
        &mut entries,
        R_WASM_TABLE_NUMBER_LEB,
        set_body_start + set_table_get_offset,
        4,
    );
    push_reloc(
        &mut entries,
        R_WASM_TABLE_NUMBER_LEB,
        set_body_start + set_table_offset,
        4,
    );
    push_reloc(
        &mut entries,
        R_WASM_TYPE_INDEX_LEB,
        call_body_start + call_type_offset,
        0,
    );
    push_reloc(
        &mut entries,
        R_WASM_TABLE_NUMBER_LEB,
        call_body_start + call_table_offset,
        4,
    );
    5u32.encode(&mut reloc);
    reloc.extend(entries);
    reloc
}

fn target_features_section() -> Vec<u8> {
    let mut features = Vec::new();
    1u32.encode(&mut features);
    features.push(b'+');
    "reference-types".encode(&mut features);
    features
}

fn padded_u32(n: u32, out: &mut Vec<u8>) {
    let mut value = n;
    for _ in 0..4 {
        out.push(((value as u8) & 0x7f) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn push_reloc(out: &mut Vec<u8>, ty: u8, offset: u32, index: u32) {
    out.push(ty);
    offset.encode(out);
    index.encode(out);
}
