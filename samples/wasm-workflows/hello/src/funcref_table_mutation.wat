(module
  (type $thunk (func (result i32)))

  (table $__indirect_function_table (import "env" "__indirect_function_table") 1 funcref)

  (func $before (result i32)
    i32.const 11)

  (func $after (result i32)
    i32.const 22)

  (func $set_to_after (export "temporal_funcref_table_set_to_after")
    i32.const 0
    (; build.rs emits this immediate as a table-index relocation against $after. ;)
    i32.const 0
    table.get $__indirect_function_table
    table.set $__indirect_function_table)

  (func $call_slot (export "temporal_funcref_table_call") (result i32)
    i32.const 0
    call_indirect (type $thunk))

)
