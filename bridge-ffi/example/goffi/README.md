
(under development)

Proto gen (with `protoc` and `protoc-gen-gogoslick` on the `PATH`):

    go run ./protogen

Build `bridge-ffi` crate and put static lib info `lib/<arch>/` dir. Remember on Windows you must use
`stable-x86_64-pc-windows-gnu` triple instead of `stable-x86_64-pc-windows-msvc` since Go statically links using gcc.
