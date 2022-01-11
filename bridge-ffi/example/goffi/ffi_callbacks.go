package goffi

/*
#cgo CFLAGS:-I${SRCDIR}/../../include
#include <sdk-core-bridge.h>

extern void go_callback_core_init(void*, tmprl_core_t*, tmprl_bytes_t*);

void callback_core_init(void* user_data, struct tmprl_core_t* core, struct tmprl_bytes_t* bytes) {
	go_callback_core_init(user_data, core, bytes);
}

extern void go_callback_core(void*, tmprl_bytes_t*);

void callback_core(void* user_data, struct tmprl_bytes_t* bytes) {
	go_callback_core(user_data, bytes);
}
*/
import "C"

// These have to be defined in a separate file due to Go rules.
// See https://github.com/golang/go/wiki/cgo#function-pointer-callbacks
