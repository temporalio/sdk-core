#pragma once

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

/**
 * A core instance owned by Core. This must be passed to tmprl_core_shutdown
 * when no longer in use which will free the resources.
 */
typedef struct tmprl_core_t tmprl_core_t;

/**
 * A runtime owned by Core. This must be passed to tmprl_runtime_free when no
 * longer in use. This must not be freed until every call to every tmprl_core_t
 * instance created with this runtime has been shutdown.
 */
typedef struct tmprl_runtime_t tmprl_runtime_t;

/**
 * A set of bytes owned by Core. This must always be passed to
 * tmprl_bytes_free when no longer in use.
 */
typedef struct tmprl_bytes_t {
  const uint8_t *bytes;
  size_t len;
  size_t cap;
  bool disable_free;
} tmprl_bytes_t;

/**
 * Free a set of bytes. The first parameter can be null in cases where a
 * tmprl_core_t instance isn't available. If the second parameter is null, this
 * is a no-op.
 */
void tmprl_bytes_free(struct tmprl_core_t *core, const struct tmprl_bytes_t *bytes);

/**
 * Create a new runtime. The result is never null and must be freed via
 * tmprl_runtime_free when no longer in use.
 */
struct tmprl_runtime_t *tmprl_runtime_new(void);

/**
 * Free a previously created runtime.
 */
void tmprl_runtime_free(struct tmprl_runtime_t *runtime);

/**
 * Create a new core instance.
 *
 * The runtime is required and must outlive this instance. The req_proto and
 * req_proto_len represent a byte array for a InitRequest protobuf message.
 *
 * The callback is invoked on completion. The first parameter of the callback
 * is a core instance if the call is successful or null if not. If present, the
 * core instance must be freed via tmprl_core_shutdown when no longer in use.
 * The second parameter of the callback is a byte array for a InitResponse
 * protobuf message which must be freed via tmprl_bytes_free.
 */
void tmprl_core_init(struct tmprl_runtime_t *runtime,
                     const uint8_t *req_proto,
                     size_t req_proto_len,
                     void *user_data,
                     void (*callback)(void *user_data, struct tmprl_core_t *core, const struct tmprl_bytes_t *resp));

/**
 * Shutdown and free a core instance.
 *
 * The req_proto and req_proto_len represent a byte array for a ShutdownRequest
 * protobuf message.
 *
 * The callback is invoked on completion with a never-null byte array for a
 * ShutdownResponse protobuf message which must be freed via tmprl_bytes_free.
 */
void tmprl_core_shutdown(struct tmprl_core_t *core,
                         const uint8_t *req_proto,
                         size_t req_proto_len,
                         void *user_data,
                         void (*callback)(void*, const struct tmprl_bytes_t*));

/**
 * Register a worker.
 *
 * The req_proto and req_proto_len represent a byte array for a RegisterWorker
 * protobuf message.
 *
 * The callback is invoked on completion with a never-null byte array for a
 * RegisterWorkflowResponse protobuf message which must be freed via
 * tmprl_bytes_free.
 */
void tmprl_register_worker(struct tmprl_core_t *core,
                           const uint8_t *req_proto,
                           size_t req_proto_len,
                           void *user_data,
                           void (*callback)(void*, const struct tmprl_bytes_t*));
