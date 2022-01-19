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
 * A set of bytes owned by Core. No fields within nor any bytes references must
 * ever be mutated outside of Core. This must always be passed to
 * tmprl_bytes_free when no longer in use.
 */
typedef struct tmprl_bytes_t {
  const uint8_t *bytes;
  size_t len;
  /**
   * For internal use only.
   */
  size_t cap;
  /**
   * For internal use only.
   */
  bool disable_free;
} tmprl_bytes_t;

/**
 * Callback called by tmprl_core_init on completion. The first parameter of the
 * callback is user data passed into the original function. The second
 * parameter is a core instance if the call is successful or null if not. If
 * present, the core instance must be freed via tmprl_core_shutdown when no
 * longer in use. The third parameter of the callback is a byte array for a
 * InitResponse protobuf message which must be freed via tmprl_bytes_free.
 */
typedef void (*tmprl_core_init_callback)(void *user_data, struct tmprl_core_t *core, const struct tmprl_bytes_t *resp);

/**
 * Callback called on function completion. The first parameter of the callback
 * is user data passed into the original function. The second parameter of the
 * callback is a never-null byte array for a response protobuf message which
 * must be freed via tmprl_bytes_free.
 */
typedef void (*tmprl_callback)(void *user_data, const struct tmprl_bytes_t *core);

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
 * req_proto_len represent a byte array for a InitRequest protobuf message. The
 * callback is invoked on completion.
 */
void tmprl_core_init(struct tmprl_runtime_t *runtime,
                     const uint8_t *req_proto,
                     size_t req_proto_len,
                     void *user_data,
                     tmprl_core_init_callback callback);

/**
 * Shutdown and free a core instance.
 *
 * The req_proto and req_proto_len represent a byte array for a ShutdownRequest
 * protobuf message. The callback is invoked on completion with a
 * ShutdownResponse protobuf message.
 */
void tmprl_core_shutdown(struct tmprl_core_t *core,
                         const uint8_t *req_proto,
                         size_t req_proto_len,
                         void *user_data,
                         tmprl_callback callback);

/**
 * Register a worker.
 *
 * The req_proto and req_proto_len represent a byte array for a RegisterWorker
 * protobuf message. The callback is invoked on completion with a
 * RegisterWorkerResponse protobuf message.
 */
void tmprl_register_worker(struct tmprl_core_t *core,
                           const uint8_t *req_proto,
                           size_t req_proto_len,
                           void *user_data,
                           tmprl_callback callback);

/**
 * Shutdown registered worker.
 *
 * The req_proto and req_proto_len represent a byte array for a
 * ShutdownWorkerRequest protobuf message. The callback is invoked on
 * completion with a ShutdownWorkerResponse protobuf message.
 */
void tmprl_shutdown_worker(struct tmprl_core_t *core,
                           const uint8_t *req_proto,
                           size_t req_proto_len,
                           void *user_data,
                           tmprl_callback callback);

/**
 * Poll workflow activation.
 *
 * The req_proto and req_proto_len represent a byte array for a
 * PollWorkflowActivationRequest protobuf message. The callback is invoked on
 * completion with a PollWorkflowActivationResponse protobuf message.
 */
void tmprl_poll_workflow_activation(struct tmprl_core_t *core,
                                    const uint8_t *req_proto,
                                    size_t req_proto_len,
                                    void *user_data,
                                    tmprl_callback callback);

/**
 * Poll activity task.
 *
 * The req_proto and req_proto_len represent a byte array for a
 * PollActivityTaskRequest protobuf message. The callback is invoked on
 * completion with a PollActivityTaskResponse protobuf message.
 */
void tmprl_poll_activity_task(struct tmprl_core_t *core,
                              const uint8_t *req_proto,
                              size_t req_proto_len,
                              void *user_data,
                              tmprl_callback callback);

/**
 * Complete workflow activation.
 *
 * The req_proto and req_proto_len represent a byte array for a
 * CompleteWorkflowActivationRequest protobuf message. The callback is invoked
 * on completion with a CompleteWorkflowActivationResponse protobuf message.
 */
void tmprl_complete_workflow_activation(struct tmprl_core_t *core,
                                        const uint8_t *req_proto,
                                        size_t req_proto_len,
                                        void *user_data,
                                        tmprl_callback callback);

/**
 * Complete activity task.
 *
 * The req_proto and req_proto_len represent a byte array for a
 * CompleteActivityTaskRequest protobuf message. The callback is invoked
 * on completion with a CompleteActivityTaskResponse protobuf message.
 */
void tmprl_complete_activity_task(struct tmprl_core_t *core,
                                  const uint8_t *req_proto,
                                  size_t req_proto_len,
                                  void *user_data,
                                  tmprl_callback callback);

/**
 * Record activity heartbeat.
 *
 * The req_proto and req_proto_len represent a byte array for a
 * RecordActivityHeartbeatRequest protobuf message. The callback is invoked
 * on completion with a RecordActivityHeartbeatResponse protobuf message.
 */
void tmprl_record_activity_heartbeat(struct tmprl_core_t *core,
                                     const uint8_t *req_proto,
                                     size_t req_proto_len,
                                     void *user_data,
                                     tmprl_callback callback);

/**
 * Request workflow eviction.
 *
 * The req_proto and req_proto_len represent a byte array for a
 * RequestWorkflowEvictionRequest protobuf message. The callback is invoked
 * on completion with a RequestWorkflowEvictionResponse protobuf message.
 */
void tmprl_request_workflow_eviction(struct tmprl_core_t *core,
                                     const uint8_t *req_proto,
                                     size_t req_proto_len,
                                     void *user_data,
                                     tmprl_callback callback);

/**
 * Fetch buffered logs.
 *
 * The req_proto and req_proto_len represent a byte array for a
 * FetchBufferedLogsRequest protobuf message. The callback is invoked
 * on completion with a FetchBufferedLogsResponse protobuf message.
 */
void tmprl_fetch_buffered_logs(struct tmprl_core_t *core,
                               const uint8_t *req_proto,
                               size_t req_proto_len,
                               void *user_data,
                               tmprl_callback callback);
