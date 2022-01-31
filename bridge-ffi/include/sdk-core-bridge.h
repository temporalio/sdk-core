#pragma once

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

/**
 * A client instance owned by Core. This must be passed to [tmprl_client_free]
 * when no longer in use which will free the resources.
 */
typedef struct tmprl_client_t tmprl_client_t;

/**
 * A runtime owned by Core. This must be passed to tmprl_runtime_free when no
 * longer in use. This must not be freed until every call to every tmprl_core_t
 * instance created with this runtime has been shutdown.
 */
typedef struct tmprl_runtime_t tmprl_runtime_t;

/**
 * A worker instance owned by Core. This must be passed to tmprl_core_shutdown
 * when no longer in use which will free the resources.
 */
typedef struct tmprl_worker_t tmprl_worker_t;

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
 * Callback called by [tmprl_worker_init] on completion. The first parameter of the
 * callback is user data passed into the original function. The second
 * parameter is a worker instance if the call is successful or null if not. If
 * present, the worker instance must be freed via [tmprl_worker_shutdown] when no
 * longer in use. The third parameter of the callback is a byte array for a
 * [InitResponse] protobuf message which must be freed via [tmprl_bytes_free].
 */
typedef void (*tmprl_worker_init_callback)(void *user_data, struct tmprl_worker_t *core, const struct tmprl_bytes_t *resp);

/**
 * Callback called by [tmprl_client_init] on completion. The first parameter of the
 * callback is user data passed into the original function. The second
 * parameter is a client instance if the call is successful or null if not. If
 * present, the client instance must be freed via [tmprl_client_free] when no
 * longer in use. The third parameter of the callback is a byte array for a
 * [InitResponse] protobuf message which must be freed via [tmprl_bytes_free].
 */
typedef void (*tmprl_client_init_callback)(void *user_data, struct tmprl_client_t *core, const struct tmprl_bytes_t *resp);

/**
 * Callback called on function completion. The first parameter of the callback
 * is user data passed into the original function. The second parameter of the
 * callback is a never-null byte array for a response protobuf message which
 * must be freed via [tmprl_bytes_free].
 */
typedef void (*tmprl_callback)(void *user_data, const struct tmprl_bytes_t *core);

/**
 * Free a set of bytes. The first parameter can be null in cases where a
 * tmprl_core_t instance isn't available. If the second parameter is null, this
 * is a no-op.
 */
void tmprl_bytes_free(struct tmprl_worker_t *core, const struct tmprl_bytes_t *bytes);

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
 * `runtime` and `client` are both required and must outlive this instance.
 * `req_proto` and `req_proto_len` represent a byte array for a [CreateWorkerRequest] protobuf
 * message.
 * The callback is invoked on completion.
 */
void tmprl_worker_init(struct tmprl_runtime_t *runtime,
                       struct tmprl_client_t *client,
                       const uint8_t *req_proto,
                       size_t req_proto_len,
                       void *user_data,
                       tmprl_worker_init_callback callback);

/**
 * Initialize process-wide telemetry. Should only be called once, subsequent calls will be ignored
 * by core.
 *
 * Unlike the other functions in this bridge, this blocks until initting is complete, as telemetry
 * should typically be initialized before doing other work.
 *
 * Returns a byte array for a [InitResponse] protobuf message which must be freed via
 * tmprl_bytes_free.
 */
const struct tmprl_bytes_t *tmprl_telemetry_init(const uint8_t *req_proto, size_t req_proto_len);

/**
 * Initialize a client connection to the Temporal service.
 *
 * The runtime is required and must outlive this instance. The `req_proto` and `req_proto_len`
 * represent a byte array for a [CreateGatewayRequest] protobuf message. The callback is invoked on
 * completion.
 */
void tmprl_client_init(struct tmprl_runtime_t *runtime,
                       const uint8_t *req_proto,
                       size_t req_proto_len,
                       void *user_data,
                       tmprl_client_init_callback callback);

/**
 * Shutdown a previously created worker.
 *
 * The req_proto and req_proto_len represent a byte array for a [bridge::ShutdownWorkerRequest]
 * protobuf message, which currently contains nothing and is unused, but the parameters are kept
 * for now.
 *
 * The callback is invoked on completion with a ShutdownWorkerResponse protobuf message.
 */
void tmprl_shutdown_worker(struct tmprl_worker_t *worker,
                           const uint8_t *req_proto,
                           size_t req_proto_len,
                           void *user_data,
                           tmprl_callback callback);

/**
 * Poll workflow activation.
 *
 * The req_proto and req_proto_len represent a byte array for a PollWorkflowActivationRequest
 * protobuf message, which currently contains nothing and is unused, but the parameters are kept
 * for now.
 *
 * The callback is invoked on completion with a PollWorkflowActivationResponse protobuf message.
 */
void tmprl_poll_workflow_activation(struct tmprl_worker_t *worker,
                                    const uint8_t *req_proto,
                                    size_t req_proto_len,
                                    void *user_data,
                                    tmprl_callback callback);

/**
 * Poll activity task.
 *
 * The req_proto and req_proto_len represent a byte array for a PollActivityTaskRequest protobuf
 * message, which currently contains nothing and is unused, but the parameters are kept for now.
 *
 * The callback is invoked on completion with a PollActivityTaskResponse protobuf message.
 */
void tmprl_poll_activity_task(struct tmprl_worker_t *core,
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
void tmprl_complete_workflow_activation(struct tmprl_worker_t *core,
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
void tmprl_complete_activity_task(struct tmprl_worker_t *core,
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
void tmprl_record_activity_heartbeat(struct tmprl_worker_t *core,
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
void tmprl_request_workflow_eviction(struct tmprl_worker_t *core,
                                     const uint8_t *req_proto,
                                     size_t req_proto_len,
                                     void *user_data,
                                     tmprl_callback callback);

/**
 * Fetch buffered logs. Blocks until complete. This is still using the callback since we might
 * reasonably change log fetching to be async in the future.
 *
 * The req_proto and req_proto_len represent a byte array for a FetchBufferedLogsRequest protobuf
 * message. The callback is invoked on completion with a FetchBufferedLogsResponse protobuf
 * message.
 */
void tmprl_fetch_buffered_logs(const uint8_t *req_proto,
                               size_t req_proto_len,
                               void *user_data,
                               tmprl_callback callback);
