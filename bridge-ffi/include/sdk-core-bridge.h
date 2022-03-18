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
 * A runtime owned by Core. This must be passed to [tmprl_runtime_free] when no longer in use. This
 * should not be freed until every call to every [tmprl_worker_t] instance created with this
 * runtime has been shutdown. In practice, since the actual runtime is behind an [Arc], it's
 * currently OK, but that's an implementation detail.
 */
typedef struct tmprl_runtime_t tmprl_runtime_t;

/**
 * A worker instance owned by Core. This must be passed to [tmprl_worker_shutdown]
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
typedef void (*tmprl_worker_init_callback)(void *user_data, struct tmprl_worker_t *worker, const struct tmprl_bytes_t *resp);

/**
 * Callback called on function completion. The first parameter of the callback
 * is user data passed into the original function. The second parameter of the
 * callback is a never-null byte array for a response protobuf message which
 * must be freed via [tmprl_bytes_free].
 */
typedef void (*tmprl_callback)(void *user_data, const struct tmprl_bytes_t *core);

/**
 * Callback called by [tmprl_client_init] on completion. The first parameter of the
 * callback is user data passed into the original function. The second
 * parameter is a client instance if the call is successful or null if not. If
 * present, the client instance must be freed via [tmprl_client_free] when no
 * longer in use. The third parameter of the callback is a byte array for a
 * [InitResponse] protobuf message which must be freed via [tmprl_bytes_free].
 */
typedef void (*tmprl_client_init_callback)(void *user_data, struct tmprl_client_t *client, const struct tmprl_bytes_t *resp);

/**
 * Free a set of bytes. The first parameter can be null in cases where a [tmprl_worker_t] instance
 * isn't available. If the second parameter is null, this is a no-op.
 */
void tmprl_bytes_free(struct tmprl_worker_t *worker, const struct tmprl_bytes_t *bytes);

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
 * Create a new worker instance.
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
 * Shutdown and free a previously created worker.
 *
 * The req_proto and req_proto_len represent a byte array for a [bridge::ShutdownWorkerRequest]
 * protobuf message, which currently contains nothing and are unused, but the parameters are kept
 * for now.
 *
 * The callback is invoked on completion with a ShutdownWorkerResponse protobuf message.
 *
 * After the callback has been called, the worker struct will be freed and the pointer will no
 * longer be valid.
 */
void tmprl_worker_shutdown(struct tmprl_worker_t *worker,
                           const uint8_t *req_proto,
                           size_t req_proto_len,
                           void *user_data,
                           tmprl_callback callback);

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
 * represent a byte array for a [CreateClientRequest] protobuf message. The callback is invoked on
 * completion.
 */
void tmprl_client_init(struct tmprl_runtime_t *runtime,
                       const uint8_t *req_proto,
                       size_t req_proto_len,
                       void *user_data,
                       tmprl_client_init_callback callback);

/**
 * Free a previously created client
 */
void tmprl_client_free(struct tmprl_client_t *client);

/**
 * Poll for a workflow activation.
 *
 * The `req_proto` and `req_proto_len` represent a byte array for a
 * [bridge::PollWorkflowActivationRequest] protobuf message, which currently contains nothing and
 * is unused, but the parameters are kept for now.
 *
 * The callback is invoked on completion with a [bridge::PollWorkflowActivationResponse] protobuf
 * message.
 */
void tmprl_poll_workflow_activation(struct tmprl_worker_t *worker,
                                    const uint8_t *req_proto,
                                    size_t req_proto_len,
                                    void *user_data,
                                    tmprl_callback callback);

/**
 * Poll for an activity task.
 *
 * The `req_proto` and `req_proto_len` represent a byte array for a
 * [bridge::PollActivityTaskRequest] protobuf message, which currently contains nothing and is
 * unused, but the parameters are kept for now.
 *
 * The callback is invoked on completion with a [bridge::PollActivityTaskResponse] protobuf
 * message.
 */
void tmprl_poll_activity_task(struct tmprl_worker_t *worker,
                              const uint8_t *req_proto,
                              size_t req_proto_len,
                              void *user_data,
                              tmprl_callback callback);

/**
 * Complete a workflow activation.
 *
 * The `req_proto` and `req_proto_len` represent a byte array for a
 * [bridge::CompleteWorkflowActivationRequest] protobuf message. The callback is invoked on
 * completion with a [bridge::CompleteWorkflowActivationResponse] protobuf message.
 */
void tmprl_complete_workflow_activation(struct tmprl_worker_t *worker,
                                        const uint8_t *req_proto,
                                        size_t req_proto_len,
                                        void *user_data,
                                        tmprl_callback callback);

/**
 * Complete an activity task.
 *
 * The `req_proto` and `req_proto_len` represent a byte array for a
 * [bridge::CompleteActivityTaskRequest] protobuf message. The callback is invoked on completion
 * with a [bridge::CompleteActivityTaskResponse] protobuf message.
 */
void tmprl_complete_activity_task(struct tmprl_worker_t *worker,
                                  const uint8_t *req_proto,
                                  size_t req_proto_len,
                                  void *user_data,
                                  tmprl_callback callback);

/**
 * Record an activity heartbeat.
 *
 * `req_proto` and `req_proto_len` represent a byte array for a
 * [bridge::RecordActivityHeartbeatRequest] protobuf message. The callback is invoked on completion
 * with a RecordActivityHeartbeatResponse protobuf message.
 */
void tmprl_record_activity_heartbeat(struct tmprl_worker_t *worker,
                                     const uint8_t *req_proto,
                                     size_t req_proto_len,
                                     void *user_data,
                                     tmprl_callback callback);

/**
 * Request a workflow eviction.
 *
 * The `req_proto` and `req_proto_len` represent a byte array for a
 * [bridge::RequestWorkflowEvictionRequest] protobuf message. The callback is invoked on completion
 * with a [bridge::RequestWorkflowEvictionResponse] protobuf message.
 */
void tmprl_request_workflow_eviction(struct tmprl_worker_t *worker,
                                     const uint8_t *req_proto,
                                     size_t req_proto_len,
                                     void *user_data,
                                     tmprl_callback callback);

/**
 * Fetch buffered logs. Blocks until complete. This is still using the callback since we might
 * reasonably change log fetching to be async in the future.
 *
 * The `req_proto` and `req_proto_len` represent a byte array for a
 * [bridge::FetchBufferedLogsRequest] protobuf message. The callback is invoked on completion with
 * a [bridge::FetchBufferedLogsResponse] protobuf message.
 */
void tmprl_fetch_buffered_logs(const uint8_t *req_proto,
                               size_t req_proto_len,
                               void *user_data,
                               tmprl_callback callback);
