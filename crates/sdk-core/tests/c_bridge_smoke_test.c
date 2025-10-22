#include "temporal-sdk-core-c-bridge.h"
#include <stdio.h>

int main(void) {
    // Just do something simple to confirm the bridge works
    struct TemporalCoreCancellationToken *tok = temporal_core_cancellation_token_new();
    temporal_core_cancellation_token_free(tok);
    printf("C bridge smoke test passed!\n");
    return 0;
}
