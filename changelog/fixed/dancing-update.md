Execute Update calls, including `Client::execute_update_with_start_workflow`, now request completion in the initial RPC to avoid an unnecessary round trip to retrieve the result.
