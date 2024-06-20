# Temporal cloud api proto files (Preview)

> These apis are currently preview only and access is restricted. They are not meant for production use and could change. Please reach out to Temporal support to request preview access.

## How to use

Copy over the protobuf files under [temporal](temporal) directory to the project directory and then use [grpc](https://grpc.io/docs/) to compile and generate code in the desired programming language.

### API Version

The client is expected to pass in a `temporal-cloud-api-version` header with the api version identifier with every request it makes to the apis. The backend will use the version to safely mutate resources. The `temporal:versioning:min_version` label specifies the minimum version of the API that supports the field.

Current Version:
https://github.com/temporalio/api-cloud/blob/main/VERSION#L1C1-L1C14

### URL

The grpc URL the clients should connect to:
```
saas-api.tmprl.cloud:443
```

## Samples

Refer [cloud-samples-go](https://github.com/temporalio/cloud-samples-go) repository for demonstration on how a project can copy and build Go clients.
