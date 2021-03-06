# AWS CloudWatch EMF Exporter for OpenTelemetry Collector

This exporter converts OpenTelemetry metrics to 
[AWS CloudWatch Embedded Metric Format(EMF)](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch_Embedded_Metric_Format_Specification.html)
and then sends them directly to CloudWatch Logs using the 
[PutLogEvents](https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_PutLogEvents.html) API.

## Data Conversion
Convert OpenTelemetry ```Int64DataPoints```, ```DoubleDataPoints```, ```SummaryDataPoints``` metrics datapoints into CloudWatch ```EMF``` structured log formats and send it to CloudWatch. Logs and Metrics will be displayed in CloudWatch console.

## Exporter Configuration

The following exporter configuration parameters are supported.

| Name              | Description                                                            | Default |
| :---------------- | :--------------------------------------------------------------------- | ------- |
| `log_group_name`  | Customized log group name                                              |"/metrics/default"|
| `log_stream_name` | Customized log stream name                                             |"otel-stream"|
| `namespace`       | Customized CloudWatch metrics namespace                                | "default" |
| `endpoint`        | Optionally override the default CloudWatch service endpoint.           |         |
| `no_verify_ssl`   | Enable or disable TLS certificate verification.                        | false   |
| `proxy_address`   | Upload Structured Logs to AWS CloudWatch through a proxy.              |         |
| `region`          | Send Structured Logs to AWS CloudWatch in a specific region. If this field is not present in config, environment variable "AWS_REGION" can then be used to set region.| determined by metadata |
| `role_arn`        | IAM role to upload segments to a different account.                    |         |
| `max_retries`     | Maximum number of retries before abandoning an attempt to post data.   |    1    |
| `dimension_rollup_option`| DimensionRollupOption is the option for metrics dimension rollup. Three options are available. |"ZeroAndSingleDimensionRollup" (Enable both zero dimension rollup and single dimension rollup)| 
| `resource_to_telemetry_conversion` | "resource_to_telemetry_conversion" is the option for converting resource attributes to telemetry attributes. It has only one config onption- `enabled`. For metrics, if `enabled=true`, all the resource attributes will be converted to metric labels by default. See `Resource Attributes to Metric Labels` section below for examples. | `enabled=false` | 
| [`metric_declarations`](#metric_declaration) | List of rules for filtering exported metrics and their dimensions. |    [ ]   |

### <metric_declaration>
A metric_declaration section characterizes a rule to be used to set dimensions for exported metrics, filtered by the incoming metrics' metric names.
| Name              | Description                                                            | Default |
| :---------------- | :--------------------------------------------------------------------- | ------- |
| `dimensions`      | List of dimension sets to be exported.                                 |  [[ ]]   |
| `metric_name_selectors` | List of regex strings to filter metric names by.                 |   [ ]    |


## AWS Credential Configuration

This exporter follows default credential resolution for the 
[aws-sdk-go](https://docs.aws.amazon.com/sdk-for-go/api/index.html).

Follow the [guidelines](https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html) for the 
credential configuration.


## Configuration Examples


### Resource Attributes to Metric Labels
`resource_to_telemetry_conversion`  option can be enabled to convert all the resource attributes to metric labels. By default, this option is disabled. Users need to set `enabled=true` to opt-in. See the config example below.

```yaml
exporters:
    awsemf:
        region: 'us-west-2'
        resource_to_telemetry_conversion:
            enabled: true
```
