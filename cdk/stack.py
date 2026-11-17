"""
INFO7375 Week 8 - Stream Processing CDK Stack

Topology:
  POST /events  ->  API Gateway  ->  EventProducer Lambda  ->  Kinesis: clickstream
                                                                   |
                                                    Kinesis Event Source (Lambda)
                                                                   |
                                                                   v
                                                        WindowAggregator Lambda
                                                          (tumbling window: 60s)
                                                                   |
                                            --------------------------------------------
                                            v                                            v
                                  DynamoDB: window_aggregates              SQS: late-events
                                  (PK=window_start, SK=metric)                    |
                                                                                   v
                                                                       LateDataHandler Lambda
                                                                       (counts late events into a
                                                                        separate late-aggregates table)

KEY DESIGN DECISIONS:
  - Tumbling windows: 60-second, non-overlapping. Simplest windowing model -
    deliberately chosen so watermark/lateness concepts are isolated, not
    conflated with sliding-window overlap logic.
  - Event timestamp vs. processing timestamp: every event carries an
    `event_time` (when the click happened) separate from `ingested_at`
    (when Kinesis received it). Windowing is done on event_time.
  - Watermark: WindowAggregator tracks the maximum event_time seen so far.
    A window is considered "closed" once the watermark passes
    window_end + GRACE_PERIOD_SECONDS. Events arriving for a closed window
    go to the late-events queue instead of updating the closed aggregate.
  - Kinesis shard count = 2: enough to demonstrate that each shard
    processes its own sequence independently (no global ordering across
    shards), which is WHY watermarks are needed in the first place.
"""
import aws_cdk as cdk
from aws_cdk import (
    aws_kinesis as kinesis,
    aws_lambda as lambda_,
    aws_lambda_event_sources as lambda_events,
    aws_dynamodb as dynamodb,
    aws_sqs as sqs,
    aws_apigateway as apigw,
    aws_cloudwatch as cw,
    aws_cloudwatch_actions as cw_actions,
    aws_sns as sns,
    aws_logs as logs,
)
from constructs import Construct


class StreamProcessingStack(cdk.Stack):
    def __init__(self, scope: Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        # -- Kinesis Data Stream --------------------------------------------
        # 2 shards: enough to show per-shard ordering without global ordering.
        # Each shard supports 1 MB/s or 1000 records/s write.
        clickstream = kinesis.Stream(
            self, "Clickstream",
            stream_name="week8-clickstream",
            shard_count=2,
            retention_period=cdk.Duration.hours(24),
            # CDK keeps a Kinesis stream when the stack is destroyed unless told otherwise.
            # Without this line, "cdk destroy" leaves the stream running (and billing by the
            # shard-hour) and the next deploy fails on the fixed stream name.
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        # -- DynamoDB: window aggregates (the "happy path" sink) -------------
        # PK = window_start (ISO string, truncated to the minute)
        # SK = metric_name (e.g. "page_views", "unique_users", "click_count")
        window_table = dynamodb.Table(
            self, "WindowAggregatesTable",
            table_name="week8-window-aggregates",
            partition_key=dynamodb.Attribute(
                name="window_start", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="metric_name", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        # -- DynamoDB: late/corrected aggregates (separate partition) -------
        # Late events do NOT silently update window_aggregates after the
        # window closes - that would make a "final" report change after
        # the fact with no audit trail. Instead they land here, and a
        # downstream consumer (or human) decides whether to reconcile.
        late_table = dynamodb.Table(
            self, "LateAggregatesTable",
            table_name="week8-late-aggregates",
            partition_key=dynamodb.Attribute(
                name="window_start", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="metric_name", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        # -- DynamoDB: watermark state ---------------------------------------
        # Tracks the maximum event_time seen per shard. Lambda is stateless
        # between invocations, so the watermark must be persisted externally.
        # PK = shard_id
        watermark_table = dynamodb.Table(
            self, "WatermarkTable",
            table_name="week8-watermarks",
            partition_key=dynamodb.Attribute(
                name="shard_id", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        # -- SQS: late events queue ------------------------------------------
        late_dlq = sqs.Queue(
            self, "LateEventsDlq",
            queue_name="week8-late-events-dlq",
            retention_period=cdk.Duration.days(14),
        )
        late_queue = sqs.Queue(
            self, "LateEventsQueue",
            queue_name="week8-late-events",
            visibility_timeout=cdk.Duration.seconds(60),
            retention_period=cdk.Duration.days(4),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3,
                queue=late_dlq,
            ),
        )

        # -- SQS: records the stream consumer gave up on ---------------------
        # The Kinesis event source mapping retries a failing batch a limited number of
        # times and then DISCARDS the records. Without a destination they vanish with no
        # trace. This queue keeps the details of every discarded batch so a human can
        # inspect and replay them (the messages hold metadata, not the records themselves).
        stream_failures = sqs.Queue(
            self, "StreamFailuresQueue",
            queue_name="week8-stream-failures",
            retention_period=cdk.Duration.days(14),
        )


        quarantine_table = dynamodb.Table(
            self, "EmailQuarantineTable",
            table_name="week8-email-quarantine",
            partition_key=dynamodb.Attribute(
                name="quarantine_id",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )


        # -- DynamoDB: email mailbox storage ----------------------------

        mailbox_table = dynamodb.Table(
            self, "EmailMailboxTable",
            table_name="week8-email-mailboxes",
            partition_key=dynamodb.Attribute(
                name="recipient",
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name="message_id",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        # Supports date-ordered inbox and date-range queries.
        mailbox_table.add_global_secondary_index(
            index_name="RecipientByDate",
            partition_key=dynamodb.Attribute(
                name="recipient",
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name="event_time_id",
                type=dynamodb.AttributeType.STRING,
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )

        # -- Shared environment ----------------------------------------------
        common_env = {
            "CLICKSTREAM_NAME":     clickstream.stream_name,
            "EMAIL_QUARANTINE_TABLE": quarantine_table.table_name,
            "EMAIL_MAILBOX_TABLE": mailbox_table.table_name,
            "WINDOW_TABLE":         window_table.table_name,
            "LATE_TABLE":           late_table.table_name,
            "WATERMARK_TABLE":      watermark_table.table_name,
            "LATE_QUEUE_URL":       late_queue.queue_url,
            "WINDOW_SIZE_SECONDS":  "60",     # tumbling window size
            "GRACE_PERIOD_SECONDS": "30",     # how long after window_end before "closed"
            "LOG_LEVEL":            "INFO",
        }

        # -- Lambda: EventProducer --------------------------------------------
        producer_fn = lambda_.Function(
            self, "EventProducer",
            function_name="week8-event-producer",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset("../lambdas/event_producer"),
            timeout=cdk.Duration.seconds(10),
            memory_size=128,
            logging_format=lambda_.LoggingFormat.JSON,
            environment=common_env,
            log_retention=logs.RetentionDays.ONE_WEEK,
        )
        clickstream.grant_write(producer_fn)
        quarantine_table.grant_write_data(producer_fn)

        # -- Lambda: WindowAggregator -----------------------------------------
        # Triggered by Kinesis. Batch window of 10s OR 100 records, whichever
        # comes first - small enough to keep lab latency observable.
        aggregator_fn = lambda_.Function(
            self, "WindowAggregator",
            function_name="week8-window-aggregator",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset("../lambdas/window_aggregator"),
            timeout=cdk.Duration.seconds(60),
            memory_size=256,
            logging_format=lambda_.LoggingFormat.JSON,
            environment=common_env,
            log_retention=logs.RetentionDays.ONE_WEEK,
        )
        aggregator_fn.add_event_source(
            lambda_events.KinesisEventSource(
                clickstream,
                starting_position=lambda_.StartingPosition.LATEST,
                batch_size=100,
                max_batching_window=cdk.Duration.seconds(10),
                # Parallelization factor > 1 lets multiple Lambda invocations
                # process different batches from the SAME shard concurrently.
                # This is OFF by default (=1) for the lab baseline - students
                # turn it on in Scenario D to observe out-of-order processing.
                parallelization_factor=1,
                # After 2 retries the records are discarded; on_failure keeps a record of
                # what was discarded instead of losing it silently.
                retry_attempts=2,
                bisect_batch_on_error=True,
                on_failure=lambda_events.SqsDlq(stream_failures),
            )
        )
        window_table.grant_read_write_data(aggregator_fn)
        watermark_table.grant_read_write_data(aggregator_fn)
        mailbox_table.grant_read_write_data(aggregator_fn)
        late_queue.grant_send_messages(aggregator_fn)

        # -- Lambda: LateDataHandler ------------------------------------------
        late_handler_fn = lambda_.Function(
            self, "LateDataHandler",
            function_name="week8-late-data-handler",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset("../lambdas/late_data_handler"),
            timeout=cdk.Duration.seconds(29),
            memory_size=128,
            logging_format=lambda_.LoggingFormat.JSON,
            environment=common_env,
            log_retention=logs.RetentionDays.ONE_WEEK,
        )
        late_handler_fn.add_event_source(
            lambda_events.SqsEventSource(
                late_queue,
                batch_size=10,
                report_batch_item_failures=True,
            )
        )
        late_table.grant_read_write_data(late_handler_fn)
        mailbox_table.grant_write_data(late_handler_fn)

        # -- API Gateway ----------------------------------------------------
        api = apigw.RestApi(
            self, "EventsApi",
            rest_api_name="week8-events-api",
            description="INFO7375 Week8 - Clickstream event ingestion",
            deploy_options=apigw.StageOptions(
                stage_name="prod",
                throttling_rate_limit=1000,
                throttling_burst_limit=2000,
                metrics_enabled=True,
            ),
        )
        events_resource = api.root.add_resource("events")
        events_resource.add_method(
            "POST",
            apigw.LambdaIntegration(producer_fn, proxy=True),
        )

        emails_resource = api.root.add_resource("emails")
        emails_resource.add_method(
            "POST",
            apigw.LambdaIntegration(producer_fn, proxy=True),
        )

        health_resource = api.root.add_resource("health")
        health_resource.add_method(
            "GET",
            apigw.MockIntegration(
                integration_responses=[apigw.IntegrationResponse(
                    status_code="200",
                    response_templates={"application/json": '{"status":"ok"}'},
                )],
                request_templates={"application/json": '{"statusCode":200}'},
            ),
            method_responses=[apigw.MethodResponse(status_code="200")],
        )

        # -- CloudWatch Alarms ----------------------------------------------
        alert_topic = sns.Topic(self, "AlertTopic",
                                display_name="INFO7375-Week8-Alerts")

        # Iterator age - THE key stream-processing health metric.
        # Measures how far behind the consumer is from the stream's tip.
        # A healthy consumer has iterator age near 0. Growing iterator age
        # means the consumer cannot keep up with the producer rate.
        cw.Alarm(
            self, "IteratorAgeAlarm",
            alarm_name="Week8-IteratorAge",
            alarm_description="WindowAggregator falling behind the stream - "
                              "iterator age > 60s",
            metric=aggregator_fn.metric(
                metric_name="IteratorAge",
                period=cdk.Duration.minutes(1),
                statistic="Maximum",
            ),
            threshold=60000,    # milliseconds
            evaluation_periods=2,
            comparison_operator=cw.ComparisonOperator.GREATER_THAN_THRESHOLD,
            treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
        ).add_alarm_action(cw_actions.SnsAction(alert_topic))

        # Late events queue depth - non-zero means events are arriving
        # after their window has closed
        cw.Alarm(
            self, "LateEventsAlarm",
            alarm_name="Week8-LateEvents",
            alarm_description="Late events queue depth > 5 - "
                              "events arriving after window closed",
            metric=late_queue.metric_approximate_number_of_messages_visible(
                period=cdk.Duration.minutes(1), statistic="Maximum",
            ),
            threshold=5,
            evaluation_periods=2,
            comparison_operator=cw.ComparisonOperator.GREATER_THAN_THRESHOLD,
            treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
        ).add_alarm_action(cw_actions.SnsAction(alert_topic))

        # Lambda errors - aggregator
        cw.Alarm(
            self, "AggregatorErrorAlarm",
            alarm_name="Week8-Aggregator-Errors",
            alarm_description="WindowAggregator error rate elevated",
            metric=aggregator_fn.metric_errors(
                period=cdk.Duration.minutes(5), statistic="Sum",
            ),
            threshold=5,
            evaluation_periods=2,
            comparison_operator=cw.ComparisonOperator.GREATER_THAN_THRESHOLD,
            treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
        ).add_alarm_action(cw_actions.SnsAction(alert_topic))

        # -- CloudWatch Dashboard -------------------------------------------
        dashboard = cw.Dashboard(
            self, "StreamDashboard",
            dashboard_name="INFO7375-Week8-StreamProcessing",
        )

        dashboard.add_widgets(
            # Row 1: the most important panel - iterator age
            cw.GraphWidget(
                title="Iterator age (ms) - consumer lag behind stream tip",
                left=[aggregator_fn.metric(
                    metric_name="IteratorAge",
                    period=cdk.Duration.minutes(1), statistic="Maximum",
                )],
                width=12,
            ),
            cw.GraphWidget(
                title="Kinesis incoming records per minute",
                left=[cw.Metric(
                    namespace="AWS/Kinesis", metric_name="IncomingRecords",
                    dimensions_map={"StreamName": clickstream.stream_name},
                    period=cdk.Duration.minutes(1), statistic="Sum",
                )],
                width=12,
            ),
            # Row 2: window aggregation health
            cw.GraphWidget(
                title="WindowAggregator invocations / errors",
                left=[
                    aggregator_fn.metric_invocations(
                        period=cdk.Duration.minutes(1), statistic="Sum", label="invocations"),
                    aggregator_fn.metric_errors(
                        period=cdk.Duration.minutes(1), statistic="Sum", label="errors"),
                ],
                width=12,
            ),
            cw.GraphWidget(
                title="Late events queue depth",
                left=[late_queue.metric_approximate_number_of_messages_visible(
                    period=cdk.Duration.minutes(1), statistic="Maximum",
                )],
                width=12,
            ),
            # Row 3: throughput and latency
            cw.GraphWidget(
                title="WindowAggregator duration p99 (ms)",
                left=[aggregator_fn.metric_duration(
                    period=cdk.Duration.minutes(1), statistic="p99",
                )],
                width=12,
            ),
            cw.GraphWidget(
                title="API Gateway request count",
                left=[cw.Metric(
                    namespace="AWS/ApiGateway", metric_name="Count",
                    dimensions_map={"ApiName": "week8-events-api"},
                    period=cdk.Duration.minutes(1), statistic="Sum",
                )],
                width=12,
            ),
            # Row 4: late data handler
            cw.GraphWidget(
                title="LateDataHandler invocations",
                left=[late_handler_fn.metric_invocations(
                    period=cdk.Duration.minutes(1), statistic="Sum",
                )],
                width=12,
            ),
            cw.GraphWidget(
                title="Late events DLQ depth",
                left=[late_dlq.metric_approximate_number_of_messages_visible(
                    period=cdk.Duration.minutes(1), statistic="Maximum",
                )],
                width=12,
            ),
            cw.GraphWidget(
                title="Discarded stream batches (failure queue depth)",
                left=[stream_failures.metric_approximate_number_of_messages_visible(
                    period=cdk.Duration.minutes(1), statistic="Maximum",
                )],
                width=12,
            ),
        )

        # -- Outputs --------------------------------------------------------
        cdk.CfnOutput(self, "ApiEndpoint",
                      value=api.url,
                      description="Base URL - POST /events, GET /health")
        cdk.CfnOutput(self, "ClickstreamName",
                      value=clickstream.stream_name,
                      description="Kinesis stream - 2 shards")
        cdk.CfnOutput(self, "WindowTableName",
                      value=window_table.table_name,
                      description="DynamoDB - closed-window aggregates")
        cdk.CfnOutput(self, "LateTableName",
                      value=late_table.table_name,
                      description="DynamoDB - late/corrected aggregates")
        cdk.CfnOutput(self, "WatermarkTableName",
                      value=watermark_table.table_name,
                      description="DynamoDB - per-shard watermark state")
        cdk.CfnOutput(self, "LateQueueUrl",
                      value=late_queue.queue_url,
                      description="SQS - events arriving after window closed")
        cdk.CfnOutput(self, "LateDlqUrl",
                      value=late_dlq.queue_url,
                      description="SQS DLQ for late events handler")
        cdk.CfnOutput(self, "StreamFailuresQueueUrl",
                      value=stream_failures.queue_url,
                      description="SQS - details of stream batches the consumer discarded")
        cdk.CfnOutput(self, "DashboardUrl",
                      value=f"https://{self.region}.console.aws.amazon.com/cloudwatch/home"
                            f"#dashboards:name=INFO7375-Week8-StreamProcessing",
                      description="CloudWatch dashboard - watch IteratorAge during Break")
