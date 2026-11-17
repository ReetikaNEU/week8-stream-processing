#!/usr/bin/env python3
import aws_cdk as cdk
from stack import StreamProcessingStack

app = cdk.App()

StreamProcessingStack(
    app,
    "Week8StreamStack",
    env=cdk.Environment(
        account=app.node.try_get_context("account"),
        region=app.node.try_get_context("region") or "us-east-1",
    ),
    description="INFO7375 Week 8 - Stream Processing Lab",
)

app.synth()
