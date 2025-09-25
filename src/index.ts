import { SQSClient, ReceiveMessageCommand } from "@aws-sdk/client-sqs";
import { ECSClient, RunTaskCommand } from "@aws-sdk/client-ecs";
import dotenv from "dotenv";
import type { S3Event } from "aws-lambda";

dotenv.config();

const sqsClient = new SQSClient({
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID || "",
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || "",
  },
  region: process.env.AWS_REGION || "us-east-1",
});

const ecsClient = new ECSClient({
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID || "",
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || "",
  },
  region: process.env.AWS_REGION || "us-east-1",
});

async function processVideo(bucketName: string, objectKey: string) {
  console.log(`Processing video from bucket: ${bucketName}, key: ${objectKey}`);

  // Run ECS task to process video in Docker container
  const command = new RunTaskCommand({
    cluster: process.env.ECS_CLUSTER_ARN,
    taskDefinition: process.env.ECS_TASK_DEFINITION_ARN,
    launchType: "FARGATE",
    networkConfiguration: {
      awsvpcConfiguration: {
        subnets: (process.env.ECS_SUBNETS || "").split(","),
        securityGroups: (process.env.ECS_SECURITY_GROUPS || "").split(","),
        assignPublicIp: "ENABLED",
      },
    },
    overrides: {
      containerOverrides: [
        {
          name: process.env.ECS_CONTAINER_NAME,
          environment: [
            { name: "BUCKET_NAME", value: bucketName },
            { name: "OBJECT_KEY", value: objectKey },
          ],
        },
      ],
    },
  });

  try {
    const response = await ecsClient.send(command);
    console.log("ECS task started:", response.tasks?.[0]?.taskArn);
  } catch (err) {
    console.error("Failed to start ECS task:", err);
  }
}

async function init() {
  const command = new ReceiveMessageCommand({
    QueueUrl: process.env.SQS_QUEUE_URL,
    MaxNumberOfMessages: 1,
    WaitTimeSeconds: 20,
  });

  try {
    while (true) {
      const response = await sqsClient.send(command);
      if (!response.Messages || response.Messages.length === 0) {
        console.log("No messages received");
        continue;
      }

      for (const message of response.Messages) {
        const { MessageId, Body } = message;
        console.log(`Received message: ${MessageId} - ${Body}`);

        let event: S3Event;
        try {
          event = JSON.parse(Body || "{}") as S3Event;
        } catch (err) {
          console.error("Failed to parse message body as S3Event", err);
          continue;
        }

        // Skip test events
        if (
          event?.Records?.length === 0 ||
          event.Records[0]?.eventName === "s3:TestEvent"
        ) {
          continue;
        }

        for (const record of event.Records) {
          const { s3 } = record;
          const bucketName = s3.bucket.name;
          const objectKey = s3.object.key;
          await processVideo(bucketName, objectKey);
        }
      }
    }
  } catch (err) {
    console.error("Error in SQS polling loop:", err);
  }
}

init();
