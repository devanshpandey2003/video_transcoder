import { SQSClient, ReceiveMessageCommand } from "@aws-sdk/client-sqs";
import dotenv from "dotenv";
import type { S3Event } from "aws-lambda";

dotenv.config();

const client = new SQSClient({
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID || "",
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || "",
  },
});

async function init() {
  const command = new ReceiveMessageCommand({
    QueueUrl: process.env.SQS_QUEUE_URL,
    MaxNumberOfMessages: 1,
    WaitTimeSeconds: 20,
  });

  try {
    while (true) {
      const response = await client.send(command);
      if (!response.Messages || response.Messages.length === 0) {
        console.log("No messages received");
        continue;
      }

      for (const message of response.Messages) {
        const { MessageId, Body } = message;
        console.log(`Received message: ${MessageId} - ${Body}`);
        // validate  and parse the event

        const event = JSON.parse(Body || "{}") as S3Event;

        if ("Serice" in event && "Event" in message) {
          if (message.Event === "s3:TestEvent") {
            continue;
          }
        }

        //Spin the docker container with ffmpeg and process the video

        for (const record of event.Records) {
          const { s3 } = record;
          const {
            bucket,
            object: { key },
          } = s3;
        }
        //dete
      }
    }
  } catch (err) {}
}
init();
