//TODO

// 1) Download the original video
// 2) start the transcoder
// 3) upload the video

const {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
} = require("@aws-sdk/client-s3");
const fs = require("node:fs/promises");
const path = require("node:path");

const ffmpeg = require("fluent-ffmpeg");

import dotenv from "dotenv";

const RESOLUTION = [
  { name: "720p", width: 1280, height: 720 },
  { name: "480p", width: 854, height: 480 },
  { name: "360p", width: 640, height: 360 },
];

dotenv.config();

const s3Client = new S3Client({
  region: "us-east-1",
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});

const BUCKET_NAME = process.env.BUCKET_NAME;
const KEY = process.env.KEY;

async function init() {
  const command = new GetObjectCommand({
    Bucket: BUCKET_NAME,
    Key: KEY,
  });

  const result = await s3Client.send(command);
  const originalFilePath = `videos/original-video.mp4`;

  await fs.writeFile(originalFilePath, result.Body);

  const originalVideoPath = path.resolve(originalFilePath);

  //start transcoding

  const promises = RESOLUTION.map((resolution) => {
    const outputPath = path.resolve(`transcoded/videos-${resolution.name}.mp4`);

    return new Promise((resolve, reject) => {
      ffmpeg(originalVideoPath)
        .output(outputPath)
        .withVideoCodec("libx264")
        .withAudioCodec("aac")
        .withSize(`${resolution.width}x${resolution.height}`)
        .on("end", async () => {
          const putCommand = new PutObjectCommand({
            Bucket: "production-bucket.dev",
            Key: outputPath,
          });
          await s3Client.send(putCommand);
          console.log(`uploaded ${outputPath}`);
          resolve();
        })
        .format("mp4")
        .run();
    });
  });

  await Promise.all(promises);
}

init().finally(() => process.exit(0));
