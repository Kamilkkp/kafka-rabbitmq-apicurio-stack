import 'reflect-metadata';

import { NestFactory } from '@nestjs/core';

import { AppModule } from './app.module.js';
import { kafkaReplayOnStart } from './config.js';
import { CdcJobQueue } from './job/cdc-job.queue.js';
import { KafkaCdcRouter } from './kafka/kafka-cdc.router.js';
import { KafkaConsumer } from './kafka/kafka-consumer.js';
import { KafkaReplay } from './kafka/kafka-replay.js';

async function main() {
  const app = await NestFactory.createApplicationContext(AppModule);
  const sources = app.get(KafkaCdcRouter).sources();
  console.log(
    `CDC consumer sources=${sources.join(', ') || 'none'} kafkaReplay=${kafkaReplayOnStart}`,
  );

  await app.get(CdcJobQueue).start();
  if (kafkaReplayOnStart) {
    await app.get(KafkaReplay).run();
  }
  await app.get(KafkaConsumer).start();

  const stop = async () => {
    await app.close();
    process.exit(0);
  };
  process.once('SIGINT', () => void stop());
  process.once('SIGTERM', () => void stop());

  await new Promise(() => undefined);
}

void main().catch((err: Error) => {
  console.error(err.message);
  process.exit(1);
});
