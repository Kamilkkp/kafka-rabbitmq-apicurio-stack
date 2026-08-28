import 'reflect-metadata';

import { NestFactory } from '@nestjs/core';

import { AppModule } from './app.module.js';
import { kafkaReplayOnStart } from './config.js';
import { KafkaCdcRouter } from './kafka/kafka-cdc.router.js';
import { KafkaReplay } from './kafka/kafka-replay.js';
import { ReplayGate } from './kafka/replay-gate.js';

async function main() {
  const app = await NestFactory.createApplicationContext(AppModule);
  const sources = app.get(KafkaCdcRouter).sources();
  console.log(
    `CDC consumer sources=${sources.join(', ') || 'none'} kafkaReplay=${kafkaReplayOnStart}`,
  );

  if (kafkaReplayOnStart) {
    await app.get(KafkaReplay).run();
    app.get(ReplayGate).finish();
  }

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
