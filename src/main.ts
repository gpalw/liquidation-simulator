import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { ConfigService } from '@nestjs/config';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  app.enableCors();

  const config = app.get(ConfigService);

  // 允许逗号分隔多个 broker，提供默认值，避免 undefined 进入 string[]
  const brokers = (config.get<string>('KAFKA_BROKER') ?? 'localhost:9092')
    .split(',')
    .map(s => s.trim())
    .filter(Boolean);

  if (brokers.length === 0) {
    throw new Error('KAFKA_BROKER is empty. Set KAFKA_BROKER in .env');
  }

  // 明确标注为 MicroserviceOptions，避免推断成宽类型
  const kafkaOptions: MicroserviceOptions = {
    transport: Transport.KAFKA,
    options: {
      client: {
        brokers,
        clientId: config.get<string>('KAFKA_CLIENT_ID') ?? 'liquidation-client',
      },
      consumer: {
        groupId: config.get<string>('KAFKA_GROUP_ID') ?? 'liquidation-consumer',
      },
    },
  };

  app.connectMicroservice<MicroserviceOptions>(kafkaOptions);

  await app.startAllMicroservices();
  await app.listen(Number(process.env.PORT) || 3000);
  console.log(`Application is running on: ${await app.getUrl()}`);
}

bootstrap();
