// src/worker.ts (新文件)

import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { ConfigService } from '@nestjs/config';

async function bootstrapWorker() {
    // 1. 我们用 AppModule 创建一个“微服务”应用
    const app = await NestFactory.createMicroservice<MicroserviceOptions>(
        AppModule,
        {
            // 2. 它只负责 Kafka
            transport: Transport.KAFKA,
            options: {
                client: {
                    brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
                },
                consumer: {
                    groupId: 'liquidation-consumer-server', //
                },
            },
        }
    );

    // 3. 启动这个 Worker
    await app.listen();

    console.log('[Worker] Microservice is listening...');
}
bootstrapWorker();