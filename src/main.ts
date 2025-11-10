import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import * as express from 'express';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  app.enableCors();

  // 让后端同时接受 /api/* 和 /*（把 /api 去掉再继续）
  app.use((req: express.Request, _res: express.Response, next: express.NextFunction) => {
    if (req.url.startsWith('/api/')) {
      req.url = req.url.replace(/^\/api/, '');
    }
    next();
  });

  await app.startAllMicroservices();

  await app.listen(Number(process.env.PORT) || 3000);
  console.log(`Application is running on: ${await app.getUrl()}`);
}

bootstrap();
