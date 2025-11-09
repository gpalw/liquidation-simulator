// src/app.module.ts

import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ConfigModule } from '@nestjs/config';
import { LiquidationModule } from './modules/liquidation/liquidation.module';
import { JobLog } from './modules/liquidation/entities/job-log.entity';
import { TaskLog } from './modules/liquidation/entities/task-log.entity';
import { RedisModule } from './modules/redis/redis.module';
import { AuthModule } from './modules/auth/auth.module';
import { ServeStaticModule } from '@nestjs/serve-static';
import { join } from 'path';

@Module({
  imports: [
    // 1. 导入配置模块 (用于读取 .env 文件等)
    ConfigModule.forRoot({
      isGlobal: true, // 让配置在全局可用
      envFilePath: '.env',
    }),

    ServeStaticModule.forRoot({
      rootPath: join(__dirname, '..', 'public'),
    }),

    // 2. 配置 TypeORM (数据库连接)
    TypeOrmModule.forRoot({
      type: 'postgres',
      host: process.env.DB_HOST,
      port: Number(process.env.DB_PORT),
      username: process.env.DB_USERNAME,
      password: process.env.DB_PASSWORD,
      database: process.env.DB_DATABASE,
      entities: [JobLog, TaskLog],
      synchronize: true,
    }),

    // 3. 导入你的新模块
    LiquidationModule,
    RedisModule,
    AuthModule,

    // ... (我们稍后会在这里添加 Kafka 和 WebSocket 模块)
  ],
  controllers: [], // 我们这个 Demo 的 API 稍后会加在 LiquidationModule 里
  providers: [],
})
export class AppModule { }