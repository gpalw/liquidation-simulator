// src/modules/redis/redis.module.ts

import { Module, Global } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { Redis } from 'ioredis';

export const REDIS_CLIENT = 'REDIS_CLIENT';

@Global() // <-- 1. 设置为全局模块
@Module({
    imports: [ConfigModule], // <-- 2. 导入配置模块
    providers: [
        {
            provide: REDIS_CLIENT, // <-- 3. 我们提供一个令牌叫 'REDIS_CLIENT'
            inject: [ConfigService],
            useFactory: (configService: ConfigService) => {
                // 4. 从 .env 文件读取配置
                const host = configService.get<string>('REDIS_HOST');
                const port = configService.get<number>('REDIS_PORT');

                // 5. 创建并返回一个新的 Redis 客户端实例
                return new Redis({
                    host: host,
                    port: port,
                });
            },
        },
    ],
    exports: [REDIS_CLIENT], // <-- 6. 导出这个令牌，以便其他模块可以注入
})
export class RedisModule { }