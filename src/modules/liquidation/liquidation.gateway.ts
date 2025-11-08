// src/modules/liquidation/liquidation.gateway.ts

import {
    WebSocketGateway,
    SubscribeMessage,
    MessageBody,
    WebSocketServer,
    OnGatewayInit,
    OnGatewayConnection,
    OnGatewayDisconnect,
} from '@nestjs/websockets';
import { Server, Socket } from 'socket.io';
import { Logger, Inject, OnModuleInit } from '@nestjs/common';
import { Redis } from 'ioredis';
import { REDIS_CLIENT } from '../redis/redis.module';

// @WebSocketGateway 告诉 Nest.js 这是一个 WebSocket 服务器。
// 我们添加 cors: { origin: '*' } 以便任何前端页面都能连接，方便测试。
@WebSocketGateway({ cors: { origin: '*' } })
export class LiquidationGateway implements OnGatewayInit, OnGatewayConnection, OnGatewayDisconnect, OnModuleInit {

    // 拿到 WebSocket 服务器的实例，以便我们广播消息
    @WebSocketServer()
    server: Server;

    private readonly logger = new Logger(LiquidationGateway.name);

    // 1. 创建一个新的 Redis 客户端专门用于“订阅”
    // (我们不能和“发布者”共用同一个客户端)
    private readonly subscriberClient: Redis;

    constructor(
        @Inject(REDIS_CLIENT) private readonly redisClient: Redis,
    ) {
        // 复制一个客户端连接，专门用于订阅
        this.subscriberClient = this.redisClient.duplicate();
    }

    // 2. 在模块初始化时，开始订阅 Redis
    async onModuleInit() {
        this.logger.log('WebSocket 网关初始化...');

        // 订阅所有以 'job-progress:' 开头的频道
        // (这使用了 'psubscribe'，即“模式订阅”)
        await this.subscriberClient.psubscribe('job-progress:*');
        this.logger.log("已订阅 Redis 频道 'job-progress:*'");

        // 3. 监听从 Redis 收到的消息
        this.subscriberClient.on('pmessage', (pattern, channel, message) => {
            // channel 是 'job-progress:jobId'
            // message 是 '{"jobId":..., "processed":..., "total":...}'

            this.logger.log(`[Redis -> WS] 收到频道 ${channel} 的消息: ${message}`);

            // 4. 将消息原封不动地转发给前端
            // 我们使用频道名作为 "事件" 名称，前端可以监听这个事件
            this.server.emit(channel, JSON.parse(message));
        });
    }

    // --- (以下是标准的 WebSocket 连接日志) ---

    afterInit(server: Server) {
        this.logger.log('WebSocket 服务器已启动');
    }

    handleConnection(client: Socket) {
        this.logger.log(`前端客户端已连接: ${client.id}`);
    }

    handleDisconnect(client: Socket) {
        this.logger.log(`前端客户端已断开: ${client.id}`);
    }

    // (可选) 允许前端主动订阅某个 Job
    @SubscribeMessage('subscribeToJob')
    handleMessage(@MessageBody() data: { jobId: string }): void {
        this.logger.log(`前端请求订阅 Job: ${data.jobId}`);
        // 可以在这里做一些逻辑，比如把这个 client.id 加入一个 "房间"
        // 但对于我们的 PoC，我们是全局广播，所以这个函数暂时是空的
    }
}