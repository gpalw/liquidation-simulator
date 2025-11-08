// src/modules/liquidation/liquidation.controller.ts

import { Controller, Post, Body, Inject, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { ClientKafka } from '@nestjs/microservices';
import { JobLog, JobStatus } from './entities/job-log.entity';

// 定义 DTO (数据传输对象)，确保传入的数据格式正确
class CreateJobDto {
    accounts_count: number;
    user_id: string;
}

@Controller('liquidation')
export class LiquidationController implements OnModuleDestroy, OnModuleInit {
    private readonly logger = new Logger(LiquidationController.name);

    constructor(
        @Inject('KAFKA_SERVICE')
        private readonly kafkaClient: ClientKafka,

        @InjectRepository(JobLog)
        private readonly jobLogRepository: Repository<JobLog>,

        // 我们把 TaskLog 和 EntityManager 移到了 Service 中
    ) { }
    async onModuleInit() {
        this.kafkaClient.subscribeToResponseOf('jobs-topic');
        this.kafkaClient.subscribeToResponseOf('tasks-topic');
        this.logger.log('[API] 正在连接到 Kafka 生产者...');
        await this.kafkaClient.connect();
        this.logger.log('[API] Kafka 生产者已连接。');
    }

    /**
     * 任务1：创建 API 控制器 (POST /job)
     * 接收Web请求，创建总作业，并向 'jobs-topic' 发送消息
     */
    @Post('job')
    async createJob(@Body() createJobDto: CreateJobDto) {
        this.logger.log(
            `[API] 接收到创建作业请求: ${JSON.stringify(createJobDto)}`,
        );

        // 1. 在 Job_Log 表中创建一条新记录
        const newJob = this.jobLogRepository.create({
            user_id: createJobDto.user_id,
            total_accounts: createJobDto.accounts_count,
            processed_accounts: 0,
            status: JobStatus.PENDING, // 使用枚举
        });
        await this.jobLogRepository.save(newJob);

        this.logger.log(`[API] Job ${newJob.job_id} 已创建, 状态: pending`);

        // 2. 向 Kafka 'jobs-topic' 发送消息
        this.kafkaClient.emit('jobs-topic', {
            jobId: newJob.job_id,
            accountsCount: createJobDto.accounts_count,
        });

        return {
            message: '作业已启动',
            jobId: newJob.job_id,
        };
    }

    /**
     * (确保Kafka客户端在模块销w毁时断开连接)
     */
    async onModuleDestroy() {
        await this.kafkaClient.close();
    }
}