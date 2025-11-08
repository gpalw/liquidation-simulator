// src/modules/liquidation/liquidation.service.ts

import { Injectable, Logger, Inject, Controller } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, EntityManager } from 'typeorm';
import { ClientKafka, MessagePattern, Payload } from '@nestjs/microservices';
import { JobLog, JobStatus } from './entities/job-log.entity';
import { TaskLog, TaskStatus } from './entities/task-log.entity';
import { Redis } from 'ioredis';
import { REDIS_CLIENT } from '../redis/redis.module';

@Controller()
@Injectable()
export class LiquidationService {
    [x: string]: any;
    private readonly logger = new Logger(LiquidationService.name);

    constructor(
        @Inject('KAFKA_SERVICE')
        private readonly kafkaClient: ClientKafka,

        @InjectRepository(JobLog)
        private readonly jobLogRepository: Repository<JobLog>,

        @InjectRepository(TaskLog)
        private readonly taskLogRepository: Repository<TaskLog>,

        // 注入事务管理器
        private readonly entityManager: EntityManager,

        @Inject(REDIS_CLIENT) private readonly redisClient: Redis,
    ) { }

    /**
     * 任务2：创建“总管Worker”
     * 监听 'jobs-topic'，并将作业拆分为N个子任务
     */
    @MessagePattern('jobs-topic')
    async handleJobCreation(@Payload() message: { jobId: string; accountsCount: number }) {
        const { jobId, accountsCount } = message;
        this.logger.log(
            `[总管Worker] 收到 Job ${jobId}, 需处理 ${accountsCount} 个账户`,
        );

        try {
            const tasksToCreate: Partial<TaskLog>[] = [];
            for (let i = 0; i < accountsCount; i++) {
                tasksToCreate.push({
                    job_id: jobId,
                    status: TaskStatus.PENDING,
                    account_id: `account_${i + 1}`, // 模拟账户ID
                });
            }

            // 1. 在 Task_Log 表中批量创建子任务记录
            // **【已修复 坑#2】**: 我们直接用 save() 的返回值，而不是再查一次
            let createdTasks: TaskLog[] = [];
            await this.entityManager.transaction(
                async (transactionalEntityManager) => {
                    createdTasks = await transactionalEntityManager.save(TaskLog, tasksToCreate, {
                        chunk: 1000, // 每1000条执行一次插入
                    });
                },
            );
            this.logger.log(`[总管Worker] 已为 Job ${jobId} 批量创建 ${createdTasks.length} 条子任务`);


            // 2. 向 Kafka 'tasks-topic' 扇出 N 条子任务消息
            this.logger.log(`[总管Worker] 开始向 'tasks-topic' 扇出 ${createdTasks.length} 条消息...`);
            for (const task of createdTasks) {
                this.kafkaClient.emit('tasks-topic', {
                    taskId: task.task_id,
                    jobId: task.job_id,
                    accountId: task.account_id,
                });
            }

            // 3. 循环结束后，更新 Job_Log 状态为 processing
            await this.jobLogRepository.update(
                { job_id: jobId },
                { status: JobStatus.PROCESSING }, // 使用我们定义的枚举
            );
            this.logger.log(`[总管Worker] Job ${jobId} 状态更新为 processing`);

        } catch (error) {
            this.logger.error(`[总管Worker] 处理 Job ${jobId} 失败:`, error);
            await this.jobLogRepository.update({ job_id: jobId }, { status: JobStatus.FAILED });
        }
    }

    /**
     * 任务3：创建“清算Worker”
     * 并发处理 'tasks-topic' 中的子任务
     */
    @MessagePattern('tasks-topic')
    async handleTaskProcessing(@Payload() message: { taskId: string; jobId: string; accountId: string }) {
        const { taskId, jobId } = message;
        const workerId = process.env.pm_id || '0';
        this.logger.log(`[清算Worker] 开始处理 Task ${taskId} (Job: ${jobId})`);

        try {
            // 1. 模拟延迟
            await new Promise(res => setTimeout(res, 500 + Math.random() * 1000));

            // 2. 更新 Task_Log 状态为 success
            await this.taskLogRepository.update({ task_id: taskId }, { status: TaskStatus.SUCCESS });
            this.logger.log(`[清算Worker] Task ${taskId} 状态更新为 success`);

            // 3. (关键) 原子地更新 Job_Log 表中的 processed_accounts 计数器
            await this.jobLogRepository.increment(
                { job_id: jobId },
                'processed_accounts',
                1,
            );

            // **【已修复 坑#3】**: 移除了有 Bug 的“完成检查”逻辑。
            // 检查Job是否完成的逻辑不应该放在这里，
            const job = await this.jobLogRepository.findOneBy({ job_id: jobId });
            if (!job) {
                this.logger.error(`[清算Worker]停止执行: 找不到 Job ${jobId} 来更新进度。`);
                return;
            }
            const processedCount = job.processed_accounts;
            const totalCount = job.total_accounts;
            /*if (processedCount >= totalCount) {
                this.logger.log(`[清算Worker] Job ${jobId} 已超过总数，退出任务!`);
                return;
            }*/

            this.logger.log(`[清算Worker ${workerId}] Task ${taskId} 处理完毕, Job ${jobId} 计数器 +1`);

            // 加入 Redis 广播逻辑
            const channel = `job-progress:${jobId}`;
            const payload = JSON.stringify({
                jobId: jobId,
                processed: processedCount,
                total: totalCount,
                workerId: workerId,
            });
            await this.redisClient.publish(channel, payload);
            this.logger.log(`[清算Worker] 已将进度广播到 Redis 频道: ${channel}`);

            if (processedCount === totalCount) {
                this.logger.log(`[清算Worker] Job ${jobId} 已全部完成!`);
                await this.jobLogRepository.update({ job_id: jobId }, { status: JobStatus.COMPLETED });
            }

        } catch (error) {
            this.logger.error(`[清算Worker] 处理 Task ${taskId} 失败:`, error);
            await this.taskLogRepository.update({ task_id: taskId }, { status: TaskStatus.FAILED });
            await this.jobLogRepository.update({ job_id: jobId }, { status: JobStatus.FAILED }); // 或 partial_failed
        }
    }
}