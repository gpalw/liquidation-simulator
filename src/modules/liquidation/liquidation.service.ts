// src/modules/liquidation/liquidation.service.ts

import { Injectable, Logger, Inject, Controller, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, EntityManager } from 'typeorm';
import { ClientKafka, MessagePattern, Payload } from '@nestjs/microservices';
import { JobLog, JobStatus } from './entities/job-log.entity';
import { TaskLog, TaskStatus } from './entities/task-log.entity';
import { Redis } from 'ioredis';
import { REDIS_CLIENT } from '../redis/redis.module';
import { ConfigService } from '@nestjs/config';

type TaskMessage = { taskId: string; jobId: string; accountId: string, errorRate?: number, retryCount?: number; };

@Controller()
@Injectable()
export class LiquidationService {
    [x: string]: any;
    private readonly logger = new Logger(LiquidationService.name);

    private CONCURRENCY_LIMIT: number;
    private TICK_RATE_MS: number;
    private taskQueue: TaskMessage[] = [];
    private activeTasksCount = 0;
    private taskInterval: NodeJS.Timeout;

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

        private readonly configService: ConfigService,
    ) {
        this.CONCURRENCY_LIMIT = this.configService.get<number>('WORKER_CONCURRENCY', 10);
        this.TICK_RATE_MS = this.configService.get<number>('WORKER_TICK_RATE_MS', 100);
    }

    onModuleInit() {
        this.logger.log('[并发引擎] 启动任务处理循环...');
        // 每 TICK_RATE_MS 毫秒“唤醒”一次，去检查队列
        this.taskInterval = setInterval(() => this._processTaskQueue(), this.TICK_RATE_MS);
    }

    onModuleDestroy() {
        this.logger.log('[并发引擎] 停止任务处理循环。');
        clearInterval(this.taskInterval);
    }

    /**
     * 任务2：创建“总管Worker”
     * 监听 'jobs-topic'，并将作业拆分为N个子任务
     */
    @MessagePattern('jobs-topic')
    async handleJobCreation(@Payload() message: { jobId: string; accountsCount: number; errorRate: number; }) {
        const { jobId, accountsCount, errorRate } = message;
        this.logger.log(
            `[总管Worker] 收到 Job ${jobId}, 账户: ${accountsCount}, 错误率: ${errorRate}%`,
        );

        try {
            const tasksToCreate: Partial<TaskLog>[] = [];
            for (let i = 0; i < accountsCount; i++) {
                tasksToCreate.push({
                    job_id: jobId,
                    status: TaskStatus.PENDING,
                    account_id: `account_${i + 1}`, // 模拟账户ID
                    retry_count: 0,
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
                    errorRate: errorRate,
                    retryCount: task.retry_count,
                });
            }

            await this.redisClient.mset({
                [this.getJobStatusKey(jobId)]: JobStatus.PROCESSING,
                [this.getJobTotalKey(jobId)]: String(createdTasks.length),
                [this.getJobProcessedKey(jobId)]: '0',
                [this.getJobFailedKey(jobId)]: '0',
            });

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
    async handleTaskProcessing(@Payload() message: TaskMessage) {
        this.taskQueue.push(message);
        this.logger.log(`[收件箱] 收到 Task ${message.taskId}，已推入队列 (当前队列: ${this.taskQueue.length})`);
    }

    private _processTaskQueue = () => {
        while (this.activeTasksCount < this.CONCURRENCY_LIMIT && this.taskQueue.length > 0) {
            const taskMessage = this.taskQueue.shift();
            if (!taskMessage) {
                continue;
            }
            this.activeTasksCount++;
            this.logger.log(`[并发引擎] 开始处理 Task ${taskMessage.taskId}。(并发: ${this.activeTasksCount}/${this.CONCURRENCY_LIMIT})`);

            this._processSingleTask(taskMessage)
                .catch(err => {
                    this.logger.error(`[并发引擎] Task ${taskMessage.taskId} 处理失败 (未捕获):`, err);
                })
                .finally(() => {
                    this.activeTasksCount--;
                    // 2. 移除递归调用 (修复“并发冲突”)
                    // this._processTaskQueue(); 
                });
        }
    }





    private _processTaskQueue2() {
        // 这个函数由 setInterval 每 100ms 触发一次

        // 当我们“有空闲额度” 并且“队列里有活儿”时
        while (this.activeTasksCount < this.CONCURRENCY_LIMIT && this.taskQueue.length > 0) {

            // 1. 从队列里拿一个任务
            const taskMessage = this.taskQueue.shift(); // .shift() 从数组头部取出
            if (!taskMessage) {
                // 理论上因为 while 循环 的检查，这里永远不会执行，但这能让 TS 满意
                continue;
            }
            // 2. 占用一个“并发额度”
            this.activeTasksCount++;

            this.logger.log(`[并发引擎] 开始处理 Task ${taskMessage.taskId}。(并发: ${this.activeTasksCount}/${this.CONCURRENCY_LIMIT})`);

            // 3. “开火，然后忘掉” (Fire-and-forget)
            //    我们 *不* `await` 它，这样 `while` 循环才能继续
            //    去启动下一个（直到额度用完）
            this._processSingleTask(taskMessage)
                .catch(err => {
                    // 确保单个任务的崩溃不会弄崩整个引擎
                    this.logger.error(`[并发引擎] Task ${taskMessage.taskId} 处理失败 (未捕获):`, err);
                })
                .finally(() => {
                    // 4. 无论成功还是失败，*必须* 释放“并发额度”
                    this.activeTasksCount--;
                    this.logger.log(`[并发引擎] Task ${taskMessage.taskId} 已完成。(并发: ${this.activeTasksCount}/${this.CONCURRENCY_LIMIT})`);
                    this._processTaskQueue();
                });
        }
    }

    private _processSingleTask = async (message: TaskMessage) => {

        const { taskId, jobId, accountId, errorRate, retryCount } = message; // <-- 修复：现在能正确拿到 errorRate 和 retryCount
        const workerId = process.env.pm_id || '0';

        try {
            // 1. 【"僵尸"检查】走 Redis
            const jobStatus = await this.redisClient.get(this.getJobStatusKey(jobId));
            if (!jobStatus) {
                this.logger.warn(
                    `[清算Worker ${workerId}] Job ${jobId} 在 Redis 中未初始化，Task ${taskId} 延时重试一次...`,
                );
                setTimeout(() => {
                    this.kafkaClient.emit('tasks-topic', message);
                }, 100);
                return;
            }
            if (jobStatus === JobStatus.CANCELLED) {
                this.logger.log(`[清算Worker ${workerId}] 忽略 Task ${taskId}，因为 Job ${jobId} 已被取消 (Redis)。`);
                return;
            }

            // 2. 模拟延迟
            await new Promise(res => setTimeout(res, 500 + Math.random() * 1000));

            // 3. ✨ 模拟随机失败 ✨
            // (修复：现在 errorRate 有定义了)
            if (errorRate && Math.random() < (errorRate / 100)) {
                this.logger.warn(`[清算Worker ${workerId}] Task ${taskId} 触发了“模拟随机失败”！`);
                throw new Error('模拟的随机网络错误');
            }

            // --- 任务成功 ---

            // 4. 更新 Task_Log 状态
            await this.taskLogRepository.update({ task_id: taskId }, { status: TaskStatus.SUCCESS });

            // 5. 更新 Job_Log 计数器 (Redis)
            const processed = await this.redisClient.incr(this.getJobProcessedKey(jobId));
            const total = Number(await this.redisClient.get(this.getJobTotalKey(jobId))) || 0;
            const failed = Number(await this.redisClient.get(this.getJobFailedKey(jobId))) || 0;

            this.logger.log(`[清算Worker ${workerId}] Task ${taskId} 成功, 进度: ${processed}+${failed}/${total}`);

            let durationInMs: number | null = null;
            let finalStatus = JobStatus.PROCESSING;

            // 6. 检查是否全部完成
            if (processed + failed === total && total > 0) {
                const endTime = new Date();
                const jobRecord = await this.jobLogRepository.findOne({ where: { job_id: jobId }, select: ['start_time'] });

                if (jobRecord?.start_time) {
                    durationInMs = endTime.getTime() - new Date(jobRecord.start_time).getTime();
                }

                finalStatus = (failed > 0) ? JobStatus.FAILED : JobStatus.COMPLETED; // <-- 修复：如果失败 > 0，最终状态也是 FAILED

                await this.jobLogRepository.update(
                    { job_id: jobId },
                    {
                        status: finalStatus,
                        end_time: endTime,
                        processed_accounts: processed,
                        failed_accounts: failed,
                    },
                );

                await this.redisClient.set(this.getJobStatusKey(jobId), finalStatus);
                this.logger.log(`[清算Worker ${workerId}] Job ${jobId} 已全部完成! 状态: ${finalStatus}`);
            }

            // 7. 【发布到 Redis】
            await this._publishProgress({
                jobId, workerId, durationInMs, processed, failed, total, status: finalStatus,
            });

        } catch (error) {
            this.logger.error(`[清算Worker ${workerId}] 处理 Task ${taskId} 失败:`, error.message);
            const MAX_RETRIES = 3;

            // 8. 【Bug #1 修复】：现在 retryCount 是正确的数字了
            const currentRetryCount = retryCount || 0;

            if (currentRetryCount < MAX_RETRIES) {
                // 9a. 增加重试次数
                await this.taskLogRepository.increment({ task_id: taskId }, 'retry_count', 1);
                this.logger.log(`[清算Worker ${workerId}] Task ${taskId} 将在第 ${currentRetryCount + 1} 次重试...`);
                // 9b. 把它重新扔回队列
                setTimeout(() => {
                    this.kafkaClient.emit('tasks-topic', { ...message, retryCount: currentRetryCount + 1 });
                }, 1000);

            } else {
                // 9c. 已达最大重试次数，彻底失败
                this.logger.error(`[清算Worker ${workerId}] Task ${taskId} 已达最大重试次数 (${MAX_RETRIES})，标记为 FAILED。`);
                await this.taskLogRepository.update({ task_id: taskId }, { status: TaskStatus.FAILED });

                // 更新 Job 的“失败”计数器
                const failed = await this.redisClient.incr(this.getJobFailedKey(jobId));
                const processed = Number(await this.redisClient.get(this.getJobProcessedKey(jobId))) || 0;
                const total = Number(await this.redisClient.get(this.getJobTotalKey(jobId))) || 0;

                let durationInMs: number | null = null;
                let finalStatus = JobStatus.FAILED; // <-- 最终状态是 FAILED

                if (processed + failed === total && total > 0) {
                    const endTime = new Date();
                    const jobRecord = await this.jobLogRepository.findOne({ where: { job_id: jobId }, select: ['start_time'] });
                    if (jobRecord?.start_time) {
                        durationInMs = endTime.getTime() - new Date(jobRecord.start_time).getTime();
                    }
                    await this.jobLogRepository.update(
                        { job_id: jobId },
                        {
                            status: finalStatus,
                            end_time: endTime,
                            processed_accounts: processed,
                            failed_accounts: failed,
                        },
                    );
                    await this.redisClient.set(this.getJobStatusKey(jobId), finalStatus);
                }

                // 10. 【关键修复】：失败后也要发布进度
                await this._publishProgress({
                    jobId, workerId, durationInMs, processed, failed, total, status: finalStatus,
                });
            }
        }
    }

    private async _processSingleTask2(message: TaskMessage) {

        const { taskId, jobId, accountId, errorRate, retryCount = 0 } = message;
        const workerId = process.env.pm_id || '0';
        try {
            // 1. 【"僵尸"检查】走 Redis
            const jobStatus = await this.redisClient.get(this.getJobStatusKey(jobId));

            if (!jobStatus) {
                // 理论上不应该发生，可以打个严重日志
                this.logger.error(
                    `[清算Worker ${workerId}] Job ${jobId} 在 Redis 中不存在，跳过 Task ${taskId}。`,
                );
                return;
            }

            if (jobStatus === JobStatus.CANCELLED) {
                this.logger.log(
                    `[清算Worker ${workerId}] 忽略 Task ${taskId}，因为 Job ${jobId} 已被取消 (Redis)。`,
                );
                return;
            }

            // 2. 模拟延迟
            await new Promise(res => setTimeout(res, 500 + Math.random() * 1000));

            // 模拟随机失败
            if (errorRate && Math.random() < (errorRate / 100)) {
                this.logger.warn(`[清算Worker ${workerId}] Task ${taskId} 触发了“模拟随机失败”！`);
                throw new Error('模拟的随机网络错误');
            }

            // 3. 更新 Task_Log 状态
            await this.taskLogRepository.update({ task_id: taskId }, { status: TaskStatus.SUCCESS });

            // 4. 更新 Job_Log 计数器
            const processed = await this.redisClient.incr(
                this.getJobProcessedKey(jobId),
            );

            const total =
                Number(await this.redisClient.get(this.getJobTotalKey(jobId))) || 0;
            const failed =
                Number(await this.redisClient.get(this.getJobFailedKey(jobId))) || 0;

            this.logger.log(
                `[清算Worker ${workerId}] Task ${taskId} 成功, 进度: ${processed}+${failed}/${total}`,
            );

            let durationInMs: number | null = null;
            let finalStatus = JobStatus.PROCESSING;

            // 6. 检查是否全部完成
            if (processed + failed === total && total > 0) {
                const endTime = new Date();
                const jobRecord = await this.jobLogRepository.findOne({
                    where: { job_id: jobId },
                    select: ['start_time'],
                });

                if (jobRecord?.start_time) {
                    durationInMs =
                        endTime.getTime() - new Date(jobRecord.start_time).getTime();
                }

                await this.jobLogRepository.update(
                    { job_id: jobId },
                    {
                        status: JobStatus.COMPLETED,
                        end_time: endTime,
                        processed_accounts: processed,
                        failed_accounts: failed,
                    },
                );

                // Redis 里也同步一下最终状态
                await this.redisClient.set(
                    this.getJobStatusKey(jobId),
                    JobStatus.COMPLETED,
                );

                finalStatus = JobStatus.COMPLETED;
            }

            // 7. 【发布到 Redis】
            await this._publishProgress({
                jobId,
                workerId,
                durationInMs,
                processed,
                failed,
                total,
                status: finalStatus,
            });

        } catch (error) {
            this.logger.error(`[清算Worker ${workerId}] 处理 Task ${taskId} 失败:`, error);
            const MAX_RETRIES = 3;
            if (retryCount < MAX_RETRIES) {
                // 9a. 增加重试次数
                await this.taskLogRepository.increment({ task_id: taskId }, 'retry_count', 1);
                this.logger.log(`[清算Worker ${workerId}] Task ${taskId} 将在第 ${retryCount + 1} 次重试...`);
                // 9b. 把它重新扔回队列
                // (我们可以加一点延迟，比如 1 秒后再重试)
                setTimeout(() => {
                    this.kafkaClient.emit('tasks-topic', { ...message, retryCount: retryCount + 1 });
                }, 1000);

            } else {
                // 9c. 已达最大重试次数，彻底失败
                this.logger.error(`[清算Worker ${workerId}] Task ${taskId} 已达最大重试次数 (${MAX_RETRIES})，标记为 FAILED。`);

                // 更新 Task 状态
                await this.taskLogRepository.update({ task_id: taskId }, { status: TaskStatus.FAILED });

                // 更新 Job 的“失败”计数器
                const failed = await this.redisClient.incr(
                    this.getJobFailedKey(jobId),
                );

                const processed =
                    Number(await this.redisClient.get(this.getJobProcessedKey(jobId))) || 0;
                const total =
                    Number(await this.redisClient.get(this.getJobTotalKey(jobId))) || 0;

                let durationInMs: number | null = null;
                let finalStatus = JobStatus.PROCESSING;

                // 如果全部结束，再刷一次 DB
                if (processed + failed === total && total > 0) {
                    const endTime = new Date();
                    const jobRecord = await this.jobLogRepository.findOne({
                        where: { job_id: jobId },
                        select: ['start_time'],
                    });

                    if (jobRecord?.start_time) {
                        durationInMs =
                            endTime.getTime() - new Date(jobRecord.start_time).getTime();
                    }

                    await this.jobLogRepository.update(
                        { job_id: jobId },
                        {
                            status: JobStatus.COMPLETED,
                            end_time: endTime,
                            processed_accounts: processed,
                            failed_accounts: failed,
                        },
                    );

                    await this.redisClient.set(
                        this.getJobStatusKey(jobId),
                        JobStatus.COMPLETED,
                    );
                    finalStatus = JobStatus.COMPLETED;
                }
                await this._publishProgress({
                    jobId,
                    workerId,
                    durationInMs,
                    processed,
                    failed,
                    total,
                    status: finalStatus,
                });
            }
        }
    }


    @MessagePattern('job-cancel-topic')
    async handleJobCancellation(@Payload() message: { jobId: string }) {
        const { jobId } = message;
        this.logger.warn(`[僵尸修复] 收到 Job ${jobId} 的“取消”请求。`);

        try {
            await this.jobLogRepository.update(
                { job_id: jobId },
                { status: JobStatus.CANCELLED } // <-- 更新数据库状态
            );

            await this.redisClient.set(
                this.getJobStatusKey(jobId),
                JobStatus.CANCELLED,
            );
            this.logger.warn(`[僵尸修复] Job ${jobId} 状态已更新为 CANCELLED。`);
        } catch (error) {
            this.logger.error(`[僵尸修复] 更新 Job ${jobId} 状态失败:`, error);
        }
    }

    private _publishProgress = async (payload: {
        jobId: string;
        workerId: string;
        durationInMs: number | null;
        processed: number;
        failed: number;
        total: number;
        status: JobStatus;
    }) => {
        const channel = `job-progress:${payload.jobId}`;
        const message = JSON.stringify({
            jobId: payload.jobId,
            processed: payload.processed,
            failed: payload.failed,
            total: payload.total,
            workerId: payload.workerId,
            duration: payload.durationInMs,
            status: payload.status,
        });
        await this.redisClient.publish(channel, message);
    }

    // 1) 统一的进度发布函数（只负责 publish，不再查 DB）
    private async _publishProgress2(payload: {
        jobId: string;
        workerId: string;
        durationInMs: number | null;
        processed: number;
        failed: number;
        total: number;
        status: JobStatus;
    }) {
        const channel = `job-progress:${payload.jobId}`;
        const message = JSON.stringify({
            jobId: payload.jobId,
            processed: payload.processed,
            failed: payload.failed,
            total: payload.total,
            workerId: payload.workerId,
            duration: payload.durationInMs,
            status: payload.status,
        });
        await this.redisClient.publish(channel, message);
    }

    private getJobStatusKey = (jobId: string) => `job:${jobId}:status`;
    private getJobTotalKey = (jobId: string) => `job:${jobId}:total`;
    private getJobProcessedKey = (jobId: string) => `job:${jobId}:processed`;
    private getJobFailedKey = (jobId: string) => `job:${jobId}:failed`;
}


