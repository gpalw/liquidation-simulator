import { Entity, Column, PrimaryGeneratedColumn, Index } from 'typeorm';

export enum TaskStatus {
    PENDING = 'pending',
    PROCESSING = 'processing',
    SUCCESS = 'success',
    FAILED = 'failed',
}

@Entity('task_log')
export class TaskLog {
    @PrimaryGeneratedColumn('uuid')
    task_id: string;

    @Index()
    @Column('uuid')
    job_id: string; // 关联到总作业

    @Column()
    account_id: string; // 模拟的账户ID

    @Column({
        type: 'enum',
        enum: TaskStatus,
        default: TaskStatus.PENDING,
    })
    status: TaskStatus;

    @Column({ default: 0 })
    retry_count: number;

    @Column({ type: 'int', nullable: true })
    processing_time_ms: number;
}