import { Entity, Column, PrimaryGeneratedColumn, Index } from 'typeorm';

export enum JobStatus {
    PENDING = 'pending',
    SPLITTING = 'splitting',
    PROCESSING = 'processing',
    COMPLETED = 'completed',
    FAILED = 'failed',
    CANCELLED = 'cancelled',
}

@Entity('job_log')
export class JobLog {
    @PrimaryGeneratedColumn('uuid')
    job_id: string;

    @Index() // 按照用户ID扩展
    @Column()
    user_id: string;

    @Column({
        type: 'enum',
        enum: JobStatus,
        default: JobStatus.PENDING,
    })
    status: JobStatus;

    @Column({ default: 0 })
    total_accounts: number;

    @Column({ default: 0 })
    processed_accounts: number;

    @Column({ default: 0 })
    failed_accounts: number;

    @Column({ type: 'timestamp' })
    start_time: Date;

    @Column({ type: 'timestamp', nullable: true })
    end_time: Date;

    @Column({ type: 'int', nullable: true })
    total_duration_ms: number;
}