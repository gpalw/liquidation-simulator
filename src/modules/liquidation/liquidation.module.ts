import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { JobLog } from './entities/job-log.entity';
import { TaskLog } from './entities/task-log.entity';
import {
    ClientOptions,
    ClientsModule,
    Transport,
} from '@nestjs/microservices';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { LiquidationController } from './liquidation.controller';
import { LiquidationService } from './liquidation.service';
import { LiquidationGateway } from './liquidation.gateway';

@Module({
    imports: [
        ConfigModule,
        TypeOrmModule.forFeature([JobLog, TaskLog]),

        ClientsModule.registerAsync([
            {
                name: 'KAFKA_SERVICE',
                imports: [ConfigModule],
                inject: [ConfigService],
                useFactory: async (
                    config: ConfigService,
                ): Promise<ClientOptions> => {
                    const broker =
                        config.get<string>('KAFKA_BROKER') ?? 'localhost:9092';

                    const options: ClientOptions = {
                        transport: Transport.KAFKA,
                        options: {
                            client: {
                                brokers: [broker],
                                clientId:
                                    config.get<string>('KAFKA_CLIENT_ID') ??
                                    'liquidation-client',
                            },
                            /*consumer: {
                                groupId:
                                    config.get<string>('KAFKA_GROUP_ID') ??
                                    'liquidation-consumer',
                            },*/
                        },
                    };

                    return options;
                },
            },
        ]),
    ],
    controllers: [LiquidationController, LiquidationService],
    providers: [LiquidationService, LiquidationGateway],
    exports: [TypeOrmModule, ClientsModule],
})
export class LiquidationModule { }
