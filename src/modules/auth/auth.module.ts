// src/modules/auth/auth.module.ts

import { Module } from '@nestjs/common';
import { PassportModule } from '@nestjs/passport';
import { ConfigModule } from '@nestjs/config';
import { AuthController } from './auth.controller';
import { GoogleStrategy } from './google.strategy';

@Module({
    imports: [
        ConfigModule,
        PassportModule,
        // (稍后我们会在这里导入 JwtModule)
    ],
    controllers: [AuthController],
    providers: [GoogleStrategy],
    exports: [],
})
export class AuthModule { }