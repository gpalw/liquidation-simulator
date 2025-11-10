// src/modules/auth/google.strategy.ts

import { PassportStrategy } from '@nestjs/passport';
import { Strategy, StrategyOptions, VerifyCallback } from 'passport-google-oauth20';
import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';

@Injectable()
export class GoogleStrategy extends PassportStrategy(Strategy, 'google') {

    constructor(configService: ConfigService) {
        super({
            // 1. 从 .env 文件读取凭证
            clientID: configService.get<string>('GOOGLE_CLIENT_ID'),
            clientSecret: configService.get<string>('GOOGLE_CLIENT_SECRET'),
            // 2. 这是我们告诉 Google 登录成功后“跳回来”的地址
            callbackURL: configService.get<string>('PUBLIC_BASE_URL') + '/auth/google/callback',
            // 3. 我们需要 Google 返回给我们的用户信息
            scope: ['email', 'profile'],
            passReqToCallback: false,
        } as StrategyOptions);
    }

    /**
     * 4. Google 认证成功后，会调用这个“验证”函数
     * 我们在这里从 profile 中提取需要的信息（比如邮箱）
     */
    async validate(accessToken: string, refreshToken: string, profile: any, done: VerifyCallback): Promise<any> {
        const { name, emails, photos } = profile;
        const user = {
            email: emails[0].value,
            firstName: name.givenName,
            lastName: name.familyName,
            picture: photos[0].value,
            accessToken,
        };
        // 5. Passport.js 会把这个 user 对象附加到 Request 上 (req.user)
        done(null, user);
    }
}