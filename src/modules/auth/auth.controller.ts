import { Controller, Get, Req, Res, UseGuards } from '@nestjs/common';
import { AuthGuard } from '@nestjs/passport';
import type { Response } from 'express';

type ReqWithUser<T = any> = Request & { user?: T };

@Controller('auth')
export class AuthController {
    @Get('google')
    @UseGuards(AuthGuard('google'))
    async googleAuth() {
        // Passport 自动处理重定向
    }



    @Get('google/callback')
    @UseGuards(AuthGuard('google'))
    googleAuthRedirect(@Req() req: ReqWithUser, @Res() res: Response) {
        const user = req.user;

        // 返回 HTML 页面执行 postMessage
        res.send(`
      <html>
        <body>
          <script>
            window.opener.postMessage(${JSON.stringify(user)}, '*');
            window.close();
          </script>
        </body>
      </html>
    `);
    }
}
