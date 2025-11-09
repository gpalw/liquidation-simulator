// ecosystem.config.js
module.exports = {
    apps: [
        {
            // 应用 1: API 和 WebSocket 网关
            name: 'api-gateway',
            script: 'dist/main.js',     //
            instances: 1,             // <-- 关键：网关只需要 1 个
            exec_mode: 'fork',
            env: { "NODE_ENV": "production" }
        },
        {
            // 应用 2: 后台清算工 (10个)
            name: 'liquidation-worker',
            script: 'dist/worker.js',   // <-- 关键：运行我们的新文件
            instances: '10',         // <-- 关键：'max' (比如 10 个)
            exec_mode: 'cluster',
            env: { "NODE_ENV": "production" }
        }
    ]
};