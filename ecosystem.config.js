// ecosystem.config.js
module.exports = {
    apps: [
        {
            name: 'liquidation-cluster', // 你的应用名称
            script: 'dist/main.js',     // PM2 要运行的入口文件

            instances: '10',             // max: "能开多少核，就开多少个进程"
            exec_mode: 'cluster',         // 必须使用“集群模式” 才能实现负载均衡

            watch: false,                 // 我们在生产中不使用 watch
            env: {
                "NODE_ENV": "production",
            }
        }
    ]
};