// app.js

// --- DOM 元素 ---
const loginScreen = document.getElementById('login-screen');
const mainApp = document.getElementById('main-app');
const googleLoginButton = document.getElementById('google-login-btn');

const progressFill = document.getElementById('progress-fill');
const progressFail = document.getElementById('progress-fail');
const progressText = document.getElementById('progress-text');
const monitor = document.getElementById('worker-monitor');
const workerCountEl = document.getElementById('worker-count');
const startButton = document.getElementById('start-button');
const userIdInput = document.getElementById('user-id');
const errorRateInput = document.getElementById('error-rate');

// 你的 Nest.js WebSocket 运行在 3001 端口
const socket = io("http://localhost:3001");

const workerStats = new Map();
let currentJobId = null;
let authenticatedUser = null; // 存储已认证的用户信息

// --- WebSocket 连接状态 ---
socket.on("connect", () => {
    console.log("已成功连接到 WebSocket 服务器!");
    // 如果已经登录了，才启用按钮
    if (authenticatedUser) {
        startButton.disabled = false;
        startButton.textContent = "开始清算";
        progressText.textContent = "连接成功！等待作业启动...";
    }
});
socket.on("disconnect", () => {
    console.log("已与 WebSocket 断开连接。");
    startButton.disabled = true;
    startButton.textContent = "已断开";
    progressText.textContent = "连接已断开。请刷新页面。";
});

// --- 1. 谷歌登录处理 ---
googleLoginButton.addEventListener('click', () => {
    // 打开一个弹窗来处理 Google 登录
    const loginWindow = window.open(
        "http://localhost:3001/auth/google",
        "googleLogin",
        "width=500,height=600"
    );
});

// --- 2. 监听来自弹窗的 postMessage ---
window.addEventListener("message", (event) => {
    // 安全检查：(我们暂时跳过 event.origin 的严格检查)

    if (event.data && event.data.email) {
        console.log("收到用户信息:", event.data);
        authenticatedUser = event.data;

        // 2a. 隐藏登录界面
        loginScreen.style.display = 'none';

        // 2b. 显示主应用
        mainApp.style.display = 'block';

        // 2c. 填充并禁用 用户ID 输入框
        userIdInput.value = authenticatedUser.email;
        userIdInput.disabled = true;

        // 2d. (如果 WebSocket 已连接) 启用“开始”按钮
        if (socket.connected) {
            startButton.disabled = false;
            startButton.textContent = "开始清算";
            progressText.textContent = "连接成功！等待作业启动...";
        }
    }
}, false);


// --- 3. 核心功能 ---

// (处理进度更新)
function handleProgressUpdate(payload) {
    const { processed, failed = 0, total, workerId, duration } = payload;

    const successPercentage = (processed / total) * 100;
    const failPercentage = (failed / total) * 100;

    progressFill.style.width = successPercentage + '%';
    progressFail.style.width = failPercentage + '%';

    progressText.innerHTML = `
        处理中: ${processed} / ${total}
        <span class="failed-text">(失败: ${failed})</span>
    `;

    // 更新 Worker 监控器 (自动发现)
    if (workerId !== undefined) {
        if (!workerStats.has(workerId)) {
            const box = document.createElement('div');
            box.className = 'worker-box';
            box.innerHTML = `<div class="id">Worker #${workerId}</div><div class="count">0</div>`;
            monitor.appendChild(box);
            workerStats.set(workerId, { element: box, count: 0 });
            workerCountEl.textContent = workerStats.size;
        }
        const worker = workerStats.get(workerId);
        worker.count++;
        worker.element.querySelector('.count').textContent = worker.count;
        // "闪烁"
        worker.element.classList.add('active');
        setTimeout(() => {
            if (worker.element) {
                worker.element.classList.remove('active');
            }
        }, 100);
    }


    // 检查是否完成
    const finishedCount = processed + failed;
    if (finishedCount === total) {
        progressText.innerHTML = `
            作业完成: ${processed} / ${total}!
            <span class="failed-text">(失败: ${failed})</span>
        `;
        progressFill.style.background = "#007bff";
        socket.off(`job-progress:${currentJobId}`);

        if (duration) {
            const seconds = (duration / 1000).toFixed(2);
            setTimeout(() => {
                alert(`作业已全部完成！\n总耗时: ${seconds} 秒。\n成功: ${processed} | 失败: ${failed}`);
            }, 100);
        } else {
            alert(`作业已全部完成！\n成功: ${processed} | 失败: ${failed}`);
        }
    }
}

// (API 调用)
startButton.addEventListener('click', async () => {
    const accountsCount = parseInt(document.getElementById('accounts-count').value);
    const errorRate = parseInt(errorRateInput.value);
    const userId = userIdInput.value; // 从已禁用的输入框获取

    // 重置界面
    monitor.innerHTML = '';
    workerStats.clear();
    workerCountEl.textContent = 0;
    progressFill.style.width = '0%';
    progressFail.style.width = '0%';
    progressFill.style.background = "#28a745";
    progressText.innerHTML = `已启动: 0 / ${accountsCount} <span class="failed-text">(失败: 0)</span>`;
    if (currentJobId) {
        socket.off(`job-progress:${currentJobId}`);
    }

    try {
        // 调用 API
        const response = await fetch("http://localhost:3001/liquidation/job", {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                accounts_count: accountsCount,
                user_id: userId,
                error_rate: errorRate
            })
        });
        const data = await response.json();
        if (!response.ok) throw new Error(data.message || 'API 请求失败');

        currentJobId = data.jobId;
        console.log(`正在监听 Redis 频道: job-progress:${currentJobId}`);

        // 认领任务 (用于“僵尸”修复)
        socket.emit('subscribeToJob', { jobId: currentJobId });

        // 监听 WebSocket 广播
        socket.on(`job-progress:${currentJobId}`, handleProgressUpdate);

    } catch (error) {
        progressText.textContent = `启动失败: ${error.message}`;
    }
});