// app.js

// --- DOM Elements ---
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

const API_BASE = '/api';           // Relative path
const WS_PATH = '/socket.io';      // Default socket.io path

// --- WebSocket Connection ---
const socket = io('/', { path: WS_PATH, withCredentials: true });

const workerStats = new Map();
let currentJobId = null;
let authenticatedUser = null; // Store authenticated user info

// --- WebSocket Connection State ---
socket.on("connect", () => {
    console.log("Successfully connected to WebSocket server!");

    // Enable button only after login
    if (authenticatedUser) {
        startButton.disabled = false;
        startButton.textContent = "Start Simulation";
        progressText.textContent = "Connected! Waiting for job to start...";
    }
});

socket.on("disconnect", () => {
    console.log("Disconnected from WebSocket server.");
    startButton.disabled = true;
    startButton.textContent = "Disconnected";
    progressText.textContent = "Connection lost. Please refresh the page.";
});

// --- 1. Google Login Handler ---
googleLoginButton.addEventListener('click', () => {
    // Open login popup
    const loginWindow = window.open(`${API_BASE}/auth/google`, "googleLogin", "width=500,height=600");
});

// --- 2. Listen for postMessage from popup ---
window.addEventListener("message", (event) => {
    // (Skipping strict event.origin check for now)

    if (event.data && event.data.email) {
        console.log("Received user info:", event.data);
        authenticatedUser = event.data;

        // 2a. Hide login screen
        loginScreen.style.display = 'none';

        // 2b. Show main app
        mainApp.style.display = 'block';

        // 2c. Fill and disable User ID input
        userIdInput.value = authenticatedUser.email;
        userIdInput.disabled = true;

        // 2d. If WebSocket connected → enable start button
        if (socket.connected) {
            startButton.disabled = false;
            startButton.textContent = "Start Simulation";
            progressText.textContent = "Connected! Waiting for job to start...";
        }
    }
}, false);


// --- 3. Core Functionality ---

// Handle progress updates
function handleProgressUpdate(payload) {
    const { processed, failed = 0, total, workerId, duration } = payload;

    const successPercentage = (processed / total) * 100;
    const failPercentage = (failed / total) * 100;

    progressFill.style.width = successPercentage + '%';
    progressFail.style.width = failPercentage + '%';

    progressText.innerHTML = `
        Processing: ${processed} / ${total}
        <span class="failed-text">(Failed: ${failed})</span>
    `;

    // Update Worker Monitor (auto discovery)
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

        // Flash effect
        worker.element.classList.add('active');
        setTimeout(() => {
            if (worker.element) {
                worker.element.classList.remove('active');
            }
        }, 100);
    }

    // Check if completed
    const finishedCount = processed + failed;
    if (finishedCount === total) {
        progressText.innerHTML = `
            Job Completed: ${processed} / ${total}!
            <span class="failed-text">(Failed: ${failed})</span>
        `;
        progressFill.style.background = "#007bff";
        socket.off(`job-progress:${currentJobId}`);

        if (duration) {
            const seconds = (duration / 1000).toFixed(2);
            setTimeout(() => {
                alert(`All tasks are completed!\nTotal Duration: ${seconds} seconds.\nSuccess: ${processed} | Failed: ${failed}`);
            }, 100);
        } else {
            alert(`All tasks are completed!\nSuccess: ${processed} | Failed: ${failed}`);
        }
    }
}

// API Call
startButton.addEventListener('click', async () => {
    const accountsCount = parseInt(document.getElementById('accounts-count').value);
    const errorRate = parseInt(errorRateInput.value);
    const userId = userIdInput.value;

    // Reset UI
    monitor.innerHTML = '';
    workerStats.clear();
    workerCountEl.textContent = 0;
    progressFill.style.width = '0%';
    progressFail.style.width = '0%';
    progressFill.style.background = "#28a745";
    progressText.innerHTML = `Started: 0 / ${accountsCount} <span class="failed-text">(Failed: 0)</span>`;

    if (currentJobId) {
        socket.off(`job-progress:${currentJobId}`);
    }

    try {
        // Call API
        const response = await fetch(`${API_BASE}/liquidation/job`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                accounts_count: accountsCount,
                user_id: userId,
                error_rate: errorRate
            })
        });

        const data = await response.json();
        if (!response.ok) throw new Error(data.message || 'API request failed');

        currentJobId = data.jobId;
        console.log(`Listening on Redis channel: job-progress:${currentJobId}`);

        // Subscribe to job (zombie worker recovery)
        socket.emit('subscribeToJob', { jobId: currentJobId });

        // Listen to WebSocket broadcasts
        socket.on(`job-progress:${currentJobId}`, handleProgressUpdate);

    } catch (error) {
        progressText.textContent = `Failed to start: ${error.message}`;
    }
});
