# 🚀 High-Reliability Batch Liquidation Simulator

This is a full-stack, distributed Proof of Concept (PoC) project built to simulate and solve a real-world Fintech architectural challenge: **how to process a massive volume (e.g., 10,000+) of batch liquidation jobs with high concurrency and high reliability.**

This project is built as a complete microservice system to **visually demonstrate** the scalability, reliability, and resilience of an advanced backend architecture.

**➡️ Live Demo:** `https://liquidation.liangwendev.com`

---

## ✨ Key Features

* **⚡ Two-Tier Concurrency Architecture:**
    1.  **Multi-Process Concurrency:** Uses **PM2** to launch multiple Worker processes (e.g., 10+) that are load-balanced using **Kafka Partitions**.
    2.  **In-Process Concurrency:** Each Worker implements an internal **concurrency pool of 10**, solving the Kafka "uneven partition" bottleneck (where one worker gets 118 tasks) and improving its efficiency by **10x**.

* **📺 Real-Time Visual Dashboard:**
    * The UI connects via **WebSocket**.
    * Backend Workers broadcast progress using **Redis Pub/Sub**.
    * The frontend **dynamically generates** a "Worker Monitor" that **visually contrasts** the "Single Process" mode (one box flashing) against the "Cluster Mode".

* **🛡️ High-Reliability & Resilience:**
    * **"Zombie Task" Cancellation:** The architecture is **connection-aware**. If the user closes their browser (WebSocket disconnects), the backend **automatically cancels** the in-progress "zombie job" to save resources.
    * **Failure & Retry Logic:** The UI supports a "Simulated Error Rate". The backend implements an **automatic retry (up to 3 times)** logic for failed tasks and visually reports permanent failures on the "failed" progress bar.

* **🔒 Security & Authentication:**
    * The entire application is protected by **Google OAuth 2.0** to prevent unauthenticated users from launching malicious jobs (like a 1M-task job) on a public server.

---

## 🛠️ Tech Stack

* **Backend:** Nest.js, Kafka.js, TypeORM, PM2, Passport.js (Google)
* **Frontend:** HTML5, CSS3, Vanilla JavaScript (ES6+), Socket.IO Client
* **Infrastructure:** Docker Compose, PostgreSQL, Kafka, Redis

---

## 🔧 Architecture Overview

The application is split by PM2 into two main services:

1.  **`api-gateway` (1 Process):**
    * Serves **frontend static files** (`index.html`, `app.js`, `style.css`).
    * Handles **HTTP APIs** (`/auth/google`, `/liquidation/job`).
    * Manages **WebSocket** connections.
    * **Subscribes** to Redis progress.
    * **Publishes** the `job-cancel-topic` message.
2.  **`liquidation-worker` (N Processes, e.g., 17):**
    * Does **not** handle HTTP or WebSockets.
    * **Listens** to Kafka topics: `jobs-topic`, `tasks-topic`, and `job-cancel-topic`.
    * **Publishes** progress to Redis.
    * Performs all database operations (`increment`, `update`).

---

## ⚙️ Local Setup & Installation

1.  **Clone the repo**
    ```bash
    git clone [YOUR_GITHUB_REPO_URL]
    cd liquidation-simulator
    ```

2.  **Install dependencies**
    ```bash
    npm install
    ```

3.  **Setup Google Credentials (Critical)**
    * Go to [Google Cloud Console](https://console.cloud.google.com/).
    * Create a new "OAuth 2.0 Client ID".
    * Under "**Authorized redirect URIs**", you **must** add:
        `http://localhost:3002/auth/google/callback`

4.  **Create `.env` file**
    * Create a `.env` file in the project root. Copy the following, and fill in your Google credentials:

    ```env
    # Database
    PORT=3002
    PUBLIC_BASE_URL=http://localhost:${PORT}
    DB_HOST=localhost
    DB_PORT=15432 # (We changed this to 15432 during debugging)
    DB_USERNAME=admin
    DB_PASSWORD=admin
    DB_DATABASE=liquidation_db

    # Kafka (Our fixed config)
    KAFKA_BROKER=localhost:9092
    
    # Redis
    REDIS_HOST=localhost
    REDIS_PORT=6379
    
    # Google Login
    GOOGLE_CLIENT_ID=PASTE_YOUR_CLIENT_ID_HERE
    GOOGLE_CLIENT_SECRET=PASTE_YOUR_CLIENT_SECRET_HERE
    JWT_SECRET=SOME_VERY_RANDOM_SECRET_KEY_FOR_JWT
    ```

5.  **Configure Kafka Partitions (Critical)**
    * Check your `ecosystem.config.js` for the `instances` count of the `liquidation-worker` (default is `'max'`).
    * Check your CPU core count (e.g., 16).
    * Open `docker-compose.yml` and ensure `KAFKA_NUM_PARTITIONS` matches your `instances` count, otherwise your workers will be idle.
        ```yaml
        kafka:
          # ...
          environment:
            # ...
            KAFKA_NUM_PARTITIONS: 10 # <-- Make sure this matches your CPU cores
        ```

6.  **Start Infrastructure (Docker)**
    ```bash
    docker-compose up -d
    ```

7.  **Build the Project**
    ```bash
    npm run build
    ```

---

## 🏁 Running the Demo

Use the built-in scripts to demonstrate the two modes.

### Demo A: Single-Process Mode (Slow)

1.  **Start (Single Worker):**
    ```bash
    npm run demo:single
    ```
    *(This starts 1 `api-gateway` and 1 `liquidation-worker`)*

2.  **Test:**
    * Open `http://localhost:3001`.
    * Login and submit a job for 300 accounts.
    * **Observe:** You will see **only one** "Worker #0" box flashing, and the progress bar will move slowly.

### Demo B: Cluster Mode (Fast)

1.  **Start (Multi-Process):**
    ```bash
    npm run demo:cluster
    ```
    *(This starts 1 `api-gateway` and N `liquidation-worker`s)*

2.  **Test:**
    * **Hard-refresh** `http://localhost:3002`.
    * Login and submit a job for 3000 accounts with an 80% error rate.
    * **Observe:** You will see **all** worker boxes flashing simultaneously, and the green (success) and red (fail) progress bars will move very quickly.

### Stopping the App

```bash
npm run demo:stop