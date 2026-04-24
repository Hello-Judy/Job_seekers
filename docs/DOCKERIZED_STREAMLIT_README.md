# Dockerized Streamlit Setup

> 队友把 Streamlit 也加进了 docker-compose.yml. 现在你**不需要**在本地装 venv 跑 streamlit, 一条 `docker compose up -d` 同时启动:
>
>   - Postgres (Airflow metadata)
>   - Airflow scheduler + webserver
>   - **Streamlit dashboard** ← 新加的
>
> 所有东西都在 Docker 里, 干净整洁.
>
> 预计配置时间: 5 分钟.

---

## 这次改了什么

```
docker-compose.yml          [改]  加了 streamlit 服务 + streamlit-data 卷
analysis/app.py             [改]  Job Search 页面布局简化(避免 3 层嵌套)
analysis/db/user_db.py      [改]  USER_DB_PATH 支持环境变量覆盖
                                   (Docker 用 /app/data/users.db 持久化)
```

---

## Phase 1: 覆盖代码 (3 分钟)

```bash
cd ~/projects/jobseekers

git checkout -b dockerized-streamlit

unzip -o ~/Downloads/jobseekers_dockerized.zip -d ~/tmp_dock
cp -r ~/tmp_dock/jobseekers_dockerized/. ./
rm -rf ~/tmp_dock

# 验证 docker-compose.yml 里加了 streamlit
grep -A 2 "streamlit:" docker-compose.yml | head -5
# 应该看到: streamlit:  image: python:3.11-slim  ...

git add .
git commit -m "Dockerized Streamlit dashboard"
```

---

## Phase 2: 重启所有服务 (5 分钟)

```bash
# 停掉所有容器
docker compose down

# 重新启动 (这次会带 streamlit)
docker compose up -d

# 看启动日志
docker compose logs -f streamlit
```

第一次启动 streamlit 容器要等 1-2 分钟, 因为它会:
1. 拉 `python:3.11-slim` 镜像 (~50MB)
2. 运行 `pip install streamlit pandas plotly snowflake-connector-python python-dotenv pyarrow`
3. 启动 streamlit server

看到这一行就说明启动好了:
```
streamlit-1  | You can now view your Streamlit app in your browser.
streamlit-1  | URL: http://0.0.0.0:8501
```

按 Ctrl+C 退出日志查看(不会停掉容器).

---

## Phase 3: 访问 Dashboard (2 分钟)

打开浏览器访问 http://localhost:8501

第一次使用:
1. 点 **Create Account** tab
2. 注册:
   - Name: `Demo User`
   - Email: `demo@jobseekers.io`
   - Password: `demo1234`
3. 登录
4. 设置 Preferences (筛选条件), 然后看 Dashboard / Job Search 等页面

---

## 验证三个服务都跑起来了

```bash
docker compose ps
```

应该看到 4 个容器都是 **Up** 状态:
- postgres
- airflow-webserver  (http://localhost:8080)
- airflow-scheduler
- streamlit          (http://localhost:8501)

---

## 🚨 常见报错

### `streamlit-1 | ModuleNotFoundError: No module named 'src.utils.config'`
查看: `docker compose logs streamlit | head -30`
原因: pip install 还没装完或者 mount 路径不对.
修复: `docker compose restart streamlit`, 等 1-2 分钟再访问.

### `Snowflake authentication failed`
原因: `.env` 没被 mount 进容器.
检查:
```bash
docker compose exec streamlit cat /app/.env | head -3
```
如果是空的, 检查你本地 `.env` 文件存在并填了凭据.

### 端口 8501 被占
你之前手动跑过 streamlit, 没关掉. 找到那个进程 kill:
```bash
lsof -i :8501
kill <PID>
```
或者改 docker-compose.yml 的 `8501:8501` 为 `8502:8501`.

### 数据库 users.db 找不到旧账号
每次重启 docker, users.db 都在 `streamlit-data` named volume 里持久化, 不会丢.
但如果你 `docker compose down -v` (加了 -v), 卷会被删, 账号会丢.
平时只 `docker compose down` 不要加 -v.

---

## 演讲时怎么 demo

这套 dockerized 架构在演讲时是个**加分项**, 你可以这么讲:

> "整个系统是 fully containerized — 我们用 Docker Compose 把 Postgres、
> Airflow scheduler、webserver 和 Streamlit dashboard 打包成 4 个服务,
> 一条 `docker compose up -d` 就能在任何机器上 1 分钟内启动完整环境.
>
> 这是 production-grade DevOps 实践: 环境一致性, 零配置漂移,
> 演示完 `docker compose down` 就清理干净, 不留任何痕迹."

演讲时直接打开 terminal 跑:
```bash
docker compose ps
```
让评委看到 4 个服务都在 Up 状态, 比截图说服力强 10 倍.

---

## 清理 (项目结束后)

```bash
# 停所有容器, 保留数据卷 (你的 Snowflake 凭据 / users.db 都还在)
docker compose down

# 完全清理 (连数据卷一起删)
docker compose down -v
```

---

跑通后告诉我:
1. `docker compose ps` 是不是 4 个服务都 Up?
2. 浏览器打开 http://localhost:8501 能看到登录页吗?
3. 注册登录后, Dashboard 5 个汇总指标的数字是多少?
