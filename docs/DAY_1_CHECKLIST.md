# Day 1 Execution Checklist

> 目标: 在今天结束前,看到 10+ 条 Adzuna 数据从 API 进入 Snowflake 的 silver 层.
>
> 预计总时间: 2-3 小时.

---

## Phase 1: 解压和初始化 repo (5分钟)

```bash
# 假设你把 jobseekers.zip 下载到了 ~/Downloads
cd ~/projects  # 或你想放项目的地方
unzip ~/Downloads/jobseekers.zip
cd jobseekers

# 初始化 git repo(方便后面推送到 GitHub)
git init
git add .
git commit -m "Day 1: project scaffolding"
```

## Phase 2: 填写 .env 凭据 (5分钟)

```bash
cp .env.example .env
```

然后用你喜欢的编辑器打开 `.env`,填入:

### Snowflake 字段怎么找
1. 登录 Snowflake: https://app.snowflake.com
2. 左下角点头像 → 查看你的 account info,会看到形如 `XXXXXXX-YYYYYYY` 的标识符
3. 填入 `SNOWFLAKE_ACCOUNT`
4. `SNOWFLAKE_USER` / `SNOWFLAKE_PASSWORD` 用你注册时设的
5. 其他字段**保持默认不用改**

### Adzuna
- `ADZUNA_APP_ID`: 登录 https://developer.adzuna.com/admin/access_details
- `ADZUNA_APP_KEY`: 同一页
- `ADZUNA_COUNTRY`: 保持 `us`

### USAJobs
- `USAJOBS_API_KEY`: 注册成功邮件里会附上
- `USAJOBS_USER_AGENT`: 填你注册时用的邮箱

**验证**: `.env` 文件里不应该还有任何 `your_xxx` 占位符.

---

## Phase 3: 在 Snowflake UI 建表 (10分钟)

打开 Snowflake 的 Worksheets 界面(左侧菜单 **Worksheets** 或 **Projects > Worksheets**).

### 3.1 运行 sql/01_create_databases.sql

新建一个 worksheet,复制 `sql/01_create_databases.sql` 的全部内容,粘贴,点右上角 **▶ Run All**(或按 Cmd/Ctrl + Shift + Enter).

**预期结果**: 最后一条 `SHOW SCHEMAS` 返回 3 行: `BRONZE`, `SILVER`, `GOLD`.

### 3.2 运行 sql/02_bronze_tables.sql

同样方式执行. 预期看到 3 张 bronze 表.

### 3.3 运行 sql/03_silver_tables.sql

同样方式执行. 预期看到 1 张 silver 表.

### 3.4 (可选) 运行 sql/04_gold_star_schema.sql

Day 3 才会用到,但现在先建好没关系.

**Checkpoint**: 在任意 worksheet 里跑:
```sql
SHOW TABLES IN DATABASE JOBSEEKERS;
```
应该看到至少 4 张表.

---

## Phase 4: 先手动测一下 Adzuna extractor (10分钟)

在跑 Airflow 之前,我们先单独测试 extractor 能不能工作. 这样如果有问题,我们能快速定位.

```bash
# 在项目根目录
python -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\activate

pip install -r requirements.txt

# 手动跑 extractor
python -m src.extractors.adzuna
```

**预期输出**(类似):
```
INFO:src.extractors.adzuna:Adzuna GET us page=1 what='software engineer intern'
INFO:src.extractors.adzuna:Adzuna returned 50 jobs on page 1
Got 50 jobs
First job title: Software Engineering Intern
First job company: Acme Corp
First job location: San Francisco, California
```

### 🚨 如果报错

**错误: `Missing required settings in .env: ADZUNA_APP_ID, ...`**
→ `.env` 文件没填好,或者你不是在项目根目录运行的.

**错误: `Adzuna error 401`**
→ `app_id` 或 `app_key` 错了. 回 Adzuna dashboard 重新复制.

**错误: `Adzuna error 403`**
→ 你可能用了错的 country code. 免费 tier 支持的国家有限,`us` 是默认可用的.

---

## Phase 5: 先手动测一下 Snowflake 连接 (10分钟)

类似地,单独测试 Snowflake loader.

在项目根目录新建一个临时测试文件 `test_snowflake.py`:

```python
from src.loaders.snowflake_loader import SnowflakeLoader

loader = SnowflakeLoader()
with loader._connect() as conn:
    cur = conn.cursor()
    cur.execute("SELECT CURRENT_VERSION(), CURRENT_DATABASE()")
    print(cur.fetchone())
```

跑一下:
```bash
python test_snowflake.py
```

**预期输出**: 类似 `('8.x.x', 'JOBSEEKERS')`.

### 🚨 如果报错

**错误: `Incorrect username or password`**
→ 回 Snowflake 登录页面验证凭据.

**错误: `250001: Could not connect to Snowflake backend after 0 attempts`**
→ 多半是 `SNOWFLAKE_ACCOUNT` 格式问题. 正确格式是 `orgname-accountname`(中间一个横线),不要包含 `.snowflakecomputing.com`.

**错误: `Database 'JOBSEEKERS' does not exist`**
→ Phase 3 没跑成功,回去重新执行 SQL.

测完把 `test_snowflake.py` 删掉.

---

## Phase 6: 启动 Airflow (15分钟)

```bash
# 如果还没装 Docker Desktop 先装.
# Mac: brew install --cask docker
# Windows: https://docs.docker.com/desktop/setup/install/windows-install/

# 起服务
docker compose up -d

# 观察启动过程(可选)
docker compose logs -f airflow-scheduler
# 看到 'Scheduler heartbeat ...' 就说明起来了, Ctrl+C 退出日志即可
```

第一次启动会拉 Airflow 镜像(~1GB),预留 5-10 分钟.

**Checkpoint**: 打开浏览器访问 http://localhost:8080
- 用户: `airflow`
- 密码: `airflow`

登录后应该看到 **DAGs** 页面,有一个叫 `adzuna_bronze_silver_e2e` 的 DAG.

### 🚨 如果报错

**DAG 列表里看不到我们的 DAG**
→ 去 **Browse > DAG Processing Errors** 看具体报错. 最常见的是 import error,因为 Airflow 容器里没装我们的依赖. docker-compose.yml 里我用 `_PIP_ADDITIONAL_REQUIREMENTS` 做了处理,首次启动会自动装,耐心等几分钟再刷新.

**端口 8080 被占**
→ 改 `docker-compose.yml` 里 `"8080:8080"` 为 `"8081:8080"`,然后访问 http://localhost:8081.

---

## Phase 7: 跑 DAG (10分钟)

1. 在 Airflow UI 里找到 `adzuna_bronze_silver_e2e`
2. 点 DAG 名字左边的**开关**,unpause 它(从灰色变蓝色)
3. 点右上角的 **▶** (Trigger DAG) 按钮
4. 点 DAG 名字进入 **Grid** 视图
5. 你会看到两个 task 依次变成**绿色**:
   - `extract_load_adzuna_to_bronze`
   - `transform_bronze_to_silver`

每个 task 点进去能看 logs,看看有没有报错.

**预期 logs**(任务成功时):
```
INFO - Adzuna returned 50 jobs on page 1
INFO - Loaded 50 Adzuna rows into BRONZE.RAW_ADZUNA
...
INFO - Adzuna MERGE affected 200 rows
```

---

## Phase 8: 验证数据落地 (5分钟)

回到 Snowflake UI,新建 worksheet,跑:

```sql
-- Bronze 层: 原始 JSON
SELECT COUNT(*) FROM JOBSEEKERS.BRONZE.RAW_ADZUNA;
-- 期望: 约 200 条 (4 个 query × 50 条/page)

-- 看一条原始数据长啥样
SELECT raw_data:title::string, raw_data:company.display_name::string
FROM JOBSEEKERS.BRONZE.RAW_ADZUNA
LIMIT 5;

-- Silver 层: 清洗后
SELECT COUNT(*) FROM JOBSEEKERS.SILVER.JOBS_UNIFIED;
-- 期望: 约 200 条

-- 看清洗效果
SELECT
    job_title,
    company_or_agency,
    location_city,
    location_state,
    salary_min,
    experience_level,
    is_entry_level
FROM JOBSEEKERS.SILVER.JOBS_UNIFIED
LIMIT 10;

-- 你的核心指标预览
SELECT experience_level, COUNT(*) AS n
FROM JOBSEEKERS.SILVER.JOBS_UNIFIED
GROUP BY experience_level;
```

**🎉 如果这些查询都有结果,Day 1 完成!**

---

## Phase 9: 提交到 GitHub (5分钟)

```bash
# 先确保 .env 没被 tracked
git status    # 确认 .env 不在列表里

# 创建 GitHub repo,然后:
git branch -M main
git remote add origin https://github.com/<your-username>/jobseekers.git
git push -u origin main
```

---

## Day 1 完成标志

- [ ] `.env` 填写完整,不包含占位符
- [ ] Snowflake 里有 JOBSEEKERS database + 3 个 schema
- [ ] 所有 4 个 SQL 文件执行成功
- [ ] `python -m src.extractors.adzuna` 本地能跑出结果
- [ ] `docker compose up -d` 成功,Airflow UI 能访问
- [ ] DAG `adzuna_bronze_silver_e2e` 手动触发两个 task 都绿
- [ ] `SELECT COUNT(*) FROM SILVER.JOBS_UNIFIED` 返回 > 0
- [ ] 代码推到 GitHub

做完这些,明天我们开始 Day 2(接入 USAJobs).

---

## 遇到问题怎么办

照着 checklist 走,如果某一步卡住了,告诉我:
1. 你卡在第几个 Phase
2. 具体错误信息的**截图**或**完整文本**
3. 你做了什么之后出现的

我们一起解决. 不要跳过 Phase 4、5 的单独测试,这些 10 分钟能帮你省 1 小时的排查时间.
