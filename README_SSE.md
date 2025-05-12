# 推特实时推送系统 (SSE版本)

这是一个高效的推特实时推送系统，使用Server-Sent Events (SSE)技术向客户端推送最新推文。系统由两个主要组件组成：轮询引擎和SSE服务器，通过Redis进行数据传输。

## 系统架构

整个系统由以下组件组成：

1. **轮询引擎 (polling_engine.py)**
   - 从Nitter RSS获取关注用户的新推文
   - 智能轮询策略：优先用户组每10秒检测一次，普通用户组每60秒检测一次
   - 将新推文存入Redis流
   - 自动处理网络故障和Nitter实例切换

2. **SSE服务器 (sse_server.py)**
   - 基于FastAPI的HTTP服务器，提供SSE端点
   - 从Redis流读取新推文，实时推送给客户端
   - 提供美观的Web界面展示推文
   - 支持按用户过滤推文

3. **启动脚本 (run_sse_system.py)**
   - 一键启动整个系统
   - 管理Redis、轮询引擎和SSE服务器的生命周期
   - 提供命令行参数配置系统

## 前提条件

- Python 3.7+
- Redis服务器
- 运行中的Nitter实例
- 以下Python库:
  - fastapi
  - uvicorn
  - redis
  - aiohttp
  - jinja2

## 快速开始

1. **安装Redis**

   请确保Redis服务器已安装并运行。如果尚未安装，可参考[Redis官方文档](https://redis.io/docs/getting-started/)进行安装。

2. **安装Python依赖**

   ```bash
   pip install fastapi uvicorn redis aiohttp jinja2
   ```

3. **准备关注列表**

   确保您有一个`following_list.json`文件，包含您想要监控的用户列表。文件格式应为：

   ```json
   [
     {
       "userId": "用户名1",
       "username": "显示名1"
     },
     {
       "userId": "用户名2",
       "username": "显示名2"
     }
     // ...
   ]
   ```

4. **启动系统**

   ```bash
   python run_sse_system.py --following-file following_list.json --nitter-instances http://localhost:8080
   ```

   启动后，打开浏览器访问 `http://localhost:8000` 查看推文界面。

## 命令行参数

### 启动脚本参数

```
python run_sse_system.py --help
```

可用参数:
- `--following-file`: 关注列表文件路径 (默认: following_list.json)
- `--nitter-instances`: Nitter实例URL，多个实例用逗号分隔 (默认: http://localhost:8080)
- `--host`: 服务器主机地址 (默认: 0.0.0.0)
- `--port`: 服务器端口 (默认: 8000)
- `--with-redis`: 同时启动Redis服务器 (可选)

### 轮询引擎参数

```
python polling_engine.py --help
```

可用参数:
- `--following-file`: 关注列表文件路径 (默认: following_list.json)
- `--nitter-instances`: Nitter实例URL，多个实例用逗号分隔 (默认: http://localhost:8080)
- `--batch-size`: 每批处理的用户数量 (默认: 15)
- `--request-rate`: 每秒请求数 (默认: 10)
- `--debug`: 启用调试日志
- `--reset-state`: 重置所有用户状态（包括ETag和最后推文ID）
- `--smart-fix`: 执行智能状态修复（默认行为，只修复问题状态）
- `--force-fix`: 强制执行状态修复，无视版本号（适用于数据格式变更）
- `--skip-state-check`: 跳过状态校验和修复步骤
- `--state-version`: 指定状态版本（当前: v1）

### SSE服务器参数

```
python sse_server.py --help
```

可用参数:
- `--host`: 服务器主机地址 (默认: 0.0.0.0)
- `--port`: 服务器端口 (默认: 8000)
- `--read-history`: 读取Redis流中的历史数据 (默认: 仅读取新数据)

## 优秀特性

- **智能轮询策略**: 根据用户活跃度自动调整轮询频率
- **容灾机制**: 支持多个Nitter实例，自动切换失效实例
- **实时推送**: 使用SSE技术，无需客户端轮询
- **高效存储**: 使用Redis流存储推文，自动清理过期数据
- **美观界面**: 提供现代化的Web界面，支持按用户筛选
- **低延迟**: 优先用户的新推文最快10秒内可见
- **低资源消耗**: 批量处理请求，避免过度请求Nitter实例

## 优先用户组

默认的优先用户组包含以下账号:
```
daidaibtc, brc20niubi, Nuanran01, binancezh, 0x0xFeng, hexiecs, CryptoDevinL, 
bwenews, dotyyds1234, 0xSunNFT, xiaomucrypto, Bitfatty, ai_9684xtpa, 
cz_binance, EmberCN, Vida_BWE
```

您可以通过修改`polling_engine.py`文件中的`priority_users`集合来自定义优先用户。

## 配置Redis

系统默认连接到本地的Redis服务器。如需修改Redis连接信息，可通过环境变量设置:

```bash
# Windows
set REDIS_HOST=127.0.0.1
set REDIS_PORT=6379
set REDIS_DB=0
set REDIS_PASSWORD=your_password

# Linux/macOS
export REDIS_HOST=127.0.0.1
export REDIS_PORT=6379
export REDIS_DB=0
export REDIS_PASSWORD=your_password
```

## 故障排除

- **Redis连接失败**: 请确保Redis服务器已启动，并检查连接信息是否正确
- **轮询引擎启动失败**: 检查Nitter实例是否可访问，以及following_list.json文件是否存在
- **SSE服务器启动失败**: 检查指定的端口是否被占用
- **网页不显示任何推文**: 检查Redis是否正常运行，轮询引擎是否成功连接到Nitter实例
- **推文更新延迟大**: 可能是由于Nitter实例响应慢导致，尝试使用其他Nitter实例

### Redis数据问题

#### 问题1: 轮询引擎重启后出现大量报错

当轮询引擎运行一段时间后退出再执行时，可能会出现大量报错（如HTTP 429错误）。这是因为引擎会记住每个用户的最后一条推文ID和ETag缓存，重启后使用过期的缓存可能导致问题。

**解决方案：**

系统现在实现了智能状态校验和恢复机制，不再需要完全清空缓存。以下是可用的几种状态管理选项：

1. **智能状态恢复**（默认行为）：

   系统启动时会自动校验所有用户状态，只清理无效或异常的状态：
   
   ```bash
   python polling_engine.py  # 默认执行智能状态校验和恢复
   ```

2. **跳过状态校验**：

   如果您确定状态数据是有效的，可以跳过状态校验步骤以加速启动：
   
   ```bash
   python polling_engine.py --skip-state-check
   ```

3. **强制修复状态**：

   如果您升级了系统或修改了数据结构，可以使用此选项清理旧版本的状态数据：
   
   ```bash
   python polling_engine.py --force-fix
   ```

4. **完全重置状态**：

   在极端情况下，您可以完全重置所有用户的状态：
   
   ```bash
   python polling_engine.py --reset-state
   ```

5. **指定状态版本**：

   如果需要兼容特定版本的状态数据：
   
   ```bash
   python polling_engine.py --state-version v2
   ```

系统使用状态版本控制机制（当前版本：v1）来管理缓存数据的格式变更，确保版本升级时能够平滑过渡。

#### 问题2: SSE服务器重启时加载旧数据

当SSE服务器重启时，默认情况下会从Redis流的开始位置读取所有历史数据，这可能导致旧推文被重新加载到页面。

**解决方案：**

现在SSE服务器默认只会读取新数据。如果需要读取历史数据，可以使用`--read-history`参数：

```bash
python sse_server.py --read-history
```

## 系统限制

- 依赖Nitter RSS格式，如Nitter更新可能需要调整代码
- Redis流默认保留最近10000条推文，可根据需要调整
- 轮询间隔受限于Nitter实例的响应能力
- 大量关注用户的情况下，完整轮询一遍可能需要较长时间 

# 在sse_server.py中也将日志级别改为DEBUG
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
) 

## 常见问题解决

### 前端无法显示后端数据

如果前端界面无法显示后端轮询的推特信息，可能是因为以下原因：

1. **SSE流读取设置问题**：默认情况下，SSE服务器只会从最新的消息开始读取，而不是历史数据。
   - 解决方案：修改`sse_server.py`中的`stream_tweets`函数，将`last_id`设置为"0-0"，从流的开始读取所有数据。
   - 同时增加`count`参数，从默认的10条增加到更多（如100条）。

2. **前端处理逻辑问题**：前端可能没有正确处理接收到的数据。
   - 解决方案：修改前端JavaScript代码，确保正确解析和显示服务器发送的数据。
   - 增加错误处理和日志记录，以便调试问题。

3. **Redis流数据不存在**：确保轮询引擎正在运行并向Redis流中写入数据。
   - 解决方案：使用Redis CLI检查流是否有数据：`redis-cli xlen tweet_stream`
   - 如果没有数据，检查轮询引擎日志，确保它能成功连接到Nitter实例。

4. **Redis连接问题**：SSE服务器可能无法连接到Redis。
   - 解决方案：运行`python test_redis_connection.py`检查Redis连接。
   - 确保Redis服务器正在运行，并且连接参数正确。

5. **手动发送测试推文**：可以使用测试脚本手动发送测试推文。
   - 解决方案：运行`python test_redis_connection.py --send-test`
   - 这将向Redis流中添加一条测试推文，帮助验证系统是否正常工作。

6. **调试模式无法打开**：如果按Ctrl+D无法打开调试模式，可能是因为浏览器已经占用了这个快捷键。
   - 解决方案：修改为使用Ctrl+Shift+D快捷键，或者添加一个调试按钮。
   - 也可以直接在浏览器控制台中查看日志。

### 在Windows上运行系统

在Windows上运行系统时，可能会遇到以下问题：

1. **命令连接符问题**：Windows PowerShell不支持`&&`命令连接符。
   - 解决方案：分开执行命令，或使用`;`作为命令分隔符。

2. **Redis服务器启动问题**：
   - 解决方案：使用`--start-redis no`参数跳过Redis自动启动，手动启动Redis服务器。

3. **端口占用问题**：
   - 解决方案：使用`--port`参数指定不同的端口号。

4. **进程启动问题**：使用`run_sse_system.py`可能导致轮询引擎无法正确启动。
   - 解决方案：使用以下两种方法之一启动系统：

#### 方法1：使用批处理文件启动（推荐）

直接双击`start_system.bat`文件，系统将自动启动轮询引擎和SSE服务器。这将打开两个命令窗口，分别运行轮询引擎和SSE服务器。

#### 方法2：使用专用Windows启动脚本

```bash
python start_windows.py --nitter-instances http://nitter.net
```

这个脚本会在单独的命令窗口中启动轮询引擎和SSE服务器，确保两个组件能够正确运行。

#### 方法3：分别启动组件

如果上述方法仍然不能解决问题，可以尝试在两个单独的命令窗口中分别启动轮询引擎和SSE服务器：

```bash
# 在第一个命令窗口中启动轮询引擎
python polling_engine.py --following-file following_list.json --nitter-instances http://nitter.net

# 在第二个命令窗口中启动SSE服务器
python sse_server.py --host 127.0.0.1 --port 8000
```

### 调试系统

如果需要调试系统，可以：

1. 在前端界面按`Ctrl+Shift+D`开启调试模式，查看接收到的消息详情。
2. 点击界面上的"调试模式"按钮打开调试面板。
3. 检查后端日志，了解数据流向。
4. 使用Redis CLI直接查询Redis中的数据：
   ```
   redis-cli xrange tweet_stream - + COUNT 10
   ```
5. 使用测试脚本验证Redis连接和数据流：
   ```
   python test_redis_connection.py
   ```

#### 使用调试模式

如果需要更详细的日志信息，可以使用`--debug-mode`参数：

```bash
python run_sse_system.py --debug-mode --nitter-instances http://nitter.net
```

通过以上步骤，可以确保前端能够显示后端轮询的所有推特信息。 

## Nitter实例优化

为了提高系统性能和稳定性，我们对Nitter实例进行了以下优化：

### Docker优化配置

我们修改了Nitter的Docker启动参数，以提高其处理能力：

```bash
docker run -d \
  -p 8080:8080 \
  -e ENABLE_RATELIMIT=false \
  -e TOR_PROXY_ENABLED=true \
  -e REQ_LIMIT=1000 \
  zedeus/nitter:latest
```

或者使用docker-compose.yml：

```yaml
services:
  nitter:
    image: zedeus/nitter:latest
    container_name: nitter
    ports:
      - "8080:8080"
    environment:
      - ENABLE_RATELIMIT=false
      - TOR_PROXY_ENABLED=true
      - REQ_LIMIT=1000
    volumes:
      - ./nitter.conf:/src/nitter.conf:ro
      - ./sessions.jsonl:/src/sessions.jsonl:ro
      - ./cache:/src/cache
    depends_on:
      - nitter-redis
    restart: unless-stopped
```

### 优化说明

1. **禁用速率限制**：`ENABLE_RATELIMIT=false` 禁用了Nitter内部的速率限制，允许更高频率的请求。
2. **启用Tor代理**：`TOR_PROXY_ENABLED=true` 启用了Tor代理，可以避免Twitter的IP封锁。
3. **增加请求限制**：`REQ_LIMIT=1000` 将请求限制从默认值提高到1000，允许更多并发请求。

### 应用优化

我们还对轮询引擎进行了以下优化：

1. **降低批处理大小**：将`BATCH_SIZE`从10降低到5，减少瞬时并发压力。
2. **降低请求速率**：将`REQUEST_RATE`设置为2，避免触发Nitter实例的限制。
3. **实现令牌桶限流**：使用令牌桶算法精确控制请求速率，避免请求波峰。
4. **智能错误处理**：实现了多层错误防御机制，包括实例健康度跟踪、指数退避策略等。

### 重启优化后的Nitter容器

我们提供了脚本来重启优化后的Nitter容器：

- Linux/Mac: `nitter/config/restart_nitter.sh`
- Windows: `nitter/config/restart_nitter.bat`

运行相应脚本即可应用优化配置并重启Nitter容器。 