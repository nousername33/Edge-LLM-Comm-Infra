# Edge-LLM-Infra

面向树莓派、RK、昇腾等异构端侧设备的 Linux C++ 分布式大模型中间件系统，为 LLM/ASR/TTS 等边缘 AI 业务提供轻量化、可扩展、可维护的分布式运行时基础设施。

## 项目背景

端侧设备（树莓派、昇腾、RK 系列等）面临算力分散、通信受限、动态扩展困难等问题。Edge-LLM-Infra 通过分层架构设计，将通信、调度、业务逻辑解耦，实现多节点 AI 业务的高效协同推理。

## 架构概览

```
┌─────────────────────────────────────────────────────────────┐
│                    node (业务节点层)                          │
│   llm_task / asr_task / tts_task ...                        │
│   继承 StackFlow，重写 setup/exit/pause/taskinfo              │
├─────────────────────────────────────────────────────────────┤
│                  infra-controller (信道管理 + 节点基类)         │
│   llm_channel_obj: ZMQ 连接池、订阅管理、JSON 协议封装          │
│   StackFlow: 事件驱动基类、RPC 注册、任务生命周期管理             │
├─────────────────────────────────────────────────────────────┤
│                  unit-manager (全局调度中心)                    │
│   服务发现 · 任务分发 · action 路由 · KV 内存库                  │
│   多协议网关 (TCP→ZMQ 转换) · 主从 Reactor TCP 框架              │
├─────────────────────────────────────────────────────────────┤
│                  network (TCP 网络库)                         │
│   主从 Reactor · epoll 多路复用 · 线程池 · Buffer 管理           │
├─────────────────────────────────────────────────────────────┤
│                  hybrid-comm (混合通信中间件)                   │
│   ZMQ 多模式封装 (PUB/SUB · PUSH/PULL · RPC)                   │
│   工厂模式动态切换 · 超时重连 · 序列化/反序列化                    │
└─────────────────────────────────────────────────────────────┘
```

## 模块说明

### 1. hybrid-comm — 底层混合通信中间件

基于 ZeroMQ 封装的多模式通信层，通过工厂模式统一创建不同通信策略的 Socket。

| 文件 | 功能 |
|------|------|
| `pzmq.hpp` | 核心通信类，支持 PUB / SUB / PUSH / PULL / RPC(REQ/REP) 五种模式，提供 action 注册/调用、超时控制、自动重连、惰性创建等能力 |
| `pzmq_data.h/.cpp` | RAII 封装 ZMQ 消息，提供序列化/反序列化（长度前缀拆包）和参数提取接口 |

**关键设计：**
- **工厂模式**：`creat(url, mode)` 根据 mode 分发到 `creat_pub` / `creat_rep` / `subscriber_url` 等具体实现，隐藏 ZMQ 底层细节
- **RPC 框架**：自定义 `ZMQ_RPC_FUN` / `ZMQ_RPC_CALL` 模式，Server 端注册 action→callback 映射，Client 端两帧发送（action + data）后阻塞等待回复
- **有状态/无状态分离**：PUB/PUSH 为无状态发送端；SUB/PULL/RPC-Server 为有状态接收端，独立线程运行 zmq_event_loop
- **超时与重连**：SNDTIMEO/RCVTIMEO 双端超时控制，RECONNECT_IVL 渐进式重连（100ms→1s）

### 2. network — TCP 网络库（主从 Reactor）

完整的 muduo 风格 Reactor TCP 框架，用于构建高并发 TCP 服务。

```
EventLoop (one per thread)
  ├── Poller (epoll 封装)
  ├── Channel (fd + events + callbacks)
  └── wakeupFd (跨线程唤醒)

TcpServer
  ├── Acceptor (监听 socket + accept)
  ├── EventLoopThreadPool (主从线程池)
  └── TcpConnection (每条连接的状态机)
       ├── Socket (RAII 封装 fd)
       ├── Channel (注册到 EventLoop)
       ├── Buffer (输入/输出缓冲)
       └── boost::any context (用户上下文)
```

| 核心类 | 职责 |
|--------|------|
| `EventLoop` | 单线程 Reactor 核心，epoll 事件循环 + 跨线程任务队列 |
| `Poller` | epoll(4) 封装，管理 Channel 的注册/注销/事件分发 |
| `Channel` | fd 事件抽象，绑定 Read/Write/Close/Error 回调 |
| `TcpServer` | TCP 服务器，支持多线程池 (setThreadNum)，连接回调/消息回调 |
| `TcpConnection` | 单连接状态机 (Disconnected→Connecting→Connected→Disconnecting)，Buffer 读写 |
| `Acceptor` | 监听 listen fd，accept 新连接并回调 |
| `EventLoopThreadPool` | 主从线程池，Round-Robin 分配连接到工作线程 |

### 3. infra-controller — 信道管理与节点基类

向上屏蔽底层通信细节，向下提供标准化业务接口。

#### llm_channel_obj（信道管理）

```
zmq_ 连接池:
  [-1] → ZMQ_PUB   (对外广播推理结果)
  [-2] → ZMQ_PUSH  (点对点推送给外部用户)
  [0..N] → ZMQ_SUB (按 work_id 订阅不同数据源)
```

- **统一 JSON 协议**：`request_id` / `work_id` / `object` / `data` / `error` / `created` 标准化字段
- **动态订阅管理**：通过 `work_id` 正则匹配（`unit.123`），自动查询 sys 获取通信 URL 后建立 SUB 连接
- **观察者模式**：`subscriber_work_id(work_id, callback)` 注册回调，收到消息后自动解析 JSON 并触发业务处理
- **流式输出支持**：控制 `enstream_` / `enoutput_` 开关，支持流式/非流式两种结果返回模式

#### StackFlow（业务节点基类）

- **事件驱动**：eventpp 线程安全事件队列，RPC 调用自动转换为 `EVENT_SETUP/EXIT/PAUSE/TASKINFO` 事件处理
- **虚函数接口**：`setup()` / `exit()` / `pause()` / `taskinfo()` 供业务节点重写
- **注册/注销机制**：`sys_register_unit()` 向 unit-manager 申请 work_id 和通信端口，自动创建 llm_channel_obj
- **内存安全**：`weak_ptr` 解决 channel ↔ task 循环引用，`sys_release_unit` 确保资源正确释放
- **系统调用封装**：`sys_sql_select/set/unset` 操作全局 KV 数据库

#### StackFlowUtil（工具函数）

- `sample_json_str_get()` — 轻量 JSON 提取（无外部依赖的字符串解析）
- `sample_get_work_id_num/name()` — work_id 格式 `unit.123` 的解析
- `decode_stream()` — 流式数据分片重组（支持乱序到达）
- `unit_call()` — 便捷的跨节点 RPC 调用封装
- `unicode_to_utf8()` — Unicode 码点转 UTF-8

### 4. unit-manager — 全局调度中心

系统的"大脑"，提供服务发现、任务分发、协议转换等功能。

#### 核心组件

| 组件 | 文件 | 职责 |
|------|------|------|
| Global KV Store | `all.h` + `main.cpp` | `std::unordered_map<std::string, std::any>` + `pthread_spinlock` 实现的线程安全内存数据库，存储所有 unit 元信息 |
| Remote Server | `remote_server.cpp` | sys RPC 服务端，注册 `sql_select/set/unset`、`register_unit`、`release_unit` action |
| ZMQ Bus | `zmq_bus.cpp` | 多协议网关基类，PULL socket 接收数据后路由到 `unit_action_match` |
| TCP Session | `session.h` + `tcp_comm.cpp` | TCP→ZMQ 桥接，每个 TCP 连接自动分配 PULL 端口，消息通过 `on_data → unit_action_match` 处理 |
| Action Router | `remote_action.cpp` | 根据 `action` 字段分发：`inference` → PUB 广播到业务节点；其他 → RPC 调用对应 unit |
| Unit Data | `unit_data.cpp` | 存储每个 unit 的 PUB 广播地址，提供 `send_msg` 将消息推送给订阅该 unit 的业务节点 |
| Config | `config.cpp` | 从 `master_config.json` 加载启动配置 |

#### 请求路由流程

```
外部 TCP 客户端
      │
      ▼
TcpServer (port 10001, 2 threads)
      │
      ▼ onConnection → new TcpSession
TcpSession (zmq_bus_com)
      │ work → ZMQ_PULL (ipc:///tmp/llm/{port}.sock)
      ▼ on_data → unit_action_match(com_id, json_str)
      │
      ├── action="inference" ──→ zmq_bus_publisher_push(work_id)
      │                              │
      │                              ▼ PUB broadcast
      │                         llm_channel_obj (SUB) → llm_task::inference
      │
      └── action="setup/exit/..." ──→ remote_call(com_id, json_str)
                                          │
                                          ▼ RPC call
                                     StackFlow::rpc_ctx_ (REP)
                                          │
                                          ▼ enqueue event
                                     event_queue → _setup/_exit/_pause/_taskinfo
                                          │
                                          ▼ virtual function
                                     llm_llm::setup/exit/pause/taskinfo
```

#### 端口管理

- TCP 监听端口：`config_tcp_server`（默认 10001）
- ZMQ 动态端口范围：`config_zmq_min_port` ~ `config_zmq_max_port`（默认 5010~5555）
- 每个 TCP 连接 / unit 注册时从端口池分配，释放时归还

### 5. node — 业务节点实现

业务开发者继承 `StackFlow` 并重写虚函数即可接入系统。

#### 示例：LLM 节点 (`node/test/src/main.cpp`)

```
llm_llm : StackFlow("llm")
  ├── llm_task_[N] (每个 work_id 对应一个 task 实例)
  │     ├── model_ / response_format_ / inputs_
  │     ├── load_model() → parse_config()
  │     └── inference() → out_callback_(data, finish)
  │
  ├── setup()   → 创建 llm_task → load_model → subscriber_work_id → 订阅用户输入
  ├── exit()    → 停止 task → stop_subscriber → 清理资源
  ├── pause()   → (继承默认实现)
  └── taskinfo()→ 返回 task 列表或指定 task 的详细信息
```

**关键设计模式：**
- **LLM 管理多个 TASK**：一个 llm_llm 节点可同时运行多个 llm_task（task_count 限制），每个 task 绑定独立的 channel
- **weak_ptr 防循环引用**：`llm_task ↔ llm_channel_obj` 通过 `weak_ptr` 相互持有，析构时安全释放
- **流式输出重组**：`decode_stream()` 处理分片数据（index + delta + finish），按 index 排序后拼接

## 项目目录结构

```
Edge-LLM-Infra/
├── build.sh                          # 一键环境安装脚本
├── hybrid-comm/                      # 底层混合通信中间件
│   ├── include/
│   │   ├── pzmq.hpp                  # ZMQ 多模式封装
│   │   ├── pzmq_data.h               # 消息 RAII 封装
│   │   └── libzmq/                   # ZMQ C 头文件
│   └── src/
│       └── pzmq_data.cpp
├── network/                          # 主从 Reactor TCP 网络库
│   ├── include/network/
│   │   ├── EventLoop.h               # Reactor 核心
│   │   ├── Poller.h                  # epoll 封装
│   │   ├── Channel.h                 # fd 事件抽象
│   │   ├── TcpServer.h              # TCP 服务器
│   │   ├── TcpConnection.h          # TCP 连接
│   │   ├── Acceptor.h               # 连接接收器
│   │   ├── TcpClient.h             # TCP 客户端
│   │   ├── EventLoopThread.h        # 事件循环线程
│   │   ├── EventLoopThreadPool.h    # 线程池
│   │   ├── Buffer.h                 # 网络缓冲区
│   │   ├── InetAddress.h            # 地址封装
│   │   ├── Socket.h                 # Socket RAII
│   │   └── Callbacks.h             # 回调类型定义
│   └── src/
│       └── CMakeLists.txt
├── infra-controller/                 # 信道管理 + 业务节点基类
│   ├── include/
│   │   ├── channel.h                 # 信道管理 (llm_channel_obj)
│   │   ├── StackFlow.h              # 业务节点基类
│   │   └── StackFlowUtil.h          # 工具函数
│   ├── src/
│   │   ├── channel.cpp
│   │   ├── StackFlow.cpp
│   │   └── StackFlowUtil.cpp
│   └── CMakeLists.txt               # 编译为 libstackflow.a
├── unit-manager/                     # 全局调度中心
│   ├── include/
│   │   ├── all.h                     # 全局 KV + 宏定义
│   │   ├── zmq_bus.h                # 多协议网关基类
│   │   ├── session.h                # TCP Session
│   │   ├── remote_server.h          # 服务发现 RPC Server
│   │   ├── remote_action.h          # Action 路由
│   │   └── unit_data.h              # Unit 元数据管理
│   ├── src/
│   │   ├── main.cpp                  # 入口
│   │   ├── zmq_bus.cpp
│   │   ├── remote_server.cpp
│   │   ├── remote_action.cpp
│   │   ├── unit_data.cpp
│   │   ├── config.cpp
│   │   └── tcp_comm.cpp
│   ├── master_config.json           # 启动配置
│   └── CMakeLists.txt
├── node/                             # 业务节点
│   └── test/
│       ├── src/main.cpp             # LLM 节点示例
│       └── CMakeLists.txt
├── sample/                           # 示例与压测工具
│   ├── test.py                      # TCP 客户端示例
│   ├── stress.py                    # TCP 并发压测工具
│   └── CMakeLists.txt               # ZMQ 通信示例
├── docker/                           # Docker 部署
│   ├── build/
│   │   ├── base.dockerfile          # 基础镜像
│   │   └── install/                  # 第三方库安装脚本
│   └── scripts/
│       ├── llm_docker_run.sh        # 容器启动
│       └── llm_docker_into.sh       # 容器进入
├── utils/
│   ├── json.hpp                     # nlohmann/json (header-only)
│   └── sample_log.h                 # 彩色日志宏
├── thirds/                           # 第三方依赖
└── install/                          # 安装输出目录
```

## 构建与运行

### 环境依赖

- Ubuntu 20.04+
- GCC/G++ 9+（支持 C++17）
- CMake 3.12+
- ZeroMQ (libzmq3-dev)
- Boost (libboost-all-dev)
- Google Glog
- eventpp
- simdjson

### 一键安装

```bash
sudo bash build.sh
```

### 编译

```bash
# 1. 编译通信框架静态库
cd infra-controller/build && cmake .. && make

# 2. 编译网络库
cd network/src/build && cmake .. && make

# 3. 编译 unit-manager
cd unit-manager/build && cmake .. && make

# 4. 编译业务节点 (LLM 示例)
cd node/test/build && cmake .. && make
```

### Docker 部署

```bash
# 构建镜像
docker build -t llm:v1.0 -f docker/build/base.dockerfile .

# 启动容器
bash docker/scripts/llm_docker_run.sh

# 进入容器
bash docker/scripts/llm_docker_into.sh
```

### 运行

```bash
# 启动 unit-manager (全局调度中心)
./unit-manager/build/unit_manager

# 启动 LLM 业务节点
./node/test/build/test

# TCP 客户端测试
python3 sample/test.py --host localhost --port 10001

# TCP 并发压测 (10000+ 连接)
python3 sample/stress.py --host localhost --port 10001 --connections 10000 --threads 100
```

## JSON 通信协议

所有组件间通信使用统一 JSON 协议，以换行符 `\n` 分隔帧：

```json
{
    "request_id": "llm_001",
    "work_id": "llm.0",
    "created": 1710000000,
    "action": "setup",
    "object": "llm.setup",
    "data": {
        "model": "DeepSeek-R1-Distill-Qwen-1.5B",
        "response_format": "llm.utf-8.stream",
        "enoutput": true
    },
    "error": {
        "code": 0,
        "message": ""
    }
}
```

| 字段 | 说明 |
|------|------|
| `request_id` | 请求标识，贯穿整个调用链路，用于追踪和响应匹配 |
| `work_id` | 工作单元标识，格式 `{unit_name}.{number}`，如 `llm.0` |
| `action` | 操作类型：`setup` / `exit` / `pause` / `taskinfo` / `inference` |
| `object` | 业务对象类型，如 `llm.utf-8.stream` |
| `data` | 业务数据载荷，结构由具体业务定义 |
| `error` | 错误信息，`code=0` 表示成功 |

## 关键设计模式

| 模式 | 应用位置 | 说明 |
|------|----------|------|
| **工厂模式** | `pzmq::creat()` | 根据 mode 创建不同 ZMQ Socket |
| **观察者模式** | channel 订阅回调、event_queue | 回调函数 + 事件监听解耦 |
| **模板方法** | `StackFlow` 虚函数 | 基类定义骨架，子类重写业务逻辑 |
| **策略模式** | ZMQ 五种通信模式 | PUB/SUB、PUSH/PULL、RPC 动态切换 |
| **主从 Reactor** | `network/` | Main Reactor accept + Sub Reactor I/O |
| **RAII** | `pzmq_data`、`Socket`、`unique_ptr` | 自动资源管理 |
| **闭包** | `std::bind` + `weak_ptr` | 回调中捕获上下文，防止循环引用 |

## 性能特性

- **ZMQ IPC 进程间通信**：单机内部使用 Unix Domain Socket，避免 TCP 协议栈开销
- **主从多 Reactor**：2 个工作线程处理 I/O，支持 10000+ TCP 并发连接
- **线程安全 KV 存储**：`pthread_spinlock` 自旋锁，低竞争场景下性能优于 mutex
- **Zero-copy 日志**：带级别过滤的宏定义日志，Release 模式可关闭调试输出
- **端口池复用**：动态分配与回收 ZMQ 端口，支持大规模 unit 注册

## License

SPDX-License-Identifier: MIT
