简体中文 | [English](README.en.md)

# DistributedZKRuntime

一个使用 Go 实现的分布式任务运行时，由单个 Coordinator 和多个 Worker 组成，
通过基于 TLS 1.3/mTLS 的 gRPC 通信，并使用 SQLite 持久化任务状态。
当前 ZK 工作负载使用 BN254 上的 Groth16 证明 MiMC 原像关系。
Worker 生成证明，Coordinator 独立验证后，才将任务成功完成的状态持久化。

## 执行模型

```text
Client -- SubmitJob --> Coordinator -- ExecuteJob --> selected Worker
                           |                           |
                     SQLite JobRecord           local WitnessStore
                           |                           |
                     verify proof <--------------- Groth16 proof
                           |
                     persist terminal state --> Client response
```

- `SubmitJob` 是同步接口：调用方等待任务终态响应或错误。
  Coordinator 在该 RPC 存续期间持有 JobID 的执行所有权，内部通过异步 RPC 执行尝试，
  每个执行会话最多尝试两次。
- Worker 进行注册并发送心跳。Coordinator 对标记为存活的 Worker 进行轮询调度，
  而非由 Worker 从任务队列拉取任务。存活状态通过周期性检查更新；
  恢复机制结合就绪通知、定时退避和扫描。
- 每次尝试先持久化任务规格和单调递增的 AttemptID，再发布对应的内存租约。
  父上下文仍有效时，基础设施故障或租约到期可以触发重试。
  有效的任务级 `Failed`/`Cancelled` 响应属于终态；父上下文取消会终止该次提交。
- 结果身份校验及租约/代次隔离检查先于证明验证。
  持久化终态前会再次执行最终接受检查，防止在验证期间发生取消或租约到期后仍接受结果。
  该机制隔离的是结果接受，不保证远端 Worker 恰好执行一次。
- SQLite 保存 JobID、状态、尝试、Worker 和任务规格。
  启动时，将有效的 `Running` 记录标记为 `Recovering`，恢复调度器复用原执行路径。
  Worker 注册表、活动任务所有权、租约和冷却状态仍保存在内存中。
  证明输出不会被持久化以供后续结果查询。
- Worker 的证明并发数受 `ProvingLimiter` 限制，容量已满时返回 `ResourceExhausted`。
  取消和截止时间检查会拒绝无效的迟到输出，但无法抢占正在执行的 Groth16 调用；
  该调用会持续占用证明槽位，直到执行结束。

当前实现仅支持单个 Coordinator，不具备 Coordinator 副本机制，也不是提供持久化
“恰好执行一次”保证的任务系统。相关源码见 [cmd/coordinator](cmd/coordinator)、
[cmd/worker](cmd/worker) 和 [internal/runtime](internal/runtime)。

## 密码学工作负载

生产任务 `zk_preimage_prove` 证明 `MiMC(secret) == digest`：secret 为私密值，
digest 为公开值。在固定的依赖版本下，当前 BN254/R1CS 电路有 **331 个约束**。
公开载荷仅包含 `witness_ref` 和十进制 `digest`；Worker 从本地 witness store 解析私密值。
Coordinator 和 Worker 均执行严格的载荷校验。

[internal/zk](internal/zk) 实现 Setup、密钥加载、证明生成和验证。
独立的 [MiMC Chain 实验](internal/zk/experimental/mimcchain) 支持固定长度 N=1,2,8,32，
**尚未接入生产任务调度**：

| 规模 | 现有证据 |
|---|---|
| N=1 | 与生产兼容的摘要计算及电路正确性验证 |
| N=2 | 真实 Groth16 正确性验证，以及经过独立审查的 CPU 试验 |
| N=8 | 一次真实 CPU Groth16 试验，已独立验收；2,641 个约束 |
| N=32 | 仅编译校准；进一步工作仍为可选且已延期 |

目前没有生产级可扩展链证明执行路径。GPU/CUDA 不在当前项目路线图中。

### 已验收的 CPU 试验

这次 N=8 执行使用了已审查的进程监督、冻结策略与源码绑定、Barrier 同步，
以及 G4/G5/D2 证据检查。执行了 Setup 一次、Prove 两次、成功 Verify 两次，
另一次针对错误 digest 的 Verify 被拒绝。没有进行自动重试。
独立 D3 审计已验收该试验，没有验收阻断项，结论为 `PASS WITH FOLLOW-UPS`。

| 指标 | 历史 N=2 | 已验收 N=8 |
|---|---:|---:|
| 约束数量 | 661 | 2,641 |
| Setup | 54.18 ms | 204.938 ms |
| Prove | 9.52 ms | 29.743 ms |
| Verify | 0.50 ms | 0.615 ms |

每项计时均为一次被计时调用的观测值，不是两次 Prove/Verify 调用的统计平均值。
可比的计时代码块仅支持描述性比较，不支持置信区间、吞吐量声明或预测性的规模增长规律。

N=8 在共同采样时点统计的进程组 RSS 峰值为 **21,484 KiB**，
包括一个 COMPLETE 就绪阶段样本和六个 COMPLETE 工作负载阶段样本。
未观察到资源停止条件触发或受管理进程残留。
有限采样不能确定整个运行期间的真实峰值，也不等于由 cgroup 强制执行的硬内存限制；
用户态 watchdog 不提供硬实时保证。

外部证据标识为 `p4-d2/n8-cpu-pilot-20261021T045408Z` 和
`p4-d3/n8-evidence-review-20261021T052144Z`。它们是本地归档引用，
不是公开下载链接，也不是构建前置条件。完整原始证据及密钥材料保留在 Git 之外。
今后的任何 Groth16 试验都需要单独明确授权、新的证据路径和资源门槛；
已验收的执行记录不构成重放指令。详见[基准测量合同](docs/benchmark-contracts.md)。

## 仓库结构

```text
cmd/client/                    Demo submission and mTLS client
cmd/coordinator/               Scheduling, leases, fencing, SQLite and recovery
cmd/worker/                    Execution service, heartbeat and shutdown
cmd/zksetup/                   Production preimage Groth16 key generation
internal/runtime/              Tasks, executor, deadlines, witnesses and admission
internal/zk/                   Production circuit, proof and payload validation
internal/zk/experimental/       Standalone MiMC Chain research and gated pilots
internal/rpcauth/               TLS identities and RPC authorization
internal/metrics/               Semantic metrics and Prometheus backend
internal/testutil/testpki/      Ephemeral test certificates
internal/workerheartbeat/      Heartbeat/re-registration protocol step
proto/                         Protobuf source and generated Go RPC bindings
scripts/dev-pki/                Explicit development PKI bootstrap and checks
docs/                          Test and measurement contracts
```

## 开发指南

使用 [go.mod](go.mod) 声明的 Go **1.27.1**。依赖包括 gnark、gnark-crypto、
grpc-go 和 modernc SQLite。需要主动进行构建检查时，可在仓库根目录执行
`go build -mod=readonly ./...`。模块缓存已完整准备好时，设置
`GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off` 可避免下载。
构建不会生成密钥、配置 witness 或启动服务。

运行服务还需要匹配的 preimage PK/VK 密钥对、受信任的 CA、
与角色和端点匹配的 mTLS 证书、Coordinator 身份允许列表、
Worker 本地 witness 文件，以及可写的 SQLite 路径。现有参数包括：
Coordinator 的 `-listen`、`-db`、`-verifying-key`、`-allow-client`、`-allow-worker`；
Worker 的 `-id`、`-advertise`、`-proving-key`、`-witness-file`、
`-max-concurrent-proofs`；以及 RPC 程序上的 `-tls-cert`、`-tls-key`、`-tls-ca`。
Worker witness 文件默认路径为 `private/witnesses.json`，需显式配置该文件或覆盖路径。
Client 仅提交固定的演示任务，并非通用任务 CLI。

[cmd/zksetup](cmd/zksetup) 默认在 `zk-artifacts/preimage` 下生成密钥，并执行真实 Setup。
[开发用 PKI 工具](scripts/dev-pki/bootstrap.sh) 要求显式指定 `--out`，
并依赖 OpenSSL 3、Go 和 Git。两者均不属于构建检查。
不要仅为运行测试而重新生成或覆盖现有部署中相互匹配的材料。
RPC 路径强制要求 mTLS；独立的 metrics HTTP 监听器默认绑定回环地址，
并非经过身份认证的任务 RPC 服务。

## 测试与研究流程

运行测试前，请阅读[当前测试执行合同](docs/testing.md)：
普通测试包含真实密码运算和集成测试夹具。
进行性能工作前，请阅读[基准测量合同](docs/benchmark-contracts.md)；
实验需要明确选择执行项，并设置预算和资源监督。
历史证据保留在 Git 之外。本地 witness、凭据、生成的密钥、数据库和二进制文件不得提交。
