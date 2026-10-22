简体中文 | [English](README.en.md)

# DistributedZKRuntime

## 项目简介

DistributedZKRuntime 是一个用 Go 实现的分布式任务运行时，采用单 Coordinator、多 Worker 模型，通过 gRPC/mTLS 通信，并使用 SQLite 保存任务状态。它以 BN254 上的 MiMC 原像 Groth16 证明为工作负载：Worker 生成证明，Coordinator 独立验证计算结果。

项目关注远端计算中的结果接受问题：请求超时、Worker 故障或 Coordinator 重启之后，系统如何区分仍然有效的结果与已经过期的尝试？下面的六个设计取舍解释了证明验证、任务身份和资源生命周期如何共同回答这个问题。

## 架构概览

Client 同步向 Coordinator 提交任务，Coordinator 对存活的 Worker 进行轮询调度，并管理执行尝试和任务状态。Worker 执行任务并返回结果；对于 ZK 成功结果，Coordinator 独立验证证明，并在将终态持久化到 SQLite 后向 Client 返回响应。SQLite 保存的任务状态也为重启恢复提供依据。

## 核心能力

- **分布式执行：** 注册、心跳与存活检测，支持 hash、sleep 和 preimage 证明任务。
- **可靠性：** AttemptID、lease、结果 fencing、故障重试、取消、SQLite 恢复和有界关闭。
- **信任与容量：** mTLS 授权、公开载荷校验、独立 Groth16 验证和 Worker 证明并发限制。
- **观测与实验：** 语义化指标、Prometheus 后端和独立 CPU MiMC Chain 实验。

## 设计取舍

### 1. Coordinator 不持有 Worker 的 secret，如何确认结果正确？

把 secret 放进提交 payload 会让 Coordinator 获得本应留在 Worker 的私密值。生产任务 `zk_preimage_prove` 因而只提交公开 digest 和 `witness_ref`，Worker 通过本地 WitnessStore 找到 secret。[ZKPreimageTask](internal/runtime/zk_preimage_task.go) 用它生成 `MiMC(secret) == digest` 的 Groth16 证明；固定依赖版本下，这个 BN254/R1CS 电路有 331 个约束。

Worker 自报 Succeeded 并不足以确认关系成立。[Coordinator](cmd/coordinator/zk_preimage_verifier.go) 使用匹配的 verifying key 和公开 digest 实际调用 Verify，无需获得 secret 或重复私密计算。这样，私密输入的持有与计算结果的检查由不同组件承担：Worker 负责生成证明，Coordinator 依据证明决定是否认可公开关系。

### 2. 有了 mTLS，为什么还要绑定 WorkerID 并检查 RPC 权限？

mTLS 能确认连接对端的身份，却不能单独决定它有权执行哪些操作。项目要求 TLS 1.3/mTLS，并通过[方法授权](internal/rpcauth/policy.go) 区分角色：Client 提交任务，Worker 注册和发送心跳，只有配置的 Coordinator 可以调用 Worker 的 ExecuteJob。连接身份与业务权限因此分别检查。

请求里的 WorkerID 仍由对端填写，注册和心跳必须将它与证书中的 Worker 身份绑定。注册还会[检查 advertised 地址与证书主机名是否匹配](cmd/coordinator/transport.go)；出站连接校验 DNS/IP SAN 和预期 URI 身份。调度时选择的逻辑 Worker，因而与实际通信的身份和端点保持对应，而不是只信任一个请求字段。

### 3. 为什么数学上正确的 Worker 结果，仍可能被拒绝？

假设 Attempt 1 的 lease 已到期，Coordinator 取消该尝试并开始 Attempt 2，旧 Worker 却仍在完成同步证明。它随后返回的证明即使正确，也不该覆盖新尝试的结果。JobID 标识任务，单调 AttemptID 区分尝试代次，lease 则限定每次尝试的有效期；证明正确性与尝试有效性是两个独立条件。

[执行路径](cmd/coordinator/execution.go) 先检查响应 JobID、AttemptID 和当前 lease，再验证证明。Verify 期间也可能发生取消或租约到期，因此 `persistTerminalResult` 在保存终态前再次检查父上下文与 lease，并在同一互斥锁下完成检查和 Save，排除本地代次在中间被替换。旧计算可能与重试重叠，但迟到的正确证明不会因此自动成为可接受结果。

### 4. Coordinator 崩溃后，如何延续任务的尝试身份？

如果 AttemptID 只在内存中，重启后从 1 开始就可能重新使用旧代次。SQLite 的 JobRecord 因而保存任务代次和完整任务规格。[分配尝试](cmd/coordinator/attempts.go) 时使用 `nextAttempt = max(inMemoryAttempt, persistedAttempt) + 1`，先保存 JobRunning 记录，再发布内存 AttemptID 和 lease，随后派发；保存失败就不会发布该次尝试。已持久化的 Attempt 7 在新进程中会继续为 Attempt 8。

启动恢复将通过任务规格校验的 Running 记录标记为 Recovering，保留代次和输入，直到真正重新执行才分配新代次。[恢复入口](cmd/coordinator/jobs.go) 获取 JobID 所有权后重新读取记录，再转换为 JobSpec，复用普通提交的 `executeJobFromSource`。恢复因此沿用相同的重试、证明验证和终态接受规则，而不是另写一套容易分叉的执行逻辑。

### 5. 取消 Groth16 任务后，为什么不立即释放并发名额？

Go context 取消是协作信号，当前同步 Groth16 Prove 不会因此自动停止。请求结束或 Coordinator 开始重试时，旧证明仍可能占用 CPU。如果立即释放名额，新计算就能在旧计算尚未完成时进入，连续取消会让真实并发数突破配置容量。

Worker 共享的 [ProvingLimiter](internal/runtime/proving_limiter.go) 因而将名额保留到任务真正退出，容量满时直接返回 ResourceExhausted。[ZKPreimageTask](internal/runtime/zk_preimage_task.go) 在 Prove 前检查取消，并在证明和编码完成后检查结果截止时间；迟到输出可以丢弃，尚未结束的计算却仍计入容量。Worker 关闭时也等待实际 handler 退出，使资源归还以计算完成为依据。

### 6. CPU Groth16 实验退出码为 0，为什么还不能算成功？

正常退出只说明程序报告了成功，并不能说明它执行了预期密码学操作或测量了正确窗口。[CPU pilot](internal/zk/experimental/mimcchain/pilot_n2_test.go) 记录实际操作的开始、结束、次数和结果；已验收的 N=8 流程还将源码、二进制与策略绑定到同一次运行。Barrier 确认目标就绪后才释放工作，使采样与执行顺序可以核对。

资源证据保留进程组 RSS 原始样本，并检查受管理进程的所有权和清理结果。[最终收集器](tools/bench/d2_collect.py) 综合操作记录、来源绑定、资源观测和截止条件判定接受，缺证或矛盾不会因为退出码为零就通过。独立检查核对的是源码绑定的 Go Verify 记录，不另行重跑数学验证；已验收 N=8 pilot 展示了这种从实际调用到最终接受的证据链。

## CPU 实验

独立的 [MiMC Chain](internal/zk/experimental/mimcchain/circuit.go) 使用固定长度哈希链，与生产 preimage 任务分开，尚未接入任务调度。N=2 与 N=8 均已完成真实 CPU Groth16 试验，其中 N=8 pilot 已独立验收。

| 指标 | N=2 | N=8 |
|---|---:|---:|
| 约束数量 | 661 | 2,641 |
| Setup | 54.18 ms | 204.938 ms |
| Prove | 9.52 ms | 29.743 ms |
| Verify | 0.50 ms | 0.615 ms |

每项计时都是单次调用的观测值，不是统计平均值，也不代表生产吞吐量或统计扩展规律。N=8 共同采样的进程组 RSS 峰值为 **21,484 KiB**；有限采样不等于完整运行峰值或硬内存限制。测量定义见[基准测量合同](docs/benchmark-contracts.md)。

## 快速开始

使用 [go.mod](go.mod) 声明的 Go **1.27.1**；完整信号测试与实验工具以 Linux 为平台。在仓库根目录构建：

```sh
go build -mod=readonly ./...
```

实际运行还需要受信任 CA 与匹配角色、端点的 mTLS 证书、身份允许列表、匹配的 Groth16 proving/verifying keys、Worker 本地 witness 数据和可写的 SQLite 路径。构建不会自动准备这些材料。测试入口与环境要求见[测试执行合同](docs/testing.md)。

## 范围与限制

当前运行时采用单 Coordinator、多 Worker 模型，通过 SQLite 持久化任务状态并支持重启恢复，尚不支持多 Coordinator 高可用。AttemptID 与 lease 共同约束结果接受；旧计算可能与重试重叠，因此结果 fencing 不等于恰好执行一次的保证，计算是否执行与结果是否被接受是两个不同问题。

生产 ZK 工作负载是在 BN254 上证明 MiMC 原像关系的 Groth16 任务。MiMC Chain 是独立实验，N=2 与 N=8 已完成 CPU pilot，N=32 仅完成编译校准。这些实验计时描述相应操作的单次观测，不代表生产系统的吞吐量。
