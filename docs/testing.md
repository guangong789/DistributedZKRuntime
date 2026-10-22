# 当前测试执行合同

本文描述现有源码的行为，并未实现新的测试分层，也未新增构建标签、测试选择器、环境开关或 CI 作业。
架构说明见 [README](../README.md)；高开销操作与计时定义见[基准测量合同](benchmark-contracts.md)。

## 常规测试实际执行的内容

未设置实验启用开关时，常规包测试和 `go test ./...` 既包含确定性的状态与生命周期检查，**也包含**真实的生产 N=1 Groth16
Setup/Prove/Verify、本机 gRPC/mTLS、临时 SQLite 数据库，以及受控的 Unix 信号子进程。
实验包的默认检查还会对 N=1,2,8 的小型 MiMC Chain 电路执行 Compile/Solve，但不会运行实验 Groth16。
信号测试在 Windows 上会跳过 Unix 专属检查。

这不是一套仅包含快速单元测试的测试集。测试会创建临时密钥、证书和数据库夹具，无需使用工作目录中的部署密钥或私有 witness 文件。
部分辅助函数会执行 Setup 或生成证明；在改变夹具生命周期或将其用于计时之前，应先检查函数实现。

| 文档分类 | 当前实际选择方式 |
|---|---|
| 状态、运行时与正确性回归 | 常规 `Test` 选择 |
| 真实集成与生产密码学 | 同样通过常规 `Test` 选择；尚未统一改为显式启用 |
| 实验校准、正确性与性能 | 使用下文列出的精确启用开关和选择器 |
| 历史实验 | 保存在外部归档中；不是常规测试入口 |

基准测试需要显式使用 `-bench` 选择。避免使用 `-bench .`：多个入口会执行真实证明，而 Go 的常规基准校准可能反复运行夹具和高开销操作。
Go 测试命令也可能写入缓存；`-mod=readonly` 只禁止修改模块文件，并不禁止所有文件系统活动。

## 必须保留的回归合同

以下文件是必须保护的测试示例，不是待删除清单。使用相似夹具的测试往往保护不同的执行边界。

| 合同 | 代表性源码 |
|---|---|
| P0：普通提交耗尽重试后，只能将本次已发布的精确代次持久化为 `Failed`；无 Worker 或未创建尝试时不得改写旧状态；存储错误仍须作为错误返回 | [submit_failure_test.go](../cmd/coordinator/submit_failure_test.go) |
| 所有权、并发 AttemptID 分配、`max(memory, store)+1`、匹配的 lease 与重试上限 | [attempts_test.go](../cmd/coordinator/attempts_test.go), [submit_test.go](../cmd/coordinator/submit_test.go) |
| 响应前持久化终态、完整任务规格与 SQLite 重新打开 | [terminal_persistence_test.go](../cmd/coordinator/terminal_persistence_test.go), [sqlite_store_test.go](../cmd/coordinator/sqlite_store_test.go) |
| 重启恢复、失败执行会话的修复、冷却期与代次/规格核对 | [recovery_failure_test.go](../cmd/coordinator/recovery_failure_test.go), [recovery_schedule_test.go](../cmd/coordinator/recovery_schedule_test.go), [ZK 重启集成](../cmd/coordinator/zk_preimage_recovery_integration_test.go) |
| 区分任务 `Cancelled`、RPC 错误与父上下文取消；持久化取消状态，父上下文取消后不再重试 | [submit_cancellation_test.go](../cmd/coordinator/submit_cancellation_test.go), [Coordinator 超时测试](../cmd/coordinator/timeout_test.go) |
| P2：TimeoutMs 的有效正值边界与严格结果截止时间，包括 Prove 之后的检查；Client 任务/RPC 截止时间仍独立于 lease | [运行时超时测试](../internal/runtime/timeout_test.go), [Worker 超时测试](../cmd/worker/timeout_test.go), [Client 时间预算](../cmd/client/execution_test.go) |
| lease 到期只取消子尝试；迟到或过期的结果，以及 Verify 跨越截止时间的结果，都不得提交 | [result_acceptance_test.go](../cmd/coordinator/result_acceptance_test.go), [proving_transport_test.go](../cmd/coordinator/proving_transport_test.go) |
| 有界证明准入、`ResourceExhausted`，以及取消后仍保留名额直到 Prove 实际返回 | [运行时准入](../internal/runtime/zk_preimage_admission_test.go), [Worker 准入](../cmd/worker/proving_admission_test.go), [Coordinator 准入](../cmd/coordinator/proving_admission_test.go) |
| P1-A：Coordinator 停止接收新工作，在关闭存储前等待 handler、dispatch 和 recovery 退出，并遵守共享关闭预算 | [Coordinator 关闭](../cmd/coordinator/shutdown_test.go), [Coordinator 信号接入](../cmd/coordinator/shutdown_signal_test.go) |
| P1-B：Worker 取消并等待执行与后台客户端退出，保留证明计算的所有权，并关闭原始 listener | [Worker 关闭](../cmd/worker/shutdown_test.go), [后台关闭](../cmd/worker/shutdown_background_test.go), [Worker 信号接入](../cmd/worker/shutdown_signal_test.go) |
| Executor 状态转换、重复准入、优雅/强制关闭，以及阻塞的提交/结果路径 | [executor_test.go](../internal/runtime/executor_test.go), [job_test.go](../internal/runtime/job_test.go) |
| 所有生产 RPC 边界上的 mTLS 证书链/EKU/IP/URI 身份、精确的方法角色与 WorkerID 绑定 | [rpcauth 测试](../internal/rpcauth), [Coordinator 传输](../cmd/coordinator/transport_test.go), [Worker 执行传输](../cmd/worker/execution_transport_test.go), [Client 传输](../cmd/client/transport_test.go) |
| 严格公开载荷、旧记录安全、私有/公开 witness 分离，以及真实证明的接受与拒绝 | [载荷解析器](../internal/zk/preimage_payload_test.go), [恢复载荷](../cmd/coordinator/zk_payload_recovery_test.go), [真实原像证明](../internal/zk/preimage_test.go), [端到端 witness 流程](../cmd/coordinator/zk_preimage_integration_test.go) |
| 指标反映已持久化的状态转换、有界标签与实际证明/验证；测试夹具不能被视为覆盖了更多生产路径 | [指标集成](../cmd/coordinator/metrics_integration_test.go), [指标后端](../internal/metrics/prometheus/metrics_test.go) |

`TestWorkerShutdownListenerClosureObservation` 用 `errors.Is(err, net.ErrClosed)` 检查**原始 listener** 的 `Accept()` 错误。
保持 listener 打开的负对照必须判定观测失败，即使后续清理关闭了该 listener。
仅凭旧端口连接失败，或 Serve 已返回，都不能得出等价结论。

当前重试语义区分有效任务 `Failed`/`Cancelled` 响应（终态）与父上下文仍有效时的 RPC `Canceled`/`DeadlineExceeded`
（基础设施故障，可进入既有重试路径）。Fencing 保护结果接受，不保证远端恰好执行一次。
受控的关闭/重新打开测试不能证明断电耐久性或多副本 Coordinator 的正确性。

## 常规命令：仅在明确选择后执行

以下命令是模板，并非本次文档变更已执行的检查。运行环境应具备声明的 Go 工具链、已缓存的依赖，
并允许本地 socket、临时文件和子进程。在离线环境中，缺少缓存依赖时应失败，而不是下载依赖。

先清理当前 shell 的相关变量，确保选择常规回归：

```sh
unset DZKR_MIMCCHAIN_CALIBRATE DZKR_MIMCCHAIN_GROTH16
unset DZKR_MIMCCHAIN_CPU_PILOT DZKR_MIMCCHAIN_CPU_AUDIT
unset DZKR_MIMCCHAIN_BARRIER_PROBE DZKR_MIMCCHAIN_START_BARRIER
unset DZKR_TEST_COORDINATOR_CHILD DZKR_TEST_COORDINATOR_ARGS
unset DZKR_TEST_WORKER_CHILD DZKR_TEST_WORKER_ARGS
export GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off
```

在仓库根目录选择所需范围，不要习惯性地逐行执行。包级和仓库级命令包含上文说明的真实工作。

```sh
# Small focused state regression.
go test -mod=readonly -count=1 -timeout=2m -run '^TestValidJobTransitions$' ./internal/runtime

# Ordinary package regression, including integration/cryptography.
go test -mod=readonly -count=1 -timeout=10m ./cmd/coordinator

# Ordinary repository regression, including real N=1 cryptography and signals.
go test -mod=readonly -count=1 -timeout=10m ./...

# Focused race check; this is not a substitute for all lifecycle/race coverage.
go test -race -mod=readonly -count=1 -timeout=2m -run '^TestProvingLimiter' ./internal/runtime

# Static Go analysis; may still build analysis inputs/write caches.
go vet -mod=readonly ./...
```

`-timeout` 限制的是测试进程，不是生产任务/RPC/lease 时长，也不是多个包的总墙钟时间；它不构成硬内存限制。
重跑前应先检查失败原因，不得仅因后续运行通过，就将无法解释的失败判为无害。
[test_bootstrap.py](../scripts/dev-pki/test_bootstrap.py) 中的显式开发 PKI 检查独立于 Go 测试选择，
会调用 OpenSSL 和 Go；它们不是临时 ECS 诊断代码。

<a id="experimental-opt-in-gates"></a>

## 实验显式启用门槛

**以下是高开销实验与合成实验的元数据，不是可随意执行的启动指令。**
包路径为 `./internal/zk/experimental/mimcchain`。必须使用精确选择器、经过评审且有界的 supervisor，以及新的外部证据目录。
环境开关本身不提供监督机制。不要在原位置运行归档启动脚本：它们可能包含固定路径，并向历史证据目录写入数据。

| 入口 / 精确 `-run` 值 | 启用开关 | 额外源码要求 / 行为 |
|---|---|---|
| `TestCompileCalibration` / `^TestCompileCalibration$` | `DZKR_MIMCCHAIN_CALIBRATE=1` | 对 N=1,2,8,32 各编译一次；不执行 Setup/Solve/Prove。由于包含 N=32，这不构成扩大实验规模的隐式授权。 |
| `TestGroth16N2` / `^TestGroth16N2$` | `DZKR_MIMCCHAIN_GROTH16=1` | 真实 N=2 正确性测试，使用两组 setup 密钥对、一个证明及正/负验证；这不是 CPU pilot 的操作预算。 |
| `TestMiMCChainCPUPilot` / `^TestMiMCChainCPUPilot$/^N2$` | `DZKR_MIMCCHAIN_CPU_PILOT=1` | 要求 `DZKR_MIMCCHAIN_CPU_N=2`、`-benchtime=1x`、实际生效的 GOMAXPROCS=4，以及下述证据/运行/源码身份。 |
| `TestMiMCChainCPUPilot` / `^TestMiMCChainCPUPilot$/^N8$` | `DZKR_MIMCCHAIN_CPU_PILOT=1` | 要求 `DZKR_MIMCCHAIN_CPU_N=8`、`-benchtime=1x`、实际生效的 GOMAXPROCS=4，并通过 D2 Collector 使用冻结的 schema-2 `mimcchain-n8-groth16-v1` profile。此处仅为元数据；后续运行需要单独授权。 |
| `TestMiMCChainCPUArtifactAudit` / `^TestMiMCChainCPUArtifactAudit$` | `DZKR_MIMCCHAIN_CPU_AUDIT=1` | 独立进程使用 pilot 已有且匹配的产物目录与运行/源码身份；以排他创建方式生成新的审计文件；执行一次 Verify，不执行 Setup/Prove。 |
| `TestMiMCChainStartBarrierProbe` / `^TestMiMCChainStartBarrierProbe$` | `DZKR_MIMCCHAIN_BARRIER_PROBE=1` | 合成探针，不执行密码学操作。`DZKR_MIMCCHAIN_BARRIER_PROBE_MODE`：空值/默认模式直接返回，`trivial` 检查微小工作，`nonzero` 在放行后失败，`preexit` 在握手前失败，`hold` 无限等待且需要外部终止。 |

一次真实 N=8 pilot 已通过独立验收；原始运行与审计仍保存在外部，作为历史证据。这并不授权再次运行。
N=8 路径要求使用已验收的 D2 Collector、新的配置/源码/二进制/策略绑定，以及资源预检；单独的 Go 选择器不是安全的启动方案。
独立的 `TestMiMCChainCPUArtifactAudit` 仍**仅适用于 N=2**。Python G4/N=8 证据检查核对已记录的 Go 调用，不重新执行 Groth16 Verify。
参见[已验收的 N=8 合同](benchmark-contracts.md#accepted-n8-cpu-pilot)。

pilot 与 N=2 产物审计的要求：

- `DZKR_MIMCCHAIN_EVIDENCE_DIR`：必须是仓库外的绝对路径。pilot 的末级目录必须尚不存在；审计必须使用与其匹配的已有末级目录，且其中不能已有审计输出。两者都采用排他创建；绝不可指向已封存的历史记录。
- `DZKR_MIMCCHAIN_RUN_ID`：pilot 要求非空，且不能包含 `/` 或 `\`；审计必须与已存储的 run ID 一致。
- `DZKR_MIMCCHAIN_SOURCE_ID`：pilot 要求 64 个十六进制字符；审计必须与夹具一致。历史 launcher 使用预检清单的 SHA256。这是调用者提供的源码绑定，不是自动发现 Git 状态。

在受监督的 barrier 合同中，supervisor 设置 `DZKR_MIMCCHAIN_START_BARRIER=1`，
将 `DZKR_MIMCCHAIN_BARRIER_SOCKET` 指向其私有 Unix socket，并将 `DZKR_MIMCCHAIN_BARRIER_TOKEN` 设为生成的 64 字符 token。
`DZKR_MIMCCHAIN_BARRIER_TIMEOUT` 默认为 `5s`；显式提供的时长必须为正且不超过 `30s`。
pilot、audit 和 probe 都在夹具工作/计时之前调用该 hook。未设置 START_BARRIER 时，Go hook 不执行任何操作；
这**不意味着**无监督的 pilot 可以作为合格测量。校准与 `TestGroth16N2` 不调用此 barrier hook。

已验收的 pilot/audit supervisor 会清除全部五个实验启用开关，然后只启用选定角色。
它设置 `GOTOOLCHAIN=local`、`GOPROXY=off`、`GOSUMDB=off`、`GOMAXPROCS=4` 和 `PYTHONDONTWRITEBYTECODE=1`；
历史 launcher 还设置了 `GOGC=100`，并保持 `GOMEMLIMIT` 未设置。
其目标使用预先构建的 Go 测试二进制，参数为 `-test.count=1`、`-test.cpu=4`、`-test.benchtime=1x`、`-test.timeout=3m`。
audit 的 Go 守卫不会独立强制检查 CPU_N、GOMAXPROCS 或 benchtime；这些设置属于已验收的受监督执行合同。
参见[历史 pilot 与资源语义](benchmark-contracts.md#p4-b2-n2-cpu-pilot)。

## 信号子进程变量不是可选启用开关

`TestCoordinatorSignalSubprocess` 使用 `DZKR_TEST_COORDINATOR_CHILD` 和 `DZKR_TEST_COORDINATOR_ARGS`；
`TestWorkerSignalSubprocess` 使用 `DZKR_TEST_WORKER_CHILD` 和 `DZKR_TEST_WORKER_ARGS`。
父测试使用编码后的启动参数，启动当前测试可执行文件；子进程走真实的 main/信号路径。
取消设置这些变量**不会**跳过父测试。不要在常规测试 shell 中手动设置子进程变量。

## 后续阶段提案：尚未实现

默认快速测试层加上显式选择的必跑集成/真实密码学检查，只是提案，不是当前的选择行为。
移动测试或添加门控需要单独授权，并补足替代执行覆盖。同样，职责单一的 Setup/Load/Prove/Verify 辅助函数以后或可减少重复；
本文未改变任何夹具、断言、测试或基准边界。未来整合时，应保留独立的原生 digest 参照计算与针对原始 listener 的关闭检查。
