# 基准测试与实验合同

本文依据源码定义执行合同，不声称产生了新的测量结果。
[测试选择](testing.md)说明当前默认行为与精确门控条件。下文所有基准入口都需要显式 `-bench` 选择，
不会在常规默认执行中运行；部分配套正确性测试则默认运行。启用任何实验前，应先阅读资源与夹具的前置条件。

以下选择器是元数据，**不是完整启动命令**。Go 基准命令还应选择 `-run '^$'`，避免常规测试成为预算之外的准备工作。
必须明确采用固定迭代次数、进程/测试/墙钟时间预算和资源监控。仅有 `-mod=readonly` 既不等于离线模式，也不提供隔离。
不要使用 `-bench .`，也不要同时启用多个实验角色。

## P3-A CPU 与执行层基准

这些基准均使用固定的生产 BN254 MiMC 原像电路（331 个约束）、真实 Groth16 和临时匹配密钥材料，无需部署 witness 或凭据。
夹具编译、Setup 和密钥生成位于计时循环之外，但仍在进程内执行并消耗资源。
Go 校准可能反复调用基准夹具；下表预热次数按每次基准调用计算。

| 包 | 入口 / 精确 `-bench` 选择器 | 计时外的预热 / 预检 | 计时操作 | 排除的工作 |
|---|---|---|---|---|
| `./internal/zk` | `BenchmarkPreimageProve` / `^BenchmarkPreimageProve$` | 3 组封装层 Prove + Verify | 生产封装层的 witness 构造、Groth16 Prove、二进制证明序列化；循环中的输出/错误检查 | Setup、密钥加载、digest 准备、最后一个证明的独立 Verify |
| `./internal/zk` | `BenchmarkPreimageVerify` / `^BenchmarkPreimageVerify$` | 3 组封装层 Prove + Verify | 二进制证明解码、公开 witness 构造与真实 Verify | Setup、密钥加载、夹具证明生成；计时路径不包含 Base64 解码 |
| `./internal/zk` | `BenchmarkLoadPreimageProver` / `^BenchmarkLoadPreimageProver$` | 一次 prover 加载预检，没有固定的 3 次证明预热 | 反复加载生产 prover，包括电路编译和 PK 文件解码 | Setup/密钥创建；最后加载的 prover 执行 Prove，以及独立 Verify |
| `./internal/zk` | `BenchmarkLoadPreimageVerifier` / `^BenchmarkLoadPreimageVerifier$` | 准备一个真实证明；加载 verifier 并验证一次 | 反复加载/解码 VK 文件 | Setup/证明生成；循环后的 Verify |
| `./internal/runtime` | `BenchmarkRuntimeExecuteJob` / `^BenchmarkRuntimeExecuteJob$` | 3 次真实任务执行，每次均独立验证 | 载荷解析、本地 witness 查找、证明准入、Prove、Base64、截止时间检查、Result 构造与结果检查 | Setup/加载、网络/持久化，以及最终输出解码/Verify |
| `./cmd/worker` | `BenchmarkWorkerExecuteJob` / `^BenchmarkWorkerExecuteJob$` | 3 次直接 handler 执行，每次均独立验证 | 生产 WorkerService handler：任务构造、每次新建的任务超时、真实证明、响应、正常日志与检查 | gRPC/mTLS、外层生命周期准入封装、Coordinator、持久化、setup，以及独立输出 Verify |

源码：[zk 基准](../internal/zk/preimage_bench_test.go)、
[运行时基准](../internal/runtime/zk_preimage_bench_test.go)、
[Worker 基准](../cmd/worker/execution_bench_test.go)。

加载结果可能反映已预热的 OS 页缓存，而非冷存储启动。运行时的父上下文没有共享截止时间；Worker 每次调用都创建新的任务截止时间。
这样可避免一个父截止时间在校准过程中到期，但并未取消生产结果截止时间检查。指标使用正常的 Noop 路径。
这些基准均未实现外部硬内存限制或墙钟时间监督器。

Prove 封装包含 witness 创建与序列化。它与 P4 在已有 witness 上直接执行纯 Groth16 Prove 的计时范围**不同**。

## P3-B1 端到端基准

入口：`./cmd/coordinator`，`BenchmarkSubmitJobEndToEnd`。
精确 `-bench`：`^BenchmarkSubmitJobEndToEnd$`；无需额外环境启用开关。
源码建议固定使用 `-benchtime=20x`，并拒绝大于 1000 的 b.N。
此上限不会消除 Go 校准调用，也不能代替外部限制。

[夹具](../cmd/coordinator/execution_bench_test.go)拥有临时 SQLite 数据库、临时 mTLS 身份、一个 Coordinator、
一个使用生产 Task/runtime 证明路径的测试 Worker 端点，以及已就绪的 Client 连接；它们均位于同一个 Go 进程中。
夹具在计时外执行新的 Setup。它不是生产 WorkerService 进程：不包含 Worker 启动、心跳/生命周期封装或恢复/存活检测循环。
Coordinator 每次尝试都会打开新的派发连接；Client 连接已经预热。

预热包含**三个成功任务**。Go 计时窗口涵盖提交工作：请求/上下文创建、真实 gRPC SubmitJob 调用和响应处理。
RPC 内包含 mTLS Worker 派发、真实证明、Coordinator 独立验证，以及响应前的 SQLite 终态 Save。
另行记录的 `RPCNanoseconds` 区间仅包围 RPC 本身，不能将其与外层 Go 计时窗口完全等同。

响应后的 SQLite Load/审计、指标 Gather、结果 JSON 和夹具清理均在 Go 计时窗口之外。
`ObservationNS` 包含审计，不是 RPC 延迟之和。分配统计覆盖计时窗口内的整个进程，而非仅 Client。
Prometheus registry 运行时不启用 HTTP exporter。

无故障结果必须满足：每个任务恰有一次被接受且已验证的尝试，完整规格被持久化为 Succeeded，Busy 为零，指标与调用核算一致。
`P3B1_RESULT` 分开记录预热行和测量行，并记录调用身份与 b.N；应保留校准调用，不能按复用的 JobID 合并。
预算包括任务 30s、lease 45s、RPC 120s、就绪 15s，以及每个清理阶段 10s。
这些是夹具配置，不是修改后的生产默认值。

此基准测量本地 RPC 链路与执行，**不代表多主机网络部署**。输出、核算和夹具生命周期都属于其合同。

## P3-B2 容量基准

入口：`./cmd/coordinator`，`BenchmarkSubmitJobCapacity`。
必须精确选择一个子项，例如 `^BenchmarkSubmitJobCapacity$/^C$`。
其他有效精确子项选择器将 C 替换为 A、B 或 D。

| 子项 | Worker 数量 | 每个 Worker 的证明容量 | 闭环 Client 数量 |
|---|---:|---:|---:|
| A | 1 | 1 | 1 |
| B | 1 | 1 | 2 |
| C | 1 | 2 | 2 |
| D | 2 | 1 | 2 |

`DZKR_BENCH_PREIMAGE_DIR` 必须指向事先在外部准备的临时目录，内含生产电路匹配的 `proving.key` 和 `verifying.key`。
[加载器](../cmd/coordinator/execution_bench_fixture_test.go)对两个文件计算哈希，检查 331 约束的电路，
并在**计时外额外执行一次 Prove + Verify 密钥对预检**。各 Worker 从同一密钥对加载独立 prover 实例。
不得为每个 Worker 生成新密钥，也不得混用密钥对身份。

每个选定子项都拥有新的夹具。测量前进行**六次成功预热**；有两个 Worker 时，每个至少处理三次。
测量中的 Client 共享一个已就绪的 Client 连接，发起有界闭环请求组，并等待执行协程全部退出。
最多允许请求 1000 个任务。请求组 watchdog 为 120s；清理使用 Worker 共享截止时间，未完成的退出等待必须如实报告，不能声称成功。
Coordinator 的任务/lease/RPC 设置与 P3-B1 夹具一致。

Go `ns/op` 是整个协调请求组及 Client 退出等待的计时区间除以 b.N，不是单个 RPC 延迟。
吞吐量按记录的观测区间内成功持久化的结果计算；吞吐量的倒数不等于单请求延迟。
持久化审计、指标 Gather、清理和 JSON 报告均在 Go 计时之外。
watchdog 触发的取消与排空仍属于观测到的失败路径，不能悄然将其排除后计为成功工作。

`P3B2_RESULT` 区分基础设施 Busy 耗尽（持久化为 Failed）、任务失败、取消、未决/删失工作和被拒绝的结果。
它核对尝试、实际证明调用、已结束结果、指标和持久化规格；过载场景无需达到 100% 成功率才算核算有效。
Valid 与 comparable 是独立的结果字段。证明调用生命周期的半开区间可确定重叠与容量边界，**不能证明 CPU 同时执行**。

源码：[容量基准](../cmd/coordinator/execution_capacity_bench_test.go)、
[容量控制](../cmd/coordinator/execution_capacity_test.go)、
[清理控制](../cmd/coordinator/execution_bench_cleanup_test.go)。
所有端点共享一个进程的 CPU/GC 预算。这不是多主机或多进程吞吐量结果，也未包含生产 Worker 生命周期/心跳。

## P4-B1 实验正确性

包路径：`./internal/zk/experimental/mimcchain`，不属于生产任务派发。
[电路](../internal/zk/experimental/mimcchain/circuit.go)在每个链节使用新的 MiMC 状态，对一个域元素进行哈希。
N 是固定的电路元数据，不是 witness；每种长度都需要独立且匹配的 setup 密钥对。没有运行时 TaskType 用于选择 N。

常规 `TestChainLengthValidation`、`TestCircuitRelations` 和 `TestReferenceConcurrent` 覆盖允许的长度、N=1 兼容性、
与原生 digest 的一致性、私有/公开数据分离，以及 N=1,2,8 的 Solve 正/负用例。N=32 仅用于编译校准。

| 入口 / 精确 `-run` 选择器 | 门控条件 | 实际密码学/编译预算 |
|---|---|---|
| `TestCompileCalibration` / `^TestCompileCalibration$` | `DZKR_MIMCCHAIN_CALIBRATE=1` | 对 N=1,2,8,32 各 Compile 一次；不执行 Solve/Setup/Prove/Verify |
| `TestGroth16N2` / `^TestGroth16N2$` | `DZKR_MIMCCHAIN_GROTH16=1` | Compile N=2，**2 次 Setup**、**1 次成功 Prove**、**2 次成功 Verify**、**2 次预期 Verify 拒绝**（错误 digest 与无关 VK） |

这些是受门控的实验测试，不是默认执行的 Groth16 工作。
校准报告 Compile 前后的分配增量和进程累计高水位 RSS，不代表各长度独立的内存峰值，也不是 Go 基准样本。
正确性测试会序列化/重新加载密钥，并生成第二组无关 setup 作为负对照。
制定高开销操作预算时，不得用该测试替代 CPU pilot。
参见[校准源码](../internal/zk/experimental/mimcchain/calibration_test.go)和[正确性源码](../internal/zk/experimental/mimcchain/circuit_test.go)。

<a id="p4-b2-n2-cpu-pilot"></a>

## P4-B2 N=2 CPU 试验

已验收的历史运行是 `pilot-n2-reconciled-20261017T121714Z`，评审记录为 `review-n2-20261017T123541Z`：
**VALID N=2 PILOT / PASS WITH FOLLOW-UPS**。这些是外部归档标识，不代表新运行，也不是统计稳定的基线。
历史证据保存在 `~/benchmark-archives/DistributedZKRuntime/` 下（`p3-b2`、`p4-b1`、`p4-b2`）；
此布局是可选的本地证据存储方式，不是常规仓库构建/测试的前置条件。绝不可改写已封存的历史记录。

[pilot 源码](../internal/zk/experimental/mimcchain/pilot_n2_test.go)使用 `TestMiMCChainCPUPilot/N2`，
精确 `-run` 为 `^TestMiMCChainCPUPilot$/^N2$`，并要求 `DZKR_MIMCCHAIN_CPU_PILOT=1`、`DZKR_MIMCCHAIN_CPU_N=2`、
benchtime1x 和实际生效的 GOMAXPROCS4。独立进程选择 `^TestMiMCChainCPUArtifactAudit$`，并设置 `DZKR_MIMCCHAIN_CPU_AUDIT=1`。
全部证据/运行/源码/barrier 前置条件见[精确门控合同](testing.md#experimental-opt-in-gates)。

| 进程 | Setup | 成功 Prove | 成功 Verify | 错误 digest 的 Verify 拒绝 |
|---|---:|---:|---:|---:|
| Pilot | 1 | 2 | 2 | 1 |
| 独立产物审计 | 0 | 0 | 1 | 0 |

因此，成功验证次数是 **pilot 2 + audit 1**，不是 pilot 执行了三次 Verify。
pilot 显式记录一组预检 Prove/Verify 和一组计时 Prove/Verify，**没有隐藏预热**。
其 17 项操作账本还记录不计时的 digest、公开 witness 构造、CCS 序列化、负向 Verify 和产物 I/O。

十个单操作计时样本：

| 操作 ID | 计时范围 |
|---|---|
| `compile` | 构造 N=2 电路并 Compile BN254 R1CS |
| `witness` | 从已准备的 digest 构造完整 witness |
| `setup` | 对已编译 CCS 执行 Groth16 Setup |
| `pk_serialize` | PK WriteTo 内存缓冲区 |
| `vk_serialize` | VK WriteTo 内存缓冲区 |
| `pk_decode` | 分配 proving-key 对象，从已序列化的内存数据 ReadFrom |
| `vk_decode` | 分配 verifying-key 对象，从已序列化的内存数据 ReadFrom |
| `prove` | 使用已有 CCS、重新加载的 PK 和完整 witness，直接执行 Groth16 Prove |
| `verify` | 使用已有 proof、重新加载的 VK 和公开 witness，直接执行 Groth16 Verify |
| `proof_serialize` | Proof WriteTo 内存缓冲区 |

[仅用于测试的驱动](../internal/zk/experimental/mimcchain/pilot_driver_test.go)使用 `testing.Benchmark` 和 `b.Loop()`。
它要求显式 `1x`、一次回调、benchmark N=1，并且恰好有一个操作开始且完成。
额外迭代、回调、错误或 panic 都会使样本无效，不会重试操作。
计数器与账本写入位于计时操作之外，仍保留少量循环/计时器调用开销。Go 在回调前执行的 GC 不在计时范围内。
链长 N=2 与基准迭代 N=1 是不同的量。

pilot 绑定源码清单/运行/夹具身份，以及 CCS、PK、VK、proof 和公开声明的产物 SHA256/大小。
launcher 记录二进制和工具身份。独立审计重新加载保存的材料、检查 CCS 元数据、重算预期公开 digest，并恰好执行一次真实 Verify；
其两项账本操作均不计时。Python 数值/资源审计独立于 Go 密码学审计，新增 Verify 调用为**零**。

[启动 barrier](../internal/zk/experimental/mimcchain/pilot_start_barrier_test.go)位于夹具和计时工作之前。
经过评审的 supervisor 检查受管理进程的 PID/start-ticks/executable 身份与 Unix 对端凭据，采集存活就绪样本，放行目标并观测确认。
它在修正后的 watchdog 外层提供有界清理。历史启动设置包括内层墙钟时间 210s、外层墙钟时间 230s、
TERM/KILL 宽限期 0.15/0.5s，以及目标测试超时 3m；runner 另有 CPU/阶段/资源停止条件。
这些边界是历史工具设置，不是可移植的生产控制，也不构成新的执行授权。这些属于历史 N=2 launcher 设置。
当前维护的 `tools/bench` 包已提供策略/源码绑定、原始进程组 RSS、G4 监督与验证，以及用于 N=8 的 D2 外层 Collector。
归档脚本可能使用固定路径或写入派生证据，不得在原位置重放。

## 资源证据与历史工具后续工作

| 术语 | 含义 / 限制 |
|---|---|
| 放行前就绪 RSS | 放行前对存活目标的观测；证明观测器就绪，不代表工作负载内存峰值 |
| 放行后存活工作负载 RSS | 放行后的有效目标样本，绑定到进程/可执行文件身份 |
| 观测采样峰值 | 已保留有效样本的最大值；可能遗漏采样间的尖峰 |
| 进程组 RSS 汇总 | runner 汇总成员采样值所得的最大值；与仅针对目标的观测具有不同总体和采样频率 |
| 生命周期峰值 | 除非经独立确认，否则未知；缺失样本或仅有就绪观测都不能确定该值 |
| 硬内存限制 | `/proc` 采样、轮询停止条件或 Go 内存提示均不提供此保证 |

已评审的 N=2 运行保留了**九个 pilot 放行后存活样本**，独立审计的放行后存活样本为**零**。
审计具有有效的放行前就绪样本；其工作负载期间的峰值仍然**未知**，不是零。
VALID 验收仅针对声明的采样资源合同，不证明完整峰值内存行为、cgroup 边界或统计稳定的性能。

N=2 评审最初提出了两项 **Phase C** 要求：

1. 在创建进程前，将精确的策略 SHA256 绑定到执行前启动记录。历史启动哈希、源码时间戳和最终归档校验和不能证明该策略在启动时已被绑定。
2. 保留带时间戳的原始进程组成员/身份及各进程 RSS，包括缺失、排除和错误观测，使独立审计者能够重新计算成员总量和进程组峰值。历史内层 runner 只保留进程组峰值汇总；外层仅针对目标的原始样本不能代替它们。

这些要求后来通过当前维护的 G2 策略/源码绑定、G3 共同原始 RSS 流，以及 G4/G5 跨产物/资源核对实现，
但未追补到历史 N=2 归档中。真实合成 Go Barrier 集成和最终工具评审在 D1/D2 之前已获验收。
合成探针仍单独门控；`hold` 需要外部终止，不是常规测试命令。

<a id="accepted-n8-cpu-pilot"></a>

## 已验收的 N=8 CPU 试验

D1 N=8 入口/profile、D2 外层 watchdog/Collector，以及一次真实的纯 CPU BN254 MiMC Chain N=8 Groth16 执行均已实现并通过独立验收。
D3 结论为 **PASS WITH FOLLOW-UPS**，验收阻塞项为零；P4-D2 与有界 N=8 里程碑已完成。N=32 仍为可选且延期的工作。

| 指标 | 历史 N=2 | 已验收 N=8 |
|---|---:|---:|
| 约束数量 | 661 | 2,641 |
| Setup | 54.184064 ms | 204.938119 ms |
| Prove | 9.516367 ms | 29.742667 ms |
| Verify | 0.500668 ms | 0.614877 ms |

每个值都是一次计时调用（`benchmark N=1`），不是两次调用的均值、中位数或最小值。
针对链长具体化后，驱动与计时范围一致。两个 pilot 都记录 Setup x1、Prove x2（预检加计时）、成功 Verify x2 和错误 Digest 拒绝 x1。
N=8 审计核对与源码绑定的 Go 验证记录，没有额外执行数学 Verify。
日期、调度、缓存/GC 和随机 setup 均有差异；描述性比值不代表统计扩展规律或吞吐量结果。

已验收的 N=8 运行保留了一个 COMPLETE 就绪样本、六个 COMPLETE 工作负载样本，以及终止时无存活成员的观测。
共同原始进程组 RSS 峰值为 **21,484 KiB**；未观测到资源停止条件触发，也没有残留的受管理进程。
该数据流不同于独立的直接目标采样峰值（约 22.090 MiB）。两者均不能确定真实生命周期最大值，也不提供硬内存隔离。
用户态 watchdog 与清理不是硬实时保证。

N=8 必须使用已验收的 D2 Collector 执行路径，采用冻结的 schema-2 profile `mimcchain-n8-groth16-v1`，
精确绑定源码/二进制/策略/argv/环境，使用新的外部路径/run ID、真实 Go Barrier，并且只尝试一次，不自动重试。
target/barrier/Supervisor/Guardian/outer 的预算仍为 **3/2/8/15/25 seconds**，由一个包含封存/审计的绝对外层截止时间约束。
启动要求新近检查确认 MemAvailable >=5 GiB，并通过其他已验收的主机/所有权门槛；采样停止条件不构成 cgroup 内存上限。
任何默认测试命令都不应启动此路径。

外部证据标识为 `p4-d2/n8-cpu-pilot-20261021T045408Z` 和 `p4-d3/n8-evidence-review-20261021T052144Z`，
位于可选的外部归档布局下。它们不是公开 URL，也不是可移植的可执行配置。
原始轨迹、进程记录、密钥和二进制应保留在外部。未来任何 Groth16 pilot 都需要新的明确授权与重新准备，不能重放已验收归档。
参见[工具合同](../tools/bench/README.md)。

## 可比性与变更控制

- P3-A 封装层 Prove 包含 witness/序列化；P4 纯 Prove 不包含。
- P3-B1 的 RPC 区间与 Go 计时窗口不同于 P3-B2 的请求组计时与吞吐量；吞吐量的倒数不是单请求延迟。
- 本地多个端点共享 CPU/GC，不测量多机器网络。
- N=2 或 N=8 每个操作的一次观测是有界 pilot，不是稳定的统计估计；不得将其发布为分布或可靠的加速比。
- 必须保留名称、选择器、标签、环境、预热、调用预算、计时范围、夹具/密钥生命周期、源码/构建/依赖/CCS/密钥/证明哈希、工具版本与资源策略。仅 HEAD 不能标识本地未跟踪的实验输入；已验收归档中的显式源码身份独立于最终 Git 检查点保存。
- 这些输入发生变化时，需要新的测量合同版本和书面差异记录。即使辅助函数重构后基准名称相同，也不自动具备可比性。进一步比较需要共同且经评审的合同，不能改写历史结果，也不能仅凭入口名称未变就推断可比。

文档不授权再次运行 Groth16 pilot、执行 N=32、修改归档或整合测试/辅助函数。GPU/CUDA 不在当前路线图内。
进一步实验执行需要单独评审的范围和资源预算。
