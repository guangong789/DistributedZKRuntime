# 本地 hash 演示

这是分布式任务生命周期的本地开发演示：一个真实 Coordinator、一个真实 Worker 和通过生成的 gRPC 客户端提交任务的 demo 进程。它使用现有运行时，不提供生产部署自动化。

## 前置条件与运行

在 Linux/WSL 上，从仓库根目录执行：

```sh
go run -mod=readonly ./cmd/demo
```

运行前需准备以下工具，命令不会自动安装它们：

- 满足 [go.mod](../go.mod) 中 Go 1.27.1 要求的本地工具链，以及已缓存的项目依赖。
- Bash、OpenSSL 3.x、Git。
- GNU `realpath`、`mktemp`、`stat`、`cmp`、`ln`、`chmod`、`mkdir`、`rm`、`grep`、`dirname`。
- 可写的 `/tmp` 和可用的 loopback 端口。无需 SQLite 命令行工具，检查使用已有 Go SQLite 驱动。

demo 检查工具是否存在、Go/OpenSSL 版本和上述 GNU 工具的版本标识。其子命令设置 `GOTOOLCHAIN=local`、`GOPROXY=off`、`GOSUMDB=off`、`GOMAXPROCS=2`；缺失的依赖缓存会导致构建失败。外层 `go run` 仍遵循当前 shell 的 Go 环境。

## 实际执行的六个阶段

终端以 Infrastructure、Task Execution、Verification 分组，最后显示 Cleanup 和成功总结；以下六个阶段及校验条件不变。输出为静态纯文本，重定向或 CI 中也无需颜色支持。

1. **准备环境。** 在 `/tmp/dzkr-demo-*` 下创建权限为 `0700` 的独立目录，调用现有 [PKI bootstrap](../scripts/dev-pki/bootstrap.sh)，离线构建生产 Coordinator/Worker。证书、私钥、二进制、SQLite 和日志均放在该目录中，不使用现有 `private/` 或 `zk-artifacts/`。
2. **启动 Coordinator。** 使用临时 SQLite 和 `-disable-zk=true`；RPC 与 metrics 仅绑定 loopback 临时端口。读取完整监听日志后，用生产 `rpcauth` 客户端凭据等待认证 gRPC `READY`，不把 TCP 端口开放当作应用就绪。
3. **注册 Worker。** 启动真实 Worker，同样关闭 ZK。等待其注册 RPC 成功后的完整日志，并检查子进程尚未退出；不通过重复提交任务探测就绪。
4. **执行 hash。** 提交 JobID `1`、`TaskType=hash`、`TimeoutMs=5000`，公开输入为 `DistributedZKRuntime local demo`。检查 RPC 无错误、响应 JobID 正确、AttemptID 为 `1`、状态为 `Succeeded`、任务错误为空，并独立计算 SHA-256 与输出比较。
5. **检查持久化。** 先关闭 Worker 和 Coordinator，避免并发写入，再只读查询 SQLite，核对 JobID、Succeeded 状态、AttemptID、WorkerID `worker-1`、TaskType、Payload 和 TimeoutMs。
6. **完成清理。** 确认所属子进程已回收、进程组已消失，再删除临时工作区。只有全部步骤成功才输出 `Demo completed successfully.` 并返回退出码 `0`。

临时身份为 `spiffe://distributedzk.local/coordinator/main`、`spiffe://distributedzk.local/worker/worker-1` 和 `spiffe://distributedzk.local/client/client-1`；允许列表与 WorkerID 使用对应身份。连接沿用 TLS 1.3/mTLS、DNS/IP SAN 与预期 URI 身份校验，没有认证绕过。

## 如何解读成功结果

固定输入的预期 SHA-256 为：

```text
cfe6de8fd07052ec56ee03cf37e24bffe41e4a90a3f3f354351a93ca68e0dc8a
```

RPC 无错误、Worker 返回 `Succeeded`、demo 独立校验 hash、SQLite 记录匹配，是分别检查的证据。Coordinator **不会独立验证普通 hash 输出**；SQLite 保存任务状态、代次、Worker 和任务规格，**不保存 hash 输出**。仅收到成功响应不足以让整个 demo 成功。

两端的 `-disable-zk` 默认仍为 `false`。demo 显式开启该选项，跳过 Groth16 keys 与 witness 文件加载，但保留 mTLS、授权、SQLite 和正常任务执行规则。禁用的 Coordinator 提前拒绝 ZK 提交；若已有 SQLite 中存在 Running/Recovering 的 ZK 记录，则拒绝启动并保留记录。该模式要求同构配置，不支持依据 Worker 能力自动路由。默认 demo 不执行 Groth16 Setup、Prove 或 Verify。

## 故障恢复导读：已有集成测试

[TestSQLiteRestartRecoversJobAfterWorkerReregistration](../cmd/coordinator/recovery_integration_test.go) 展示了另一条受控证据链。在仓库根目录可单独运行：

```sh
GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off GOMAXPROCS=2 \
go test -mod=readonly -p=1 -count=1 -timeout=30s \
  -run '^TestSQLiteRestartRecoversJobAfterWorkerReregistration$' -v ./cmd/coordinator
```

- Coordinator A 派发 Attempt 1，受控 Worker 等待通道释放；SQLite 已保存 Running 1 和完整 hash 任务规格。
- 测试直接调用共享执行路径，取消其上下文来结束旧 RPC，刻意避开外层 SubmitJob 的取消终态保存，以模拟崩溃留下 Running；随后关闭 store A，只把 SQLite 文件交给全新 store B 与空内存状态的 Coordinator B。
- 启动恢复标记为 Recovering 1，不分配新代次。测试为原 Worker 发送 heartbeat，新注册表拒绝该请求；现有 heartbeat 恢复逻辑通过同一客户端重新注册，唤醒恢复循环。
- 重新执行产生 Attempt 2，保留任务规格，保存 Succeeded 2。此后才释放旧 Worker，让其返回 Attempt 1；测试确认 SQLite 仍为 Succeeded 2，没有第三次执行，JobID 所有权已释放。

这是 **真实 SQLite close/reopen、真实本地 gRPC 与受控 Worker** 的集成测试，不是杀死并重启生产进程。该测试显式使用明文测试传输，不验证 mTLS；Worker 返回预设的 `done`，不执行生产 hash 计算。旧 RPC 的 dispatch 已因取消结束，迟到返回没有新 Coordinator 的消费路径；测试证明这次旧返回不改变已接受状态，不应解读为跨进程直接接收旧响应的独立 fencing 测试，也不证明恰好执行一次。它与实际使用 mTLS、真实 hash 执行的 demo 互为补充；demo 本身不注入故障。

## 清理与排障

正常退出和收到中断时，demo 先向所属子进程组发送 SIGINT，最多等待 7 秒，再按需发送 SIGKILL 并最多等待 3 秒回收直接子进程；强制终止或清理不完整均导致非零退出。只管理自己启动的进程，不按名称寻找或终止其他服务。

已创建工作区的失败运行会报告并保留诊断目录，不自动清除。查看其中的 `pki.log`、`build.log`、`coordinator.log`、`worker.log`（文件是否存在取决于失败阶段）。目录可能包含临时开发 CA 和节点私钥；不要上传、提交或公开整个目录。诊断结束且确认所属进程已退出后，只删除错误信息给出的本次目录，不使用通配符批量清理。前置检查失败时可能尚未创建工作区。

| 现象 | 检查事项 |
|---|---|
| 缺工具或版本不符 | 核对上述依赖及 PATH；程序不会自动安装。 |
| PKI 或构建失败 | 查看对应日志；确认 OpenSSL 3、Go 版本及离线依赖缓存。 |
| mTLS 身份不匹配 | 查看启动/注册日志，核对该次临时证书的角色和地址配置；不要关闭证书验证。 |
| 端口冲突或就绪超时 | 查看子进程日志。Worker 临时端口从预留释放到实际绑定之间有短暂竞争窗口，冲突会失败，不会自动重跑整个 demo。 |
| SQLite 校验不匹配 | 该次演示失败，即使 RPC 曾成功；查看保留日志及数据库，不把成功响应当作持久化证据。 |

程序总上下文为 150 秒，另有前置检查、PKI、构建、就绪、RPC 和 SQLite 阶段截止时间；退出清理另有上述预算。失败时先确认具体阶段，不通过扩大超时或绕过认证掩盖原因。

## 范围

演示只覆盖一个本地 Coordinator、一个 Worker 和一次 hash-first 执行。它不执行自动故障注入，不证明高可用、分布式共识、恰好执行一次或任意工作负载支持。ZK 能力与 CPU 实验仍是项目的既有部分，相关结果见[项目 README](../README.md)；演示没有重跑或扩展这些实验。
