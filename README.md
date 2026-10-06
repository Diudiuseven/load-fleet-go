# load-fleet-go

多执行器协同的负载生成与结果汇总系统。同一条负载曲线被拆分到多个
在**本地进程内**模拟的执行器上并行施加；协调与派发只走**内存通道**，
不依赖任何外部服务。支持运行中动态增删执行器、失败接管、背压限速、
取消收敛，并产出机器可读、可复现、可核对的汇总结论。

## 快速开始

```go
cfg := fleet.Config{
    Seed:         42,
    TotalActions: 100000,
    ShardCount:   64,
    ShardStrategy: fleet.ShardModulo,
    MaxExecutors: 8,
    BatchSize:    32,
    MaxInflight:  4,
}
c := fleet.NewCoordinator(cfg)
c.AddExecutor(fleet.NewExecutor("e1", cfg.MaxInflight))
c.AddExecutor(fleet.NewExecutor("e2", cfg.MaxInflight))
rep, err := c.Run(ctx) // rep 为机器可读的 *fleet.Report（可 JSON 序列化）
```

运行中可随时 `AddExecutor` / `RemoveExecutor` / `UpdateConfig`。

## 确定性与分片语义

- **曲线确定性**：动作 `i` 的负载与调度偏移是 `(Seed, i)` 的纯函数。
  相同 `Seed` 与配置下，曲线逐字节一致，与执行器数量无关。
- **虚拟分片（slot）**：曲线被划分为 `ShardCount` 个虚拟分片，
  `action -> slot` 的映射只取决于 `(ShardStrategy, Seed, ShardCount)`，
  **不随执行器数量变化**。分片**完备且互不重叠**：每个动作恰好属于
  一个分片，各分片内动作按下标升序构成确定的有序流。
- **分片策略**：
  - `ShardModulo`：动作 `i` 归属分片 `i % ShardCount`；
  - `ShardHash`：归属 `mix64(Seed ^ i) % ShardCount`（确定性散列）；
  - `ShardRange`：连续下标区间，分片大小最多相差 1。
- **slot -> executor 的指派**随成员变化动态调整；再均衡只移动分片
  **尚未派发**的尾部，已提交进度原地保留。因此无论执行器数量多少、
  如何扩缩，按分片归属重新拼接后的动作序列与累计统计完全一致。

## 动态成员与失败接管

- 每个分片记录 `committed`（已连续完成前缀）与 `dispatched`。
- 执行器退出、失联或超时时，其分片被收回并重新指派给存活执行器，
  从 `committed` 处继续派发：**不丢、不重、不留空隙**。
- 崩溃的执行器（goroutine 退出）记为 `lost`；心跳停止的执行器记为
  `timeout`；被优雅移除的记为 `completed`；取消时记为 `canceled`，
  在最终报告中可区分。
- 被接管分片上仍在旧执行器飞行中的批次，若最终上报，会成为重复样本，
  由汇总层按动作下标去重，**不会重复计入**。

## 汇总规则

- 汇总器以**动作下标**为唯一键去重：任何执行器的重发、迟到上报、
  接管后的重放都只计一次；`Report.Duplicates` 记录被丢弃的重复数。
- 样本按调度偏移 `At` 对齐到**同一条时间轴**（`Report.Timeline`），
  再分片前后的累计结果无缝合并。
- 动作延迟是负载的确定性函数，因此单执行器全量运行与多执行器运行的
  分位数（p50/p90/p99）**完全一致**（误差为 0，满足"允许误差内一致"）。
- `Report.Executors` 逐执行器给出：分片归属（`Slots`）、样本计数
  （`Samples`）、派发量（`Dispatched`）与推进进度（`Committed`）。
  所有执行器的 `Samples` 之和恒等于 `Completed`。

## 背压与流速控制

- 每个执行器的在飞动作数上限为 `MaxInflight * BatchSize`，超限即停止
  向其派发；执行器收件箱容量有界，发送是非阻塞的，慢执行器不会让
  协调方无限堆积。
- `Coordinator.Lag()` 实时返回每个活跃执行器的滞后（已派发未确认
  动作数），可用于观测与外部限速决策。
- `ctx` 取消后，协调方立即关闭所有执行器收件箱并等待其收敛，
  返回 `Canceled=true` 的部分报告，所有 goroutine 及时释放。

## 配置项

| 字段 | 含义 | 默认 | 运行中可改 |
|---|---|---|---|
| `Seed` | 曲线随机种子 | 0 | 否（改动被忽略） |
| `TotalActions` | 动作总数 | 0 | 否 |
| `ShardCount` | 虚拟分片数 | 16 | 否 |
| `ShardStrategy` | 分片策略 | `ShardModulo` | 否 |
| `MaxExecutors` | 执行器数量上限 | 64 | 是 |
| `BatchSize` | 每批次动作数 | 8 | 是（仅影响未派发部分） |
| `MaxInflight` | 每执行器在飞批次数上限 | 4 | 是（仅影响未派发部分） |
| `HeartbeatInterval` | 心跳间隔 | 50ms | 是 |
| `HeartbeatTimeout` | 心跳超时（超时即接管） | 500ms | 是 |
| `ActionInterval` | 曲线时间轴上相邻动作间隔 | 1ms | 否 |

运行中的 `UpdateConfig` 只影响**尚未派发**的部分；已派发或已完成的
部分不会被改写，不可变字段（种子、总数、分片数、策略）的修改会被忽略。

## 测试

```sh
go test -race ./...
```

覆盖：确定性分片（三种策略的完备/互斥/有序）、单执行器与多执行器
结论一致、不同执行器数量结论一致、动态扩缩容、重复上报去重、
执行器崩溃接管（`lost`）、心跳超时接管（`timeout`）、背压滞后上界、
取消收敛与资源释放、执行器上限、配置热更新边界、报告 JSON 往返。
