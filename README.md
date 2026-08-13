# go-clusterha

`go-clusterha` 是一个可嵌入 Go 服务的三节点高可用组件库，提供 Raft 选主、业务领导权校验、强一致元数据、版本化快照复制、Follower 本地读校验和透明转发能力。

项目最初衍生自 Categraf Meraki 插件的高可用改造需求。Meraki 服务原本是单实例进程，既负责采集上游 API、维护内存缓存，也负责执行 SSID 控制动作；直接启动多个实例会带来重复采集、状态不一致和多实例同时控制外部系统的风险。因此本项目抽取出一套通用集群层，让业务服务可以保持“单 Leader 采集和写控制、多节点可读 Serving”的模式，而不依赖 Redis、数据库、对象存储或共享文件系统。

## 主要功能

- 基于 HashiCorp Raft 的三节点 voter 集群，提供 Leader election、quorum commit、membership 配置和 FSM snapshot/restore。
- 业务领导权（business leadership）门控：只有当前 Raft Leader、已提交 leadership epoch、且近期确认多数派的节点，才能执行采集、外部写操作或发布快照。
- 强一致元数据 FSM：支持单 key 和批量 metadata 写入，带 revision compare-and-set、command id 幂等校验和持久化 apply fault 保护。
- 版本化快照模型：业务数据以 `Manifest` + content-addressed blob 保存，Raft log 只复制小体积 manifest 和控制元数据，不直接承载大数据 blob。
- 快照耐久复制：Leader 在提交 manifest 前先把 blob 写入本地并复制到 voter quorum，只有达到 durable ACK 后才提交 generation。
- 本地只读 Serving 校验：Follower 只有在本地拥有目标 generation 的完整 blob，并能证明与 Leader/commit index 一致时，才本地响应。
- Follower 透明转发：本地无法安全读取时，可通过 mTLS 内部通道转发到已验证 Leader，避免负载均衡器感知 Leader。
- 内部 mTLS：Raft transport 和 internal HTTP API 都要求证书认证，并校验证书组织与 `cluster_id`、证书 CN 与节点身份。
- 成员替换流程：支持新节点以 `join_existing=true` 启动后，由 Leader 执行 `JoinVoter`，先作为 non-voter 追平再提升为 voter；支持移除非当前 Leader 节点。
- 滚动升级能力门控：通过 `CapabilityGate` 控制协议、命令、manifest/dataset schema 和 Leader/Voter 最低能力等级，避免旧二进制误处理新格式。
- 外部 HTTP 辅助接口：提供 `/livez`、`/readyz`、`/api/v1/cluster/status`、`/api/v1/cluster/nodes`、`/api/v1/cluster/snapshot/status`。

## 适用场景

适合以下类型的服务：

- 上游 API 有限速或副作用，只允许一个实例负责采集、同步或外部控制。
- 多个实例都需要接收查询请求，且查询结果应来自同一个已提交快照。
- 业务状态可以拆成“小体积强一致控制元数据”和“大体积可校验快照 blob”。
- 希望部署普通负载均衡器，客户端不需要识别 Leader。
- 不希望额外引入 Redis、etcd、数据库、对象存储或 NFS。
- 需要支持节点故障后自动重新选主，并在 Leader 切换期间继续提供 last-good 快照读服务。

典型例子包括：

- Meraki、云厂商、SaaS API 等带限速的集中式采集服务。
- 需要单主执行控制动作、多副本提供查询接口的运维服务。
- 周期生成大快照、客户端按 generation 读取指标或状态的服务。

## 不适用场景

本项目不是通用任务调度器，也不是分布式数据库。以下场景不建议直接使用：

- 多个实例需要同时分片采集以提升上游吞吐。
- 业务写入量很高，且所有业务数据都需要进 Raft log。
- 需要跨机房强一致、多 Raft group 或大规模节点管理。
- 希望用两节点集群实现自动故障切换。两节点 Raft 在任意一节点故障后无法形成多数派。
- 希望保证外部 API exactly-once。组件能保证本地控制状态一致，但外部系统写 API 仍应由业务层做期望状态协调和对账。

## 架构模型

推荐生产初始拓扑固定为 3 个 voter：

```text
Client / LB
  |-- service-a  <--- Raft + internal mTLS ---> service-b
  |-- service-b  <--- Raft + internal mTLS ---> service-c
  |-- service-c  <--- Raft + internal mTLS ---> service-a

Current Leader
  |-- collect upstream APIs
  |-- write replicated metadata
  |-- publish snapshot manifest after blob quorum ACK
  |-- execute external control actions

Followers
  |-- store committed blobs locally
  |-- serve verified local reads
  |-- proxy unsafe reads/writes to Leader when application chooses to do so
```

组件只负责集群通用能力，不包含 Meraki、SSID 或任何具体业务逻辑。业务层负责：

- 调用上游 API。
- 定义 dataset schema、编码和反序列化逻辑。
- 构建 `Manifest` 和 blob。
- 决定哪些 HTTP 接口可以读本地快照，哪些必须转发 Leader。
- 在 `SubscribeLeadership` 事件中启动/停止采集器和外部控制器。

## 安装

```bash
go get github.com/kongfei605/go-clusterha
```

当前 module 使用 Go `1.25.0`，主要依赖：

- `github.com/hashicorp/raft`
- `github.com/hashicorp/raft-boltdb/v2`

## 配置示例

配置结构体带有 `toml` tag。字段名如下：

```toml
[cluster]
enabled = true
cluster_id = "meraki-prod"
node_id = "meraki-a"

raft_bind_addr = "0.0.0.0:9200"
raft_advertise_addr = "10.0.0.1:9200"
internal_api_bind_addr = "0.0.0.0:9201"
internal_api_advertise_addr = "https://10.0.0.1:9201"
data_dir = "/var/lib/meraki-service/clusterha"

bootstrap_expect = 3
join_existing = false
initial_members = [
  "meraki-a=10.0.0.1:9200|https://10.0.0.1:9201",
  "meraki-b=10.0.0.2:9200|https://10.0.0.2:9201",
  "meraki-c=10.0.0.3:9200|https://10.0.0.3:9201",
]
membership_admin_node_ids = ["meraki-a", "meraki-b", "meraki-c"]

read_consistency = "bounded"
max_leader_contact_age = "2s"
max_quorum_verification_age = "2s"
emergency_stale_read = false
emergency_stale_read_max_age = "15m"

snapshot_publish_interval = "1m"
snapshot_publish_retry_min = "5s"
snapshot_publish_retry_max = "1m"
snapshot_freshness_max_age = "5m"
snapshot_durable_policy = "voter_quorum"
generation_retention = 3
generation_pin_ttl = "5m"
max_snapshot_blob_bytes = 536870912
max_snapshot_blob_puts = 2

[cluster.internal_tls]
enabled = true
ca_file = "/etc/clusterha/ca.pem"
cert_file = "/etc/clusterha/meraki-a.pem"
key_file = "/etc/clusterha/meraki-a-key.pem"
server_name = "clusterha.internal"
```

关键约束：

- `bootstrap_expect` 当前生产拓扑必须为 `3`。
- `initial_members` 必须包含 3 个初始 voter；格式为 `node_id=raft_host:port|https://internal_host:port`。
- `raft_advertise_addr` 和 `internal_api_advertise_addr` 不能使用 `0.0.0.0` 或 `::`。
- 集群启用时必须开启 `internal_tls.enabled`，并提供 CA、证书和私钥。
- 节点证书 CN 应为 `node_id`，证书 Organization 应包含 `cluster_id`。

## 基本用法

### 启动节点并注册集群 HTTP 接口

```go
package main

import (
	"context"
	"net/http"

	clusterha "github.com/kongfei605/go-clusterha"
)

func start(cfg clusterha.Config, appHandler http.Handler) (*clusterha.Node, error) {
	node, err := clusterha.NewNode(cfg)
	if err != nil {
		return nil, err
	}

	node.SetServingReadiness(func() (bool, string) {
		// 返回业务服务是否已经加载好可对外 Serving 的内存视图。
		return true, ""
	})
	node.SetProxyHandler(appHandler)

	if err := node.Start(context.Background()); err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	clusterha.NewHTTPHandler(node).RegisterRoutes(mux)
	mux.Handle("/", appHandler)

	go func() {
		_ = http.ListenAndServe(cfg.ExternalAPIBindAddr, mux)
	}()
	return node, nil
}
```

当 `enabled=false` 时，`NewNode` 会返回 disabled/ready 状态，方便业务继续以原单实例模式运行。

### 只在 Leader 上执行采集或控制

```go
events := node.SubscribeLeadership(ctx)
for event := range events {
	if event.Active {
		startCollector(event.Epoch)
		startController(event.Epoch)
		continue
	}
	stopCollector()
	stopController()
}
```

执行外部写操作前，业务代码应再次调用：

```go
if err := node.VerifyBusinessLeadership(ctx); err != nil {
	return err
}
```

这个校验会确认本节点仍是 Raft Leader、持有当前 `leader_epoch`，并通过 `VerifyLeader` + `Barrier` 证明仍有多数派。

### 写入强一致元数据

适合保存 runtime feature、控制状态机状态、generation manifest 指针等小体积状态：

```go
state := node.Metadata()
revision, err := node.PutMetadata(ctx, "runtime/features/enable-x", "runtime/features", map[string]bool{
	"feature_x": true,
}, state.Revision)
if err != nil {
	return err
}
_ = revision
```

批量写入使用 `PutMetadataBatch`，失败时不会部分提交：

```go
_, err := node.PutMetadataBatch(ctx, "controller/state/update", map[string]any{
	"runtime/features": features,
	"ssid/state":       ssidState,
}, node.Metadata().Revision)
```

`commandID` 应由业务层保证稳定唯一。相同 command id 和相同 payload 会被视为幂等；相同 command id 携带不同 payload 会被拒绝。

### 发布快照

业务层先把每个 dataset 编码成 blob，计算 sha256 hash，再构造 manifest。`PublishSnapshot` 会：

1. 校验 manifest 和当前 capability gate。
2. 将 blob 写入本地 CAS。
3. 把 blob 推送到 voter 节点。
4. 达到 voter quorum durable ACK 后提交 manifest 到 Raft FSM。

```go
data := []byte(`{"ok":true}`)
blobHash, err := clusterha.NewBlobStore("/tmp/build-only").PutBytes(data)
if err != nil {
	return err
}

status := node.Status()
manifest := clusterha.Manifest{
	SchemaVersion: clusterha.CurrentManifestSchemaVersion,
	Generation: clusterha.Generation{
		ClusterID:   status.ClusterID,
		LeaderEpoch: status.LeaderEpoch,
		Sequence:    1,
	},
	CreatedAt: time.Now().UTC(),
	Datasets: map[string]clusterha.DatasetRef{
		"metrics/wan": {
			Name:              "metrics/wan",
			SchemaVersion:     clusterha.CurrentDatasetSchemaVersion,
			Scope:             "global",
			BlobHash:          blobHash,
			Encoding:          "json",
			RecordCount:       1,
			CollectedAt:       time.Now().UTC(),
			SourceSuccess:     true,
			Required:          true,
			UncompressedBytes: int64(len(data)),
		},
	},
}

_, err = node.PublishSnapshot(ctx, "snapshot/metrics/wan/1", manifest, map[string]io.Reader{
	blobHash: bytes.NewReader(data),
})
```

对于较大的 dataset，可以使用 `DatasetRef.Shards` 表达多个 shard；读取时使用 `OpenSnapshotDatasetShardAt`。

### 读取快照并在必要时转发 Leader

业务接口通常先固定一个 generation，再读取对应 dataset：

```go
func serveWAN(w http.ResponseWriter, r *http.Request) {
	manifest, ok := node.ActiveSnapshot()
	if !ok {
		http.Error(w, "snapshot unavailable", http.StatusServiceUnavailable)
		return
	}

	generation := manifest.Generation
	if err := node.VerifyLocalRead(r.Context(), generation); err != nil {
		if proxyErr := node.ProxyToLeader(w, r, generation); proxyErr == nil {
			return
		}
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	reader, ref, err := node.OpenSnapshotDatasetAt(generation, "metrics/wan")
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	defer reader.Close()

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set(clusterha.HeaderTargetGeneration, generation.String())
	w.Header().Set("X-Dataset-Records", strconv.FormatInt(ref.RecordCount, 10))
	_, _ = io.Copy(w, reader)
}
```

`read_consistency` 支持：

- `bounded`：Follower 在近期仍与 Leader 有联系，且本地已应用到目标 committed generation 时可本地读。
- `linearizable`：Follower 会向 Leader 做 barrier 校验后再确认本地读安全。

开启 `emergency_stale_read=true` 后，`VerifyLocalReadResult` 可在无法证明强一致但本地 snapshot 未超过 `emergency_stale_read_max_age` 时返回 `Stale=true`，由业务接口决定是否降级返回陈旧数据。

## 外部 HTTP 接口

把 `NewHTTPHandler(node).RegisterRoutes(mux)` 注册到业务服务的外部 HTTP mux 后，会暴露：

| Method | Path | 说明 |
|---|---|---|
| `GET` | `/livez` | 进程存活检查。 |
| `GET` | `/readyz` | 集群路径和业务 Serving readiness 均满足时返回 200，否则 503。 |
| `GET` | `/api/v1/cluster/status` | 当前节点角色、Leader、term/index、generation、capability、FSM 健康等状态。 |
| `GET` | `/api/v1/cluster/nodes` | 当前 Raft membership。 |
| `GET` | `/api/v1/cluster/snapshot/status` | 快照相关简要状态。 |

成员写操作和 blob 复制只暴露在 internal API 上，并且要求 mTLS 成员证书；外部 HTTP handler 不提供 membership 写接口。

## 成员替换

当前推荐用于替换节点的流程：

1. 新节点使用新的 `node_id`、独立 `data_dir` 和 `join_existing=true` 启动。
2. 新节点配置里的 `initial_members` 仍填写原 3 个 voter。
3. Leader 调用 `JoinVoter(ctx, member)`，其中 `member` 包含新节点的 Raft advertise 地址和 internal API URL。
4. 组件先把新节点加入为 non-voter，等待其 FSM 和 committed generation 追平，再提升为 voter。
5. 确认新节点成为 voter 后，如需移除旧节点，先确保旧节点不是当前 Leader，再调用 `RemoveServer(ctx, oldNodeID)`。

`AddVoter` 当前故意 fail closed；不要绕过 `JoinVoter` 安全流程直接修改 membership。

## 升级与兼容

每个节点上报 `NodeCapabilities`，集群通过 `CapabilityGate` 控制当前允许使用的协议、命令和快照 schema。滚动升级建议：

1. 先部署兼容旧 gate 的新二进制。
2. 等所有 voter 健康并追平。
3. 调用 `ActivateCapabilities` 提升 gate。
4. 新功能只在 gate 激活后开始写入新 schema 或要求更高能力等级。

如果 FSM 遇到不支持的命令版本、manifest schema 或不可恢复的 apply 错误，会记录持久化 apply fault，并在状态接口中暴露 `fsm_healthy=false`，防止故障节点继续产生不一致状态。

## 持久化目录

`data_dir` 下主要包含：

```text
raft/
  raft.db
  snapshots/
  fsm-apply-fault.json
cas/
  objects/
  staging/
  quarantine/
```

- `raft/` 保存 Raft log、stable store 和 FSM snapshot。
- `cas/objects/` 保存按 `sha256:<hex>` 寻址的业务 snapshot blob。
- `cas/quarantine/` 保存被检测为损坏并替换掉的 blob 文件。

不要把 `data_dir` 放在临时目录或多个节点共享的 NFS 目录上。Follower 对快照的 ACK 以本地持久化和校验完成为前提。

## 测试

```bash
go test ./...
```

完整集成测试会启动多节点 Raft 集群，验证选主、元数据复制、快照复制、Follower 本地读、成员替换和能力门控。兼容性源码测试需要本地 git 历史，并显式设置：

```bash
CLUSTERHA_RUN_SOURCE_COMPAT=1 go test ./... -run TestNNPlusOneSourceCompatibility
```

