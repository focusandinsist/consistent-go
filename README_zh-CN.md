<div align="center">

# consistent-go

**面向 Go 的有界负载一致性哈希：固定分区、增量再平衡、清晰可观测。**

[![Go Reference](https://pkg.go.dev/badge/github.com/focusandinsist/consistent-go/consistent.svg)](https://pkg.go.dev/github.com/focusandinsist/consistent-go/consistent)
[![Go Version](https://img.shields.io/badge/Go-1.21%2B-00ADD8?logo=go&logoColor=white)](https://go.dev/)
[![License](https://img.shields.io/github/license/focusandinsist/consistent-go)](LICENSE)
[![GitHub Stars](https://img.shields.io/github/stars/focusandinsist/consistent-go?style=flat&logo=github)](https://github.com/focusandinsist/consistent-go/stargazers)

[English](README.md) | [简体中文](README_zh-CN.md)

</div>

`consistent-go` 是一个小巧、并发安全的进程内路由库，适用于数据库分片、分布式缓存、会话亲和负载均衡和稳定任务分配。它把虚拟节点哈希环、固定分区表和可配置负载上限组合在一起，让拓扑变更可观测，也更容易转化为实际数据迁移计划。

## 为什么选择 consistent-go？

- **固定分区**：key 先映射到有限分区，再由分区映射到成员。可以直接检查、统计和迁移分区，而不必逐个寻找发生移动的 key。
- **有界负载**：分区放置遵守可配置的单成员上限，降低纯概率哈希环产生热点的风险。
- **增量再平衡**：新增成员只检查其虚拟节点影响的区间；删除成员只重映射原来属于它的分区。
- **短查询路径**：key 定位只需一次 64 位哈希、一次取模和一次分区表读取，不需要遍历哈希环。
- **并发安全 API**：公开操作支持并发访问，需要取消语义的接口接受 `context.Context`。
- **可靠的哈希默认值**：默认使用 xxHash64，同时提供 MurmurHash3 和精简的自定义 `Hasher` 接口。

## 工作原理

```text
                         虚拟节点
成员 ────────────────────────► 哈希环
                                  │
                                  │ 分配所有权
                                  ▼
key ── xxHash64 ──► 分区 ID ──► 分区表 ──► 成员
     hash % count    [0, N)       有界负载
```

这层固定分区正是它与基础一致性哈希环的重要区别：拓扑变化最终表现为有限、可检查的分区迁移，业务系统可以据此生成并执行数据搬迁计划。

## 快速开始

安装：

```bash
go get github.com/focusandinsist/consistent-go/consistent
```

创建哈希环并定位一个 key：

```go
package main

import (
	"context"
	"fmt"
	"log"

	consistent "github.com/focusandinsist/consistent-go/consistent"
)

func main() {
	ctx := context.Background()

	ring, err := consistent.NewWithMembers(
		[]string{"cache-a", "cache-b", "cache-c"},
		consistent.Config{
			PartitionCount:    271,
			ReplicationFactor: 128,
			Load:              1.25,
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	owner, err := ring.LocateKey(ctx, []byte("user:42"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("owner:", owner)

	if err := ring.Add(ctx, "cache-d"); err != nil {
		log.Fatal(err)
	}

	fmt.Println("partition load:", ring.LoadDistribution(ctx))
}
```

如果成员由服务发现动态提供，可以先创建空环：

```go
ring, err := consistent.New(consistent.Config{})
if err != nil {
	log.Fatal(err)
}

_ = ring.Add(ctx, "node-a")
_ = ring.Add(ctx, "node-b")
_ = ring.Remove(ctx, "node-a")
```

## 核心 API

| 操作 | 用途 |
| --- | --- |
| `New`、`NewWithMembers` | 创建空环或带初始成员的环 |
| `Add`、`Remove` | 修改成员并执行增量再平衡 |
| `LocateKey` | 查询 key 当前所属成员 |
| `FindPartitionID` | 查询 key 的稳定分区编号 |
| `GetPartitionOwner` | 查询指定分区的所有者 |
| `LocateReplicas` | 返回不重复的相邻候选成员 |
| `LoadDistribution` | 查看每个成员承载的分区数 |
| `AverageLoad` | 查看当前单成员分区容量上限 |

配置字段为零时使用默认值：

| 配置 | 默认值 | 含义 |
| --- | ---: | --- |
| `PartitionCount` | `271` | 固定分区总数 |
| `ReplicationFactor` | `20` | 每个成员的虚拟节点数 |
| `Load` | `1.25` | 相对平均值允许的负载系数 |
| `Hasher` | xxHash64 | 哈希环使用的哈希实现 |

修改 `PartitionCount` 或 `Hasher` 会大规模改变 key 的位置，应当按数据迁移处理，不能视为普通的线上配置变更。

## 适用场景

| 场景 | 项目提供的能力 |
| --- | --- |
| 数据库分片 | 稳定分区编号和可枚举所有权 |
| 分布式缓存 | 确定性路由和有限范围重映射 |
| 会话亲和负载均衡 | 成员变化时保持相对稳定的后端选择 |
| 任务分配 | 为租户、任务或队列提供稳定所有权 |
| 副本规划 | 有序且不重复的相邻候选成员 |

## 它有什么不同？

| 方案 | 固定分区 | 负载上限 | 任意成员删除 | 副本候选 | 可枚举增量迁移 |
| --- | :---: | :---: | :---: | :---: | :---: |
| 基础虚拟节点哈希环 | 否 | 否 | 通常支持 | 部分支持 | 否 |
| Jump Consistent Hash | 否 | 否 | 否 | 否 | 否 |
| `consistent-go` | **是** | **是** | **是** | **是** | **是** |

项目有意只解决本地路由问题。它**不提供**服务发现、故障探测、集群共识、网络复制或真实数据搬迁，这些职责应由接入它的系统承担。

## 测试

仓库将核心库和黑盒场景测试拆成了两个独立 module：

```bash
cd consistent
go test ./...

cd ../test
go test -short ./...
```

场景测试涵盖数据库分片、缓存故障转移、会话亲和负载均衡、并发成员变更和长时间稳定性。完整结构参见[测试指南](docs/testing-guide.md)。

## 文档

- [架构、取舍与开源项目对比](docs/project-overview-20260814.md)
- [哈希函数选择指南](docs/hash-function-guide.md)
- [测试指南](docs/testing-guide.md)

## 运行约束

- 所有路由进程必须使用相同的成员列表、成员名称、哈希算法、分区数、复制因子和负载系数。
- 成员变更需要由外部控制面一致地下发，本库自身不执行集群共识。
- `LoadDistribution` 统计的是分区数，不是字节数、请求数、CPU 或每个成员上的 key 数量。
- 自定义哈希器必须提供高质量、确定性的 64 位分布。CRC 一类校验和不适合这里的规则分区输入。

## 参与贡献

欢迎提交 Issue 和 Pull Request。高质量贡献最好包含聚焦的测试、清晰的行为说明；如果修改查询热路径，也请提供基准对比。

如果这个项目帮你省去了重复实现路由层的工作，欢迎[点亮一个 Star](https://github.com/focusandinsist/consistent-go)。这会让更多 Go 开发者发现它。

## 许可证

[MIT](LICENSE) © focusandinsist
