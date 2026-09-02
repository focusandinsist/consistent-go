# P0：成员缓存会在第二次读取时返回空列表

c.cachedMembers = make([]string, 0, len(members))
copy(c.cachedMembers, members)

切片长度是 0，只有容量，copy 不会复制任何元素。
触发方式：
c.GetMembers(ctx) // 第一次可能正常
c.GetMembers(ctx) // 第二次返回空列表
影响：
- 控制面误判集群没有成员；
- 成员同步、监控、扩缩容逻辑可能错误；
- 文档中的“返回成员列表副本”无法成立。



-----------------------------------------------
# Remove 失败时不会回滚，可能留下部分损坏状态


-----------------------------------------------
# P1：平均负载计算存在整数截断

问题原因：

`averageLoad` 原先先执行整数除法，再转换为 `float64`：

```go
float64(c.partitionCount / uint64(len(c.members))) * c.config.Load
```

当分区数不能被成员数整除时，小数部分会在计算前丢失。例如 10 个分区、3 个成员、负载系数 1.0，原实现计算为 3，正确上限应为 `ceil(10.0 / 3.0) = 4`。

这会导致合法配置在初始化或再平衡时错误返回 `ErrInsufficientSpace`，并且与 `validateConfig` 使用的浮点公式不一致。

解决方法：

先将分区数和成员数转换为 `float64`，再执行除法和负载系数计算：

```go
avgLoad := (float64(c.partitionCount) / float64(len(c.members))) * c.config.Load
return math.Ceil(avgLoad)
```

验证：

- 新增 `TestAverageLoad_NonDivisiblePartitionCount` 回归测试；
- 验证 10 个分区、3 个成员时平均负载上限为 4；
- 定向测试通过。
