# Talent Plan 2.0 路径二（TinyKV）实验报告

陈奕骁（华中科技大学 计算机 大二）

chenyixiao@foxmail.com

github ID: poorpool

[TOC]

## 测试方法和完成情况

项目在 Linux 上完成，测试时需要**首先创建一个文件夹 `/home/poorpool/tinykvtmp`**，这是因为我的电脑的 tmpfs 太小。如果测试机不需要这类修改，可在 `kv/test_raftstore/cluster.go` 第 59 行删去 `/home/poorpool/tinykvtmp` 的字样。

1, 2a, 3a, 3c, 4abc 一定可以一次通过;

2b 在我的电脑上不能一次通过，但是将 2b 的测试分两半测都可以通过，感觉可能是和我的电脑的配置有关的玄学问题；

2c 也可能出现和 2b 一样的玄学问题；

3b 领导转换可以通过，ConfChange 有 和 Split 可能在最后几个测试出错，也可能出现和 2b 一样的玄学问题。某些测试可能耗时甚久。小概率甚至可能出现卡死的情况，十分钟后会自己失败；

没有为不带 2A/3C 这类字样的 Test 进行特意修改。

提交了一次 pull request（# 236）

## 解题思路

### 1

主要要实现 `kv/storage/standalone_storage` 中的两个文件以及 `kv/server/server.go` 的部分函数。

阅读文档，知道自己要干什么，合理调用 engine_util 的函数即可。记得及时关闭文档中提示的一些东西。

### 2A

反复阅读 Raft 论文，结合测试理清逻辑和行为。这里一定要时刻小心，回归论文，不然在后面必然发现自己 Raft 模块写错了……

#### 2AA

实现领导人选举。在 Step 函数中处理消息，把消息直接 append 到 msgs 作为发送。tick() 表示模拟的时间变动，处理超时等情况。

处理请求投票的时候，能投票不能投票都要发请求投票的 response，方便候选人尽早结束选举，无论是成功还是失败。候选人收到请求投票的 response 时既统计得票是否过半，也统计得票无论如何都再也不可能过半了，方便尽早结束选举。

#### 2AB

实现日志复制。按照论文处理即可。特别注意的是领导人向群众广播心跳时，群众接收到心跳无论如何必定回复，收到心跳回复（且不下台）的领导立刻向群众发送附加日志 RPC，以便发现复活或者恢复通信的其他节点并同步日志。

#### 2AC

实现 rawnode.go 中用于交互的 Ready 结构体。Advance 时更新 stable 和 apply 等数据。

### 2B

要做的事情主要是实现 peer_msg_handler.go 的 proposeRaftCommand，即把 put、get 等操作转化成 []byte 提交给 raft 模块去同步，以及实现 HandleRaftReady。HandleRaftReady 先调用 peer_storage.go 的 SaveReadyState 保存一些状态，然后将提交未 apply 的日志条目一个一个还原成 put、get 等请求，一个一个处理实现，通过 callback 响应，最后更新 apply state。

### 2C

实现日志压缩和快照。

在 apply 的时候如果发现是压缩日志命令，就使用 d.ScheduleCompactLog 计划上压缩日志。Raft 模块中如果要发送快照就调用 `storage.Snapshot()`，**成功返回快照**时便可发送，接收者做出相应处理，并在 ready 中记录上这个快照，以便在 SaveReadyState 的时候应用快照。

### 3A

在 raft 模块中增加领导人转换和单个节点的增删。领导人转换需要当前领导人首先帮助未来领导人更新日志（至少和自己一样新），然后向其发送立即开始选举指令，同时自己退位。单个节点增删修改 `r.Prs` 即可。需要注意的是删除节点可能导致 commit 变化，就像收到附加日志 RPC 的回复一样。

### 3B

在 peer_msg_handler.go 中增加领导人转换、单个成员增删、region 分割。

领导人转换直接调用 rawnode 的函数。成员更改时，注意要使用 rawnode 的 ProposeConfChange，以将其和普通的命令区分开。要更新 region 的信息，同时调用 rawnode 的函数。区域分割时修改原 region，创建新 region，创建 peer，发送开始消息。

因为 region 会变化，所以还要额外加上判定 put、get、delete、region 分割时候 key 在不在当前区域，snap（类似于scan）时 RegionEpoch 能不能对号等的逻辑。

### 3C

这是一个独立的部分，实现调度器和负载均衡的部分功能。文档非常完善，按着做就可以了。

实现 `scheduler/server/cluster.go` 中的 processRegionHeartbeat，更新集群信息。3B split 的时候也应该加上这类操作。

实现 `scheduler/server/schedulers/balance_region.go` 中的 Schedule，返回 Operator。region 和 store 是多对多的关系，通过 region 的 size 选择合适的 store 进行迁移，使迁移前后 store 的 region size 的差值比较大，达到缓解压力、负载均衡的目的。

### 4A、4B、4C

又是一个独立的部分，实现事务。文档非常完善，按着做就可以了。分为 prewrite 和 commit/rollback 两部分，用 lock 表示加锁解锁。对应着 CFDefault、CFWrite、CFLock 三个 column family，解决了 project 1 中 column family 是干什么的疑惑 ^_^。

## 刚开始写的时候记的一些笔记

### ProtoBuf

ProtoBuf 是结构数据序列化方法，可简单类比于 XML。感觉就和我以前写 java 返回 json 是一个类型，只不过更轻、更快。需要双方维护一个协议约束文件，以.proto结尾。

### RPC 和 gRPC

RPC 是远程过程调用，广义上来讲，所有本应用程序外的调用都可以归类为 RPC，不管是分布式服务，第三方服务的 HTTP 接口，还是读写 Redis 的一次请求。RESTfull 就是一种实现 RPC 的方式。

gRPC 是一款 RPC 框架，使用 Protobuf 进行数据编码，提高数据压缩率；使用 HTTP2.0 弥补了 HTTP1.1 的不足；同样在调用方和服务方使用协议约定文件，提供参数可选，为版本兼容留下缓冲空间。

### 实现简单数据库

`kv/storage/standalone_storage/standalone_storage.go`

do all read/write operations through `engine_util` provided methods.

### Implement service handlers

`t := i.(T)` 是类型断言，i 是接口变量，T 为转换的目标类型。`t, ok := i.(T)` 更有好，正常 true,不正常 false。

engines.go 里头有一个将 *WriteBatch 写到 engine 的 kv db 里头的函数。

### 思考

有三个节点123，起初客户端向3 propose了数据，同步了。一段时间后因为某种原因，1和2、1和3之间的联系被切断了。此后客户端一直向3 propose数据。因为随机化原因，3 成为了任期6的leader，1成为了任期7的candidate。现在联系恢复，3应该怎么做，1应该怎么回答，才是正确的?

答：3向1发心跳，1发现任期不行，拒绝。1发现回来的response任期大，成为follower（没有leader！！！！！！！）一段时间后重新开始选举。
