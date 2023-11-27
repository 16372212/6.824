# 共识算法

> Consensus is a fundamental problem in fault-tolerant distributed systems，Consensus involves multiple servers agreeing on values

共识是容错的一个基本问题。共识算法解决的是分布式系统对某个提案（Proposal），集群中所有节点都达成一致意见的过程

# raft是什么

raft算是一种共识算法，解决了一个分布式集群如何达成同意values的问题

![](../pic/raft_ini.png)

> etcd Raft 模块设计实现上抽象了网络、存储、日志等模块，它本身并不会进行网络、存储相关的操作，上层应用需结合自己业务场景选择内置的模块或自定义实现网络、存储、日志等模块。唐聪，《etcd 实战课》

# 复制状态机（Replicated state machines）

- 约定存储的是日志

# raft共识算法实现

## 1. 节点状态（Server states）

每个节点只会有三种状态，leader, follower, 或者 candidate.

## 2. 任期

Raft 里的逻辑时钟，从 0 开始，随着每次选举的发起而递增

## 3. RPCs

节点之间的通讯方式，最基础的共识算法，只需要两种 

1. RPC，RequestVote RPCs 
2. AppendEntries RPCs

# 步骤

## 1. Leader Election
### 名词定义
1. Append Entries: leader -> followers, follower reply

2. Term: 是什么，何时增加修改？

> raft divides time into terms of arbitrary length

每次有新的

如果有leader，那么所有的节点维护的term是一致的。否则，在竞选状态时，以高的term为大，谁的term高谁的话语权高

> tips: 相同term下，如果有的candidate a term_a 比 candidate b term_b来的早(term_a < term<b ), 那可能会投票给term_a而不是term_b



### 基本思路

这里主要按照论文中的图片来即可。

- leader send heartbeat RPCs no more than ten times per second.

```go
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool)

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool

func beginElection()

- 批量发选票，批量收获所有的结果。如果结果中true的票数 > false的票数，它就成功变成leader

- candidate时处理其他leader发来的心跳信息，是自己变成follower还是继续candidate？此时candidate的term肯定是大于老leader的term的

- 如果新leader收到老leader的心跳信息，会怎么样呢？

- follower投票完还会变成candidate吗：会，自己超时后也会开启投票

```

### 特殊情况

1. split vote: 

要考虑到，有的follower如果失联，这时判断split vote时就不能只判断是否是peers个数的一半了。


2. node becomes candidates at the same time, and send RequestVote at the same time:

每一个term，follower只能为一个candidate投票
