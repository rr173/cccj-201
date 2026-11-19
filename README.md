# shardstore

一个本地分片关系存储模块：纯 Python 标准库实现（`sqlite3` + unix socket），
不依赖任何现成分布式数据库或云服务。提供常驻服务（多进程共享）与 Python
客户端 API，数据与控制记录全部落在 SQLite 文件中。

## 数据模型

- 一张表，主键为二元组 `(tenant TEXT, key INT64)`。全序：**tenant 按
  UTF-8 字节序，key 按有符号 64 位整数序**；路由按完整二元键比较，绝不
  拼接成字符串。
- 行含三个可空列：文本、整数（精确 int64）、BLOB（可逆往返）。
- 分片覆盖半开键区间 `[lo, hi)`，整个键域无洞且不重叠（初始一个分片
  覆盖 `[-∞, +∞)`）。
- 行多版本：每个版本带全局 `commit_seq`，历史保留到没有任何活跃读取 /
  split 拷贝依赖为止。

## 布局

```
<dir>/
  control.sqlite     路由纪元、提交决定日志（含载荷，恢复依据）、读令牌、
                     split 状态机
  shards/<id>.db     每个分片一个文件：kv(tenant,k,seq,op,vt,vi,vb) +
                     frontier(已应用提交前沿)
```

## 提交协议（单写者串行裁定）

1. 按 `request_id` 去重：同 ID 同内容 → 返回原回执不再执行；同 ID 不同
   内容 → `CONFLICT` 拒绝。
2. 写入提交决定记录（seq、载荷、目标分片集合）——这是持久的提交点。
3. 把这一批 put/delete 按调用者次序应用到所有涉及分片（每分片一个事务；
   delete 不存在的行是空操作）。
4. 推进可见前沿 `visible_seq`。

`commit_seq` 全局递增；一批跨分片的写只在第 4 步之后对读取可见，因此
**全部行变化在同一个 seq 一起可见**，读不会混到两次提交之间。崩溃后按
日志把未应用完的决定重放到各分片（幂等），已确认写入不丢、同一
`request_id` 不会重执行；未返回的写入可能最终提交也可能最终取消，但
重试得到唯一结果。

## 读取

`open_read()` 取得一个**读令牌**：钉住 `(read_seq, epoch)`（一个已提交
seq + 一个完整路由代号）。`get` / `scan` 都按该读取点由对应分片承担；
scan 按键全序、不重不漏、可分页，翻页沿用同一读取点。令牌必须显式
`release`——服务不用超时猜测读者是否还在。NULL / int64 / BLOB 类型在
结果中精确保留（线协议里 int64 走十进制字符串、BLOB 走 base64）。

## 在线 split

`split(shard, boundary)` 把一个现有分片在内部边界键处拆成两片：

- 注册后旧路由继续服务；**其他写不等拷贝完成**——提交路径把落在源范围
  的写**双写**到两个目标片，并把源/目标三片前沿一起推进到该提交 seq。
- 后台 worker 分批拷贝 `<= copy_seq` 的每键最新版本（游标持久化，重启
  续拷；内存有界，与数据总量无关）。
- 拷贝完成且**两片目标具备同一完整提交前沿**后，在一个控制事务里把新
  路由纪元整体切换（不会先切左片再切右片）；写只经历这个短暂的发布
  屏障。
- 旧读取点与翻页跨过切换仍按原语义返回（旧分片继续服务旧令牌）；旧
  分片在没有任何未释放令牌 / 恢复证据依赖后才能回收（`reclaim()`）。
- 一次只允许一个 split；边界不在分片内部、重复范围、未知分片都拒绝且
  不改路由。

崩溃恢复：重放未应用的提交决定；未发布的 split 从持久状态继续（拷贝
游标、双写状态都在控制库）；发布事务是原子的，不会出现两套同时生效的
路由；回收中断则重做（删文件幂等）。

## 运行

```bash
# 启动服务（常驻，多进程经 unix socket 访问）
python3 -m shardstore.server --dir /var/lib/mystore --sock /tmp/mystore.sock

# 跑完整验收（多进程：服务子进程 + 客户端子进程；含全部崩溃演练）
python3 acceptance.py            # 约 1 分钟
SCALE_ROWS=200000 python3 acceptance.py   # 更大规模
ONLY="main drill" python3 acceptance.py   # 只跑某个阶段
KEEP=1 python3 acceptance.py     # 保留工作目录便于检查
```

## 客户端 API 摘要

```python
from shardstore.client import Store, put, delete

s = Store("/tmp/mystore.sock")
rcpt = s.write("req-1", [put("t", 1, text="a", integer=2, blob=b"\x00"),
                         delete("t", 2)])          # -> {"commit_seq": N, ...}
tok = s.open_read()                                 # 钉住 (read_seq, epoch)
row = s.get(tok, "t", 1)                            # Row | None
page = s.scan(tok, lo=("t", 0), hi=("t", 9), limit=100)
for row in s.scan_all(tok, page=1000): ...          # 自动翻页
s.release(tok)
s.split("s1", ("t", 4))                             # 在线分裂
s.migration_status()                                # 迁移阶段 + 各片前沿
s.routing(); s.reclaim()
```

测试钩子（验收脚本使用）：`pause_copy(on)` 暂停/恢复拷贝线程（同步点），
`arm_crash(point)` 在指定阶段点强制退出（`after_decision`、`during_copy`、
`after_copy`、`after_catchup`、`before_publish`、`after_publish`、
`during_reclaim`）。

## 验收覆盖

`acceptance.py` 用一个**独立的串行字典模型**（每 seq 留全量小快照，不调
用正式路由/迁移代码）核对：复合键排序、空值/BLOB/int64 保真、重传回执
与冲突拒绝、主演练（写 1..6 → 旧扫描读 1、2 → 暂停拷贝时单事务改 2、
删 4、插 7 → split 于 4 → 旧扫描余下 3,4,5,6、新扫描 1,2',3,5,6,7）、
在复制后/追赶后/路由决定前后/回收中强制退出并重开、交错重传
request_id、跨片事务、活跃旧令牌阻止回收、release 后可回收、两个迁移
请求竞争（恰一个胜出）、10 万行运行时生成数据的大范围复制有界内存
（RSS 采样）与多进程并发读者。
