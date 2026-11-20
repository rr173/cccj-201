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
  control.sqlite     路由纪元、提交决定日志（含变更载荷：既是恢复依据
                     也是变更订阅日志）、订阅游标/世代、读令牌、split 状态机
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

## 持久化变更订阅

提交决定记录同时就是**变更日志**：每个 `write` 的载荷（调用者原样
put/delete 序列、请求 ID、提交序号）在 `commits.deliver_json`
持久落盘，与行数据同库同事务，不复制到另一套不受保留规则约束的数据库。
下游按全局 `commit_seq` 递增消费。

### 注册与消费世代

```python
sub = s.subscribe("etl-a", after_seq=0)   # 从 0 号提交之后开始
gen = sub["generation"]                    # 消费世代（新名称从 1 开始）
```

- 名称稳定；同名以**不同** `after_seq` 再次注册 → `SUBSCRIPTION_EXISTS`，
  绝不悄悄重置进度。同名同 `after_seq` 再次注册是进程重连：游标与已确认
  前缀保留，但启动**新的消费世代**（旧世代立即失效，见 `s.claim(name)` 可
  单独接管）。
- 一个订阅可被多个进程访问，但任何时刻只有一个有效世代：旧世代的
  `pull`/`ack`/`replay` 一律 `STALE_GENERATION`，不能推进游标。
- 订阅位置、可交付范围、积压量：
  `s.subscribe_status(name)`、`s.subscriptions()`。

### 拉取（交付单元）

```python
page = s.pull(name, gen, max_commits=100, max_bytes=1 << 20)
for ch in page:                  # ch.commit_seq / request_id / ops / size_bytes
    handle(ch.ops)               # ops 即原始调用序列（可直接再传给 write）
s.ack(name, gen, page.next_after)
```

- 结果按 `commit_seq` **严格递增**；每个 `commit_seq` 是一个**不可拆分**
  的交付单元，保留原 `request_id`、提交序号与调用者操作次序（含重复键
  put、delete 及对不存在键的 delete）。多个提交绝不合并成一个无法还原
  边界的结果。
- 只有**已完整应用**（`applied=1`）的提交可交付；已决定但未应用/部分应用
  的提交不会出现在结果中。
- `max_commits` / `max_bytes` 只在单元之间截断；**单个提交超出字节预算
  仍整体交付**，并以 `page.over_budget`、`page.oversize_seq` 和该单元的
  `over_budget` 明确标出。
- 拉取与确认分离，且**拉取自身不移动游标**：交付总是从已确认前缀
  重新开始，因此同一确认状态下的重复拉取返回**逐字节相同**的提交序列与
  `size_bytes`（无论客户端是否重连）。
- `s.pause(name, gen)` / `s.resume(name, gen)`：暂停时返回空页且不推进
  任何游标；暂停状态落盘，重启后保持。

### 确认

`s.ack(name, gen, through_seq)` 确认**连续已交付前缀**：

- 重复确认幂等（同值成功），不连续/越过已交付范围 → `BAD_ACK`，
  回退确认 → `BAD_ACK`；被拒绝的确认不改变进度。
- 客户端/服务重启后游标来自持久状态；服务在确认期间被强制结束不会出现
  半推进。

### 重放

`s.replay(name, gen, after_seq, ...)` 从保留范围内已交付的位置重新投递
（不移动游标）。早于实际保留下界、或越过已交付范围的请求 → `OUT_OF_RANGE`，
错误 `detail` 中报告 `min_after` / `max_after` / `log_low` / `log_high`，
绝不静默从较新位置开始。

### 日志清理

`s.cleanup(up_to)` 释放 `<= up_to` 的变更载荷（行版本 GC 合约不在此列）：

- 上界不能越过**任何未确认订阅**仍需要的提交（`acked_seq < up_to` 的
  订阅即阻断），也不能越过未应用的恢复记录；否则 `CLEANUP_BLOCKED`，
  `detail.safe_up_to` 给出安全上界，`detail.blocked_by` 指出阻断者。
  快订阅不会被慢订阅拖住，只要慢订阅的未确认前缀之上不清理即可。
- 载荷释放与保留区间移动在**同一个控制事务**中；崩溃只可能看到完整的
  旧区间或完整的新区间，不会出现部分可见的边界。
- 清理不影响提交恢复、活跃读取，也不影响 `request_id` 去重回执
  （`receipt_json` 单独保留）：相同 ID 同内容重传仍返回原回执，不重执行；
  冲突内容仍 `CONFLICT`。
- 新订阅或重放的 `after_seq` 早于实际保留下界 → `OUT_OF_RANGE` 并报告
  可用范围。无订阅者时，上界仍由保留载荷决定，含义稳定：空日志的
  `min_after` = 已提交末尾。

所有订阅元数据（原点、已确认前缀、已交付上界、世代、暂停位）与变更载荷
都在控制库中；重启后一致。

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

## 一致性快照导出与空目录恢复

在线把某个**已提交水位**导出成一份与源目录、分片文件名、绝对路径无关的
便携备份，再在另一个空目录恢复成一个可读写的新实例。导出全程源服务继续
接受写入与 split，无需停服，也不要求永久保留历史变更载荷。

### 一致性点（读令牌 + MVCC + 路由纪元）

`export` 先 `open_read()` 取得一个普通读令牌，钉住
`(read_seq=watermark, epoch)`。行版本只按该点从对应纪元的分片流式读取；
令牌未释放前，MVCC GC 与 split 回收都不能删除它依赖的行版本或分片文件，
因此导出期间并发的提交、split、清理都不可能让不同分片来自不同提交点、
漏行、重复行或混用路由。水位之后才提交的写不会进入备份。

### 备份格式（自描述、可离线校验）

```
<backup>/
  MANIFEST.json   信任根：format / format_version / 水位 commit_seq /
                  导出时纪元 / 行与回执记录数 / 与分片无关的全局键范围 /
                  完整路由视图（仅标签 + 半开键界）/ 每段 length + SHA-256
  seg-000001.dat  长度前缀 JSON 帧流（uint32 大端长度 + JSON；int64 走
  seg-000002.dat  十进制字符串、BLOB 走 base64）。先若干 rows 段，再若干
  ...             receipts 段；段按 64MiB 目标大小滚动，单条超大值整条落盘
```

- `row` 帧按 `(tenant UTF-8 字节序, 有符号 key)` 全局严格排序、主键唯一；
  NULL / int64 / BLOB 精确保留。
- `receipt` 帧是每个已决定提交的幂等回执（seq、request_id、request_hash、
  原始 receipt），按 seq 严格有序。**历史变更载荷不进备份**：清理后仍保留
  的回执照样导出，恢复后相同 request_id 仍返回原回执、冲突仍 `CONFLICT`，
  但不会把已清理的提交载荷伪装成可重放的变更流。
- 清单与每段的长度、SHA-256 都可直接对备份字节重新计算验证；备份里没有
  源数据目录、源分片文件名或绝对路径。

### 发布原子性

写入全部进入独立临时目录（`.snapshot-<随机>` / `.restore-<随机>`）：

- 导出：数据段与 `MANIFEST.json` 全部落盘并**自校验通过**后，一次
  `os.replace` 原子发布；中断只留下无法被识别为备份的匿名临时数据
  （没有 manifest 的目录 `inspect` 报 `BACKUP_INCOMPLETE`）。
- 恢复：只允许写入**不存在或为空**的目标目录。先做完整离线校验（清单、
  格式版本、每段长度/摘要、帧格式、全局排序与主键唯一、路由覆盖），再在
  暂存目录构建数据目录并回读交叉核对，最后一次原子 rename 发布。任何阶段
  失败或进程退出，目标都不会呈现为可用半成品，也绝不覆盖已有数据
  （非空目标报 `TARGET_NOT_EMPTY`）。

### 恢复后的语义

- 当前行值、复合键排序、NULL/int64/BLOB 与水位的逻辑状态逐字节一致；
  恢复实例的 `visible_seq = watermark`，下一次写入取 `watermark+1`，序号
  连续不复用。
- 备份内 request_id 的原始回执仍用于去重，恢复不会重做这些请求。
- 订阅**从水位重新建立**：源订阅者、未确认游标、读令牌、split 状态都不
  迁移；新订阅的最早位置是 `watermark`（报告中的
  `subscription_resume_after_seq`，保留日志下界为 `watermark+1`）。
- 全程流式、内存有界：导出按分页游标读行、按批读回执；恢复按批写分片、
  逐帧校验，不把整库或全部回执装入内存。

### 入口

```python
rep = s.snapshot_export("/backups/b1")     # 在线导出（服务端绝对路径）
s.snapshot_inspect("/backups/b1")          # 仅校验并回报水位/摘要/边界
s.snapshot_restore("/backups/b1", "/data/newstore")  # 仅空目录可恢复
```

离线（不经过服务）也可检查与恢复：

```bash
python3 -m shardstore.snapshot inspect /backups/b1
python3 -m shardstore.snapshot restore /backups/b1 /data/newstore
```

错误以稳定码返回：`CORRUPT_BACKUP`、`UNSUPPORTED_FORMAT`、
`BACKUP_INCOMPLETE`、`BACKUP_NOT_FOUND`、`BACKUP_EXISTS`、
`TARGET_NOT_EMPTY`。崩溃测试钩子见下文（`*_snapshot_*` /
`*_restore_*` 点）。

## 运行

```bash
# 启动服务（常驻，多进程经 unix socket 访问）
python3 -m shardstore.server --dir /var/lib/mystore --sock /tmp/mystore.sock

# 跑完整验收（多进程：服务子进程 + 客户端子进程；含全部崩溃演练）
python3 acceptance.py            # 约 1 分钟
SCALE_ROWS=200000 python3 acceptance.py   # 更大规模
ONLY="main drill" python3 acceptance.py   # 只跑某个阶段
KEEP=1 python3 acceptance.py     # 保留工作目录便于检查

# 变更订阅验收（独立提交模型、多进程、崩溃/清理边界）
python3 subscription_acceptance.py
SUB_BACKLOG=2000 python3 subscription_acceptance.py
ONLY="cleanup and receipts" python3 subscription_acceptance.py

# 一致性快照验收（独立水位字典模型 + 直接对备份字节校验，多进程崩溃演练）
python3 snapshot_acceptance.py
SNAP_SCALE_ROWS=120000 python3 snapshot_acceptance.py
ONLY="crash" python3 snapshot_acceptance.py
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

# 变更订阅
sub = s.subscribe("etl-a", after_seq=0)             # {"generation": 1, ...}
gen = sub["generation"]
p = s.pull("etl-a", gen, max_commits=100, max_bytes=1 << 20)
for ch in p:                                        # ch.commit_seq/request_id/ops
    consume(ch.ops)
s.ack("etl-a", gen, p.next_after)                   # 连续已处理前缀
s.pause("etl-a", gen); s.resume("etl-a", gen)
r = s.replay("etl-a", gen, after_seq=41)            # 保留范围内重放，不动游标
s.claim("etl-a")                                    # 新消费世代，旧世代作废
s.subscribe_status("etl-a"); s.subscriptions()
s.cleanup(100)                                      # 释放载荷，绝不越过未确认前缀

# 一致性快照（路径为服务端绝对路径；恢复目标必须是空目录）
rep = s.snapshot_export("/backups/b1")             # 水位/计数/键范围/段摘要
s.snapshot_inspect("/backups/b1")                  # 离线校验 + 订阅恢复边界
s.snapshot_restore("/backups/b1", "/data/new")     # 原子发布，绝不覆盖已有数据
```

测试钩子（验收脚本使用）：`pause_copy(on)` 暂停/恢复拷贝线程（同步点），
`arm_crash(point)` 在指定阶段点强制退出（`after_decision`、`during_copy`、
`after_copy`、`after_catchup`、`before_publish`、`after_publish`、
`during_reclaim`、`during_pull`、`during_ack`、`during_cleanup`、
`during_snapshot_segments`、`before_snapshot_manifest`、
`after_snapshot_manifest`、`during_restore_verify`、
`during_restore_build`、`before_restore_publish`、
`after_restore_publish`）。段滚动阈值可用环境变量
`SHARDSTORE_SEG_TARGET`（字节，>=1024）调小以演练多段备份。

## 验收覆盖

`acceptance.py` 用一个**独立的串行字典模型**（每 seq 留全量小快照，不调
用正式路由/迁移代码）核对：复合键排序、空值/BLOB/int64 保真、重传回执
与冲突拒绝、主演练（写 1..6 → 旧扫描读 1、2 → 暂停拷贝时单事务改 2、
删 4、插 7 → split 于 4 → 旧扫描余下 3,4,5,6、新扫描 1,2',3,5,6,7）、
在复制后/追赶后/路由决定前后/回收中强制退出并重开、交错重传
request_id、跨片事务、活跃旧令牌阻止回收、release 后可回收、两个迁移
请求竞争（恰一个胜出）、10 万行运行时生成数据的大范围复制有界内存
（RSS 采样）与多进程并发读者。

`subscription_acceptance.py` 用**独立的提交模型**（有序的
`(request_id, 原始 ops)` 列表，不调用被测订阅/清理实现）核对：交付单元
边界与操作次序、重复键与不存在键删除、重试逐字节一致、计数/字节预算与
超预算单提交、世代切换与旧世代确认无效、慢订阅阻止越界清理而快订阅不受
阻、清理后 request_id 回执与冲突、过期重放明确失败、空流/有符号边界键/
NULL/BLOB、拉取/确认/清理期间强制结束并重开、多进程争用同一订阅、
迁移期间的新提交、大积压分页的有界内存。

`snapshot_acceptance.py` 用**按水位保留全量逻辑行与请求回执的独立字典
模型**，并**直接对备份字节**（独立实现的帧解析、长度与 SHA-256 重算，
不调用导出/恢复实现）核对：跨多分片的 NULL/BLOB/有符号边界键事务、导出
与并发写入及 split 同时进行时备份恰好等于清单水位而不含之后的写、恢复后
行/排序/类型逐字节一致且序号从水位连续、旧 request_id 返回原回执而冲突
被拒、订阅只能从水位重建；在导出段写入/清单发布、恢复校验/分片构建/最终
发布各点强制退出并重开，确认只有完整备份可恢复、空目标不暴露半成品、
已有目录不被覆盖；并覆盖损坏/截断/篡改段、伪造长度与摘要、未知格式版本、
重复主键、乱序帧、缺失段与清单、空库、清理后仍保留的幂等回执、多个源
分片映射到恢复布局、8MiB 超大单值，以及运行时生成大数据量的有界内存
（RSS 采样）与多段滚动。

