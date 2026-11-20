#pragma once

#include <Client/ConnectionPool.h>
#include <Client/IConnections.h>
#include <Client/ConnectionPoolWithFailover.h>
#include <Common/UniqueLock.h>
#include <Core/SettingsEnums.h>
#include <Core/UUID.h>
#include <Interpreters/ClientInfo.h>
#include <Storages/IStorage_fwd.h>
#include <Interpreters/StorageID.h>
#include <sys/types.h>
#include <atomic>


namespace DB
{

class Context;

struct UnavailableShardTracker;
using UnavailableShardTrackerPtr = std::shared_ptr<UnavailableShardTracker>;

class IThrottler;
using ThrottlerPtr = std::shared_ptr<IThrottler>;

struct Progress;
using ProgressCallback = std::function<void(const Progress & progress)>;
// 用于统计和传输查询执行过程中的行数、字节数、数据块数及 rows_before_limit 等性能剖析信息
struct ProfileInfo;
using ProfileInfoCallback = std::function<void(const ProfileInfo & info)>;

struct ClusterFunctionReadTaskResponse;
using ClusterFunctionReadTaskResponsePtr = std::shared_ptr<ClusterFunctionReadTaskResponse>;

class RemoteQueryExecutorReadContext;

class ParallelReplicasReadingCoordinator;

using TaskIterator = std::function<ClusterFunctionReadTaskResponsePtr(size_t)>;

/// This class allows one to launch queries on remote replicas of one shard and get results
// RemoteQueryExecutor 是实现节点间网络通信与分布式查询执行的核心引擎类。它是底层 TCP 客户端连接（IConnections）与上层向量化执行流水线（RemoteSource）之间的核心桥梁。
// RemoteQueryExecutor 类的作用
// 分布式查询发送与会话管理：负责从连接池获取一个或多个副本连接（支持负载均衡与故障转移），向远程节点发送查询语句（或序列化的 QueryPlan）、执行阶段（Stage）、标量上下文（Scalars）以及 GLOBAL IN/JOIN 所需的临时外部表（ExternalTables）。
// 同步与异步流式读取：支持同步阻塞读取（readBlock / read）和基于协程/纤程（Fiber）的非阻塞异步读取（sendQueryAsync / readAsync），将远程传回的原生协议数据包（Packet）解析为数据块（Block）、总计行（Totals）、极值（Extremes）、进度（Progress）或异常。
// 高级分布式协同调度：
// 支持集群表函数（如 s3Cluster）的动态任务分发（TaskIterator）。
// 支持多副本并行读取（ParallelReplicas），与协调器（ParallelReplicasReadingCoordinator）交互以动态分配 Part 和 Mark 范围。
// 连接状态一致性与容错保障：实现分片不可用跳过（skip_unavailable_shards）策略；在查询提前终止（如 LIMIT 满足或客户端取消）时，执行取消（Cancel）与数据包排空（Drain），防止连接池中的 TCP 连接因残留未读数据而发生协议错乱。
class RemoteQueryExecutor
{
public:
    // 指向异步读取上下文类，内部封装了基于 Fiber 的协程堆栈，用于在非阻塞 I/O 遇到 EAGAIN 时挂起当前读取状态并返回 fd。
    using ReadContext = RemoteQueryExecutorReadContext;

    /// To avoid deadlock in case of OOM and timeout in CancellationChecker
    // 带有内存超限（OOM）和取消检查器（CancellationChecker）阻塞保护的互斥锁守卫，防止持有锁期间因内存检查触发死锁。
    using LockAndBlocker = LockAndOverCommitTrackerBlocker<std::lock_guard, std::mutex>;

    /// We can provide additional logic for RemoteQueryExecutor
    /// For example for s3Cluster table function we provide an Iterator over tasks to do.
    /// Nodes involved into the query send request for a new task and we answer them using this object.
    /// In case of parallel reading from replicas we provide a Coordinator object
    /// Every replica will tell us about parts and mark ranges it wants to read and coordinator will
    /// decide whether to deny or to accept that request.
    struct Extension
    {
        std::shared_ptr<TaskIterator> task_iterator = nullptr;
        std::shared_ptr<ParallelReplicasReadingCoordinator> parallel_reading_coordinator = nullptr;
        std::optional<IConnections::ReplicaInfo> replica_info = {};
    };

    /// Takes a connection pool for a node (not cluster)
    RemoteQueryExecutor(
        ConnectionPoolPtr pool,
        const String & query_,
        SharedHeader header_,
        ContextPtr context_,
        ThrottlerPtr throttler = nullptr,
        const Scalars & scalars_ = Scalars(),
        const Tables & external_tables_ = Tables(),
        QueryProcessingStage::Enum stage_ = QueryProcessingStage::Complete,
        std::optional<Extension> extension_ = std::nullopt,
        ConnectionPoolWithFailoverPtr connection_pool_with_failover_ = nullptr,
        std::shared_ptr<const QueryPlan> query_plan_ = nullptr);

    /// Takes already set connection.
    RemoteQueryExecutor(
        Connection & connection,
        const String & query_,
        SharedHeader header_,
        ContextPtr context_,
        ThrottlerPtr throttler_ = nullptr,
        const Scalars & scalars_ = Scalars(),
        const Tables & external_tables_ = Tables(),
        QueryProcessingStage::Enum stage_ = QueryProcessingStage::Complete,

        std::optional<Extension> extension_ = std::nullopt);

    /// Accepts several connections already taken from pool.
    /// The optional `pool` parameter keeps the connection pool alive while entries are in use,
    /// preventing use-after-free when the pool would otherwise be destroyed before the entries.
    RemoteQueryExecutor(
        ConnectionPoolEntries && connections_,
        const String & query_,
        SharedHeader header_,
        ContextPtr context_,
        const ThrottlerPtr & throttler = nullptr,
        const Scalars & scalars_ = Scalars(),
        const Tables & external_tables_ = Tables(),
        QueryProcessingStage::Enum stage_ = QueryProcessingStage::Complete,
        std::shared_ptr<const QueryPlan> query_plan_ = nullptr,
        std::optional<Extension> extension_ = std::nullopt,
        ConnectionPoolWithFailoverPtr pool = nullptr);

    /// Takes a pool and gets one or several connections from it.
    RemoteQueryExecutor(
        const ConnectionPoolWithFailoverPtr & pool,
        const String & query_,
        SharedHeader header_,
        ContextPtr context_,
        const ThrottlerPtr & throttler = nullptr,
        const Scalars & scalars_ = Scalars(),
        const Tables & external_tables_ = Tables(),
        QueryProcessingStage::Enum stage_ = QueryProcessingStage::Complete,
        std::shared_ptr<const QueryPlan> query_plan_ = nullptr,
        std::optional<Extension> extension_ = std::nullopt,
        GetPriorityForLoadBalancing::Func priority_func = {});

    ~RemoteQueryExecutor();

    /// Create connection and send query, external tables and scalars.
    ///
    /// @param query_kind - kind of query, usually it is SECONDARY_QUERY,
    ///                     since this is the queries between servers
    ///                     (for which this code was written in general).
    ///                     But clickhouse-benchmark uses the same code,
    ///                     and it should pass INITIAL_QUERY.
    void sendQuery(ClientInfo::QueryKind query_kind = ClientInfo::QueryKind::SECONDARY_QUERY, AsyncCallback async_callback = {});
    void sendQueryUnlocked(ClientInfo::QueryKind query_kind = ClientInfo::QueryKind::SECONDARY_QUERY, AsyncCallback async_callback = {}) TSA_REQUIRES(was_cancelled_mutex);

    int sendQueryAsync();

    struct ReadResult
    {
        enum class Type : uint8_t
        {
            Data,
            ParallelReplicasToken,
            FileDescriptor,
            Finished,
            Nothing
        };

        explicit ReadResult(Block block_)
            : type(Type::Data)
            , block(std::move(block_))
        {}

        explicit ReadResult(int fd_)
            : type(Type::FileDescriptor)
            , fd(fd_)
        {}

        explicit ReadResult(Type type_)
            : type(type_)
        {
            chassert(type != Type::Data && type != Type::FileDescriptor);
        }

        Type getType() const { return type; }

        Block getBlock()
        {
            chassert(type == Type::Data);
            return std::move(block);
        }

        int getFileDescriptor() const
        {
            chassert(type == Type::FileDescriptor);
            return fd;
        }

        const Type type;
        Block block;
        const int fd{-1};
    };

    /// Read next block of data. Returns empty block if query is finished.
    Block readBlock();

    ReadResult read();

    /// Async variant of read. Returns ready block or file descriptor which may be used for polling.
    ReadResult readAsync();

    /// Receive all remain packets and finish query.
    /// It should be cancelled after read returned empty block.
    void finish();

    /// Cancel query execution. Sends Cancel packet and ignore others.
    /// This method may be called from separate thread.
    void cancel();

    /// Get totals and extremes if any.
    Block getTotals() { return std::move(totals); }
    Block getExtremes() { return std::move(extremes); }

    /// Set callback for progress. It will be called on Progress packet.
    void setProgressCallback(ProgressCallback callback);

    /// Set callback for profile info. It will be called on ProfileInfo packet.
    void setProfileInfoCallback(ProfileInfoCallback callback);

    /// Set the query_id. For now, used by performance test to later find the query
    /// in the server query_log. Must be called before sending the query to the server.
    void setQueryId(const std::string& query_id_) { chassert(!sent_query); query_id = query_id_; }

    /// Specify how we allocate connections on a shard.
    void setPoolMode(PoolMode pool_mode_) { pool_mode = pool_mode_; }

    void setMainTable(StorageID main_table_) { main_table = std::move(main_table_); }

    void setLogger(LoggerPtr logger) { log = logger; }

    void setUnavailableShardTracker(UnavailableShardTrackerPtr tracker) { unavailable_shard_tracker = std::move(tracker); }

    void setDistributedFanout(size_t total_connections) { distributed_fanout = total_connections; }

    const Block & getHeader() const { return *header; }
    const SharedHeader & getSharedHeader() const { return header; }

    IConnections & getConnections() { return *connections; }

    bool needToSkipUnavailableShard();

    /// Reports a skipped shard to `unavailable_shard_tracker` (if any), enforcing the
    /// `max_skip_unavailable_shards_num` / `max_skip_unavailable_shards_ratio` limits.
    /// Throws `TOO_MANY_UNAVAILABLE_SHARDS` once the limits are exceeded.
    void reportShardSkipped();
    // 用于判断在并行副本读取（Parallel Replicas）模式下，当前分配的远程副本是否不可用。
    // extension 存储了分布式并行读取的扩展上下文（RemoteQueryExecutor::Extension）。如果为空，说明当前是一个普通的分布式查询，不涉及并行副本读取扩展，直接返回 false。
    // 检查并行读取协调器（ParallelReadingCoordinator）指针是否非空。
    // 检查刚刚通过 create_connections(...) 创建出的连接集合中，可用连接数量是否为 0。
    // 背景：在 Parallel Replicas 模式下，每个并行读取流（Stream）通常绑定特定的目标副本。如果目标副本宕机、网络不可达或被跳过，连接池返回的有效连接数就会为 0。
    bool isReplicaUnavailable() const { return extension && extension->parallel_reading_coordinator && connections->size() == 0; }

    /// return true if parallel replica packet was processed
    bool processParallelReplicaPacketIfAny();

    bool isFinished() const { return finished; }

    bool isCancelled() const { LockAndBlocker lock(was_cancelled_mutex); return was_cancelled; }

private:
    RemoteQueryExecutor(
        const String & query_,
        SharedHeader header_,
        ContextPtr context_,
        const Scalars & scalars_,
        const Tables & external_tables_,
        QueryProcessingStage::Enum stage_,
        std::shared_ptr<const QueryPlan> query_plan_,
        std::optional<Extension> extension_,
        GetPriorityForLoadBalancing::Func priority_func = {});

    SharedHeader header;
    Block totals;
    Block extremes;
    /// 创建连接的回调函数（可能是单连接或带故障转移的连接池）
    std::function<std::unique_ptr<IConnections>(AsyncCallback)> create_connections;
    // create_connections屌用后返回的链接对象
    std::unique_ptr<IConnections> connections;
    std::unique_ptr<ReadContext> read_context;
    // 存储将要发送到远程分片/副本执行的 SQL 查询语句字符串。
    // 在分布式查询中，这通常不是用户最初输入的原始 SQL，而是经过 StorageDistributed 或 InterpreterSelectQuery 重写后的子查询 SQL（例如把分布式表名替换成了本地表名 local_table，或者下推了 WHERE 条件和 GROUP BY 聚合逻辑）。
    const String query;
    // 存储序列化的查询执行计划（Query Plan）。
    // 传统模式下，ClickHouse 只向远程节点发送 query 字符串，远程节点需要自己重新解析 AST 并生成执行计划；此时 query_plan 为空指针（nullptr）。
    // 当启用查询计划序列化下推（如新版 Query Analyzer 下的某些分布式执行优化，对应配置 serialize_query_plan = 1）时，发起节点会直接构建好逻辑执行计划存入 query_plan。
    std::shared_ptr<const QueryPlan> query_plan;
    // 当前发往远程节点的子查询唯一标识符（Query ID）。
    String query_id;
    // 存储当前查询的只读执行上下文，是 ClickHouse 查询执行期间获取环境配置的核心入口。
    ContextPtr context;

    ProgressCallback progress_callback;
    ProfileInfoCallback profile_info_callback;

    /// Scalars needed to be sent to remote servers
    // Scalars（通常是 std::map<String, Block> 的别名），映射关系为“标量子查询的内部唯一名称 -> 仅包含 1 行 1 列结果的 Block 数据块”。
    // 存储在发起节点（Initiator）本地预先计算好的标量子查询结果，需要随查询一并发送给远程节点。
    Scalars scalars;
    /// Temporary tables needed to be sent to remote servers
    // Tables（通常是 std::map<String, StoragePtr> 的别名），映射关系为“表名 -> 表存储对象指针（一般为内存临时表 StorageMemory）”。
    // 存储需要随查询下发到远程节点的临时表/外部表。
    Tables external_tables;
    // 指定远程分片节点应该将这条子查询执行到哪一个阶段就停止，并把中间结果返回给发起节点。这是 ClickHouse 分布式查询执行（Distributed Query Execution）两阶段聚合架构的核心开关。
    // FetchColumns（读取列数据阶段）：远程节点只负责从磁盘读取并过滤（WHERE）原始列数据，不做聚合或排序，直接把原始行流式传回发起节点。
    // WithMergeableState（可合并状态阶段 —— 最常用）：
    // 远程节点执行本地过滤（WHERE）、部分聚合（GROUP BY 生成中间聚合状态，如 AggregateFunctionState）、部分排序或部分 LIMIT。
    // 远程节点不会计算最终结果（例如 avg() 不会做除法，而是返回 sum 和 count 的中间二进制状态），而是把可合并的中间数据块发回给发起节点，由发起节点做最终的全局合并（Merge）。
    // Complete（完全执行阶段）：远程节点独立完成整个查询的所有阶段（包括最终聚合、合并、排序、全局 LIMIT 等），直接返回最终成品结果（常见于查询条件恰好命中了分片键，所有数据都在单个分片上的优化场景）。
    QueryProcessingStage::Enum stage;

    std::optional<Extension> extension;
    /// Initiator identifier for distributed task processing
    std::shared_ptr<TaskIterator> task_iterator;

    /// Streams for reading from temporary tables and following sending of data
    /// to remote servers for GLOBAL-subqueries
    //外部表/临时表的数据
    std::vector<ExternalTablesData> external_tables_data; // STYLE_CHECK_ALLOW_STD_CONTAINERS
    std::mutex external_tables_mutex;

    /// Connections to replicas are established, but no queries are sent yet
    bool established = false;

    /// Query is sent (used before getting first block)
    bool sent_query { false };

    /** All data from all replicas are received, before EndOfStream packet.
      * To prevent desynchronization, if not all data is read before object
      * destruction, it's required to send cancel query request to replicas and
      * read all packets before EndOfStream
      */
    bool finished = false;

    /** Test-only. True only while this executor's reading thread is parked at the
      * `remote_query_executor_receive_packet_pause` failpoint, so that the drain pause in
      * `finish` cannot be satisfied by a sibling shard. False unless the failpoints are enabled.
      */
    std::atomic_bool in_receive_packet_window = false;

    /** Cancel query request was sent to all replicas because data is not needed anymore
      * This behaviour may occur when:
      * - data size is already satisfactory (when using LIMIT, for example)
      * - an exception was thrown from client side
      */
    mutable std::mutex was_cancelled_mutex;
    bool was_cancelled TSA_GUARDED_BY(was_cancelled_mutex) = false;

    /// Whether this replica has sent its initial announcement. Until it does, the only packet it can
    /// owe us is that announcement - see `tryCancel`.
    std::atomic_bool announcement_received = false;

    /// Set when `tryCancel` left a packet undrained, so the destructor knows the connection is
    /// unsynchronised and must be disconnected rather than returned to the pool.
    std::atomic_bool drain_was_skipped = false;

    /** An exception from replica was received. No need in receiving more packets or
      * requesting to cancel query execution
      */
    bool got_exception_from_replica = false;

    /** Whether the shard has already returned any data block with rows.
      * Used by `skip_unavailable_shards_mode = unavailable_or_exception_before_processing`
      * to decide whether an exception arrived before the shard started returning results.
      */
    bool got_data_from_replica = false;

    /// Cached `skip_unavailable_shards` settings, read once at construction time.
    const bool skip_unavailable_shards;
    const SkipUnavailableShardsMode skip_unavailable_shards_mode;

    /** Returns true if an exception packet received from the shard should be silently
      * ignored according to `skip_unavailable_shards` and `skip_unavailable_shards_mode`.
      */
    bool shouldIgnoreShardException(int exception_code) const;

    /** Unknown packet was received from replica. No need in receiving more packets or
      * requesting to cancel query execution
      */
    bool got_unknown_packet_from_replica = false;

#if defined(OS_LINUX) || defined(OS_DARWIN)
    bool packet_in_progress = false;
#endif

    PoolMode pool_mode = PoolMode::GET_MANY;
    StorageID main_table = StorageID::createEmpty();

    LoggerPtr log = getLogger("RemoteQueryExecutor");

    UnavailableShardTrackerPtr unavailable_shard_tracker;
    bool shard_skip_reported = false;

    /// Total number of remote connections across all shards, used to scale interactive_delay.
    // 发起节点（Initiator）在执行一条分布式查询时，一共同时向多少个远程分片/副本发起了子查询请求。
    // 例如，查询一张有 10 个分片的分布式表，那么 distributed_fanout 就是 10；如果开启了并行副本读取（每个分片选 2 个副本），扇出度就是 20。
    size_t distributed_fanout = 0;

    GetPriorityForLoadBalancing::Func priority_func;

    const bool read_packet_type_separately = false;

    /// Send all scalars to remote servers
    void sendScalars();

    /// Send all temporary tables to remote servers
    void sendExternalTables();

    void processReadTaskRequest();

    void processMergeTreeReadTaskRequest(ParallelReadRequest request);
    void processMergeTreeInitialReadAnnouncement(InitialAllRangesAnnouncement announcement);

    /// If wasn't sent yet, send request to cancel all connections to replicas
    void cancelUnlocked() TSA_REQUIRES(was_cancelled_mutex);
    void tryCancel(const char * reason) TSA_REQUIRES(was_cancelled_mutex);

    /// Returns true if query was sent
    bool isQueryPending() const;

    /// Returns true if exception was thrown
    bool hasThrownException() const;

    /// Process packet for read and return data block if possible.
    ReadResult processPacket(Packet packet);
};

ThrottlerPtr getThrottler(const ContextPtr & context);
}
