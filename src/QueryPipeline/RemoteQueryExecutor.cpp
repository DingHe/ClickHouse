#include <Core/ProtocolDefines.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <QueryPipeline/RemoteQueryExecutorReadContext.h>
#include <QueryPipeline/UnavailableShardTracker.h>

#include <Columns/ColumnConst.h>
#include <Common/CurrentThread.h>
#include <Common/FailPoint.h>
#include <Common/Logger.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Common/logger_useful.h>
#include <Core/Protocol.h>
#include <Core/Settings.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Transforms/LimitsCheckingTransform.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/SelectQueryInfo.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <Interpreters/ProcessList.h>
#include <IO/ConnectionTimeouts.h>
#include <Client/ConnectionEstablisher.h>
#include <Client/MultiplexedConnections.h>
#include <Client/HedgedConnections.h>
#include <Storages/MergeTree/ParallelReplicasReadingCoordinator.h>
#include <Storages/StorageMemory.h>

#include <Columns/ColumnBLOB.h>

#include <Access/AccessControl.h>
#include <Access/User.h>
#include <Access/Role.h>

namespace ProfileEvents
{
    extern const Event SuspendSendingQueryToShard;
    extern const Event ReadTaskRequestsReceived;
    extern const Event MergeTreeReadTaskRequestsReceived;
    extern const Event ParallelReplicasAvailableCount;
    extern const Event DistributedShardsSkipped;
}

namespace DB
{
namespace Setting
{
    extern const SettingsSeconds max_execution_time;
    extern const SettingsSeconds max_estimated_execution_time;
    extern const SettingsBool skip_unavailable_shards;
    extern const SettingsSkipUnavailableShardsMode skip_unavailable_shards_mode;
    extern const SettingsOverflowMode timeout_overflow_mode;
    extern const SettingsBool use_hedged_requests;
    extern const SettingsBool push_external_roles_in_interserver_queries;
    extern const SettingsMilliseconds parallel_replicas_connect_timeout_ms;
    extern const SettingsUInt64 max_network_bandwidth;
    extern const SettingsUInt64 max_network_bytes;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_PACKET_FROM_SERVER;
    extern const int SYSTEM_ERROR;
    extern const int UNKNOWN_TABLE;
    extern const int UNKNOWN_DATABASE;
    extern const int BAD_ARGUMENTS;
}

namespace FailPoints
{
    extern const char remote_query_executor_cancel_before_send[];
    extern const char remote_query_executor_receive_packet_pause[];
    extern const char remote_query_executor_finish_drain_pause[];
}

ThrottlerPtr getThrottler(const ContextPtr & context)
{
    const Settings & settings = context->getSettingsRef();

    ThrottlerPtr user_level_throttler;
    if (auto process_list_element = context->getProcessListElement())
        user_level_throttler = process_list_element->getUserNetworkThrottler();

    /// Network bandwidth limit, if needed.
    ThrottlerPtr throttler;
    if (settings[Setting::max_network_bandwidth] || settings[Setting::max_network_bytes])
    {
        throttler = std::make_shared<Throttler>(
            settings[Setting::max_network_bandwidth],
            settings[Setting::max_network_bytes],
            "Limit for bytes to send or receive over network exceeded.",
            user_level_throttler);
    }
    else
        throttler = user_level_throttler;

    return throttler;
}


RemoteQueryExecutor::RemoteQueryExecutor(
    const String & query_,
    SharedHeader header_,
    ContextPtr context_,
    const Scalars & scalars_,
    const Tables & external_tables_,
    QueryProcessingStage::Enum stage_,
    std::shared_ptr<const QueryPlan> query_plan_,
    std::optional<Extension> extension_,
    GetPriorityForLoadBalancing::Func priority_func_)
    : header(header_)
    , query(query_)
    , query_plan(std::move(query_plan_))
    , context(context_)
    , scalars(scalars_)
    , external_tables(external_tables_)
    , stage(stage_)
    , extension(extension_)
    , skip_unavailable_shards(context->getSettingsRef()[Setting::skip_unavailable_shards])
    , skip_unavailable_shards_mode(context->getSettingsRef()[Setting::skip_unavailable_shards_mode])
    , priority_func(priority_func_)
    , read_packet_type_separately(context->canUseParallelReplicasOnInitiator() && !context->getSettingsRef()[Setting::use_hedged_requests])
{
    if (stage == QueryProcessingStage::QueryPlan && !query_plan)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query plan is not passed for QueryPlan processing stage");
}

RemoteQueryExecutor::RemoteQueryExecutor(
    ConnectionPoolPtr pool,
    const String & query_,
    SharedHeader header_,
    ContextPtr context_,
    ThrottlerPtr throttler,
    const Scalars & scalars_,
    const Tables & external_tables_,
    QueryProcessingStage::Enum stage_,
    std::optional<Extension> extension_,
    ConnectionPoolWithFailoverPtr connection_pool_with_failover_,
    std::shared_ptr<const QueryPlan> query_plan_)
    : RemoteQueryExecutor(query_, header_, context_, scalars_, external_tables_, stage_, query_plan_, extension_)
{
    create_connections = [this, pool, throttler, extension_, connection_pool_with_failover_](AsyncCallback)
    {
        const Settings & settings = context->getSettingsRef();
        auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithoutFailover(settings)
                            .withUnsecureConnectionTimeout(settings[Setting::parallel_replicas_connect_timeout_ms])
                            .withSecureConnectionTimeout(settings[Setting::parallel_replicas_connect_timeout_ms]);

        ConnectionPoolWithFailover::TryResult result;
        std::string fail_message;
        if (main_table)
        {
            auto table_name = main_table.getQualifiedName();

            ConnectionEstablisher connection_establisher(pool, &timeouts, settings, log, &table_name);
            connection_establisher.run(result, fail_message);
        }
        else
        {
            ConnectionEstablisher connection_establisher(pool, &timeouts, settings, log, nullptr);
            connection_establisher.run(result, fail_message);
        }

        ConnectionPoolEntries connection_entries;
        if (!result.entry.isNull() && result.is_usable)
        {
            chassert(result.entry->isConnected());

            const auto protocol_version = result.entry->getServerRevision(ConnectionTimeouts{});
            const auto parallel_replicas_version = result.entry->getParallelReplicasProtocolVersion();
            const auto query_plan_serialization_version = result.entry->getQueryPlanSerializationVersion();

            if (extension_ && extension_->parallel_reading_coordinator)
            {
                // consider only replicas with support of stream id, otherwise we can get incorrect result
                // replicas with older version considered as unavailable
                if (protocol_version >= DBMS_MIN_REVISION_WITH_PARALLEL_REPLICAS
                    && parallel_replicas_version >= DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_STREAM_ID
                    && (!query_plan || query_plan_serialization_version >= DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_PARALLEL_REPLICAS))
                {
                    ProfileEvents::increment(ProfileEvents::ParallelReplicasAvailableCount);

                    connection_entries.emplace_back(std::move(result.entry));
                }
                else
                {
                    LOG_DEBUG(
                        log,
                        "Disconnecting replica {} (protocol_version={}, parallel_replicas_version={}, "
                        "query_plan_serialization_version={}): "
                        "remote replica doesn't support stream id (requires parallel_replicas_version >= {}) or query plan serialization "
                        "for parallel replicas (requires query_plan_serialization_version >= {})",
                        result.entry->getDescription(),
                        protocol_version,
                        parallel_replicas_version,
                        query_plan_serialization_version,
                        DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_STREAM_ID,
                        DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_PARALLEL_REPLICAS);
                    result.entry->disconnect();
                }
            }
            else
            {
                connection_entries.emplace_back(std::move(result.entry));
            }
        }
        else
        {
            chassert(!fail_message.empty());
            if (result.entry.isNull())
            {
                LOG_DEBUG(log, "Failed to connect to replica {}. {}", pool->getAddress(), fail_message);
                if (connection_pool_with_failover_)
                    connection_pool_with_failover_->incrementErrorCount(pool);
            }
            else
                LOG_DEBUG(log, "Replica is not usable for remote query execution: {}. {}", pool->getAddress(), fail_message);
        }

        auto res = std::make_unique<MultiplexedConnections>(std::move(connection_entries), context, throttler);
        if (extension_ && extension_->replica_info)
            res->setReplicaInfo(*extension_->replica_info);

        return res;
    };
}

RemoteQueryExecutor::RemoteQueryExecutor(
    Connection & connection,
    const String & query_,
    SharedHeader header_,
    ContextPtr context_,
    ThrottlerPtr throttler,
    const Scalars & scalars_,
    const Tables & external_tables_,
    QueryProcessingStage::Enum stage_,
    std::optional<Extension> extension_)
    : RemoteQueryExecutor(query_, header_, context_, scalars_, external_tables_, stage_, nullptr, extension_)
{
    create_connections = [this, &connection, throttler, extension_](AsyncCallback)
    {
        auto res = std::make_unique<MultiplexedConnections>(connection, context, throttler);
        if (extension_ && extension_->replica_info)
            res->setReplicaInfo(*extension_->replica_info);
        return res;
    };
}

RemoteQueryExecutor::RemoteQueryExecutor(
    ConnectionPoolEntries && connections_,
    const String & query_,
    SharedHeader header_,
    ContextPtr context_,
    const ThrottlerPtr & throttler,
    const Scalars & scalars_,
    const Tables & external_tables_,
    QueryProcessingStage::Enum stage_,
    std::shared_ptr<const QueryPlan> query_plan_,
    std::optional<Extension> extension_,
    ConnectionPoolWithFailoverPtr pool)
    : RemoteQueryExecutor(query_, header_, context_, scalars_, external_tables_, stage_, std::move(query_plan_), extension_)
{
    /// Capture `pool` in the lambda to prevent the connection pool from being destroyed
    /// while entries are still in use. The Entry objects hold raw references (via PoolEntryHelper)
    /// back to the pool's internal PooledObject and PoolBase structures, so the pool must
    /// outlive all Entry objects.
    create_connections = [this, connections_, throttler, extension_, pool](AsyncCallback) mutable
    {
        auto res = std::make_unique<MultiplexedConnections>(std::move(connections_), context, throttler);
        if (extension_ && extension_->replica_info)
            res->setReplicaInfo(*extension_->replica_info);
        return res;
    };
}

RemoteQueryExecutor::RemoteQueryExecutor(
    const ConnectionPoolWithFailoverPtr & pool,
    const String & query_,
    SharedHeader header_,
    ContextPtr context_,
    const ThrottlerPtr & throttler,
    const Scalars & scalars_,
    const Tables & external_tables_,
    QueryProcessingStage::Enum stage_,
    std::shared_ptr<const QueryPlan> query_plan_,
    std::optional<Extension> extension_,
    GetPriorityForLoadBalancing::Func priority_func_)
    : RemoteQueryExecutor(query_, header_, context_, scalars_, external_tables_, stage_, std::move(query_plan_), extension_, priority_func_)
{
    create_connections = [this, pool, throttler](AsyncCallback async_callback)->std::unique_ptr<IConnections>
    {
        const Settings & current_settings = context->getSettingsRef();
        auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(current_settings);

#if defined(OS_LINUX)
        if (current_settings[Setting::use_hedged_requests])
        {
            std::shared_ptr<QualifiedTableName> table_to_check = nullptr;
            if (main_table)
                table_to_check = std::make_shared<QualifiedTableName>(main_table.getQualifiedName());

            auto res = std::make_unique<HedgedConnections>(
                pool, context, timeouts, throttler, pool_mode, table_to_check, std::move(async_callback), priority_func);
            if (extension && extension->replica_info)
                res->setReplicaInfo(*extension->replica_info);
            return res;
        }
#endif

        ConnectionPoolEntries connection_entries;
        std::optional<bool> skip_unavailable_endpoints;
        if (extension && extension->parallel_reading_coordinator)
            skip_unavailable_endpoints = true;

        if (main_table)
        {
            auto try_results = pool->getManyChecked(
                timeouts,
                current_settings,
                pool_mode,
                main_table.getQualifiedName(),
                std::move(async_callback),
                skip_unavailable_endpoints,
                priority_func);
            connection_entries.reserve(try_results.size());
            for (auto & try_result : try_results)
                connection_entries.emplace_back(std::move(try_result.entry));
        }
        else
        {
            connection_entries = pool->getMany(
                timeouts, current_settings, pool_mode, std::move(async_callback), skip_unavailable_endpoints, priority_func);
        }

        auto res = std::make_unique<MultiplexedConnections>(std::move(connection_entries), context, throttler);
        if (extension && extension->replica_info)
            res->setReplicaInfo(*extension->replica_info);
        return res;
    };
}

RemoteQueryExecutor::~RemoteQueryExecutor()
{
    /// We should finish establishing connections to disconnect it later,
    /// so these connections won't be in the out-of-sync state.
    if (read_context && !established)
    {
        /// Set was_cancelled, so the query won't be sent after creating connections.
        {
            LockAndBlocker lock(was_cancelled_mutex);
            was_cancelled = true;
        }

        /// Cancellation may throw (i.e. some timeout), and in case of pipeline
        /// had not been properly created properly (EXCEPTION_BEFORE_START)
        /// cancel will not be sent, so cancellation will be done from dtor and
        /// will throw.
        try
        {
            read_context->cancel();
        }
        catch (...)
        {
            tryLogCurrentException(log);
        }
    }

    /** If interrupted in the middle of the loop of communication with replicas, then interrupt
      * all connections, then read and skip the remaining packets to make sure
      * these connections did not remain hanging in the out-of-sync state.
      */
    if (established || ((isQueryPending() || drain_was_skipped) && connections))
    {
        /// May also throw (so as cancel() above)
        try
        {
            connections->disconnect();
        }
        catch (...)
        {
            tryLogCurrentException(log);
        }
    }
}

/** If we receive a block with slightly different column types, or with excessive columns,
  *  we will adapt it to expected structure.
  */
static Block adaptBlockStructure(const Block & block, const Block & header)
{
    /// Special case when reader doesn't care about result structure. Deprecated and used only in Benchmark, PerformanceTest.
    if (header.empty())
        return block;

    Block res;
    res.info = block.info;

    for (const auto & elem : header)
    {
        ColumnPtr column;

        if (elem.column && isColumnConst(*elem.column))
        {
            /// We expect constant column in block.
            /// If block is not empty, then get value for constant from it,
            /// because it may be different for remote server for functions like version(), uptime(), ...
            if (block.rows() > 0 && block.has(elem.name))
            {
                /// Const column is passed as materialized. Get first value from it.
                ///
                /// TODO: check that column contains the same value.
                /// TODO: serialize const columns.
                auto col = block.getByName(elem.name);
                if (const auto * blob = typeid_cast<const ColumnBLOB *>(col.column.get()))
                    col.column = blob->convertFrom();
                col.column = col.column->cut(0, 1);

                column = castColumn(col, elem.type);

                if (!isColumnConst(*column))
                    column = ColumnConst::create(column, block.rows());
                else
                    /// It is not possible now. Just in case we support const columns serialization.
                    column = column->cloneResized(block.rows());
            }
            else
                column = elem.column->cloneResized(block.rows());
        }
        else
        {
            const auto & col = block.getByName(elem.name);
            if (auto * blob = typeid_cast<ColumnBLOB *>(col.column->assumeMutable().get()))
            {
                blob->addCast(col.type, elem.type);
                column = col.column;
            }
            else
                column = castColumn(col, elem.type);
        }

        res.insert({column, elem.type, elem.name});
    }
    return res;
}

// RemoteQueryExecutor 负责分布式查询执行过程中，作为客户端向远程节点（Replica/Shard）发送查询请求并接收结果。
// sendQuery 是发送查询的入口方法，主要作用是在线程安全的加锁保护下发送完整的查询请求协议包。
// ClientInfo::QueryKind query_kind 表示当前查询的类型（源于 ClientInfo 类）。INITIAL_QUERY（初始查询）：由外部客户端（如 clickhouse-client 或 HTTP 接口）直接发起的原始查询。SECONDARY_QUERY（二级/子查询）：由分布式表引擎（Distributed）或某些表函数（如 remote()）在执行初始查询时，拆分并下发到各个分片节点的子查询。
// AsyncCallback async_callback 用于支持 ClickHouse 的异步网络 I/O（基于 Fiber/协程或 Epoll 事件驱动）。
// 当 async_callback 为空时：发送查询采用同步阻塞模式，直接在当前线程等待 socket 可写并发送数据。
// 当 async_callback 非空时：采用异步模式。如果底层 socket 暂时不可写（触发 EAGAIN / EWOULDBLOCK），不会阻塞当前工作线程，而是将文件描述符（fd）和等待事件注册给外部调度器（例如查询管道的异步执行器），待 socket 就绪后再恢复执行。

void RemoteQueryExecutor::sendQuery(ClientInfo::QueryKind query_kind, AsyncCallback async_callback)
{
    /// Query cannot be canceled in the middle of the send query,
    /// since there are multiple packets:
    /// - Query
    /// - Data (multiple times)
    ///
    /// And after the Cancel packet none Data packet can be sent, otherwise the remote side will throw:
    ///
    ///     Unexpected packet Data received from client
    ///
    // ClickHouse 的原生 TCP 通信协议中，客户端发送一个查询并不是只发一个数据包，而是由多个连续的数据包组成：首先是 Query 包（包含 SQL 文本、设置参数、ClientInfo 等），
    // 紧接着可能跟着零个或多个 Data 包（用于传递外部表 external tables 数据或临时表数据，最后以一个空 Data 包表示结束）
    // 协议状态机限制：如果在发送这一系列包的中途，另一个线程触发了查询取消（向远程节点发送了 Cancel 包），而发送线程随后又把剩余的 Data 包发了出去，远程服务端在收到 Cancel 后已切换状态，此时再收到 Data 包就会违反协议状态机，直接抛出异常：Unexpected packet Data received from client。
    // 防中途取消（互斥保护）：确保当前线程在完整发送 Query 包和所有 Data 包期间，其他线程无法获取 was_cancelled_mutex 来执行 cancel() 操作（即阻止在发送中途向服务端发送 Cancel 包）。
    LockAndBlocker guard(was_cancelled_mutex);
    sendQueryUnlocked(query_kind, async_callback);
}
// 实际执行查询发送逻辑的核心方法
// 不仅负责发送 SQL 语句本身，还涵盖了故障注入测试、连接建立、副本可用性检查、并行读取协调、版本号校验、角色权限透传（RBAC/行级策略），以及标量/执行计划/外部表的发送。
void RemoteQueryExecutor::sendQueryUnlocked(ClientInfo::QueryKind query_kind, AsyncCallback async_callback)
{
    /// Emulate a concurrent cancel() landing right before the query is sent.
    // ClickHouse 的故障注入（FailPoint）宏，仅在测试模式下激活。用于模拟“查询刚刚准备发送前，另一个线程并发调用了 cancel()”的竞态场景。
    fiu_do_on(FailPoints::remote_query_executor_cancel_before_send, { was_cancelled = true; });
    // 如果查询已经发送过（保证幂等性），或者在进入临界区前已经被取消，直接返回，不执行任何网络操作。
    if (sent_query || was_cancelled)
        return;
    // 调用工厂方法创建并获取与目标分片/副本的连接对象（IConnections，可能是单副本连接或带故障转移的多副本连接池）。
    connections = create_connections(async_callback);
    // 在当前函数作用域内将 async_callback 绑定到 connections 上，
    // 确保后续的 sendQuery、sendScalars、sendQueryPlan、sendExternalTables 都能在异步模式下运行；函数退出时自动清理/重置回调。
    // RAII 是 Resource Acquisition Is Initialization 的缩写，中文通常翻译为“资源获取即初始化”。
    // 用来确保程序在任何情况下（包括正常结束、提前 return、甚至发生异常崩溃退出时）都不会发生资源泄漏。
    AsyncCallbackSetter<IConnections> async_callback_setter(connections.get(), async_callback);

    const auto & settings = context->getSettingsRef();
    // 检查目标副本是否全部不可用，或者当前配置允许跳过不可用的分片（如 skip_unavailable_shards = 1）。
    if (isReplicaUnavailable() || needToSkipUnavailableShard())
    {
        /// To avoid sending the query again in the read(), we need to update the following flags:
        // 更新状态标志
        // 防止后续调用 read() 拉取数据时误以为查询还没发出去而重试发送
        was_cancelled = true;
        finished = true;
        sent_query = true;

        /// We need to tell the coordinator not to wait for this replica.
        // 如果当前处于“多副本并行读取（Parallel Replicas）”模式，extension 会持有并行读取协调器（parallel_reading_coordinator）。
        // 此时必须显式通知协调器将该副本标记为不可用，否则协调器会一直死等该副本的数据分配请求。
        if (extension && extension->parallel_reading_coordinator)
        {
            // ClickHouse 源码中自定义的一个调试断言宏（Macro）（定义在 src/base/defines.h 中）
            // 在开启断言的构建模式下（如 Debug 模式，或开启了 ENABLE_ASSERTS）：
            // 它会求值括号内的条件表达式（例如 chassert(extension->replica_info)）。
            // 如果条件为 true（非空/非零）：什么都不做，继续向下执行。
            chassert(extension->replica_info);
            extension->parallel_reading_coordinator->markReplicaAsUnavailable(extension->replica_info->number_of_current_replica);
        }

        return;
    }
    // 标记物理连接已经成功建立并可用（在异常或取消清理时用于判断连接状态）。
    established = true;
    // 从当前查询的 Settings 中提取包含故障转移策略的 TCP 连接超时、发送超时和接收超时配置。
    auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(settings);
    // 拷贝当前上下文的 ClientInfo（包含初始用户、初始 Query ID、客户端地址、版本号等）
    ClientInfo modified_client_info = context->getClientInfo();
    // 将 query_kind 更新为传入的参数值。
    modified_client_info.query_kind = query_kind;

    /// A distributed query must carry a known initiator version: the receiving server uses it for
    /// version-gated compatibility decisions (e.g. whether to enable the analyzer, see `TCPHandler`).
    /// A zero version means the initiating query context was not populated as an initial query
    /// (a real client always reports its version, and a server that (re-)initiates a query fills it
    /// with its own version). Sending zero silently triggers wrong compatibility downgrades on the
    /// remote, so fail loudly instead.
    // 强制校验发起端版本号不能全为 0。
    // 背景原因：远端服务器（TCPHandler）在接收分布式查询时，会依据 ClientInfo 中的发起端版本号做版本门控兼容性决策（例如是否启用新版 Query Analyzer）。如果版本号为 0，远端会错误地触发低版本降级逻辑导致诡异 Bug，因此这里选择直接抛出 LOGICAL_ERROR 暴露问题。
    if (modified_client_info.client_version_major == 0
        && modified_client_info.client_version_minor == 0
        && modified_client_info.client_version_patch == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Sending a distributed query with unknown (zero) client version. "
            "The query context was not initialized as an initial query");

    /// Forward this node's current roles so the remote scopes row policies the same way (gated by the setting).
    /// Reset first against stale/injected values, and skip when initial_user was rewritten (remote(user=>...)).
    // 在节点间查询中透传当前用户已激活的角色列表，确保远端节点在应用行级安全策略（Row Policies）时拥有与发起节点一致的权限上下文。
    // 清空旧值，防止恶意注入或残留脏数据。
    modified_client_info.current_roles.reset();
    if (context->getSettingsRef()[Setting::push_external_roles_in_interserver_queries]
        // 仅当 initial_user == current_user 时才透传（如果使用了类似 remote('host', db, table, 'other_user', 'pass') 显式重写了连接用户，则不能把原用户的角色带过去）。
        && modified_client_info.initial_user == modified_client_info.current_user)
    {
        const auto & access_control = context->getAccessControl();
        Strings current_role_names;
        for (const auto & role_id : context->getCurrentRoles())
        {
            /// tryReadName: skip a concurrently-dropped role (its policies already target nobody).
            if (auto name = access_control.tryReadName(role_id))
                current_role_names.push_back(*name);
        }
        modified_client_info.current_roles = std::move(current_role_names);
    }
    // 如果存在 extension（即启用了多副本并行读取），告诉远端节点“你需要主动向发起节点（Initiator）请求数据分片（Marks/Ranges）以协同完成扫描”。
    if (extension)
        modified_client_info.collaborate_with_initiator = true;

    // Collect all roles granted on this node and pass those to the remote node
    // 收集的是发起节点上授予给 initial_user 的所有可用角色（例如通过 LDAP/外部认证系统在本地映射的角色，远端节点本地可能没有这些授权元数据），
    // 一并打包发送给远端节点供其做完整的 RBAC 鉴权。
    Strings local_granted_roles;
    if (context->getSettingsRef()[Setting::push_external_roles_in_interserver_queries])
    {
        auto user = context->getAccessControl().read<User>(modified_client_info.initial_user, false);
        boost::container::flat_set<String> granted_roles;
        if (user)
        {
            const auto & access_control = context->getAccessControl();
            for (const auto & e : user->granted_roles.getElements())
            {
                // `tryReadNames` instead of `readNames` because the original user might have a dropped role.
                auto names = access_control.tryReadNames(e.ids);
                granted_roles.insert(names.begin(), names.end());
            }
        }
        local_granted_roles.insert(local_granted_roles.end(), granted_roles.begin(), granted_roles.end());
    }
    // 设置分布式查询的扇出度（即当前查询一共拆分发往多少个分片节点），常用于远端节点的线程池调度或资源动态分配优化。
    if (distributed_fanout > 0)
        connections->setDistributedFanout(distributed_fanout);
    // 核心网络发送调用！将超时设置、SQL 文本 query、query_id、查询执行阶段 stage（如 FetchColumns / WithMergeableState / Complete）、
    // 修改后的 modified_client_info、是否使用压缩（true）以及 local_granted_roles 序列化为 Protocol::Client::Query 数据包发送到远端。
    connections->sendQuery(timeouts, query, query_id, stage, modified_client_info, true, local_granted_roles);
    // 状态流转，说明连接已从“刚建立握手”阶段进入“已发送查询”阶段。
    established = false;
    // 正式标记查询头已发送完成。
    sent_query = true;
    // 发送附属数据包（标量、执行计划、外部表）
    // 发送本地预先计算好的标量子查询结果（例如 WHERE x = (SELECT max(y) FROM local_table) 中算出的常量结果块），避免每个远端分片重复计算。
    sendScalars();
    // 如果启用了序列化查询计划下推（如新版 Analyzer 下的某些分布式执行模式），将构建好的 QueryPlan 直接发送给远端执行。
    if (query_plan)
        connections->sendQueryPlan(*query_plan);
    // 发送查询附带的外部表/临时表数据块（Protocol::Client::Data 包），并在最后发送一个空的数据块（Empty Block）作为整个请求发送阶段结束的标志。此后远端节点正式开始执行查询。
    sendExternalTables();
}

int RemoteQueryExecutor::sendQueryAsync()
{
#if defined(OS_LINUX) || defined(OS_DARWIN)
    LockAndBlocker lock(was_cancelled_mutex);
    if (was_cancelled)
        return -1;

    if (!read_context)
        read_context = std::make_unique<ReadContext>(
            *this,
            /*suspend_when_query_sent*/ true,
            read_packet_type_separately);

    /// If query already sent, do nothing. Note that we cannot use sent_query flag here,
    /// because we can still be in process of sending scalars or external tables.
    if (read_context->isQuerySent())
        return -1;

    read_context->resume();

    if (read_context->isQuerySent())
        return -1;

    ProfileEvents::increment(ProfileEvents::SuspendSendingQueryToShard); /// Mostly for testing purposes.
    return read_context->getFileDescriptor();
#else
    sendQuery();
    return -1;
#endif
}

Block RemoteQueryExecutor::readBlock()
{
    while (true)
    {
        auto res = read();

        if (res.getType() == ReadResult::Type::Data)
            return res.getBlock();
    }
}

// 分布式查询执行器中同步拉取远程节点返回数据（或控制信号）的核心主循环方法（异步协程模式下对应的是 readAsync）
// 会按需触发查询发送，随后在一个 while (true) 循环中不断接收远程节点发回的原生 TCP 数据包（Packet），经过 processPacket 解析状态机处理后，直到拿到真正的数据块（Block）或并行副本协调令牌（ParallelReplicasToken），或者直到查询结束/被取消才返回。
// 返回值 RemoteQueryExecutor::ReadResult：
// 封装了读取结果的变体/状态对象，包含一个类型标识 Type 和具体负载数据，主要有以下几种返回类型：
// ReadResult::Type::Data：携带一个 Block（数据块）。如果 Block 非空，表示从远程读到了一批查询结果行；如果 Block 为空（Block()），表示当前远程流已经正常读取完毕（EOF）或已被取消。
// ReadResult::Type::ParallelReplicasToken：用于多副本并行读取（Parallel Replicas）模式下，向本地协调器或下游算子传递同步协调令牌。
// ReadResult::Type::Nothing（在 processPacket 内部产生）：表示收到了进度包（Progress）、性能剖析信息（ProfileInfo）、日志包（Log）等非数据控制包，外层 read() 不会退出循环，而是继续接收下一个包。
RemoteQueryExecutor::ReadResult RemoteQueryExecutor::read()
{
    // ClickHouse 的执行管道采用按需拉取（Pull-based）模型。只有当下游算子第一次调用 read() 真正要数据时，如果查询还没发出去，才调用 sendQuery() 建立连接并下发 SQL、标量和外部表。
    if (!sent_query)
    {
        sendQuery();

        /// `connections` stays null if sendQuery() was cancelled before sending,
        /// so guard the dereference below (as every other use of it does).
        {
            LockAndBlocker lock(was_cancelled_mutex);
            if (was_cancelled)
                return ReadResult(Block());
        }

        if (context->getSettingsRef()[Setting::skip_unavailable_shards] && (0 == connections->size()))
            return ReadResult(Block());
    }

    while (true)
    {
        // 2. 主接收循环与接收前取消检查
        {
            LockAndBlocker lock(was_cancelled_mutex);
            if (was_cancelled)
                return ReadResult(Block());
        }

        /// Parks the reader in the window this fix is about: `was_cancelled` has just been checked
        /// and the mutex released, so a parallel `onUpdatePorts` can cancel and drain these
        /// connections before `receivePacket` below runs.
        fiu_do_on(FailPoints::remote_query_executor_receive_packet_pause, {
            in_receive_packet_window = true;
            FailPointInjection::notifyPauseAndWaitForResume(FailPoints::remote_query_executor_receive_packet_pause);
            in_receive_packet_window = false;
        });
        // 调用底层连接接口从网络 socket 读取并反序列化一个完整的原生协议数据包（Packet，可能是 Data、Exception、Progress、EndOfStream 等）。
        // 在同步模式下，如果远端还没发来数据，线程会阻塞在这里等待（直到收到数据或触发 receive_timeout 超时异常）。
        auto packet = connections->receivePacket();

        LockAndBlocker lock(was_cancelled_mutex);
        if (was_cancelled)
            return ReadResult(Block());
        // 解析处理数据包并决定是否返回给上游
        auto anything = processPacket(std::move(packet));

        if (anything.getType() == ReadResult::Type::Data || anything.getType() == ReadResult::Type::ParallelReplicasToken)
            return anything;
    }
}

RemoteQueryExecutor::ReadResult RemoteQueryExecutor::readAsync()
{
#if defined(OS_LINUX) || defined(OS_DARWIN)
    if (!read_context)
    {
        LockAndBlocker lock(was_cancelled_mutex);
        if (was_cancelled)
            return ReadResult(Block());

        read_context = std::make_unique<ReadContext>(
            *this,
            /*suspend_when_query_sent*/ false,
            read_packet_type_separately);
    }

    while (true)
    {
        LockAndBlocker lock(was_cancelled_mutex);
        if (was_cancelled)
            return ReadResult(Block());

        if (packet_in_progress)
        {
            chassert(read_context->readPacketTypeSeparately());
            chassert(read_context->hasReadTillPacketType());

            /// packet type is handled already, read and parse packet itself
            if (!read_context->hasReadPacket() && !read_context->read())
                return ReadResult(read_context->getFileDescriptor());

            packet_in_progress = false;
            auto read_result = processPacket(read_context->getPacket());
            if (read_result.getType() == ReadResult::Type::Data || read_result.getType() == ReadResult::Type::ParallelReplicasToken)
                return read_result;
        }

        read_context->resume();

        if (isReplicaUnavailable() || needToSkipUnavailableShard())
        {
            /// We need to tell the coordinator not to wait for this replica.
            /// But at this point it may lead to an incomplete result set, because
            /// this replica committed to read some part of there data and then died.
            if (extension && extension->parallel_reading_coordinator)
            {
                chassert(extension->parallel_reading_coordinator);
                extension->parallel_reading_coordinator->markReplicaAsUnavailable(extension->replica_info->number_of_current_replica);
            }

            return ReadResult(Block());
        }

        /// Check if packet is not ready yet.
        if (read_context->isInProgress())
            return ReadResult(read_context->getFileDescriptor());

        /// if reading separately packet header and body enabled, try to read packet itself this time
        if (read_context->readPacketTypeSeparately() && !read_context->hasReadPacket() && !read_context->read())
            return ReadResult(read_context->getFileDescriptor());

        auto read_result = processPacket(read_context->getPacket());
        if (read_result.getType() == ReadResult::Type::Data || read_result.getType() == ReadResult::Type::ParallelReplicasToken)
            return read_result;
    }
#else
    return read();
#endif
}
// ClickHouse 原生 TCP 协议接收端的核心状态机分发函数。
// 每当底层连接从远程节点读取到一个完整的 Packet 数据包后，都会交由该方法根据 packet.type 进行路由处理，并将其转化为上层执行管道能理解的 ReadResult。
RemoteQueryExecutor::ReadResult RemoteQueryExecutor::processPacket(Packet packet)
{
    switch (packet.type)
    {
        // 多副本并行读取（MergeTree 表）模式下，远端副本读完当前分配的分区范围（Marks/Ranges）后，向发起节点（Initiator）请求下一批读取任务。processMergeTreeReadTaskRequest 会向协调器申请新分片范围并将响应发回给该副本，随后返回 ParallelReplicasToken。
        case Protocol::Server::MergeTreeReadTaskRequest:
            chassert(packet.request.has_value());
            processMergeTreeReadTaskRequest(packet.request.value());
            return ReadResult(ReadResult::Type::ParallelReplicasToken);
        // 远端副本在开始扫描前，向发起节点汇报自己本地拥有的所有数据分片范围（Initial Read Announcement），由协调器统一汇总并切分调度。
        case Protocol::Server::MergeTreeAllRangesAnnouncement:
            chassert(packet.announcement.has_value());
            processMergeTreeInitialReadAnnouncement(packet.announcement.value());
            return ReadResult(ReadResult::Type::ParallelReplicasToken);
        // 用于外部表/数据湖表函数（如 s3Cluster、hdfsCluster）的动态任务分配请求，发起节点收到后分配下一个文件任务给远端 Worker，返回 Nothing 继续等待数据包。
        case Protocol::Server::ReadTaskRequest:
            processReadTaskRequest();
            break;
        // 早期实验性功能 allow_experimental_query_deduplication 使用的 PartUUIDs 包已被废弃。如果连上了老版本节点且收到该包，仅打印一条警告日志并忽略，不中断查询。
        case Protocol::Server::PartUUIDs:
            LOG_WARNING(
                log,
                "The remote server has sent no longer supported packet (Server::PartUUIDs). allow_experimental_query_deduplication feature "
                "has been deprecated. Consider upgrading the remote server ({})",
                connections->dumpAddresses());
            break;
        case Protocol::Server::Data:
            /// Note: `packet.block.rows() > 0` means it's a header block.
            /// We can actually return it, and the first call to RemoteQueryExecutor::read
            /// will return earlier. We should consider doing it.
            // 远端节点在发实际数据前通常会先发一个仅有表头列结构、0 行数据的 Header Block（rows() == 0），用于校验结构。
            // 这里判断 !packet.block.empty() && (packet.block.rows() > 0)，跳过 0 行的 Header Block（返回 Nothing 继续收包）。
            if (!packet.block.empty() && (packet.block.rows() > 0))
            {
                got_data_from_replica = true;
                return ReadResult(adaptBlockStructure(packet.block, *header));
            }
            break;  /// If the block is empty - we will receive other packets before EndOfStream.

        case Protocol::Server::Exception:
            got_exception_from_replica = true;

            if (shouldIgnoreShardException(packet.exception->code()))
            {
                if (log)
                    LOG_ERROR(log,
                        "Ignoring exception from connection(s) {} due to `skip_unavailable_shards_mode` setting: {}",
                        connections->dumpAddresses(),
                        packet.exception->displayText());

                reportShardSkipped();

                /// The server terminated the query with this exception and will not send `EndOfStream`,
                /// so mark the executor finished to signal end of data.
                finished = true;
                return ReadResult(Block{});
            }

            packet.exception->rethrow();
            break;

        case Protocol::Server::EndOfStream:
            if (!connections->hasActiveConnections())
            {
                finished = true;
                /// TODO: Replace with Type::Finished
                return ReadResult(Block{});
            }
            break;

        case Protocol::Server::Progress:
            /** We use the progress from a remote server.
              * We also include in ProcessList,
              * and we use it to check
              * constraints (for example, the minimum speed of query execution)
              * and quotas (for example, the number of lines to read).
              */
            if (progress_callback)
                progress_callback(packet.progress);
            break;

        case Protocol::Server::ProfileInfo:
            /// Use own (client-side) info about read bytes, it is more correct info than server-side one.
            if (profile_info_callback)
                profile_info_callback(packet.profile_info);
            break;
        // 当查询包含 WITH TOTALS 或启用了 extremes = 1 时，远端节点会在正常数据块之外额外发送计算好的总计行（Totals）或极值行（Extremes）。
        // 本地收到后同样用 adaptBlockStructure 对齐表头，暂存在成员变量 totals 和 extremes 中，供下游专门的读取接口提取。
        case Protocol::Server::Totals:
            totals = packet.block;
            if (!totals.empty())
                totals = adaptBlockStructure(totals, *header);
            break;

        case Protocol::Server::Extremes:
            extremes = packet.block;
            if (!extremes.empty())
                extremes = adaptBlockStructure(packet.block, *header);
            break;
        // 8. 服务端内部日志与监控事件透传（Log & ProfileEvents）
        case Protocol::Server::Log:
            /// Pass logs from remote server to client
            if (auto log_queue = CurrentThread::getInternalTextLogsQueue())
                log_queue->pushBlock(std::move(packet.block));
            break;

        case Protocol::Server::ProfileEvents:
            /// Pass profile events from remote server to client
            if (auto profile_queue = CurrentThread::getInternalProfileEventsQueue())
                if (!profile_queue->emplace(std::move(packet.block)))
                    throw Exception(ErrorCodes::SYSTEM_ERROR, "Could not push into profile queue");
            break;
        // 9. 时区更新与未知协议包兜底（TimezoneUpdate & default）
        case Protocol::Server::TimezoneUpdate:
            break;

        default:
            got_unknown_packet_from_replica = true;
            throw Exception(
                ErrorCodes::UNKNOWN_PACKET_FROM_SERVER,
                "Unknown packet {} from one of the following replicas: {}",
                packet.type,
                connections->dumpAddresses());
    }

    return ReadResult(ReadResult::Type::Nothing);
}

void RemoteQueryExecutor::processReadTaskRequest()
{
    /// A ReadTaskRequest arrives only from a worker running a cluster table function or object storage
    /// source with distributed reads. Serving it needs a task iterator, which only the legitimate
    /// dispatch paths install; its absence means the source was reached through an outer distribution.
    if (!extension)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "A cluster table function (s3Cluster, urlCluster, fileCluster, ...) cannot be nested inside "
            "another distributed query");

    if (!extension->task_iterator)
    {
        if (extension->parallel_reading_coordinator)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "A cluster table function or object storage cluster source cannot use distributed "
                "processing inside a query that runs with parallel replicas");

        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Received a cluster function read task request, but the query executor has neither a task "
            "iterator nor a parallel replicas coordinator");
    }

    if (!extension->replica_info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Replica info is not initialized");

    ProfileEvents::increment(ProfileEvents::ReadTaskRequestsReceived);

    auto response = (*extension->task_iterator)(extension->replica_info->number_of_current_replica);
    connections->sendClusterFunctionReadTaskResponse(*response);
}

void RemoteQueryExecutor::processMergeTreeReadTaskRequest(ParallelReadRequest request)
{
    if (!extension || !extension->parallel_reading_coordinator)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Coordinator for parallel reading from replicas is not initialized");

    ProfileEvents::increment(ProfileEvents::MergeTreeReadTaskRequestsReceived);
    auto response = extension->parallel_reading_coordinator->handleRequest(std::move(request));
    connections->sendMergeTreeReadTaskResponse(response);
}

void RemoteQueryExecutor::processMergeTreeInitialReadAnnouncement(InitialAllRangesAnnouncement announcement)
{
    if (!extension || !extension->parallel_reading_coordinator)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Coordinator for parallel reading from replicas is not initialized");

    /// Followers only block on the response when they actually need it (in-order modes pin
    /// per-split parts via the response). In `Default` mode the caller discards the response,
    /// so the round-trip would be pure overhead — skip it on both sides.
    const bool send_response = announcement.mode != CoordinationMode::Default;

    announcement_received = true;

    auto response = extension->parallel_reading_coordinator->handleInitialAllRangesAnnouncement(std::move(announcement));
    if (send_response)
        connections->sendMergeTreeAllRangesAnnouncementResponse(response);
}

void RemoteQueryExecutor::finish()
{
    LockAndBlocker guard(was_cancelled_mutex);

    /** If one of:
      * - nothing started to do;
      * - received all packets before EndOfStream;
      * - received exception from one replica;
      * - received an unknown packet from one replica;
      * then you do not need to read anything.
      */
    if (!isQueryPending() || hasThrownException() || was_cancelled)
    {
        /// If the query was never sent there is nothing to drain, but we must still mark the
        /// executor as finished. Otherwise a RemoteSource whose output is closed before it sends
        /// its query (e.g. an empty-build ANY INNER JOIN that short-circuits the probe side) keeps
        /// re-entering its drain path via prepare()/work() and spins forever, because isFinished()
        /// never becomes true.
        if (!sent_query)
        {
            /// Also mark the executor cancelled, not just finished. `RemoteSource::work()` may
            /// already be queued for execution (its `prepare()` ran before the output port was
            /// closed), and both `sendQuery` and `sendQueryAsync` gate only on `was_cancelled` -
            /// never on `finished`. Without this the query is still sent after we declared the
            /// executor finished, and nothing releases it afterwards: `finish()` returns early
            /// from here on, and the destructor's `isQueryPending()` is false because `finished`
            /// is set, so the connection is returned to the pool without a `Cancel` packet and
            /// without a disconnect. A parallel-replicas follower is then left blocked in
            /// `receivePartitionMergeTreeReadTaskResponse` for the whole `receive_timeout`,
            /// holding the table's shared lock and stalling a subsequent `DROP TABLE` (#109265).
            was_cancelled = true;
            finished = true;
        }
        else if (was_cancelled && !finished && connections)
        {
            /// The query was already cancelled (e.g. concurrently from the pipeline) but its
            /// connections may still hold undelivered packets - the server keeps sending the data,
            /// `ProfileInfo` and `EndOfStream` that were produced before it observed the cancel.
            /// We do not drain them here after cancellation, because the read side may already be
            /// torn down and reading from it could throw or crash (see #95466). But such connections
            /// must not be returned to the connection pool in this out-of-sync state - otherwise the
            /// next user of the connection would read a stale packet during establishment, failing
            /// with "Unexpected packet from server (expected TablesStatusResponse, got ProfileInfo)"
            /// (see #93018). So disconnect them, forcing a clean reconnect on reuse. This mirrors the
            /// cleanup done in the destructor, but performs it eagerly so it cannot be skipped if
            /// `finished` later becomes true through another path.
            connections->disconnect();
            finished = true;
        }
        return;
    }

    /// To make sure finish is only called once
    SCOPE_EXIT({ finished = true; });

    /** If you have not read all the data yet, but they are no longer needed.
      * This may be due to the fact that the data is sufficient (for example, when using LIMIT).
      */

    /// Send the request to abort the execution of the request, if not already sent.
    tryCancel("Cancelling query because enough data has been read");

    /// If connections weren't created yet, query wasn't sent or was already finished, nothing to do.
    if (!connections || !sent_query || finished)
        return;

    /// `tryCancel` above may have torn the read side down without draining the packet that was in
    /// flight. The loop below reads from those same connections, and reading from a canceled buffer
    /// aborts ("ReadBuffer is canceled. Can't read from it."). Disconnect instead of draining, exactly
    /// as the already-cancelled branch above does and for the same reasons: the read side may be gone
    /// (#95466), and an out-of-sync connection must not go back to the pool (#93018). `SCOPE_EXIT`
    /// marks the executor finished on the way out.
    if (drain_was_skipped)
    {
        connections->disconnect();
        return;
    }

    /// Get the remaining packets so that there is no out of sync in the connections to the replicas.
    /// We do this manually instead of calling drain() because we want to process Log, ProfileEvents and Progress
    /// packets that had been sent before the connection is fully finished in order to have final statistics of what
    /// was executed in the remote queries
    while (connections->hasActiveConnections() && !finished)
    {
        Packet packet = connections->receivePacket();

        switch (packet.type)
        {
            case Protocol::Server::EndOfStream:
                finished = true;
                break;

            case Protocol::Server::Exception:
                got_exception_from_replica = true;

                if (shouldIgnoreShardException(packet.exception->code()))
                {
                    if (log)
                        LOG_ERROR(log,
                            "Ignoring exception from connection(s) {} due to `skip_unavailable_shards_mode` setting: {}",
                            connections->dumpAddresses(),
                            packet.exception->displayText());

                    reportShardSkipped();

                    /// Stop draining: the server terminated the query with this exception.
                    finished = true;
                    break;
                }

                packet.exception->rethrow();
                break;

            case Protocol::Server::Log:
                /// Pass logs from remote server to client
                if (auto log_queue = CurrentThread::getInternalTextLogsQueue())
                    log_queue->pushBlock(std::move(packet.block));
                break;

            case Protocol::Server::ProfileEvents:
                /// Pass profile events from remote server to client
                if (auto profile_queue = CurrentThread::getInternalProfileEventsQueue())
                    if (!profile_queue->emplace(std::move(packet.block)))
                        throw Exception(ErrorCodes::SYSTEM_ERROR, "Could not push into profile queue");
                break;

            case Protocol::Server::ProfileInfo:
                /// Use own (client-side) info about read bytes, it is more correct info than server-side one.
                if (profile_info_callback)
                    profile_info_callback(packet.profile_info);
                break;

            case Protocol::Server::Progress:
                if (progress_callback)
                    progress_callback(packet.progress);
                break;

            default:
                break;
        }
    }

    /// Reached only with this executor's own reader parked above, i.e. with its connections
    /// cancelled and fully drained - the state that reader will observe when it wakes.
    if (in_receive_packet_window)
        FailPointInjection::pauseFailPoint(FailPoints::remote_query_executor_finish_drain_pause);
}

void RemoteQueryExecutor::cancel()
{
    LockAndBlocker guard(was_cancelled_mutex);
    cancelUnlocked();
}

void RemoteQueryExecutor::cancelUnlocked()
{
    {
        LockAndBlocker lock(external_tables_mutex);

        /// Stop sending external data.
        for (auto & vec : external_tables_data)
            for (auto & elem : vec)
                elem->is_cancelled = true;
    }

    if (finished || hasThrownException())
        return;

    tryCancel("Cancelling query");
}
// 将当前查询在发起节点（Initiator）本地预先计算好的标量子查询结果（scalars），通过底层网络连接发送给远程节点。
void RemoteQueryExecutor::sendScalars()
{
    connections->sendScalarsData(scalars);
}
// 将本地临时表/外部表（如 GLOBAL IN、GLOBAL JOIN 产生的内存中间表，或物化 CTE 表）发送给远程节点的核心方法。
void RemoteQueryExecutor::sendExternalTables()
{
    // 获取当前建立的活跃连接数。在普通模式下为 1；在多副本并行对冲读取（HedgedConnections）模式下，可能同时存在多个连接副本，每个连接都需要独立流式读取外部表数据，因此需要准备 count 份读取上下文。
    size_t count = connections->size();

    {
        // 防止在构建或重置外部表读取状态时与并发取消（cancel()）或异常清理逻辑发生数据竞争。
        std::lock_guard lock(external_tables_mutex);
        // 清空历史残留数据并预分配 count 个元素的内存空间，避免循环中多次动态扩容。
        external_tables_data.clear();
        external_tables_data.reserve(count);
        // 2. 配置流式读取的超时与速率限制
        // StreamLocalLimits limits;：定义本地数据流读取限制结构体，用于在从本地内存表读取并发送数据给远端的过程中执行超时检查。
        StreamLocalLimits limits;
        const auto & settings = context->getSettingsRef();
        limits.mode = LimitsMode::LIMITS_TOTAL;
        // 最大执行时间
        limits.speed_limits.max_execution_time = settings[Setting::max_execution_time];
        // 超时处理策略：抛异常 throw 或中止 break
        limits.timeout_overflow_mode = settings[Setting::timeout_overflow_mode];
        // 最大预估执行时间
        limits.speed_limits.max_estimated_execution_time = settings[Setting::max_estimated_execution_time];
        // 3. 外层循环：为每个连接构建独立的外部表数据列表
        for (size_t i = 0; i < count; ++i)
        {
            // 为第 i 个连接创建一个 ExternalTablesData（即 std::vector<std::unique_ptr<ExternalTableData>>）。因为每个连接通过网络发送数据的进度可能不同（受各自 TCP 窗口或异步协程挂起影响），必须各自持有独立的读取管道状态。
            ExternalTablesData res;
            // 遍历当前查询上下文中注册的所有外部表（table.first 为表名，table.second 为 StoragePtr）。
            for (const auto & table : external_tables)
            {
                StoragePtr cur = table.second;
                /// Send only temporary tables with StorageMemory
                // 通过动态类型转换过滤存储引擎——只发送基于内存存储引擎（StorageMemory）的临时表，跳过其他非内存表。
                auto storage_memory = std::dynamic_pointer_cast<StorageMemory>(cur);
                if (!storage_memory)
                    continue;

                /// Skip sending Materialized CTEs when they are not built.
                /// It is required to be able CTE materialization plan with parallel replicas (avoiding
                /// circular dependency between CTE materialization and parallel replicas external tables.
                // 背景与作用：当查询使用了 WITH ... AS MATERIALIZED (...)（物化公用表表达式）且开启了多副本并行读取（Parallel Replicas）时，可能存在多层查询计划。
                // 如果某个外层子查询下发时，该物化 CTE 尚未在本地执行构建完成（!materialized_cte->isBuilt()），直接发送会导致死锁或循环依赖（远端等待外部表数据，而本地等待远端返回来物化 CTE）。
                auto materialized_cte = storage_memory->getMaterializedCTE();
                if (materialized_cte != nullptr && !materialized_cte->isBuilt())
                {
                    LOG_DEBUG(log, "Skipping sending CTE '{}' because it has not been materialized yet", materialized_cte->cte_name);
                    continue;
                }
                // 5. 封装 ExternalTableData 与构建读取管道的回调函数
                auto data = std::make_unique<ExternalTableData>();
                data->table_name = table.first;
                data->creating_pipe_callback = [cur, limits, my_context = this->context]()
                {
                    SelectQueryInfo query_info;
                    auto metadata_snapshot = cur->getInMemoryMetadataPtr(my_context, false);
                    auto storage_snapshot = cur->getStorageSnapshot(metadata_snapshot, my_context);
                    QueryProcessingStage::Enum read_from_table_stage = cur->getQueryProcessingStage(
                        my_context, QueryProcessingStage::Complete, storage_snapshot, query_info);

                    QueryPlan plan;
                    cur->read(
                        plan,
                        metadata_snapshot->getColumns().getNamesOfPhysical(),
                        storage_snapshot, query_info, my_context,
                        read_from_table_stage, DEFAULT_BLOCK_SIZE, 1);

                    auto builder = plan.buildQueryPipeline(QueryPlanOptimizationSettings(my_context), BuildQueryPipelineSettings(my_context));

                    builder->resize(1);
                    builder->addTransform(std::make_shared<LimitsCheckingTransform>(builder->getSharedHeader(), limits));

                    return builder;
                };
                // 立即调用刚才定义的 Lambda，生成初始的 QueryPipelineBuilder 实例赋值给 data->pipe。
                data->pipe = data->creating_pipe_callback();
                res.emplace_back(std::move(data));
            }
            external_tables_data.push_back(std::move(res));
        }
    }

    connections->sendExternalTablesData(external_tables_data);
}

void RemoteQueryExecutor::tryCancel(const char * reason)
{
    if (was_cancelled)
        return;

    was_cancelled = true;

    /// A parallel-replicas follower that has not announced yet is still planning, and the only packet
    /// it owes us is the announcement itself - which is worthless once we are cancelling, because it
    /// exists to let the coordinator assign ranges we are no longer going to assign. Waiting for it
    /// means waiting out that replica's whole planning phase (measured at ~290 ms on TPC-H q22 at
    /// sf=100). Replicas that already announced are drained as before: they may be mid-block.
    ///
    /// Only for a query that reads with parallel replicas. `announcement_received` can only ever be set
    /// by `processMergeTreeInitialReadAnnouncement`, so without this guard every ordinary distributed
    /// query - which never announces - would skip the drain too, including on the normal completion path
    /// through `finish()`, and hand a connection back to the pool part-way through a packet.
    const bool skip_drain = extension && extension->parallel_reading_coordinator && !announcement_received;

    if (read_context)
    {
        if (skip_drain)
            read_context->skipDrainOnCancel();

        read_context->cancel();
    }

    /// Query could be cancelled during connection creation, query sending or data receiving.
    /// We should send cancel request if connections were already created, query were sent
    /// and remote query is not finished.
    if (connections && sent_query && !finished)
    {
        connections->sendCancel();
        LOG_TRACE(log, "({}) {}", connections->dumpAddresses(), reason);
    }

    /// Skipping the drain leaves the socket part-way through a packet, so it must never go back to the
    /// pool. The destructor only disconnects while the query is still pending, and `finish()` marks it
    /// finished right after calling this - so record it and disconnect there unconditionally.
    if (skip_drain)
        drain_was_skipped = true;
}

bool RemoteQueryExecutor::isQueryPending() const
{
    return (sent_query || read_context) && !finished;
}

bool RemoteQueryExecutor::hasThrownException() const
{
    return got_exception_from_replica || got_unknown_packet_from_replica;
}

bool RemoteQueryExecutor::shouldIgnoreShardException(int exception_code) const
{
    if (!skip_unavailable_shards)
        return false;

    /// Never silence `LOGICAL_ERROR` as a shard skip, in any mode: it denotes a programming error
    /// rather than an expected shard failure, so hiding it would mask real bugs (same as for INSERTs).
    if (exception_code == ErrorCodes::LOGICAL_ERROR)
        return false;

    switch (skip_unavailable_shards_mode)
    {
        case SkipUnavailableShardsMode::UNAVAILABLE:
            /// Connection-related failures are skipped while establishing the connection, not here,
            /// so at this point (a server `Exception` packet) this mode ignores nothing.
            return false;
        case SkipUnavailableShardsMode::UNAVAILABLE_OR_TABLE_MISSING:
            return exception_code == ErrorCodes::UNKNOWN_TABLE || exception_code == ErrorCodes::UNKNOWN_DATABASE;
        case SkipUnavailableShardsMode::UNAVAILABLE_OR_EXCEPTION_BEFORE_PROCESSING:
            /// Ignore only an exception that arrived before the shard returned any data; once some data
            /// was returned, an exception means partial results and must not be silently accepted.
            return !got_data_from_replica;
    }
}

void RemoteQueryExecutor::setProgressCallback(ProgressCallback callback)
{
    LockAndBlocker guard(was_cancelled_mutex);
    progress_callback = std::move(callback);

    if (extension && extension->parallel_reading_coordinator)
        extension->parallel_reading_coordinator->setProgressCallback(progress_callback);
}

void RemoteQueryExecutor::setProfileInfoCallback(ProfileInfoCallback callback)
{
    LockAndBlocker guard(was_cancelled_mutex);
    profile_info_callback = std::move(callback);
}
// 用于在普通分布式查询中判断当前目标分片（Shard）是否因为全部副本不可用且开启了容错配置而需要被跳过。
bool RemoteQueryExecutor::needToSkipUnavailableShard()
{
    // 检查当前查询上下文的设置项 skip_unavailable_shards 是否开启（设为 1 或 true，默认通常为 0）。
    // 业务含义：在 ClickHouse 分布式查询中，默认强一致性策略下，只要有一个分片的所有副本都宕机，整个查询就会直接失败报错。开启 skip_unavailable_shards = 1 后，允许查询容忍部分分片不可用，跳过故障分片并返回其余健康分片的数据。
    // 检查前面 create_connections(async_callback) 创建连接后，当前分片中实际建立的有效连接数是否为 0（即该分片下的所有副本全部连接失败或处于不可用状态）。
    if (context->getSettingsRef()[Setting::skip_unavailable_shards] && (0 == connections->size()))
    {
        // 当同时满足“允许跳过”且“可用连接数为 0”时，调用该方法记录分片被跳过的监控信息（例如向服务端日志输出 Warning 级别日志、递增相关的 ProfileEvents 计数器或通知集群状态管理模块）。
        reportShardSkipped();
        return true;
    }
    return false;
}

void RemoteQueryExecutor::reportShardSkipped()
{
    if (shard_skip_reported)
        return;
    shard_skip_reported = true;

    ProfileEvents::increment(ProfileEvents::DistributedShardsSkipped);

    /// Throws `TOO_MANY_UNAVAILABLE_SHARDS` if the configured `max_skip_unavailable_shards_num` /
    /// `max_skip_unavailable_shards_ratio` limits are exceeded, so the safety bounds apply to every
    /// silently skipped shard regardless of why it was skipped (no connections or an ignored exception).
    if (unavailable_shard_tracker)
        unavailable_shard_tracker->onShardSkipped();
}

bool RemoteQueryExecutor::processParallelReplicaPacketIfAny()
{
#if defined(OS_LINUX) || defined(OS_DARWIN)

    if (!read_context->readPacketTypeSeparately())
        return false;

    OpenTelemetry::SpanHolder span_holder{"RemoteQueryExecutor::processParallelReplicaPacketIfAny"};

    LockAndBlocker lock(was_cancelled_mutex);
    if (was_cancelled)
        return false;

    // try to read packet type if hasn't read it yet
    if (!read_context->hasReadTillPacketType() && !read_context->read())
        return false;

    packet_in_progress = true;

    const auto packet_type = read_context->getPacketType();
    if (packet_type == Protocol::Server::MergeTreeReadTaskRequest || packet_type == Protocol::Server::MergeTreeAllRangesAnnouncement)
    {
        // try to read packet if hasn't read it yet
        if (!read_context->hasReadPacket() && !read_context->read())
            return false;

        packet_in_progress = false;
        processPacket(read_context->getPacket());
        return true;
    }

#endif

    return false;
}
}
