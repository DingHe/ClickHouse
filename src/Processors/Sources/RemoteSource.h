#pragma once

#include <Processors/ISimpleTransform.h>
#include <Processors/ISource.h>
#include <Processors/RowsBeforeStepCounter.h>
#include <QueryPipeline/Pipe.h>

#include <Core/UUID.h>

#include <Common/EventFD.h>

namespace DB
{

// 该文件定义了与远程节点（分片或副本）进行网络通信并拉取查询结果的核心数据源体系
// RemoteSource（远程主数据源）
// 网络数据拉取桥梁：继承自 ISource，内部封装了 RemoteQueryExecutor，负责向远程 ClickHouse 节点发送查询请求并流式接收返回的数据包，将其转化为本地执行流水线中的 Chunk。
// 全异步非阻塞 I/O 支持：支持异步发送查询（async_query_sending）与异步读取网络数据（async_read）。通过重写 schedule() 和 onAsyncJobReady()，将底层网络 Socket 的文件描述符（fd）注册到流水线调度器的事件循环（如 epoll/kqueue）中，避免网络等待阻塞工作线程。
// 分布式聚合状态与统计透传：支持将远程节点发来的两阶段聚合元数据（如分桶号 bucket_num）封装为 AggregatedChunkInfo 挂载到 Chunk 上，并能接收远程节点统计的 rows_before_limit 等信息。

class RemoteQueryExecutor;
using RemoteQueryExecutorPtr = std::shared_ptr<RemoteQueryExecutor>;

/// Source from RemoteQueryExecutor. Executes remote query and returns query result chunks.
class RemoteSource final : public ISource
{
public:
    /// Flag add_aggregation_info tells if AggregatedChunkInfo should be added to result chunk.
    /// AggregatedChunkInfo stores the bucket number used for two-level aggregation.
    /// This flag should be typically enabled for queries with GROUP BY which are executed till WithMergeableState.
    RemoteSource(RemoteQueryExecutorPtr executor, bool add_aggregation_info_, bool async_read_, bool async_query_sending_);
    ~RemoteSource() override;

    Status prepare() override;
    void work() override;
    String getName() const override { return "Remote"; }

    void setRowsBeforeLimitCounter(RowsBeforeStepCounterPtr counter) override { rows_before_limit.swap(counter); }
    void setRowsBeforeAggregationCounter(RowsBeforeStepCounterPtr counter) override { rows_before_aggregation.swap(counter); }

    /// Stop reading from stream if output port is finished.
    void onUpdatePorts() override;

    int schedule() override;

    void onAsyncJobReady() override;

    void setStorageLimits(const std::shared_ptr<const StorageLimitsList> & storage_limits_) override;

protected:
    std::optional<Chunk> tryGenerate() override;
    void onCancel() noexcept override;

private:
    std::atomic_bool was_query_sent = false;
    bool need_drain = false;
    bool add_aggregation_info = false;
    RemoteQueryExecutorPtr query_executor;
    RowsBeforeStepCounterPtr rows_before_limit;
    RowsBeforeStepCounterPtr rows_before_aggregation;

    const bool async_read;
    const bool async_query_sending;
    bool is_async_state = false;
    int fd = -1;
    size_t rows = 0;
    bool manually_add_rows_before_limit_counter = false;
    std::atomic_bool preprocessed_packet = false;
#if defined(OS_LINUX) || defined(OS_DARWIN)
    EventFD startup_event_fd;
#endif
};

/// Totals source from RemoteQueryExecutor.
class RemoteTotalsSource final : public ISource
{
public:
    explicit RemoteTotalsSource(RemoteQueryExecutorPtr executor);
    ~RemoteTotalsSource() override;

    String getName() const override { return "RemoteTotals"; }

protected:
    Chunk generate() override;

private:
    RemoteQueryExecutorPtr query_executor;
};

/// Extremes source from RemoteQueryExecutor.
class RemoteExtremesSource final : public ISource
{
public:
    explicit RemoteExtremesSource(RemoteQueryExecutorPtr executor);
    ~RemoteExtremesSource() override;

    String getName() const override { return "RemoteExtremes"; }

protected:
    Chunk generate() override;

private:
    RemoteQueryExecutorPtr query_executor;
};

struct UnmarshallBlocksTransform : ISimpleTransform
{
public:
    explicit UnmarshallBlocksTransform(SharedHeader header_)
        : ISimpleTransform(header_, header_, false)
    {
    }

    String getName() const override { return "UnmarshallBlocksTransform"; }

    void transform(Chunk & chunk) override;
};

/// Create pipe with remote sources.
Pipe createRemoteSourcePipe(
    RemoteQueryExecutorPtr query_executor,
    bool add_aggregation_info,
    bool add_totals,
    bool add_extremes,
    bool async_read,
    bool async_query_sending,
    size_t parallel_marshalling_threads);
}
