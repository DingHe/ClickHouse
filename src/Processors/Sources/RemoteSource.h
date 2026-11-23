#pragma once

#include <Processors/ISimpleTransform.h>
#include <Processors/ISource.h>
#include <Processors/RowsBeforeStepCounter.h>
#include <QueryPipeline/Pipe.h>

#include <Core/UUID.h>

#include <Common/EventFD.h>

namespace DB
{

class RemoteQueryExecutor;
using RemoteQueryExecutorPtr = std::shared_ptr<RemoteQueryExecutor>;

/// Source from RemoteQueryExecutor. Executes remote query and returns query result chunks.
// 流水线与网络层的适配器：
// 将 RemoteQueryExecutor 从远端分片拉取到的 Block 转换为流水线标准的 Chunk，并在需要时附加两阶段聚合的桶信息（AggregatedChunkInfo）。
// 同步与异步双模调度枢纽：深度集成 ClickHouse 流水线调度器的异步状态机（Status::Async、schedule()、onAsyncJobReady()）,当开启 async_read 或 async_query_sending 时，它能在网络遇到 EAGAIN 阻塞时主动向调度器返回文件描述符（fd）并让出工作线程，待 epoll/kqueue 事件就绪后再恢复执行。
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
// 专用于从 RemoteQueryExecutor 中提取远端节点返回的 WITH TOTALS 汇总数据块（Totals），输出到流水线的 Totals 专用端口。
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
// 极值行源算子
// 专用于从 RemoteQueryExecutor 中提取远端节点返回的极值数据块（Extremes，最小/最大值），输出到流水线的 Extremes 专用端口。
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
// 并行数据块反序列化算子
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
// 远程数据源管道工厂函数
Pipe createRemoteSourcePipe(
    RemoteQueryExecutorPtr query_executor,
    bool add_aggregation_info,
    bool add_totals,
    bool add_extremes,
    bool async_read,
    bool async_query_sending,
    size_t parallel_marshalling_threads);
}
