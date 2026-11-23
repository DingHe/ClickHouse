#pragma once

#include <Storages/MergeTree/IMergeTreeReadPool.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <Storages/MergeTree/MergeTreeSelectAlgorithms.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/RequestResponse.h>
#include <Processors/Chunk.h>

namespace DB
{

struct PrewhereExprInfo;

struct LazilyReadInfo;
using LazilyReadInfoPtr = std::shared_ptr<LazilyReadInfo>;

// 单次读取调用的返回结果封装，同时携带数据块 Chunk、读取行数/字节数进度增量、显式结束标志 is_finished（用于在 PREWHERE 过滤掉全部行时仍能上报进度或结束状态）以及本次读取涉及的 read_mark_ranges。
struct ChunkAndProgress
{
    // 本次读取产出的列式数据块。
    Chunk chunk;
    // 本次底层读取操作扫描/读取的行数增量（用于进度统计与限流/配额核算）。
    size_t num_read_rows = 0;
    // 本次底层读取操作涉及的字节数增量。
    size_t num_read_bytes = 0;
    /// Explicitly indicate that we have read all data.
    /// This is needed to occasionally return empty chunk to indicate the progress while the rows are filtered out in PREWHERE.
    // 显式指示是否已读完所有数据。即使 chunk 为空，当过滤掉所有行或到达末尾时该标志可明确表达状态。
    bool is_finished = false;
    /// Mark ranges that were read in this call (used for per-block virtual row generation).
    // 本次读取实际覆盖的标记（Mark）区间列表，供有序读取生成虚拟边界行等逻辑使用。
    MarkRanges read_mark_ranges;
};

// 多副本并行读取（Parallel Replicas）的副本侧通信封装，负责向协调器发送初始全量范围声明（sendInitialRequest）以及按需申请动态读取任务（sendReadRequest / sendReadInOrderRequest）。
class ParallelReadingExtension
{
public:
    ParallelReadingExtension(
        MergeTreeAllRangesCallback all_callback_,
        MergeTreeReadTaskCallback callback_,
        size_t number_of_current_replica_,
        size_t total_nodes_count_,
        String stream_id_
    );

    std::optional<InitialAllRangesAnnouncementResponse> sendInitialRequest(
        CoordinationMode mode,
        RangesInDataPartsDescription description,
        size_t mark_segment_size,
        size_t min_marks_per_request) const;

    std::optional<ParallelReadResponse> sendReadRequest(CoordinationMode mode, size_t min_marks_per_request) const;

    std::optional<ParallelReadResponse> sendReadInOrderRequest(
        CoordinationMode mode, size_t min_marks_per_request, const RangesInDataPartsDescription & description) const;

    size_t getTotalNodesCount() const { return total_nodes_count; }
    size_t getNumberOfCurrentReplica() const { return number_of_current_replica; }
    MergeTreeAllRangesCallback getAllRangesCallback() const { return all_callback; }
    MergeTreeReadTaskCallback getReadTaskCallback() const { return callback; }

private:
    // 上报初始全量 Mark 范围声明的回调。
    MergeTreeAllRangesCallback all_callback;
    // 请求下一批并行读取任务的回调。
    MergeTreeReadTaskCallback callback;
    // 当前副本在并行副本组中的编号。
    const size_t number_of_current_replica;
    // 参与并行读取的副本总节点数。
    const size_t total_nodes_count;
    // 当前数据流的唯一标识字符串。
    const String stream_id;
};

/// Per-part inputs for read-time skip-index filtering; the part itself comes from the read task.
// 封装按 Part 划分的跳数索引/投影索引读取范围、读提示及剩余 Mark 计数器，
// 在多个读取任务/线程间共享索引过滤结果池（MergeTreeIndexReadResultPool），并在某个 Part 剩余 Mark 归零时释放其索引缓存以控制内存占用。
struct SkipIndexReadInput
{
    MarkRanges ranges;
    RangesInDataPartReadHints read_hints;
    size_t part_starting_offset_in_query = 0;
};
using RangesByIndex = std::unordered_map<size_t, SkipIndexReadInput>;
using ProjectionRangesByIndex = std::unordered_map<size_t, RangesInDataParts>;
class MergeTreeIndexReadResultPool;
using MergeTreeIndexReadResultPoolPtr = std::shared_ptr<MergeTreeIndexReadResultPool>;

/// A simple wrapper to allow atomic counters to be mutated even when accessed through a const map.
struct MutableAtomicSizeT
{
    mutable std::atomic_size_t value;
};

using PartRemainingMarks = std::unordered_map<size_t, MutableAtomicSizeT>;

/// Provides shared context needed to build filtering indexes (e.g., skip indexes or projection indexes) during data reads.
struct MergeTreeIndexBuildContext
{
    /// For each part, stores all ranges need to be read.
    const RangesByIndex read_ranges;

    /// For each part, stores a set of read ranges grouped by projection.
    const ProjectionRangesByIndex projection_read_ranges;

    /// Thread-safe shared pool for reading and building index filters. Must not be null (enforced in constructor).
    const MergeTreeIndexReadResultPoolPtr index_reader_pool;

    /// Tracks how many marks are still being processed for each part during the execution phase. Once the count reaches
    /// zero for a part, its cached index can be released to free resources.
    const PartRemainingMarks part_remaining_marks;

    MergeTreeIndexBuildContext(
        RangesByIndex read_ranges_,
        ProjectionRangesByIndex projection_read_ranges_,
        MergeTreeIndexReadResultPoolPtr index_reader_pool_,
        PartRemainingMarks part_remaining_marks_);

    MergeTreeIndexReadResultPtr getPreparedIndexReadResult(const MergeTreeReadTask & task) const;
};

using MergeTreeIndexBuildContextPtr = std::shared_ptr<MergeTreeIndexBuildContext>;

struct LazyMaterializingRows;
using LazyMaterializingRowsPtr = std::shared_ptr<LazyMaterializingRows>;

/// Base class for MergeTreeThreadSelectAlgorithm and MergeTreeSelectAlgorithm
// 从 MergeTree 引擎的数据分区（Data Parts）中流式读取数据块（Block/Chunk）的核心处理器。
// 任务拉取与执行驱动：从读取任务池（MergeTreeReadPool）中获取读取任务（MergeTreeReadTask），委托底层选择算法（IMergeTreeSelectAlgorithm / MergeTreeSelectAlgorithmPtr）完成数据块读取。
// 过滤与延迟物化编排：整合行级过滤（Row-level Filter）、PREWHERE 过滤表达式、读时跳数/投影索引结果复用（MergeTreeIndexBuildContext）以及延迟物化（Lazy Materializing），在读取阶段尽早裁剪无关数据。
// 高级查询语义支持：支持向数据块附加 Part 层级信息（服务 FINAL 归并）、生成有序读取（Read-in-Order）所需的每块虚拟边界行（Virtual Row）以协助 MergingSortedTransform 动态重排源优先级，并收集过滤统计指标与取消信号。

class MergeTreeSelectProcessor : private boost::noncopyable
{
public:
    MergeTreeSelectProcessor(
        MergeTreeReadPoolPtr pool_,
        MergeTreeSelectAlgorithmPtr algorithm_,
        const FilterDAGInfoPtr & row_level_filter_,
        const PrewhereInfoPtr & prewhere_info_,
        const IndexReadTasks & index_read_tasks_,
        const ExpressionActionsSettings & actions_settings_,
        const MergeTreeReaderSettings & reader_settings_,
        MergeTreeIndexBuildContextPtr merge_tree_index_build_context_ = {},
        LazyMaterializingRowsPtr lazy_materializing_rows_ = {},
        const ColumnsDescription * columns_ = nullptr);

    String getName() const;

    static Block transformHeader(
        Block block,
        const FilterDAGInfoPtr & row_level_filter,
        const PrewhereInfoPtr & prewhere_info);

    Block getHeader() const { return result_header; }

    /// Reads a single MergeTreeReadTask in a stateless manner.
    /// Can be called concurrently and is used, for example, by SingleProjectionIndexReader.
    ChunkAndProgress readCurrentTask(MergeTreeReadTask & current_task, IMergeTreeSelectAlgorithm & task_algorithm) const;

    /// Reads using the standard task-based algorithm, managing task state internally.
    /// Not thread-safe: must not be called concurrently on the same MergeTreeSelectProcessor instance.
    ChunkAndProgress read();

    void cancel() noexcept;

    const MergeTreeReaderSettings & getSettings() const { return reader_settings; }

    static PrewhereExprInfo getPrewhereActions(
        const FilterDAGInfoPtr & row_level_filter,
        const PrewhereInfoPtr & prewhere_info,
        const IndexReadTasks & index_read_tasks,
        const ExpressionActionsSettings & actions_settings,
        bool enable_multiple_prewhere_read_steps,
        bool force_short_circuit_execution,
        const ColumnsDescription * columns = nullptr);

    void addPartLevelToChunk(bool add_part_level_) { add_part_level = add_part_level_; }

    /// Enable per-block virtual row generation for read-in-order optimization.
    /// When set, after each block read, a virtual row carrying the next mark's PK boundary
    /// is emitted so that MergingSortedTransform can reprioritize sources.
    void setVirtualRowConversions(ExpressionActionsPtr virtual_row_conversions_, Block pk_block_header_, bool read_in_reverse_order_);

    void onFinish() const;

private:
    friend class SingleProjectionIndexReader;

    const MergeTreeReadPoolPtr pool;
    const MergeTreeSelectAlgorithmPtr algorithm;

    const FilterDAGInfoPtr row_level_filter;
    const PrewhereInfoPtr prewhere_info;
    const ExpressionActionsSettings actions_settings;
    const PrewhereExprInfo prewhere_actions;

    const LazilyReadInfoPtr lazily_read_info;

    const MergeTreeReaderSettings reader_settings;
    const MergeTreeReadTask::BlockSizeParams block_size_params;

    /// Current task to read from.
    MergeTreeReadTaskPtr task;
    /// A result of getHeader(). A chunk which this header is returned from read().
    Block result_header;

    ReadStepsPerformanceCounters read_steps_performance_counters;

    /// Should we add part level to produced chunk. Part level is useful for next steps if query has FINAL
    bool add_part_level = false;

    /// Shared context used for building indexes during query execution.
    MergeTreeIndexBuildContextPtr merge_tree_index_build_context;

    LazyMaterializingRowsPtr lazy_materializing_rows;

    StorageID storage_id = StorageID::createEmpty();
    size_t prewhere_step_offset = 0;

    void logPredicateStatistics() const;

    /// For per-block virtual row generation (read-in-order optimization).
    // 指向表达式动作链（ExpressionActions）的共享指针；当未启用按主键有序归并优化时，该指针为空（nullptr）。
    // 存储从主索引（Primary Index）提取出的主键边界列转换到下游归并算子所需排序键列（ORDER BY 表达式）的计算逻辑。
    ExpressionActionsPtr virtual_row_conversions;
    /// Precomputed header with PK column names/types; cloned and filled from index per block.
    // 预计算好的主键表头模板（仅包含主键列的列名、数据类型和空列壳，不含实际行数据）。
    // 作为构建虚拟行的列结构原型，避免每次生成虚拟行时重复解析元数据和创建列类型。
    Block pk_block_header;
    // 默认为 false（正序读取），当查询为降序排列（如 ORDER BY pk DESC）且触发了逆序读取优化时为 true。
    // 决定在计算虚拟行时如何查找“下一个 Mark 的主键边界”：
    // false（正序读取 ASC）：当前读完了 [start_mark, end_mark)，那么下一个尚未读取的边界是正向递增方向的 end_mark 的主键值（代表该流后续数据的下界）。
    // true（逆序读取 DESC）：从后往前读取，当前读完了 [start_mark, end_mark)，下一个边界则是反向递减方向的 start_mark（或 start_mark - 1） 的主键值（代表该流后续数据的上界）。
    bool read_in_reverse_order = false;
    // 用于暂存刚生成但尚未向上游输出的虚拟行数据块。
    std::optional<ChunkAndProgress> pending_virtual_row;

    ChunkAndProgress buildVirtualRowFromIndex(const MergeTreeReadTask & current_task, const MarkRanges & read_mark_ranges) const;

    LoggerPtr log = getLogger("MergeTreeSelectProcessor");
    // 作为多线程并发取消标志位，实现轻量级的无锁状态通知：
    // 写入方（取消触发线程）：当客户端断开连接、执行 KILL QUERY、查询超时，或者下游算子（如 LIMIT 算子）已收集够足够行数并调用 MergeTreeSelectProcessor::cancel() 时，另一个线程会并发地将该变量原子置为 true。
    std::atomic<bool> is_cancelled{false};
};

}
