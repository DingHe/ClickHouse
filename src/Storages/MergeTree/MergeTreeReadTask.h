#pragma once

#include <map>
#include <vector>
#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/IMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/MergeTree/MergeTreeReadersChain.h>
#include <Storages/MergeTree/PatchParts/MergeTreePatchReader.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/StorageSnapshot.h>
#include <boost/core/noncopyable.hpp>

// MergeTreeReadTask.h 定义了 ClickHouse MergeTree 存储引擎在执行读取（SELECT、后台 Merge、Mutation、索引构建等）时的最小工作单元（Task）及其配套元数据、读取器组合与读取链上下文。
// MergeTreeReadTask 扮演着连接上层调度池（MergeTreeReadPool / MergeTreeSelectProcessor）与底层列式文件读取器（IMergeTreeReader / MergeTreeRangeReader）的桥梁：
// 封装单次分配的读取工作量：一个 MergeTreeReadTask 代表“从某个特定 Data Part 中读取若干段连续的数据颗粒范围（MarkRanges）”。
// 解耦“静态 Part 级元数据”与“动态 Task 级执行状态”：
// 同一张表的同一个 Data Part 可能被拆分成几十个 MergeTreeReadTask 分发给不同线程并发读取。所有线程共享同一份只读的 MergeTreeReadTaskInfo（静态元数据），而各自持有独立的 MergeTreeReadTask（动态进度、mark_ranges、读取器状态）。
// 编排多阶段延迟物化流水线（MergeTreeReadersChain）：
// 管理主列读取器（main）、多步 PREWHERE 读取器（prewhere）、轻量级更新补丁读取器（patches）以及二级索引读取器（prepared_index），并在切换相邻任务时支持底层读取器（Readers）的无缝复用（releaseReaders()），避免重复打开磁盘文件与解析 .mrk 索引。
// 自适应块大小控制与缓存统计追踪：
// 结合 MergeTreeBlockSizePredictor 动态计算每次读取的行数，同时追踪 PREWHERE 未命中的 Mark 范围（供 QueryConditionCache 使用）以及运行时数据流统计信息。
namespace DB
{

class UncompressedCache;
class MarkCache;

struct MergeTreeBlockSizePredictor;
using MergeTreeBlockSizePredictorPtr = std::shared_ptr<MergeTreeBlockSizePredictor>;

class IMergeTreeDataPart;
using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
using MergeTreeReaderPtr = std::unique_ptr<IMergeTreeReader>;

class DeserializationPrefixesCache;
using DeserializationPrefixesCachePtr = std::shared_ptr<DeserializationPrefixesCache>;

class MergedPartOffsets;
using MergedPartOffsetsPtr = std::shared_ptr<MergedPartOffsets>;

struct MergeTreeIndexReadResult;
using MergeTreeIndexReadResultPtr = std::shared_ptr<MergeTreeIndexReadResult>;

struct MergeTreeIndexBuildContext;
using MergeTreeIndexBuildContextPtr = std::shared_ptr<MergeTreeIndexBuildContext>;

struct LazyMaterializingRows;
using LazyMaterializingRowsPtr = std::shared_ptr<LazyMaterializingRows>;

class RuntimeDataflowStatisticsCacheUpdater;
using RuntimeDataflowStatisticsCacheUpdaterPtr = std::shared_ptr<RuntimeDataflowStatisticsCacheUpdater>;

enum class MergeTreeReadType : uint8_t
{
    /// By default, read will use MergeTreeReadPool and return pipe with num_streams outputs.
    /// If num_streams == 1, will read without pool, in order specified in parts.
    Default,
    /// Read in sorting key order.
    /// Returned pipe will have the number of ports equals to parts.size().
    /// Parameter num_streams_ is ignored in this case.
    /// User should add MergingSorted itself if needed.
    InOrder,
    /// The same as InOrder, but in reverse order.
    /// For every part, read ranges and granules from end to begin. Also add ReverseTransform.
    InReverseOrder,
    /// A special type of reading where every replica
    /// talks to a remote coordinator (which is located on the initiator node)
    /// and who spreads marks and parts across them.
    ParallelReplicas,
};

/// Read task for index.
/// Some indexes (e.g. inverted text index) may read special virtual columns.
struct IndexReadTask
{
    NamesAndTypesList columns;
    MergeTreeIndexWithCondition index;
    bool is_final = false;
};

/// Ordered map to ensure deterministic iteration order.
/// `IndexReadTasks` may be copied (e.g. into `MergeTreeReadPoolBase`) and then
/// iterated independently in `getPrewhereActions` / `getReadTaskColumns`.
/// `std::unordered_map` does not guarantee the same iteration order after copy,
/// which leads to mismatched prewhere readers and actions.
using IndexReadTasks = std::map<String, IndexReadTask>;
using IndexReadColumns = std::map<String, VirtualColumnsDescription>;

struct MergeTreeReadTaskColumns
{
    /// Column names to read during WHERE
    NamesAndTypesList columns;
    /// Column names to read during each PREWHERE step
    NamesAndTypesLists pre_columns;
    /// Column names to read from patch parts.
    NamesAndTypesLists patch_columns;

    String dump() const;
    Names getAllColumnNames() const;
    void moveAllColumnsFromPrewhere();
};

struct MergeTreeReadTaskInfo
{
    /// Part (owned or borrowed) to read while performing this task.
    /// Owned parts wrap a concrete IMergeTreeDataPart (LoadedMergeTreeDataPartInfoForReader);
    /// stateless-worker parts use BorrowedMergeTreeDataPartInfoForReader.
    MergeTreeDataPartInfoForReaderPtr data_part_info;
    /// Parent part of the projection part (projections are coordinator-only)
    DataPartPtr parent_part;
    /// For `part_index` virtual column
    size_t part_index_in_query{};
    /// For `part_starting_offset` virtual column
    size_t part_starting_offset_in_query{};
    /// Alter converversionss that should be applied on-fly for part.
    AlterConversionsPtr alter_conversions;
    /// `_part_offset` mapping used to merge projections with `_part_offset`.
    MergedPartOffsetsPtr merged_part_offsets;
    /// Prewhere steps that should be applied to execute on-fly mutations for part.
    PrewhereExprSteps mutation_steps;
    /// Whether `mutation_steps` holds steps for on-fly mutations, as opposed to only the step that
    /// applies an already materialized lightweight-delete mask.
    bool has_on_fly_mutation_steps = false;
    /// Patches that should be applied for part.
    PatchPartsForReader patch_parts;
    /// Column names to read during PREWHERE and WHERE
    MergeTreeReadTaskColumns task_columns;
    /// Shared initialized size predictor. It is copied for each new task.
    MergeTreeBlockSizePredictorPtr shared_size_predictor;
    /// Shared constant fields for virtual columns.
    VirtualFields const_virtual_fields;
    /// Index read tasks.
    IndexReadTasks index_read_tasks;
    /// The amount of data to read per task based on size of the queried columns.
    size_t min_marks_per_task = 0;
    size_t approx_size_of_mark = 0;
    /// Cache of the columns prefixes for this part.
    DeserializationPrefixesCachePtr deserialization_prefixes_cache;
    /// Extra info for optimizations - exact row processing, calculated virtual columns.
    RangesInDataPartReadHints read_hints;
};

using MergeTreeReadTaskInfoPtr = std::shared_ptr<const MergeTreeReadTaskInfo>;

/// A batch of work for MergeTreeSelectProcessor
struct MergeTreeReadTask : private boost::noncopyable
{
public:
    /// Extra params that required for creation of reader.
    struct Extras
    {
        UncompressedCache * uncompressed_cache = nullptr;
        MarkCache * mark_cache = nullptr;
        PatchJoinCache * patch_join_cache = nullptr;
        MergeTreeReaderSettings reader_settings;
        StorageSnapshotPtr storage_snapshot{};
        ValueSizeMap value_size_map{};
        ReadBufferFromFileBase::ProfileCallback profile_callback{};
    };

    struct Readers
    {
        MergeTreeReaderPtr main;
        std::vector<MergeTreeReaderPtr> prewhere;
        MergeTreePatchReaders patches;
        MergeTreeReaderPtr prepared_index;

        void updateAllMarkRanges(const MarkRanges & ranges, const std::vector<MarkRanges> & patches_ranges);
    };

    struct BlockSizeParams
    {
        UInt64 max_block_size_rows = DEFAULT_BLOCK_SIZE;
        UInt64 preferred_block_size_bytes = 1000000;
        UInt64 preferred_max_column_in_block_size_bytes = 0;
        double min_filtration_ratio = 0.00001;
    };

    /// The result of reading from task.
    struct BlockAndProgress
    {
        Block block;
        MarkRanges read_mark_ranges;
        /// Per-granule unmatched marks: marks where all rows were filtered out by PREWHERE.
        /// Populated only when use_query_condition_cache is enabled.
        /// Superset of what addPrewhereUnmatchedMarks recorded with the old coarse approach,
        /// because it captures individual filtered-out granules even in partially-passing batches.
        MarkRanges unmatched_mark_ranges;
        size_t row_count = 0;
        size_t num_read_rows = 0;
        size_t num_read_bytes = 0;
    };

    MergeTreeReadTask(
        MergeTreeReadTaskInfoPtr info_,
        Readers readers_,
        MarkRanges mark_ranges_,
        std::vector<MarkRanges> patches_mark_ranges_,
        const BlockSizeParams & block_size_params_,
        MergeTreeBlockSizePredictorPtr size_predictor_,
        RuntimeDataflowStatisticsCacheUpdaterPtr updater_);

    void initializeReadersChain(
        const PrewhereExprInfo & prewhere_actions,
        MergeTreeIndexBuildContextPtr index_build_context,
        LazyMaterializingRowsPtr lazy_materializing_rows,
        const ReadStepsPerformanceCounters & read_steps_performance_counters,
        bool collect_predicate_statistics);

    void initializeIndexReader(const MergeTreeIndexBuildContextPtr & index_build_context, const LazyMaterializingRowsPtr & lazy_materializing_rows);

    BlockAndProgress read();
    bool isFinished() const { return mark_ranges.empty() && readers_chain.isCurrentRangeFinished(); }

    const MergeTreeReadTaskInfo & getInfo() const { return *info; }
    const MergeTreeReadersChain & getReadersChain() const { return readers_chain; }
    const IMergeTreeReader & getMainReader() const { return *readers.main; }

    void addPrewhereUnmatchedMarks(const MarkRanges & mark_ranges_);
    const MarkRanges & getPrewhereUnmatchedMarks() { return prewhere_unmatched_marks; }

    /// Returns true if a reader earlier in the chain than PREWHERE can skip whole marks based on
    /// secondary indexes (skip-index or projection-index). When true, marks that appear in
    /// `read_mark_ranges` with `row_count == 0` may have been filtered before PREWHERE evaluated
    /// them, so they must not be attributed to the PREWHERE predicate in the QueryConditionCache.
    /// See Issue #104781.
    bool readersChainCanSkipMarksBeforePrewhere() const;

    /// Returns true if on-fly mutations or patch parts are applied earlier in the readers chain
    /// than PREWHERE (and therefore than the downstream WHERE filter too). When true, a mark may be
    /// fully filtered by the mutation rather than by the predicate, so it must not be attributed to
    /// the PREWHERE or WHERE predicate in the QueryConditionCache.
    bool appliesMutationsBeforePrewhere() const;

    Readers releaseReaders() { return std::move(readers); }

    size_t getNumMarksToRead() const { return mark_ranges.getNumberOfMarks(); }

    static Readers createReaders(
        const MergeTreeReadTaskInfoPtr & read_info,
        const Extras & extras,
        const MarkRanges & ranges,
        const std::vector<MarkRanges> & patches_ranges);

    static MergeTreeReadersChain createReadersChain(
        const Readers & readers,
        const PrewhereExprInfo & prewhere_actions,
        const ReadStepsPerformanceCounters & read_steps_performance_counters,
        bool collect_predicate_statistics);

private:
    using DataflowCacheUpdateCallback = std::function<void(
        const ColumnsWithTypeAndName & columns,
        const NameSet & partially_read_columns,
        size_t read_bytes,
        std::optional<bool> & should_continue_sampling)>;

    UInt64 estimateNumRows() const;

    /// Shared information required for reading.
    // 指向当前分区的共享只读元数据（MergeTreeReadTaskInfo）。
    MergeTreeReadTaskInfoPtr info;

    /// Readers for data_part of this task.
    /// May be reused and released to the next task.
    // 当前任务持有的底层列读取器集合（main、prewhere、patches、prepared_index）。任务结束时可通过 releaseReaders() 移交给同分区的下一个任务复用。
    Readers readers;

    /// Range readers chain to read mark_ranges from data_part.
    // 读取器流水线链条，负责按顺序串联索引过滤、动态 Mutation、Patch 合并、多步 PREWHERE 过滤以及主列延迟物化读取。
    MergeTreeReadersChain readers_chain;

    /// Ranges to read from data_part.
    // 当前任务尚未读取完毕的主分区 Mark 范围队列。每调用一次 read() 就会从头部消耗掉一部分区间，直到为空。
    MarkRanges mark_ranges;

    /// Ranges to read from patch parts.
    // 与主分区 mark_ranges 对应的各补丁分区（Patch Parts）待读取 Mark 范围列表。
    std::vector<MarkRanges> patches_mark_ranges;

    /// Tracks which mark ranges are not matched by PREWHERE (needed for query condition cache)
    // 累计记录当前任务执行期间所有被 PREWHERE 完全过滤掉的 Mark 范围，任务完成时写入全局 QueryConditionCache。
    MarkRanges prewhere_unmatched_marks;
    // 单块行数与字节数限制参数配置。
    BlockSizeParams block_size_params;

    /// Used to satistfy preferred_block_size_bytes limitation
    // 动态块大小预测器，根据已读列的实际字节大小动态调整下一步读取的行数，使输出块大小贴近 preferred_block_size_bytes。
    MergeTreeBlockSizePredictorPtr size_predictor;
    // 运行时数据流统计缓存更新器，用于在查询执行期间采样收集数据特征（如列基数、过滤率）以优化后续执行计划。
    RuntimeDataflowStatisticsCacheUpdaterPtr updater;
    // 内部封装的回调函数，用于向 updater 汇报本批次读取的列、部分读取的列集合、读取字节数，并接收 should_continue_sampling 采样控制信号。
    DataflowCacheUpdateCallback dataflow_cache_update_cb;
};

using MergeTreeReadTaskPtr = std::unique_ptr<MergeTreeReadTask>;

}
