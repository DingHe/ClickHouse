#pragma once

#include <Common/VectorWithMemoryTracking.h>
#include <Core/Block_fwd.h>
#include <Processors/IProcessor_fwd.h>

#include <functional>

namespace DB
{

class Chain;
class EnabledQuota;
struct StreamLocalLimits;

class Pipe;
using Pipes = std::vector<Pipe>; // STYLE_CHECK_ALLOW_STD_CONTAINERS

class ReadProgressCallback;

using OutputPortRawPtrs = std::vector<OutputPort *>; // STYLE_CHECK_ALLOW_STD_CONTAINERS

/// Pipe is a set of processors which represents the part of pipeline.
/// Pipe contains a list of output ports, with specified port for totals and specified port for extremes.
/// All output ports have same header.
/// All other ports are connected, all connections are inside processors set.
// Pipe 是查询执行引擎管道（QueryPipeline）的核心构建块之一。
// 它代表了查询执行图的一部分（一组相互连接的 Processor 集合），通常由表引擎（如 IStorage::read）返回，用于向上传递数据、汇总行（Totals）以及极值（Extremes）
// Pipe 类的核心作用
// 管理数据流片段：封装了一组负责数据读取、过滤、转换的 IProcessor 处理器。
// 统一端口与表头契约：维护了一组输出端口（output_ports），以及专门用于汇总数据的 totals_port 和极值的 extremes_port。所有输出端口必须具有相同的表头结构（Header）。
// 构建查询流水线：作为 IStorage::read 的返回值，它提供了丰富的组合、变换、调整并行度（resize）等方法，方便上层 QueryPipeline 组装成完整的执行图。
class Pipe
{
public:
    /// Default constructor creates empty pipe. Generally, you cannot do anything with it except to check it is empty().
    /// You cannot get empty pipe in any other way. All transforms check that result pipe is not empty.
    Pipe();
    /// Create from source. Source must have no input ports and single output.
    // 从一个单独的源处理器（Source）创建 Pipe。要求该源处理器没有输入端口且只有一个输出端口。
    explicit Pipe(ProcessorPtr source);
    /// Create from source with specified totals end extremes (may be nullptr). Ports should be owned by source.
    // 从源处理器创建 Pipe，并显式指定其总计端口（totals）和极值端口（extremes）（可以为 nullptr）。要求这些端口必须归该源处理器所有。
    explicit Pipe(ProcessorPtr source, OutputPort * output, OutputPort * totals, OutputPort * extremes);
    /// Create from processors. Use all not-connected output ports as output_ports. Check invariants.
    // 从一组现有的处理器集合创建 Pipe。会自动将所有未连接的输出端口作为该 Pipe 的输出端口，并执行内部不变式检查。
    explicit Pipe(std::shared_ptr<Processors> processors_);

    Pipe(const Pipe & other) = delete;
    Pipe(Pipe && other) = default;
    Pipe & operator=(const Pipe & other) = delete;
    Pipe & operator=(Pipe && other) = default;
    // 返回该 Pipe 中所有输出端口共享的数据块表头（Header，包含列名和数据类型）。
    const Block & getHeader() const { return *header; }
    // 返回共享的表头智能指针（SharedHeader）。
    const SharedHeader & getSharedHeader() const { return header; }
    // 判断当前 Pipe 是否为空（即内部的处理器列表是否为空）。
    bool empty() const { return processors->empty(); }
    // 返回当前 Pipe 的主输出端口数量（对应并行数据流的数量）。
    size_t numOutputPorts() const { return output_ports.size(); }
    // 返回每个步骤可并行执行的最大处理器数量。
    size_t maxParallelStreams() const { return max_parallel_streams; }
    // 获取指定索引位置 (pos) 的主输出端口指针。
    OutputPort * getOutputPort(size_t pos) const { return output_ports[pos]; }
    // 获取总计（Totals）端口指针（若无则返回 nullptr）。
    OutputPort * getTotalsPort() const { return totals_port; }
    // 获取极值（Extremes）端口指针（若无则返回 nullptr）。
    OutputPort * getExtremesPort() const { return extremes_port; }

    /// Add processor to list, add it output ports to output_ports.
    /// Processor shouldn't have input ports, output ports shouldn't be connected.
    /// Output headers should have same structure and be compatible with current header (if not empty()).
    // 向 Pipe 中添加一个新的数据源处理器。要求该处理器不能有输入端口，其输出端口不能已被连接，且输出表头必须与当前 Pipe 的表头兼容。
    void addSource(ProcessorPtr source);

    /// Add totals and extremes.
    // 为 Pipe 添加总计数据源。
    void addTotalsSource(ProcessorPtr source);
    // 为 Pipe 添加极值数据源。
    void addExtremesSource(ProcessorPtr source);

    /// Drop totals and extremes. All three discard through a `DroppingTransform` on the data path.
    // 丢弃总计（Totals）端口，通过 DroppingTransform 在数据路径上静默丢弃。
    void dropTotals();
    // 丢弃极值（Extremes）端口。
    void dropExtremes();
    // 同时丢弃总计和极值端口。
    void dropTotalsAndExtremes();

    /// Add processor to list. It should have size() input ports with compatible header.
    /// Output ports should have same headers.
    /// If totals or extremes are not empty, transform shouldn't change header.
    // 向每个输出端口添加一个转换处理器（Transform）。要求该转换处理器具有与输出端口数量相匹配的输入端口数，且表头必须兼容。
    void addTransform(ProcessorPtr transform);
    void addTransform(ProcessorPtr transform, OutputPort * totals, OutputPort * extremes);
    void addTransform(ProcessorPtr transform, InputPort * totals, InputPort * extremes);

    void addTransform(
        ProcessorPtr transform,
        InputPort * totals_in, InputPort * extremes_in,
        OutputPort * totals_out, OutputPort * extremes_out);

    enum class StreamType : uint8_t
    {
        Main = 0, /// Stream for query data. There may be several streams of this type.
        Totals,  /// Stream for totals. No more than one.
        Extremes, /// Stream for extremes. No more than one.
    };

    using ProcessorGetterSharedHeader = std::function<ProcessorPtr(const SharedHeader & header)>;
    using ProcessorGetterSharedHeaderWithStreamKind = std::function<ProcessorPtr(const SharedHeader & header, StreamType stream_type)>;

    /// Add transform with single input and single output for each port.
    void addSimpleTransform(const ProcessorGetterSharedHeader & getter);
    void addSimpleTransform(const ProcessorGetterSharedHeaderWithStreamKind & getter);

    /// Add chain to every output port.
    void addChains(VectorWithMemoryTracking<Chain> chains);

    /// Changes the number of output ports if needed. Adds (Strict)ResizeProcessor.
    // 调整 Pipe 的输出端口数量（即改变并行读取或处理的并发度）。底层会自动插入合适的调整处理器（如 ResizeProcessor 或 StrictResizeProcessor）。
    void resize(size_t num_streams, bool strict = false, UInt64 min_outstreams_per_resize_after_split = 0);

    /// Watermark-aware pair to resize. Adds CalibrateWatermarksProcessor.
    // 感知水印（Watermark）的调整大小对，会插入 CalibrateWatermarksProcessor 处理器。
    void calibrateWatermarks(size_t num_streams);

    using Transformer = std::function<Processors(const OutputPortRawPtrs & ports)>;

    /// Transform Pipe in general way.
    // 以通用方式转换整个 Pipe。传入一个自定义的 Transformer 函数（接收当前所有原始输出端口指针，返回一组新的处理器），并可选择是否进行端口检查。
    void transform(const Transformer & transformer, bool check_ports = true);

    /// Unite several pipes together. They should have same header.
    // 将多个独立的 Pipe 合并为一个 Pipe。要求所有待合并的 Pipe 必须具有相同的表头。
    static Pipe unitePipes(Pipes pipes);

    /// Get processors from Pipe. Use it with caution, it is easy to lose totals and extremes ports.
    // 从一个 Pipe 中剥离并移走所有的处理器。需谨慎使用，因为这样很容易丢失 totals 和 extremes 端口。
    static Processors detachProcessors(Pipe pipe) { return *std::move(pipe.processors); }
    /// Get processors from Pipe without destroying pipe (used for EXPLAIN to keep QueryPlan).
    // 获取内部处理器列表的常量引用（不破坏 Pipe 本身，主要用于 EXPLAIN 语句以保留 QueryPlan）。
    const Processors & getProcessors() const { return *processors; }
    // 获取内部处理器列表的共享智能指针。
    std::shared_ptr<Processors> getProcessorsPtr() { return processors; }

private:
    /// Header is common for all output below.
    SharedHeader header;
    // 存储当前 Pipe 内部所有处理器的共享指针列表。
    std::shared_ptr<Processors> processors;

    /// Output ports. Totals and extremes are allowed to be empty.
    // 主输出端口的原始指针向量。
    OutputPortRawPtrs output_ports;
    // 总计端口与极值端口的指针（允许为空）。
    OutputPort * totals_port = nullptr;
    OutputPort * extremes_port = nullptr;

    /// It is the max number of processors which can be executed in parallel for each step.
    /// Usually, it's the same as the number of output ports.
    // 每个步骤可并行执行的最大处理器数量（通常等于输出端口的数量）。
    size_t max_parallel_streams = 0;

    /// If is set, all newly created processors will be added to this too.
    /// It is needed for debug. See QueryPipelineProcessorsCollector.
    // 调试用途。如果设置了该指针，所有新创建的处理器也会被同步收集到这里（例如用于 QueryPipelineProcessorsCollector）。
    Processors * collected_processors = nullptr;

    /// Discards the requested streams through a `DroppingTransform`. Requires a data output to forward,
    /// which holds for every caller: totals and extremes can only be added to a non-empty pipe, and
    /// `addTransform` rejects an empty one.
    // 通过 DroppingTransform 丢弃指定的流。
    void dropStreams(bool drop_totals, bool drop_extremes);

    /// This methods are for QueryPipeline. It is allowed to complete graph only there.
    /// So, we may be sure that Pipe always has output port if not empty.
    // 供 QueryPipeline 使用的内部检查方法，判断 Pipe 是否已经完成（即非空且输出端口为空）。
    bool isCompleted() const { return !empty() && output_ports.empty(); }
    // 重载的私有管道合并方法，支持收集器和允许空表头的特殊场景。
    static Pipe unitePipes(Pipes pipes, Processors * collected_processors, bool allow_empty_header);
    // 为管道设置终点处理器（Sink）。
    void setSinks(const Pipe::ProcessorGetterSharedHeaderWithStreamKind & getter);
    // 用于添加拆分与调整大小的组合转换。
    void addSplitResizeTransform(size_t num_streams, size_t min_outstreams_per_resize_after_split, bool strict);

    friend class QueryPipelineBuilder;
    friend class QueryPipeline;
};

}
