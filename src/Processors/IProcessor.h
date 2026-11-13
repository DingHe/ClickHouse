#pragma once

#include <Processors/Port.h>
#include <Common/ProcessorMemoryStats.h>
#include <Common/Stopwatch.h>

#include <atomic>
#include <list>
#include <memory>
#include <vector>
#include <Processors/ProcessorsProfileLogInfo.h>
#include <Processors/IProcessor_fwd.h>
#include <fmt/format.h>

class EventCounter;

namespace DB
{

class IQueryPlanStep;

struct StorageLimits;
using StorageLimitsList = std::list<StorageLimits>;

class RowsBeforeStepCounter;
using RowsBeforeStepCounterPtr = std::shared_ptr<RowsBeforeStepCounter>;

class IProcessor;
using ProcessorPtr = std::shared_ptr<IProcessor>;
using Processors = std::list<ProcessorPtr>;

class StepWallClock;


using StepWallClockPtr = std::shared_ptr<StepWallClock>;

/** Processor is an element (low level building block) of a query execution pipeline.
  * It has zero or more input ports and zero or more output ports.
  *
  * Blocks of data are transferred over ports.
  * Each port has fixed structure: names and types of columns and values of constants.
  *
  * Processors may pull data from input ports, do some processing and push data to output ports.
  * Processor may indicate that it requires input data to proceed and indicate that it needs data from some ports.
  *
  * Synchronous work must only use CPU - don't do any sleep, IO wait, network wait.
  *
  * Processor may want to do work asynchronously (example: fetch data from remote server)
  *  - in this case it will initiate background job and allow to subscribe to it.
  *
  * Processor may throw an exception to indicate some runtime error.
  *
  * Different ports may have different structure. For example, ports may correspond to different resultsets
  *  or semantically different parts of result.
  *
  * Processor may modify its ports (create another processors and connect to them) on the fly.
  * Example: first execute the subquery; on basis of subquery result
  *  determine how to execute the rest of query and build the corresponding pipeline.
  *
  * Processor may simply wait for another processor to execute without transferring any data from it.
  * For this purpose it should connect its input port to another processor, and indicate need of data.
  *
  * Examples:
  *
  * Source. Has no input ports and single output port. Generates data itself and pushes it to its output port.
  *
  * Sink. Has single input port and no output ports. Consumes data that was passed to its input port.
  *
  * Empty source. Immediately says that data on its output port is finished.
  *
  * Null sink. Consumes data and does nothing.
  *
  * Simple transformation. Has single input and single output port. Pulls data, transforms it and pushes to output port.
  * Example: expression calculator.
  *
  * Squashing or filtering transformation. Pulls data, possibly accumulates it, and sometimes pushes it to output port.
  * Examples: DISTINCT, WHERE, squashing of blocks for INSERT SELECT.
  *
  * Accumulating transformation. Pulls and accumulates all data from input until it it exhausted, then pushes data to output port.
  * Examples: ORDER BY, GROUP BY.
  *
  * Limiting transformation. Pulls data from input and passes to output.
  * When there was enough data, says that it doesn't need data on its input and that data on its output port is finished.
  *
  * Resize. Has arbitrary number of inputs and arbitrary number of outputs.
  * Pulls data from whatever ready input and pushes it to randomly chosen free output.
  * Examples:
  * Union - merge data from number of inputs to one output in arbitrary order.
  * Split - read data from one input and pass it to arbitrary output.
  *
  * Concat. Has many inputs and only one output. Pulls all data from first input until it is exhausted,
  *  then all data from second input, etc. and pushes all data to output.
  *
  * Ordered merge. Has many inputs but only one output. Pulls data from selected input in specific order, merges and pushes it to output.
  *
  * Fork. Has one input and many outputs. Pulls data from input and copies it to all outputs.
  * Used to process multiple queries with common source of data.
  *
  * Select. Has one or multiple inputs and one output.
  * Read blocks from inputs and check that blocks on inputs are "parallel": correspond to each other in number of rows.
  * Construct a new block by selecting some subset (or all) of columns from inputs.
  * Example: collect columns - function arguments before function execution.
  *
  *
  * TODO Processors may carry algebraic properties about transformations they do.
  * For example, that processor doesn't change number of rows; doesn't change order of rows, doesn't change the set of rows, etc.
  *
  * TODO Ports may carry algebraic properties about streams of data.
  * For example, that data comes ordered by specific key; or grouped by specific key; or have unique values of specific key.
  * And also simple properties, including lower and upper bound on number of rows.
  *
  * TODO Processor should have declarative representation, that is able to be serialized and parsed.
  * Example: read_from_merge_tree(database, table, Columns(a, b, c), Piece(0, 10), Parts(Part('name', MarkRanges(MarkRange(0, 100), ...)), ...))
  * It's reasonable to have an intermediate language for declaration of pipelines.
  *
  * TODO Processor with all its parameters should represent "pure" function on streams of data from its input ports.
  * It's in question, what kind of "pure" function do we mean.
  * For example, data streams are considered equal up to order unless ordering properties are stated explicitly.
  * Another example: we should support the notion of "arbitrary N-th of M substream" of full stream of data.
  */
// IProcessor 是查询执行流水线（Query Pipeline）中最底层的构建块（Low-level building block）。所有的算子、数据源（Source）、数据沉淀端（Sink）以及各种转换（Transformations，如过滤、聚合、排序、JOIN 等）都继承自 IProcessor。
// IProcessor 类的核心作用
// 执行单元的抽象：封装了零个或多个输入端口（InputPort）和输出端口（OutputPort），通过端口之间的连接和数据块（Block）的传递来构成执行图。
// 状态驱动的执行模型：通过 prepare() 方法进行“即时”（O(1)、无阻塞）计算和状态检查，返回诸如 Ready（可执行同步工作）、NeedData（缺数据）、PortFull（输出端口满）、Async（异步等待）等状态，由调度器（Executor）驱动执行。
// 支持同步与异步操作：允许同步使用 CPU 进行数据转换，也支持通过 schedule() / scheduleForEvent() 挂起并利用 epoll 机制处理异步 I/O（例如远程网络请求、文件异步读写）。
// 动态拓扑调整：处理器可以在运行时通过 UpdatePipeline 状态动态地向流水线中添加或删除邻居节点（例如在获取子查询结果后动态构建剩余的执行计划）。
// 运维观测与统计：内建了详细的耗时统计（CPU 时间、输入/输出等待时间）、数据量统计（行数、字节数）、内存溢写（Spillable）以及与 QueryPlan 步骤的映射关系。
class IProcessor
{
protected:
    // 存储该处理器所有的输入端口。
    InputPorts inputs;
    // 存储该处理器所有的输出端口。
    OutputPorts outputs;

public:

    IProcessor();
    // 带参构造函数，使用指定的输入和输出端口列表初始化处理器。
    IProcessor(InputPorts inputs_, OutputPorts outputs_);
    // 返回处理器的具体类型名称（例如 "ExpressionTransform", "MergeTreeSource" 等）。
    virtual String getName() const = 0;
    // 结合处理器名称和索引生成全局唯一的处理器标识字符串。
    String getUniqID() const { return fmt::format("{}_{}", getName(), processor_index); }

    enum class Status : uint8_t
    {
        /// Processor needs some data at its inputs to proceed.
        /// You need to run another processor to generate required input and then call 'prepare' again.
        NeedData,

        /// Processor cannot proceed because output port is full or not isNeeded().
        /// You need to transfer data from output port to the input port of another processor and then call 'prepare' again.
        PortFull,

        /// All work is done (all data is processed or all output are closed), nothing more to do.
        Finished,

        /// No one needs data on output ports.
        /// Unneeded,

        /// You may call 'work' method and processor will do some work synchronously.
        Ready,

        /// You may call 'schedule' method and processor will return a descriptor.
        /// You need to poll this descriptor and call work() afterwards.
        Async,

        /// Processor wants to add new processors and/or remove finished neighbours.
        /// Update must be obtained by updatePipeline() call.
        UpdatePipeline,
    };

    static std::string statusToName(std::optional<Status> status);

    /** Method 'prepare' is responsible for all cheap ("instantaneous": O(1) of data volume, no wait) calculations.
      *
      * It may access input and output ports,
      *  indicate the need for work by another processor by returning NeedData or PortFull,
      *  or indicate the absence of work by returning Finished or Unneeded,
      *  it may pull data from input ports and push data to output ports.
      *
      * The method is not thread-safe and must be called from a single thread in one moment of time,
      *  even for different connected processors.
      *
      * Instead of all long work (CPU calculations or waiting) it should just prepare all required data and return Ready or Async.
      *
      * Thread safety and parallel execution:
      * - no methods (prepare, work, schedule) of single object can be executed in parallel;
      * - method 'work' can be executed in parallel for different objects, even for connected processors;
      * - method 'prepare' cannot be executed in parallel even for different objects,
      *   if they are connected (including indirectly) to each other by their ports;
      */
    // 核心驱动方法。执行所有廉价的、瞬时的（O(1) 数据量、无等待）计算和状态检查，不直接进行耗时的 CPU 或 I/O 操作。
    // 返回一个 Status 枚举值指示下一步动作（如 Ready, NeedData, PortFull, Finished, Async, UpdatePipeline 等）。
    virtual Status prepare();

    /// Optimization for prepare in case we know ports were updated.
    using UpdatedInputPorts  = std::vector<InputPort *>;
    using UpdatedOutputPorts = std::vector<OutputPort *>;
    // prepare() 的优化重载版本。当已知某些特定端口被更新时，可以直接利用此信息，默认实现会回退调用无参的 prepare()。
    virtual Status prepare(const UpdatedInputPorts & /*updated_input_ports*/, const UpdatedOutputPorts & /*updated_output_ports*/) { return prepare(); }

    /** You may call this method if 'prepare' returned Ready.
      * This method cannot access any ports. It should use only data that was prepared by 'prepare' method.
      *
      * Method work can be executed in parallel for different processors.
      */
    // 当 prepare() 返回 Ready 时被调度器调用。执行实际的同步计算工作（注意：此方法不能访问任何端口状态，只能使用 prepare() 准备好的数据）。不同处理器的 work() 可以并行执行。
    virtual void work();

    /** Executor must call this method when 'prepare' returned Async.
      * This method cannot access any ports. It should use only data that was prepared by 'prepare' method.
      *
      * This method should instantly return epollable file descriptor which will be readable when asynchronous job is done.
      * When descriptor is readable, method `work` is called to continue data processing.
      *
      * NOTE: it would be more logical to let `work()` return ASYNC status instead of prepare. This will get
      * prepare() -> work() -> schedule() -> work() -> schedule() -> .. -> work() -> prepare()
      * chain instead of
      * prepare() -> work() -> prepare() -> schedule() -> work() -> prepare() -> schedule() -> .. -> work() -> prepare()
      *
      * It is expected that executor epoll using level-triggered notifications.
      * Read all available data from descriptor before returning ASYNC.
      */
    // 当 prepare() 返回 Async 时被调用。应当立即返回一个可用于 epoll 轮询的文件描述符（如 socket 或文件 fd）。当该描述符变为可读时，调度器会再次调用 work() 继续处理。
    virtual int schedule();

    /** This method is similar to schedule() but also returns epoll events mask and an optional timeout.
      * Note that file descriptor returned by schedule() will be polled for read (EPOLLIN event) and errors
      * but for ISink implementations that write data to network or to files it is necessary to poll for write (EPOLLOUT) events as well.
      *
      * The third element is a timeout in milliseconds. If non-negative, the processor will be re-dispatched after
      * the timeout even when the fd has not become readable. A value of -1 means "no timeout" (block until fd fires).
      */
    // 类似于 schedule()，但不仅支持读轮询（EPOLLIN），还支持写轮询（EPOLLOUT，例如 Sink 向网络或文件写数据时需要）以及毫秒级超时设置。在 Linux/Darwin 系统上生效。
#if defined(OS_LINUX) || defined(OS_DARWIN)
    virtual std::tuple<int, uint32_t, Int64> scheduleForEvent();
#endif

    /* The method is called right after asynchronous job is done
     * i.e. when file descriptor returned by schedule() is readable.
     * The sequence of method calls:
     * ... prepare() -> schedule() -> onAsyncJobReady() -> work() ...
     * See also comment to schedule() method
     *
     * It allows doing some preprocessing immediately after asynchronous job is done.
     * The implementation should return control quickly, to avoid blocking another asynchronous completed jobs
     * created by the same pipeline.
     *
     * Example, scheduling tasks for remote workers (file descriptor in this case is a socket)
     * When the remote worker asks for the next task, doing it in onAsyncJobReady() we can provide it immediately.
     * Otherwise, the returning of the next task for the remote worker can be delayed by current work done in the pipeline
     * (by other processors), which will create unnecessary latency in query processing by remote workers
     */
    // 当异步任务完成（即 schedule() 返回的 fd 变为可读）时紧接着被调用，允许在执行 work() 之前做一些轻量的前置处理。
    virtual void onAsyncJobReady() {}

    /** You must call this method if 'prepare' returned UpdatePipeline.
      * This method cannot access any port, but it can create new ports for current processor.
      *
      * Method should return set of new already connected processors or disconnected finished processors.
      * All returned processors must be connected only to each other or current processor.
      *
      * Method can't move data from/to port or perform calculations.
      * 'prepare' should be called again after this operation.
      */
    struct PipelineUpdate
    {
        Processors to_add;
        Processors to_remove;
    };
    // 当 prepare() 返回 UpdatePipeline 时被调用。用于在运行时动态向流水线中添加新处理器（to_add）或移除已完成的邻居处理器（to_remove）。
    virtual PipelineUpdate updatePipeline();

    /// Why the processor is being cancelled, chosen by the caller of cancel.
    enum class CancelReason : uint8_t
    {
        NotCancelled,           /// Default state: no cancellation happened yet.
        Unknown,                /// Cancelled without a specific reason (legacy callers).
        CancelledByUser,        /// User killed the query or closed the session.
        CancelledByTimeout,     /// Query time limit exceeded.
        PartialResult,          /// Consumer has enough data; stop ingress and drain compute.
        Exception,              /// Pipeline is being torn down due to an error.
    };

    /// In case if query was cancelled executor will wait till all processors finish their jobs.
    /// Generally, there is no reason to check this flag. However, it may be reasonable for long operations (e.g. i/o).
    // 检查当前处理器是否已被取消。
    bool isCancelled() const { return is_cancelled.load(std::memory_order_acquire); }
    // 根据指定的取消原因（如用户手动终止、超时、部分结果足够、异常等）取消当前处理器。
    virtual void cancel(CancelReason reason) noexcept;
    void cancel() noexcept { cancel(CancelReason::Unknown); }

    /// Additional method which is called in case if ports were updated while work() method.
    /// May be used to stop execution in rare cases.
    virtual void onUpdatePorts() {}

    virtual ~IProcessor() = default;

    auto & getInputs() { return inputs; }
    auto & getOutputs() { return outputs; }

    UInt64 getInputPortNumber(const InputPort * input_port) const;

    UInt64 getOutputPortNumber(const OutputPort * output_port) const;

    const auto & getInputs() const { return inputs; }
    const auto & getOutputs() const { return outputs; }

    /// Debug output.
    String debug() const;
    void dump() const;

    /// Used to print pipeline.
    void setDescription(const std::string & description_) { processor_description = description_; }
    const std::string & getDescription() const { return processor_description; }

    /// Helpers for pipeline executor.
    void setStream(size_t value) { stream_number = value; }
    size_t getStream() const { return stream_number; }
    constexpr static size_t NO_STREAM = std::numeric_limits<size_t>::max();

    /// Step of QueryPlan from which processor was created
    void setQueryPlanStep(const IQueryPlanStep * step, size_t group = 0);

    void setQueryPlanStepGroup(size_t group) { query_plan_step_group = group; }

    /// Copy the query step fields from parent processor to child processor
    /// The group can be adjusted manually, since even though the processors can be
    /// coming from the same step, they can belong to different groups (stages)
    void inheritQueryPlanStepFromParent(const IProcessor & parent, size_t group);

    const IQueryPlanStep * getQueryPlanStep() const { return query_plan_step; }
    const String & getStepUniqID() const { return step_uniq_id; }
    size_t getQueryPlanStepGroup() const { return query_plan_step_group; }
    const String & getPlanStepName() const { return plan_step_name; }
    const String & getPlanStepDescription() const { return plan_step_description; }

    uint64_t getElapsedNs() const { return elapsed_ns; }
    uint64_t getInputWaitElapsedNs() const { return input_wait_elapsed_ns; }
    uint64_t getOutputWaitElapsedNs() const { return output_wait_elapsed_ns; }

    struct PortDataCounters
    {
        size_t rows = 0;
        size_t bytes = 0;
    };

    /// The getter can be used only after running the query, for counting
    /// the exact number of rows/bytes per port
    /// Should be used only on the pors belonging to the processor
    PortDataCounters getPortDataCounters(const Port & port) const;

    struct ProcessorDataStats
    {
        size_t input_rows = 0;
        size_t input_bytes = 0;
        size_t output_rows = 0;
        size_t output_bytes = 0;
    };

    ProcessorDataStats getProcessorDataStats() const;

    ProcessorsProfileLogInfo getProcessorsProfileLogInfo() const;

    struct ReadProgressCounters
    {
        uint64_t read_rows = 0;
        uint64_t read_bytes = 0;
        uint64_t total_rows_approx = 0;
        uint64_t total_bytes = 0;
    };

    struct ReadProgress
    {
        ReadProgressCounters counters;
        const StorageLimitsList & limits;
    };

    /// Set limits for current storage.
    /// Different limits may be applied to different storages, we need to keep it per processor.
    /// This method needs to be overridden only for sources.
    virtual void setStorageLimits(const std::shared_ptr<const StorageLimitsList> & /*storage_limits*/) {}

    /// This method is called for every processor without input ports.
    /// Processor can return new progress for the last read operation.
    /// You should zero internal counters in the call, in order to make in idempotent.
    virtual std::optional<ReadProgress> getReadProgress() { return std::nullopt; }

    /// Set rows_before_limit counter for current processor.
    /// This counter is used to calculate the number of rows right before any filtration of LimitTransform.
    virtual void setRowsBeforeLimitCounter(RowsBeforeStepCounterPtr /* counter */) { }

    /// Set rows_before_aggregation counter for current processor.
    /// This counter is used to calculate the number of rows right before AggregatingTransform.
    virtual void setRowsBeforeAggregationCounter(RowsBeforeStepCounterPtr /* counter */) { }

    /// Returns true if processor can spill memory to disk.
    /// Aggregate, join and sort processors can be spillable.
    /// For unspillable processors, the memory usage is not tracked.
    // 返回布尔值，指示该处理器是否支持将过大的内存数据溢写到磁盘。
    inline bool isSpillable() const { return spillable; }
    // 获取当前处理器占用的内存统计信息。
    virtual ProcessorMemoryStats getMemoryStats()
    {
        return {};
    }

    // If the in-memory data's size is not larger then bytes, it doesn't spill
    // 请求处理器尝试将指定大小的数据溢写到磁盘，返回是否成功溢写。
    virtual bool spillOnSize(size_t /*bytes*/) { return false; }

protected:
    /// May be called in parallel with work().
    virtual void onCancel() noexcept {}
    // 原子布尔变量，标记当前处理器是否已被取消。
    std::atomic<bool> is_cancelled{false};
    // 指示该处理器是否支持将内存数据溢写（Spill）到磁盘（例如大型聚合、JOIN 或排序算子）。
    bool spillable = false;

private:
    /// For:
    /// - elapsed_ns
    friend class ExecutionThreadContext;
    /// For
    /// - input_wait_elapsed_ns
    /// - output_wait_elapsed_ns
    friend class ExecutingGraph;
    // 处理器的文本描述信息，用于调试或打印执行图。
    std::string processor_description;

    /// For processors_profile_log
    // 处理器实际运行消耗的 CPU 总时间（纳秒）。
    uint64_t elapsed_ns = 0;
    // 用于测量和记录等待输入数据所花费的时间。
    Stopwatch input_wait_watch;
    uint64_t input_wait_elapsed_ns = 0;
    // 用于测量和记录因为输出端口满而等待输出所花费的时间。
    Stopwatch output_wait_watch;
    uint64_t output_wait_elapsed_ns = 0;
    // 标识该处理器属于哪一个并发执行流（Stream）。
    size_t stream_number = NO_STREAM;
    // 指向生成该处理器的上层 QueryPlan 步骤指针。
    const IQueryPlanStep * query_plan_step = nullptr;
    // 对应计划步骤的唯一标识字符串。
    String step_uniq_id;
    // 查询计划步骤的分组编号（Stage 级别）。
    size_t query_plan_step_group = 0;
    // 处理器在步骤内的索引编号。
    size_t processor_index = 0;
    // 对应计划步骤的名称和详细描述。
    String plan_step_name;
    String plan_step_description;

};


}
