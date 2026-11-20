#pragma once

#include <Core/Block_fwd.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <Processors/Port.h>

#include <atomic>
#include <mutex>


namespace DB
{
// ISource 是所有数据源算子（Sources）的抽象基类
// ISource 类的作用
// 数据源的抽象：作为查询计划的源头，ISource 负责从底层存储引擎（如 MergeTree）、文件、网络或系统表中主动生成或读取数据。
// 无输入、单输出：与普通的转换算子不同，数据源通常没有输入端口（inputs.empty()），但拥有一个输出端口（output），负责源源不断地向流水线向下游推送数据块（Chunk）。
// 生命周期与状态管理：封装了数据生成的标准状态机逻辑（通过重写 prepare() 和 work()），简化了具体数据源（如文件读取、表扫描）的实现负担。
// 进度监控与配额统计：内建了读取进度上报机制（ReadProgress）、行数/字节数统计以及存储配额限制（StorageLimits）。
class ISource : public IProcessor
{
private:
    // 互斥锁。用于保护读取进度计数器在多线程并发访问时的线程安全性。
    std::mutex read_progress_mutex;
    // 记录读取进度的计数器结构体，包含已读取的行数、字节数以及估计的总行数/总字节数。
    ReadProgressCounters read_progress;
    // 原子布尔变量，标记当前数据源是否已经设置过读取进度。
    std::atomic_bool read_progress_was_set = false;
    // 布尔标志，指示是否自动计算和上报读取进度。
    bool auto_progress;

protected:
    // 引用类型，指向该数据源唯一的输出端口。数据生成后通过此端口推向对端。
    OutputPort & output;
    // 标记是否存在输入（对于纯 Source 而言通常为 false，由基类框架或状态机内部使用）。
    bool has_input = false;
    // 布尔标志，标记当前数据源是否已经把所有数据生成完毕（即到达文件末尾或 EOF）。
    bool finished = false;
    // 布尔标志，标记在生成数据过程中是否捕获并发生了异常。
    bool got_exception = false;
    // 缓存当前正在处理或准备推送的数据单元（包含 Chunk 或异常对象）。
    Port::Data current_chunk;
    // 存储限制列表的共享指针，用于在读取过程中实施配额管控（如最大读取行数、最大读取字节数限制）。
    std::shared_ptr<const StorageLimitsList> storage_limits;
    // 核心数据生成虚函数。子类（如具体的表扫描 Source）必须实现该方法，用于同步生成并返回下一个数据块（Chunk）。如果数据读完，通常返回空的 Chunk。
    virtual Chunk generate();
    // 尝试生成数据的虚函数。与 generate() 不同，它返回 std::optional<Chunk>。如果暂时没有数据可读但并没有结束（例如需要等待异步 I/O 或锁），可以返回 std::nullopt。默认实现通常会调用 generate()。
    virtual std::optional<Chunk> tryGenerate();
    // 当数据源正常结束（Finish）时被调用，默认实现为空，子类可在此做清理工作。
    virtual void onFinish() {}
    // 进度更新内部方法。用于在每次成功读取数据后，累加已读取的行数和字节数到 read_progress 中。
    void progress(size_t read_rows, size_t read_bytes);

public:
    // 构造函数。
    // 接收表头（SharedHeader），初始化唯一的输出端口，并可选择是否启用自动进度上报（默认开启）。
    explicit ISource(SharedHeader header, bool enable_auto_progress = true);
    ~ISource() override;

    Status prepare() override;
    void work() override;
    void cancel(CancelReason reason) noexcept override;
    using IProcessor::cancel;

    OutputPort & getPort() { return output; }
    const OutputPort & getPort() const { return output; }

    void setStorageLimits(const std::shared_ptr<const StorageLimitsList> & storage_limits_) override;

    /// Default implementation for all the sources.
    std::optional<ReadProgress> getReadProgress() override;

    void addTotalRowsApprox(size_t value);
    void addTotalBytes(size_t value);
};

using SourcePtr = std::shared_ptr<ISource>;

}
