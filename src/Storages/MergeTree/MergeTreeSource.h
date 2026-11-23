#pragma once
#include <Processors/ISource.h>

namespace DB
{

class MergeTreeSelectProcessor;
using MergeTreeSelectProcessorPtr = std::unique_ptr<MergeTreeSelectProcessor>;

struct ChunkAndProgress;
// 将 MergeTree 底层数据读取状态机接入流水线调度器的核心数据源适配器（Source Processor）
// MergeTreeSource 类的作用
// 桥接存储层与流水线执行层：它本身不直接解析 .bin/.mrk 物理文件，而是持有并驱动底层的 MergeTreeSelectProcessor（负责 Part 切换、主键索引/跳数索引过滤、PREWHERE 裁剪及实际列数据块读取），将底层产出的 ChunkAndProgress 转化为流水线标准的 Chunk 输出给下游算子。
// 向上汇报执行进度与取消通知：在每次拉取数据后，将底层统计的读取行数和字节数增量（Progress）实时累加到 ISource 的进度计数器中，并响应流水线发来的取消信号（onCancel()）。
// 向上汇报执行进度与取消通知：在每次拉取数据后，将底层统计的读取行数和字节数增量（Progress）实时累加到 ISource 的进度计数器中，并响应流水线发来的取消信号（onCancel()）。
class MergeTreeSource final : public ISource
{
public:
    explicit MergeTreeSource(MergeTreeSelectProcessorPtr processor_, const std::string & log_name_);
    ~MergeTreeSource() override;

    std::string getName() const override;

    Status prepare() override;

#if defined(OS_LINUX)
    int schedule() override;
#endif

protected:
    // 数据块生成的核心驱动方法
    std::optional<Chunk> tryGenerate() override;
    // 当查询被外部中断（如 KILL QUERY、客户端断连或同查询其他分片抛出异常）时由流水线调度器在多线程环境下安全调用
    void onCancel() noexcept override;

private:
    // 指向 MergeTreeSelectProcessor 的独占智能指针（std::unique_ptr）。
    // 是真正执行 MergeTree 读取逻辑的核心引擎，内部维护了读取任务池（IMergeTreeReadPool）、当前正在读取的 DataPart、列读取器（MergeTreeReader）以及 PREWHERE 过滤执行器。
    MergeTreeSelectProcessorPtr processor;
    // 存储当前源算子的日志与执行计划展示名称（包含所用的读取算法和池类型信息）。
    const std::string log_name;
    // 用于封装 Linux 环境下基于异步读取接口（IAsynchronousReader / AsynchronousReadIndirectBufferFromFile）的非阻塞读取上下文、异步任务 Future/句柄以及唤醒通知 fd。
    // 仅当启用异步文件读取且产生等待时按需分配和持有。
#if defined(OS_LINUX)
    struct AsyncReadingState;
    std::unique_ptr<AsyncReadingState> async_reading_state;
#endif
    // 对底层 processor->read() 返回的 ChunkAndProgress 结构体进行解包与后处理：
    Chunk processReadResult(ChunkAndProgress chunk);
};

}
