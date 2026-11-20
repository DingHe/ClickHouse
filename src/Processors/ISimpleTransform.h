#pragma once

#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <Processors/Port.h>

namespace DB
{

/** Has one input and one output.
  * Simply pull a block from input, transform it, and push it to output.
  */
// ISimpleTransform 是最常用的单进单出转换算子（Transform）的抽象基类。它继承自 IProcessor，专门用于处理“一对一”的数据流转换场景（即接收一个数据块，经过处理后输出一个数据块，例如 WHERE 过滤、列表达式计算、数据格式转换等）。
// ISimpleTransform 类的作用
// 单进单出拓扑抽象：强制绑定一个输入端口（input）和一个输出端口（output），封装了从输入拉取数据、调用转换逻辑、再推送到输出的标准闭环。
// 高度简化的算子开发接口：派生类无需关心复杂的底层端口状态机（prepare）和并发调度（work），只需专注于实现核心的 transform(Chunk &) 纯虚函数即可。
// 背压与流量均衡控制：内置了 set_input_not_needed_after_read 机制，防止在多线程并发下单个算子的输入端口过度囤积未处理的数据块，从而避免数据倾斜和内存膨胀。
// 异常处理与短路退出支持：原生支持透传或转换上游抛出的异常（std::exception_ptr），并允许派生类通过 stopReading() 主动提前终止数据读取。
class ISimpleTransform : public IProcessor
{
protected:
    // 引用类型，指向该转换算子唯一的输入端口，负责从上游拉取数据块（Chunk）或异常。
    InputPort & input;
    // 引用类型，指向该转换算子唯一的输出端口，负责将转换后的数据块或异常推送给下游。
    OutputPort & output;
    // 输入数据缓冲区。用于暂存从 input 端口拉取下来的原始数据单元（包含 Chunk 或 std::exception_ptr）。
    Port::Data input_data;
    // 输出数据缓冲区。用于存放经过 transform() 处理后、准备推送到 output 端口的结果数据单元。
    Port::Data output_data;
    // 布尔标志，指示 input_data 缓冲区中当前是否已经持有一个拉取下来、待处理的输入数据。
    bool has_input = false;
    // 布尔标志，指示 output_data 缓冲区中当前是否已经生成了转换完毕、待推送给下游的输出数据。
    bool has_output = false;
    // 布尔标志，标记当前算子是否已经主动决定不再需要后续输入数据（例如 LIMIT 算子已收集够行数，触发短路退出）。
    bool no_more_data_needed = false;
    // 常量布尔标志，在构造时指定。如果为 true，当拉取到空数据块（0 行）或转换后产生空数据块时，框架会自动跳过它，不会触发后续处理或向输出推送空块。
    const bool skip_empty_chunks;

    /// Set input port NotNeeded after chunk was pulled.
    /// Input port will become needed again only after data was transformed.
    /// This allows to escape caching chunks in input port, which can lead to uneven data distribution.
    // 流控开关（默认为 true）。指示在每次从 input 拉取一个数据块后，是否立即将输入端口标记为 NotNeeded，
    // 直到当前数据块在 work() 中被彻底转换完后才重新恢复需求。这可以防止上游线程在多个并发 Worker 之间产生数据积压（缓存不均）。
    bool set_input_not_needed_after_read = true;
    // 双参数转换虚函数。允许子类在保持 input_chunk 不变（或独立构造输出）的情况下，将结果写入 output_chunk。
    virtual void transform(Chunk & input_chunk, Chunk & output_chunk)
    {
        // 先调用单参数的 transform(input_chunk) 进行原地修改，然后通过 output_chunk.swap(input_chunk) 将结果交换到输出块中。
        transform(input_chunk);
        output_chunk.swap(input_chunk);
    }

    virtual bool needInputData() const { return true; }
    void stopReading() { no_more_data_needed = true; }

public:
    ISimpleTransform(Block input_header_, Block output_header_, bool skip_empty_chunks_);
    ISimpleTransform(SharedHeader input_header_, SharedHeader output_header_, bool skip_empty_chunks_);
    // 核心纯虚函数。所有继承 ISimpleTransform 的具体算子必须实现此方法，用于对传入的 Chunk 进行原地（In-place）的数据计算、列增删或过滤变换。
    virtual void transform(Chunk &) = 0;
    // 异常转换虚函数。当上游通过端口传来异常（而非正常数据块）时，框架会调用此方法。默认实现为空（即原样透传异常给下游），子类可重写以拦截、包装或忽略特定异常。
    virtual void transform(std::exception_ptr &) {}

    Status prepare() override;
    void work() override;

    virtual void onFinish() {}

    InputPort & getInputPort() { return input; }
    OutputPort & getOutputPort() { return output; }

    void setInputNotNeededAfterRead(bool value) { set_input_not_needed_after_read = value; }
};

}
