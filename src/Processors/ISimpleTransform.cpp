#include <Processors/ISimpleTransform.h>


namespace DB
{

ISimpleTransform::ISimpleTransform(Block input_header_, Block output_header_, bool skip_empty_chunks_)
    : IProcessor({std::move(input_header_)}, {std::move(output_header_)})
    , input(inputs.front())
    , output(outputs.front())
    , skip_empty_chunks(skip_empty_chunks_)
{
}

ISimpleTransform::ISimpleTransform(SharedHeader input_header_, SharedHeader output_header_, bool skip_empty_chunks_)
    : IProcessor({std::move(input_header_)}, {std::move(output_header_)})
    , input(inputs.front())
    , output(outputs.front())
    , skip_empty_chunks(skip_empty_chunks_)
{
}
// 单进单出转换算子的核心状态机调度方法。
// 负责协调下游输出端口与上游输入端口的状态，完成数据的推（Push）、拉（Pull）、背压传递（Backpressure）以及短路退出控制，最终向调度器返回当前算子的下一步动作状态。
ISimpleTransform::Status ISimpleTransform::prepare()
{
    /// Check can output.
    // 1. 检查输出端状态与背压传递
    // 检查下游输出端口是否已经关闭（例如下游的 LIMIT 算子已经收够行数，主动断开了连接）。
    if (output.isFinished())
    {
        // 既然下游不再需要数据，当前算子也立即关闭自己的输入端口，将“停止信号”继续向上游传递。
        input.close();
        onFinish();
        return Status::Finished;
    }
    // 检查输出端口当前是否不可写（即下游还没把上次推送的数据拉走，或者下游暂时不需要数据）。
    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    /// Output if has data.
    // 2. 推送已转换好的输出数据
    // 检查本地输出缓冲区 output_data 中是否存有上次 work() 转换完成的结果。
    if (has_output)
    {
        // 通过移动语义将数据单元（Chunk 或异常）无锁推送到输出端口。
        output.pushData(std::move(output_data));
        has_output = false;

        if (!no_more_data_needed)
            return Status::PortFull;

    }

    /// Stop if don't need more data.
    // 3. 主动短路退出检查
    // 响应子类的主动终止请求
    if (no_more_data_needed)
    {
        input.close();
        output.finish();
        return Status::Finished;
    }

    /// Check can input.
    // 4. 拉取新的输入数据
    if (!has_input)
    {
        // 处理上游自然结束（EOF）
        if (input.isFinished())
        {
            output.finish();
            onFinish();
            return Status::Finished;
        }
        // 申明数据需求并等待上游数据
        input.setNeeded();

        if (!input.hasData())
            return Status::NeedData;
        // 从输入端口拉取数据单元存入 input_data
        input_data = input.pullData(set_input_not_needed_after_read);
        has_input = true;

        if (input_data.exception)
            /// No more data needed. Exception will be thrown (or swallowed) later.
            input.setNotNeeded();
    }

    /// Now transform.
    return Status::Ready;
}
// 单进单出转换算子的核心计算执行方法。当 prepare() 确认输入数据已就绪且输出端口有空闲位置，并返回 Status::Ready 后，流水线调度器会分配工作线程调用该方法。它负责处理上游传来的异常或数据块，执行具体的转换逻辑（transform），并管理输出状态与空块规范化。
void ISimpleTransform::work()
{
    // 1. 透传或转换上游异常
    if (input_data.exception)
    {
        transform(input_data.exception);

        output_data = std::move(input_data);
        has_input = false;
        has_output = true;
        return;
    }
    // 2. 执行核心数据块转换与异常捕获
    try
    {
        transform(input_data.chunk, output_data.chunk);
    }
    catch (DB::Exception &)
    {
        output_data.exception = std::current_exception();
        has_output = true;
        has_input = false;
        return;
    }

    has_input = !needInputData();

    if (!skip_empty_chunks || output_data.chunk)
        has_output = true;

    if (has_output && !output_data.chunk && !getOutputPort().getHeader().empty())
        /// Support invariant that chunks must have the same number of columns as header.
        output_data.chunk = Chunk(getOutputPort().getHeader().cloneEmpty().getColumns(), 0);
}

}
