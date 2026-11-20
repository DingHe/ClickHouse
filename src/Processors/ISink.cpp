#include <Processors/ISink.h>
#include <Processors/Port.h>

namespace DB
{

ISink::ISink(SharedHeader header)
    : IProcessor({std::move(header)}, {}), input(inputs.front())
{
}
// 数据终点算子（ISink）的核心状态机准备方法。调度器（Pipeline Executor）会周期性地调用该方法，用来检查 ISink 当前处于什么状态，并决定下一步是调用 work() 去消费数据、请求更多数据、还是结束执行。
ISink::Status ISink::prepare()
{
    // 检查是否需要执行初始化启动逻辑
    // 如果 was_on_start_called 为 false（说明该 Sink 刚被调度，还没有执行过生命周期钩子 onStart()），则返回 Status::Ready。
    if (!was_on_start_called)
        return Status::Ready;
    // 如果 has_input 为 true（说明之前已经从输入端口拉取并缓存了一个数据块到 current_chunk 中，但还没来得及消费），则直接返回 Status::Ready，告诉调度器可以继续调用 work() 去处理这个缓存块。
    if (has_input)
        return Status::Ready;
    // 检查上游输入端口是否已经关闭或结束（即不会再有新数据输入）。
    if (input.isFinished())
    {
        // 如果 was_on_finish_called 已经是 true（说明收尾工作也做完了），则返回 Status::Finished，告知调度器该 Sink 算子正式宣告结束。
        if (!was_on_finish_called)
            return Status::Ready;

        return Status::Finished;
    }
    // 申明数据需求并检查是否有数据可读
    input.setNeeded();
    // 检查输入端口当前是否还没有数据（hasData() 返回 false）。
    // 如果没有数据可读，则返回 Status::NeedData，挂起当前算子，等待上游推送数据。
    if (!input.hasData())
        return Status::NeedData;
    // 拉取数据并准备就绪
    current_chunk = input.pull(true);
    has_input = true;
    return Status::Ready;
}

void ISink::work()
{
    if (!was_on_start_called)
    {
        was_on_start_called = true;
        onStart();
    }
    else if (has_input)
    {
        has_input = false;
        consume(std::move(current_chunk));
    }
    else if (!was_on_finish_called)
    {
        was_on_finish_called = true;
        onFinish();
    }
}

}
