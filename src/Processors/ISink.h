#pragma once

#include <Core/Block_fwd.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>


namespace DB
{
// ISink 是所有数据终点算子（Sinks）的抽象基类
// 专门用于充当查询执行图或数据写入链路的终点（例如向表引擎写入数据的 MergeTreeSink，或者向客户端返回结果的块输出终点）
// 数据消费者的抽象：作为查询计划的终点，ISink 负责从上游算子接收并消费数据。它拥有一个输入端口（input），但没有输出端口（outputs.empty()）。
// 生命周期管理：封装了从启动（onStart）、持续消费（consume）到结束（onFinish）的标准生命周期钩子，确保资源能够正确初始化和清理。
// 状态驱动模型：通过重写 prepare() 和 work() 方法，实现标准的单缓冲数据拉取与消费逻辑，简化了具体写入端算子的开发。
class ISink : public IProcessor
{
protected:
    // 指向该数据 sink 唯一的输入端口。上游算子会将数据推送到这个端口，供 sink 拉取并消费。
    InputPort & input;
    // 本地数据块缓冲区。用于缓存从输入端口拉取下来、正准备被 consume() 消费的当前数据块（Chunk）。
    Chunk current_chunk;
    // 指示当前缓冲区中是否已经成功拉取并持有一个待消费的 Chunk。
    bool has_input = false;
    // 用于记录生命周期钩子 onStart() 是否已经被调用过，确保在整个执行过程中 onStart() 只会被初始化调用一次。
    bool was_on_start_called = false;
    // 布尔标志，用于记录生命周期钩子 onFinish() 是否已经被调用过，确保在销毁或结束时清理逻辑只执行一次。
    bool was_on_finish_called = false;
    // 纯虚函数。
    // 派生类（如具体的存储写入 Sink）必须实现该方法。它是核心的业务逻辑入口，接收一个数据块（Chunk），并负责将其写入到对应的底层存储、文件或网络流中。
    virtual void consume(Chunk block) = 0;
    // 生命周期回调钩子。
    // 在 sink 开始消费第一个数据块之前被调用，默认实现为空。子类可以重写它来进行写前的准备工作（如打开文件、开启事务等）。
    virtual void onStart() {}
    // 生命周期回调钩子。
    // 在 sink 消费完所有数据并正常结束时被调用，默认实现为空。子类可以重写它来进行写后的清理工作（如关闭连接、提交事务、刷盘等）。
    virtual void onFinish() {}

public:
    explicit ISink(SharedHeader header);

    Status prepare() override;
    void work() override;

    InputPort & getPort() { return input; }
};

}
