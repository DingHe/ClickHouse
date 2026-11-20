#pragma once

#include <Processors/ISource.h>


namespace DB
{
// 核心职责是将内存中已有的单个 Chunk（数据块）封装为一个标准的流水线数据源算子（Source）
// SourceFromSingleChunk 类的作用
// 内存数据块到流水线算子的适配器：当执行引擎在内存里直接生成了少量固定数据（例如常量查询 SELECT 1、子查询的预计算结果、空结果集占位、系统信息返回等），无需经过复杂的磁盘扫描或网络读取，可直接通过此类将单个 Chunk 接入 QueryPipeline 中参与后续的算子计算。
// 一次性数据输出语义：该数据源只持有一个 Chunk。在第一次被调度生成数据时，它会将内部持有的 Chunk 完整移交（std::move）给下游；在第二次调用时，由于内部数据已清空，它会直接返回一个空 Chunk，从而通知基类 ISource 数据已全部生成完毕（进入 finished 状态）。
// final 终结类设计：类声明带有 final 关键字，表明它是一个功能专一、高度内聚的终端实现类，不允许被进一步继承扩展（如果需要输出多个数据块，注释明确指出应使用 SourceFromChunks）。
class SourceFromSingleChunk final : public ISource
{
/// If the source consists of multiple chunks you can instead use SourceFromChunks.
public:
    SourceFromSingleChunk(SharedHeader header, Chunk chunk_);
    explicit SourceFromSingleChunk(SharedHeader data);
    String getName() const override;

protected:
    Chunk generate() override;

private:
    // 内部数据块缓存。用于存储初始化时传入的唯一待输出数据块（Chunk）。当 generate() 被第一次调用时，该成员变量的所有权会通过移动语义转移出去，自身变为空状态（0 行 0 列）。
    Chunk chunk;
};

}
