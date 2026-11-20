#include <Processors/Sources/SourceFromSingleChunk.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Processors/Transforms/AggregatingTransform.h>

namespace DB
{

SourceFromSingleChunk::SourceFromSingleChunk(SharedHeader header, Chunk chunk_) : ISource(std::move(header)), chunk(std::move(chunk_))
{
}

// 负责将一个完整的 Block（由 SharedHeader 指向，实际包含数据和元信息）拆解并封装为符合流水线标准的 ISource 算子——将结构（Header）与纯数据（Chunk）分离，同时保留聚合计算所需的分桶与溢出元信息。
// 参数 SharedHeader data： SharedHeader 通常是 std::shared_ptr<const Block> 的别名。
// 在这里它不仅代表表头结构，实际上是一个携带了实际列数据、行数以及 BlockInfo（块级附加信息）的完整 Block 对象（例如来自老式执行接口转换或内存常量块）。
SourceFromSingleChunk::SourceFromSingleChunk(SharedHeader data) : ISource(std::make_shared<const Block>(data->cloneEmpty())), chunk(data->getColumns(), data->rows())
{
    const auto & sample = getPort().getHeader();
    bool has_aggregate_functions = false;
    for (auto & type : sample.getDataTypes())
        if (typeid_cast<const DataTypeAggregateFunction *>(type.get()))
            has_aggregate_functions = true;

    if (has_aggregate_functions)
    {
        auto info = std::make_shared<AggregatedChunkInfo>();
        info->bucket_num = data->info.bucket_num;
        info->is_overflows = data->info.is_overflows;
        info->out_of_order_buckets = data->info.out_of_order_buckets;
        chunk.getChunkInfos().add(std::move(info));
    }
}

String SourceFromSingleChunk::getName() const
{
    return "SourceFromSingleChunk";
}

Chunk SourceFromSingleChunk::generate()
{
    return std::move(chunk);
}

}
