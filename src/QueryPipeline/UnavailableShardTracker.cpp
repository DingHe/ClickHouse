#include <QueryPipeline/UnavailableShardTracker.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_MANY_UNAVAILABLE_SHARDS;
}
// 核心上报与熔断检查方法（由 RemoteQueryExecutor::reportShardSkipped() 在确认跳过某个分片时调用）。
void UnavailableShardTracker::onShardSkipped()
{
    // 对原子计数器 unavailable_count 执行原子自增（fetch_add(1)），获取最新的已跳过分片总数 current_unavailable。
    size_t count = ++unavailable_count;
    // 绝对数量阈值检查：检查是否配置了 max_unavailable_num 限制（例如 max_unavailable_num > 0 或启用了数量检查）且 current_unavailable > max_unavailable_num。
    if (max_unavailable_num > 0 && count > max_unavailable_num)
        throw Exception(
            ErrorCodes::TOO_MANY_UNAVAILABLE_SHARDS,
            "Too many unavailable shards: {} out of {} total shards are unavailable, "
            "max_skip_unavailable_shards_num is set to {}",
            count, total_shards, max_unavailable_num);
    // 比例阈值检查：在 total_shards > 0 且配置了 max_unavailable_ratio 限制的前提下，计算当前跳过比例 static_cast<Float64>(current_unavailable) / total_shards，判断是否大于 max_unavailable_ratio。
    if (max_unavailable_ratio > 0 && total_shards > 0
        && static_cast<Float64>(count) / static_cast<Float64>(total_shards) > max_unavailable_ratio)
        throw Exception(
            ErrorCodes::TOO_MANY_UNAVAILABLE_SHARDS,
            "Too many unavailable shards: {} out of {} total shards are unavailable ({:.1f}%), "
            "max_skip_unavailable_shards_ratio is set to {}",
            count, total_shards, 100.0 * static_cast<double>(count) / static_cast<double>(total_shards), max_unavailable_ratio);
}

}
