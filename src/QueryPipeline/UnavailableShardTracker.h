#pragma once

#include <atomic>
#include <memory>

#include <base/types.h>

namespace DB
{

/// Tracks the number of unavailable shards that were skipped during distributed query execution.
/// Shared across all RemoteQueryExecutor instances for a single query.
/// If the number or ratio of skipped shards exceeds the configured thresholds, throws an exception.
// 跨分片共享的不可用计数器：在一次涉及多个分片的分布式查询中，每个分片都会创建一个独立的 RemoteQueryExecutor 实例（通常在不同工作线程上并发建立连接或读取数据）。
// UnavailableShardTracker 通过 std::shared_ptr（即 UnavailableShardTrackerPtr）被所有 RemoteQueryExecutor 共同持有，实现全局状态的跨线程共享。
// 跳过分片的阈值熔断控制：在启用 skip_unavailable_shards 时，虽然允许个别故障分片被跳过以保证查询可用性，但如果不可用分片的绝对数量或占比过高，结果集将失去业务可信度。该类负责实时评估这两个指标，一旦突破配置的上限阈值，立即抛出 TOO_MANY_UNAVAILABLE_SHARDS 异常中断查询。
struct UnavailableShardTracker
{
    // 原子无锁计数器，初始值为 0。用于线程安全地累计当前分布式查询中已经被判定为不可用并跳过的分片总数。
    // 由于多个 RemoteQueryExecutor 可能在不同工作线程上同时检测到连接超时或节点异常并上报跳过，使用 std::atomic 保证了计数的并发安全与高性能。
    std::atomic<size_t> unavailable_count{0};
    // 参与本次分布式查询的总分片数量。作为计算不可用分片比例（unavailable_count / total_shards）时的分母基准。
    size_t total_shards;
    // 允许跳过的不可用分片最大绝对数量阈值（对应查询配置项 max_skip_unavailable_shards_num）。当跳过分片数超过此值时触发异常。
    size_t max_unavailable_num;
    // 允许跳过的不可用分片最大比例阈值（双精度浮点数，取值范围通常在 0.0 到 1.0 之间，对应查询配置项 max_skip_unavailable_shards_ratio）。当跳过分片数占 total_shards 的比例超过此值时触发异常。
    Float64 max_unavailable_ratio;

    UnavailableShardTracker(size_t total_shards_, size_t max_num_, Float64 max_ratio_)
        : total_shards(total_shards_)
        , max_unavailable_num(max_num_)
        , max_unavailable_ratio(max_ratio_)
    {
    }

    /// Called when a shard is determined to be unavailable and would be skipped.
    /// Throws if the configured thresholds are exceeded.
    void onShardSkipped();
};

using UnavailableShardTrackerPtr = std::shared_ptr<UnavailableShardTracker>;

}
