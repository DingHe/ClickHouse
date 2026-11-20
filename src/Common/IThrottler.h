#pragma once

#include <Poco/Net/Throttler.h>
#include <base/types.h>
#include <boost/core/noncopyable.hpp>
#include <memory>

namespace DB
{

/// Interface for throttling operations, allowing to limit the speed of operations in tokens per second.
/// Tokens are usually refer to bytes, but can be any unit of work.
// 是所有速率限制器（限流器）的统一抽象基类接口。它为分布式网络读写（如 RemoteQueryExecutor）、本地磁盘扫描、副本数据拉取以及备份恢复任务提供一致的流量整形与速率控制抽象。
// 统一的速率限制抽象接口：以“令牌/秒（tokens per second）”作为度量单位对操作速率进行上限控制。在 ClickHouse 中，一个“令牌（token）”通常代表 1 字节（Byte） 的网络或磁盘数据传输量，也可以用于限制每秒请求数或行数等任意工作单元。
//  无缝桥接底层网络库（Poco::Net）：公有继承自 Poco::Net::Throttler，使得 ClickHouse 自定义的令牌桶限流器可以直接传递给底层的 Poco HTTP/TCP 套接字缓冲区（如 ReadBufferFromHTTP、WriteBufferFromHTTP）作为速率控制回调，无需额外的适配层。
// Poco::Net::Throttler  提供最基础的限流执行虚接口
// 提供最基础的限流执行虚接口 显式删除拷贝构造函数和拷贝赋值运算符，防止限流器内部的原子计数器和时间戳状态被意外复制。
class IThrottler : public Poco::Net::Throttler, public boost::noncopyable
{
public:
    /// Is throttler already accumulated some sleep time and throttling.
    // 查询当前限流器是否已经实际触发了限流行为
    // 即是否已经累计产生了休眠等待时间，正在主动压制吞吐速率
    virtual bool isThrottling() const = 0;

    /// Returns the number of tokens available for use.
    /// NOTE: it might refill the bucket state, that is why it is not const.
    // 查询当前令牌桶（Token Bucket）中立即可用的剩余令牌数量（返回值为有符号 64 位整数 Int64，若处于透支休眠欠账状态则可能为负数或 0）。
    virtual Int64 getAvailable() = 0;

    /// Returns the maximum speed in tokens per second.
    // 获取当前限流器配置的最大允许平均速率（单位：令牌/秒，例如配置 max_network_bandwidth = 104857600 时返回 104857600 字节/秒）。
    virtual UInt64 getMaxSpeed() const = 0;

    /// Returns the maximum burst size in tokens.
    // 获取令牌桶的最大突发容量上限（单位：令牌数）。
    virtual UInt64 getMaxBurst() const = 0;
};

using ThrottlerPtr = std::shared_ptr<IThrottler>;

}
