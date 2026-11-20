#pragma once

#if defined(OS_LINUX) || defined(OS_DARWIN)

#include <atomic>
#include <Common/StackfulCoroutine.h>
#include <Common/TimerDescriptor.h>
#include <Common/Epoll.h>
#include <Common/AsyncTaskExecutor.h>
#include <Client/Connection.h>
#include <Client/IConnections.h>
#include <Poco/Timespan.h>

// RemoteQueryExecutorReadContext 继承自 AsyncTaskExecutor，是将同步网络协议解析代码转化为非阻塞事件驱动模型的核心上下文类。它仅在支持 epoll 或 kqueue（通过 <Common/Epoll.h> 兼容层）的 Linux 和 macOS 平台上启用（其他平台提供空的降级存根）。
// RemoteQueryExecutorReadContext 类的作用
// 有栈协程与多路复用 I/O 桥接器：它内部维护了一个独立的局部 Epoll 实例，将底层网络连接 Socket（connection_fd）、定时器（timer）以及用于外部取消唤醒的管道（pipe_fd）统一聚合到一个单一的文件描述符（epoll.getFileDescriptor()）中，暴露给外层的 RemoteSource 和 PipelineExecutor 监听。
// 非阻塞数据包读取状态机：支持以同步代码逻辑在协程内发送查询、读取 ClickHouse 原生协议数据包（Packet）。当遇到网络 EAGAIN 时，协程自动挂起；支持分阶段读取（先读包类型 Type，再读包体 Body），以便在收到取消请求或特殊包时快速响应。
// 超时与并发取消安全唤醒：通过 TimerDescriptor 精确控制连接、发送、接收三类 Socket 超时；通过匿名管道 pipe_fd 实现多线程环境下的即时取消唤醒——当查询被取消时，向 pipe_fd 写入数据即可立即唤醒正在 epoll 上阻塞等待的调度器，并协同协程完成安全排空或快速终止。
namespace Poco::Net
{
class Socket;
}

namespace DB
{

class MultiplexedConnections;
class RemoteQueryExecutor;

class RemoteQueryExecutorReadContext : public AsyncTaskExecutor
{
public:
    explicit RemoteQueryExecutorReadContext(
        RemoteQueryExecutor & executor_, bool suspend_when_query_sent_, bool read_packet_type_separately_);

    ~RemoteQueryExecutorReadContext() override;

    /// returns true if packet or packet has been read
    /// packet type is read separately if read_packet_type_separately is true
    // 核心读取驱动入口（由 RemoteQueryExecutor::sendQueryAsync() 和 readAsync() 调用）
    // 内部调用基类 resume() 恢复协程执行；当协程挂起或完成返回后，检查是否成功读到了预期的部分（包类型或完整数据包）。
    // 若读到数据（或读取已彻底结束）则返回 true；若因等待网络 I/O 而挂起（is_in_progress 为真）则返回 false。
    bool read();
    // 返回原子标志 is_in_progress，表示最近一次 read() 尝试尚未收到完整数据，当前正处于等待网络事件（挂起）的进行中状态。
    bool isInProgress() const { return is_in_progress.load(std::memory_order_relaxed); }

    /// Cancel without waiting for the packet that is already in flight. Only valid when the
    /// connection will be disconnected rather than returned to the pool, since it leaves the
    /// socket part-way through a packet.
    // 将原子标志 skip_drain_on_cancel 置为 true。
    // 通知协程在收到取消指令时，无需等待并排空正在传输途中的半截数据包。注意：仅当连接随后会被直接物理断开而非归还连接池时才允许调用，否则会导致 Socket 流处于半包未同步状态。
    void skipDrainOnCancel() { skip_drain_on_cancel.store(true, std::memory_order_relaxed); }
    // 检查当前读取上下文是否已被取消
    bool isCancelled() const { return AsyncTaskExecutor::isCancelled() || is_pipe_alarmed; }
    // 判断异步查询请求是否已经成功发送到远端节点。
    bool isQuerySent() const { return is_query_sent;  }
    // 返回内部 epoll 实例的文件描述符（epoll.getFileDescriptor()）。外层 RemoteSource::schedule() 会将该 FD 返回给流水线调度器进行事件监听。
    int getFileDescriptor() const { return epoll.getFileDescriptor(); }
    // 移出并返回已经完整读取到的 Packet 对象，同时将 has_read_packet_part 状态重置为 PacketPart::None，为读取下一个数据包做准备。
    Packet getPacket();
    // 返回当前已读取到的数据包类型标识（packet.type）。通常在开启 read_packet_type_separately 且 hasReadTillPacketType() 为真时调用，用于提前决策包体处理方式。
    UInt64 getPacketType() const;
    // 判断当前是否刚好读取完数据包的类型头（has_read_packet_part == PacketPart::Type），包体尚未读取。
    bool hasReadTillPacketType() const { return has_read_packet_part == PacketPart::Type; }
    // 判断当前是否已经完整读取了整个数据包（has_read_packet_part == PacketPart::Body）。
    bool hasReadPacket() const { return has_read_packet_part == PacketPart::Body; }
    // 返回配置标志 read_packet_type_separately，指示是否启用了“包类型与包体分离读取”模式。
    bool readPacketTypeSeparately() const { return read_packet_type_separately; }

private:
    // 检查定时器 timer 是否超时
    bool checkTimeout(bool blocking = false);
    // 在每次 resume() 恢复协程前调用：检查当前是否已取消，或调用 checkTimeout() 检查在挂起等待期间是否已触发 Socket 超时，以及检查 epoll 就绪事件（更新 is_timer_alarmed 或 is_pipe_alarmed）。
    bool checkBeforeTaskResume() override;
    void afterTaskResume() override {}

    void processAsyncEvent(int fd, Poco::Timespan socket_timeout, AsyncEventTimeoutType type, const std::string & description, uint32_t events) override;
    void clearAsyncEvent() override;

    void cancelBefore() override;
    // 引用指向外层的 RemoteQueryExecutorReadContext 实例，以便在协程内读写共享状态和调用 executor 的连接方法。
    struct Task : public AsyncTask
    {
        explicit Task(RemoteQueryExecutorReadContext & read_context_) : read_context(read_context_) {}

        RemoteQueryExecutorReadContext & read_context;

        void run(AsyncCallback async_callback, SuspendCallback suspend_callback) override;
    };

    /// true if no data has been received on the latest attempt to read
    // 标记当前是否正处于异步网络等待中（即上次 resume() 是因为遇到 EAGAIN 而挂起，尚未收到新数据）
    std::atomic_bool is_in_progress = false;
    // 控制取消时是否跳过残留数据包的排空。
    std::atomic_bool skip_drain_on_cancel = false;
    // 数据包读取阶段枚举。状态流转路径为：分离模式下 None -> Type -> Body -> None；非分离模式下 None -> Body -> None。
    enum class PacketPart : uint8_t
    {
        None = 0,
        Type = 1,
        Body = 2
    };
    /// depending on read_packet_type_separately, possible value transitions are
    /// None -> Type -> Body -> None
    /// None -> Body -> None
    // 记录当前 packet 缓冲区已经读取到了哪个阶段。
    std::atomic<PacketPart> has_read_packet_part = PacketPart::None;
    // 数据包缓冲区，存放正在接收或已接收完成的远程协议数据包。
    Packet packet;
    // 引用指向所属的远程查询执行器，通过它访问底层连接集合 connections、查询语句及配置参数。
    RemoteQueryExecutor & executor;

    /// Here we have three descriptors we are going to wait:
    /// * connection_fd is a descriptor of connection. It may be changed in case of reading from several replicas.
    /// * timer is a timerfd descriptor to manually check socket timeout
    /// * pipe_fd is a pipe we use to cancel query and socket polling by executor.
    /// We put those descriptors into our own epoll which is used by external executor.
    // 定时器文件描述符封装（Linux 下为 timerfd，macOS 下为 kqueue 定时器），已加入内部 epoll，用于在异步等待 Socket 时提供非阻塞的超时触发能力。
    TimerDescriptor timer;
    // 当前异步等待操作的超时时间阈值。
    Poco::Timespan timeout;
    // 当前等待操作的超时阶段类型（CONNECT、RECEIVE 或 SEND），用于超时报错分类。
    AsyncEventTimeoutType timeout_type{};
    // 记录 timer 定时器是否已经触发到期事件。
    std::atomic_bool is_timer_alarmed = false;
    // 当前正在等待网络 I/O 的远端连接 Socket 文件描述符（在多副本轮询场景下可能会动态切换）。
    int connection_fd = -1;
    // 匿名管道的一对文件描述符（pipe_fd[0] 为读端，注册在 epoll 中；pipe_fd[1] 为写端）。用于在多线程取消查询时，通过向 pipe_fd[1] 写入数据打破 epoll 的阻塞等待。
    int pipe_fd[2] = { -1, -1 };
    // 标记取消管道 pipe_fd[0] 是否已收到写入信号（即外部触发了取消）。
    std::atomic_bool is_pipe_alarmed = false;
    // 局部事件多路复用器实例，统一聚合监听 connection_fd、timer 和 pipe_fd[0] 三个事件源。
    Epoll epoll;
    // 当前 connection_fd 对应的远端节点地址/连接描述字符串，用于超时或网络异常时的错误日志打印。
    std::string connection_fd_description;
    // 指示在协程内发送完查询请求后是否立即主动挂起一次（用于 sendQueryAsync() 与 readAsync() 分离调度的场景）。
    bool suspend_when_query_sent = false;
    // 记录查询请求是否已经在协程内发送完毕。
    bool is_query_sent = false;
    // 指示是否将数据包类型的读取与包体的读取拆分为两次独立的挂起恢复周期。
    const bool read_packet_type_separately = false;
};

}

#else

namespace DB
{
class RemoteQueryExecutorReadContext
{
public:
    void cancel() {}
    void setTimer() {}
    void skipDrainOnCancel() {}
};

}
#endif
