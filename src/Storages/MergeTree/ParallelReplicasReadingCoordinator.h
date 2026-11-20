#pragma once

#include <Storages/MergeTree/RequestResponse.h>

#include <memory>
#include <mutex>
#include <set>
#include <unordered_map>
#include <unordered_set>

namespace DB
{
struct Progress;
// 进度回调函数类型，用于将各个副本初始声明中统计的预计读取总行数（total_rows_to_read）汇总上报给查询进度追踪器。
using ProgressCallback = std::function<void(const Progress & progress)>;
// 全部读取完成回调函数类型。当所有数据流的所有副本均读取完毕时触发，参数 used_replicas 传递了本次查询实际参与读取的副本编号集合。
using ReadCompletedCallback = std::function<void(const std::set<size_t> & used_replicas)>;

/// The main class to spread mark ranges across replicas dynamically
// 在 ClickHouse 的多副本并行读取（Parallel Replicas）架构中，
// 是运行在查询发起节点（Initiator）上的核心状态机与动态任务调度中枢。
// 通过网络协议与同一分片内的多个副本节点交互，动态协调各副本对 MergeTree 数据分片（Data Parts）和标记范围（Mark Ranges）的扫描分配，从而实现高吞吐并行读取并保证数据既不重复也不遗漏。
// 动态调度与防重防漏（Mark Ranges 分发）：在多副本并行执行同一条查询时，不同副本可能持有不完全一致的 Part 集合（例如部分副本尚未完成后台 Merge，或存在复制延迟）。该协调器收集各副本的初始 Part 声明，选定一个权威的“快照副本（Snapshot Replica）”作为基准视图，然后根据副本的实时读取速度按需动态派发 Mark Ranges（默认模式），或基于一致性哈希静态切分。
// 多表/多数据流隔离管理（Per-Stream 协调）：一条 SQL 查询可能涉及多张表（如 JOIN 或 UNION ALL），每张表对应一个独立的流标识（stream_id）。协调器内部维护了 stream_to_coordinator 映射，为每个数据流分配独立的底层算法实现（ImplInterface）和权威 Part 注册表。
// 节点故障与不可用副本容错：当某个参与并行读取的副本连接失败或超时被跳过时，接收 markReplicaAsUnavailable 通知，解除对其初始声明的阻塞等待，并将本应由其处理的任务重新分配给其他健康副本。
// 全局进度汇总与读取完成通知：聚合所有副本上报的待读取总行数并触发 ProgressCallback；当所有副本均完成各自任务时，触发 ReadCompletedCallback 通知外层执行引擎。
class ParallelReplicasReadingCoordinator
{
public:
	// 前向声明的内部抽象策略接口。
	// 具体的 Mark Ranges 分配算法（如默认动态队列抢占模式 DefaultCoordinator、按主键顺序读取模式 InOrderCoordinator、逆序读取模式 ReverseOrderCoordinator 等）均继承自 ImplInterface，实现策略模式解耦。
    class ImplInterface;
	// 构造函数。传入参与本次并行读取的固定副本总数 replicas_count_，初始化常量成员 replicas_count。后续所有副本通信中的 replica_number 均必须严格小于该值（范围 [0, replicas_count - 1]）。
    explicit ParallelReplicasReadingCoordinator(size_t replicas_count_);
    ~ParallelReplicasReadingCoordinator();
	// 处理某个副本在启动扫描前发来的初始全量数据范围声明（由发起节点收到 MergeTreeAllRangesAnnouncement 网络包，或本地发起副本同步调用时触发）。
    InitialAllRangesAnnouncementResponse handleInitialAllRangesAnnouncement(InitialAllRangesAnnouncement announcement);
	// 处理某个副本在执行扫描过程中发来的下一批读取任务请求（由 RemoteQueryExecutor::processMergeTreeReadTaskRequest 或本地副本调用）。
    ParallelReadResponse handleRequest(ParallelReadRequest request);

    /// Called when some replica is unavailable and we skipped it.
    /// This is needed to "finalize" reading state e.g. spread all the marks using
    /// consistent hashing, because otherwise coordinator will continue working in
    /// "pending" state waiting for the unavailable replica to send the announcement.
	// 当连接某个副本失败或发生超时将其跳过时调用（例如在 RemoteQueryExecutor 发现副本不可用时）。
    void markReplicaAsUnavailable(size_t replica_number);

    /// needed to report total rows to read
	// 设置进度回调函数，保存在 progress_callback 中，并在创建新的 ImplInterface 时传递给底层实现以汇报预计总行数。
    void setProgressCallback(ProgressCallback callback);
	// 返回当前选定的权威快照副本编号（若尚未有任何副本发送初始声明且未手动绑定，则返回 std::nullopt）。
    std::optional<size_t> getSnapshotReplicaNum() const { return snapshot_replica_num; }

    /// The fixed number of replicas the coordinator was sized for. Replica numbers in announcements
    /// must stay below it, so a reused coordinator must keep being fed the same set of replicas.
	// 返回构造时设定的固定副本总数 replicas_count。
    size_t getReplicasCount() const { return replicas_count; }

    /// Pin the snapshot replica to a specific replica_num before any announcement arrives.
    /// Called by the initiator-local replica during pipeline build (synchronously, before any
    /// follower announcement can reach the coordinator).
	// 在任何网络声明到达之前，显式将权威快照副本锁定为指定的 replica_num。通常由发起节点本地的副本读取流水线在构建阶段同步调用（确保发起节点自身作为权威基准，避免与远端跟随副本发生竞态）。
    void setSnapshotReplicaNum(size_t replica_num);
	// 注册全部并行读取完成时的回调函数 read_completed_callback。
    void setReadCompletedCallback(ReadCompletedCallback callback);

private:
	// 检查当前协调器管理的所有数据流（stream_to_coordinator）是否都已经彻底完成读取。只有当所有流协调器均报告完成时才返回 true。
    bool isReadingCompleted() const;
	// 根据数据流 ID（stream_id，通常由表 UUID 或查询流标识生成）在 stream_to_coordinator 中查找已存在的底层协调器实现指针，找不到则返回空指针。
    std::shared_ptr<ImplInterface> getCoordinator(const String & stream_id) const;
	// 根据 stream_id 获取对应的协调器；若尚不存在，则根据协调模式 mode（如 Default、WithOrder、ReverseOrder）工厂实例化一个具体的 ImplInterface 子类对象，将之前暂存的 progress_callback 和 unavailable_replicas 同步给它，并存入 stream_to_coordinator。
    std::shared_ptr<ImplInterface> getOrCreateCoordinator(const String & stream_id, CoordinationMode mode);
	// 互斥锁，保护协调器内部所有状态映射表、副本集合及回调触发逻辑在多线程并发网络请求（多个 RemoteQueryExecutor 线程与本地执行线程同时调用）下的线程安全。
    std::mutex mutex;
	// 常量，记录本次并行读取配置的副本总数上限。
    const size_t replicas_count{0};
	// 暂存外部传入的进度回调闭包，用于在按需创建每个表的 ImplInterface 时将其透传给底层实现。
    ProgressCallback progress_callback; // store the callback only to bypass it to coordinator implementation
	// 有序集合，记录本次查询中实际发送过初始声明并参与了并行读取的副本编号，最终在完成时传递给 read_completed_callback。
    std::set<size_t> replicas_used;
	// 存储被选定为“权威快照基准”的副本编号。其余副本的 Part 列表都会与该快照副本对齐，以确保所有副本读取的是同一逻辑时间点的数据快照。
    std::optional<size_t> snapshot_replica_num;
	// 可选的读取完成回调闭包。
    std::optional<ReadCompletedCallback> read_completed_callback;
	// 原子布尔哨兵标志，确保即使在多线程并发完成最后一个分片时，read_completed_callback 也严格只被触发执行一次。
    std::atomic_bool is_reading_completed{false};

    /// `markReplicaAsUnavailable` might be called before any coordinator is created.
    /// In this case we remember the unavailable replicas and apply when coordinators are created.
	// 记录已经标记为不可用的副本编号集合
    std::unordered_set<size_t> unavailable_replicas;

    /// Per-table coordinators. Each table gets its own ImplInterface instance.
	// 从流标识符（stream_id）到对应表级协调器实现（ImplInterface）的哈希映射表，支持单条查询中多张并行读取表的独立协调。
    std::unordered_map<String, std::shared_ptr<ImplInterface>> stream_to_coordinator;

    /// Authoritative parts for each stream, captured from the snapshot replica's announcement.
	// 从流标识符（stream_id）到权威 Part 范围描述（RangesInDataPartsDescription）的映射表，保存从快照副本的初始声明中捕获的权威数据分片列表，供后续校验和对齐其他副本的声明。
    std::unordered_map<String, RangesInDataPartsDescription> stream_to_registered_parts;
};
// 协调器的共享智能指针别名，供发起节点的本地读取算子与多个远程副本执行器（RemoteQueryExecutor）多线程共享持有。
using ParallelReplicasReadingCoordinatorPtr = std::shared_ptr<ParallelReplicasReadingCoordinator>;

}
