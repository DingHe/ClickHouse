#pragma once

#include <base/defines.h>
#include <Common/Exception.h>
#include <Common/OvercommitTracker.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
};

/** UniqueLock provides RAII-style locking mechanism for a mutex.
 ** It's intended to be used like std::unique_lock but with TSA annotations
  */
// UniqueLock.h 定义了两个带有 Clang 线程安全分析（TSA，Thread Safety Analysis）注解的 RAII 锁包装器模板类：UniqueLock 和 LockAndOverCommitTrackerBlocker。
//

// 解决标准库 std::unique_lock 缺乏 TSA 静态检查的痛点：
// C++ 标准库的 std::unique_lock 没有标记 Clang 的线程安全分析宏（如 TSA_SCOPED_LOCKABLE、TSA_ACQUIRE、TSA_RELEASE）。
// 在 ClickHouse 中，大量成员变量使用 TSA_GUARDED_BY(mutex) 保护，如果直接使用 std::unique_lock，编译器无法在编译期验证加锁与解锁的配对关系。
// UniqueLock 对 std::unique_lock 进行薄包装，完整补齐了静态线程安全注解。
// 防重复加锁与防重复解锁检查：通过内置布尔状态 locked 显式禁止对非递归互斥锁的重复加锁或未加锁直接解锁，一旦违反立即抛出 LOGICAL_ERROR 异常。
template <typename Mutex>
class TSA_SCOPED_LOCKABLE UniqueLock
{
public:
    explicit UniqueLock(Mutex & mutex_) TSA_ACQUIRE(mutex_) : unique_lock(mutex_) { }
	// 默认析构函数（带 TSA_RELEASE() 声明在对象销毁时释放锁）
	// 当 UniqueLock 离开作用域销毁时，底层 std::unique_lock 的析构函数会自动判断若仍持有锁则解锁。
    ~UniqueLock() TSA_RELEASE() = default;
	// 显式手动加锁方法（适用于在同一个作用域内先调用 unlock() 临时释放锁后，再次重新加锁的场景）。
    void lock() TSA_ACQUIRE()
    {
        /// Don't allow recursive_mutex for now.
        if (locked)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't lock twice the same mutex");
        unique_lock.lock();
        locked = true;
    }

    void unlock() TSA_RELEASE()
    {
        if (!locked)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't unlock the mutex without locking it first");
        unique_lock.unlock();
        locked = false;
    }

    std::unique_lock<Mutex> & getUnderlyingLock() { return unique_lock; }

private:
    std::unique_lock<Mutex> unique_lock;
    bool locked = true;
};
_LIBCPP_CTAD_SUPPORTED_FOR_TYPE(UniqueLock);


// 防止持锁期间触发内存超限（OOM）导致死锁：在 ClickHouse 的内存超限追踪机制（OvercommitTracker）中，当查询线程申请内存超出软限制时，可能会挂起当前线程等待其他线程释放内存或触发查询取消检查。
// 如果此时该线程正持有某个互斥锁，而其他线程（或取消回调线程）恰好也需要获取同一个锁，就会发生死锁。
// 原子化组合加锁与阻塞屏蔽：该类将“获取互斥锁”与“在当前线程激活 OvercommitTrackerBlockerInThread（临时屏蔽内存超限等待）”绑定在同一个 RAII 作用域内，确保只要持有该锁，当前线程就不会因为内存超限检查而挂起阻塞。
// template<typename> typename TUniqueLock：模板模板参数（Template Template Parameter），表示要使用的锁包装器类型（例如传入 UniqueLock 或 std::lock_guard）。
// typename Mutex：具体的互斥锁类型（例如 std::mutex）。
// 类声明修饰符 TSA_SCOPED_LOCKABLE：声明该组合对象本身也是一个受 Clang 线程安全分析追踪的作用域锁。
template <template<typename> typename TUniqueLock, typename Mutex>
class TSA_SCOPED_LOCKABLE LockAndOverCommitTrackerBlocker
{
public:
	// 调用 TUniqueLock(mutex_) 获取并持有互斥锁，存入 lock；
    explicit LockAndOverCommitTrackerBlocker(Mutex & mutex_) TSA_ACQUIRE(mutex_) : lock(TUniqueLock(mutex_)) {}
    ~LockAndOverCommitTrackerBlocker() TSA_RELEASE() = default;

    TUniqueLock<Mutex> & getUnderlyingLock() { return lock; }

private:

    TUniqueLock<Mutex> lock;
    OvercommitTrackerBlockerInThread blocker = {};
};

}
