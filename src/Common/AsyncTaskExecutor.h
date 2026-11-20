#pragma once

#include <atomic>
#include <mutex>
#include <base/types.h>
#include <Common/Epoll.h>
#include <Common/StackfulCoroutine.h>
#include <Common/CoroutineStack.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Poco/Timespan.h>

#if defined(OS_LINUX)
#include <sys/epoll.h>
#endif

// AsyncTaskExecutor.h 定义了基于有栈协程（Stackful Coroutine）执行异步任务的基础设施
// 使得原本采用同步阻塞写法编写的底层网络或 I/O 逻辑（如从 Socket 读取数据包），无需改写成碎片化的状态机回调，即可在遇到 I/O 阻塞时自动挂起协程、让出工作线程，并在事件就绪后恢复执行。
namespace DB
{
// 定义异步 I/O 等待事件的超时类型枚举，用于区分当前挂起是在等待哪一阶段的超时（方便在超时抛出异常时给出精准的错误提示）
enum class AsyncEventTimeoutType : uint8_t
{
    // 建立网络连接阶段的超时。
    CONNECT,
    // 从 Socket 接收数据阶段的超时
    RECEIVE,
    // 向 Socket 发送数据阶段的超时。
    SEND,
    // 无特定超时类型或不受超时限制。
    NONE,
};
// 异步事件回调函数类型。当底层任务尝试对某个文件描述符（如 Socket）执行会阻塞的操作（遇到 EAGAIN）时调用此回调。
// 参数依次为：文件描述符 fd、超时时长 timeout、超时类型 timeout_type、描述符描述信息 fd_description 以及监听的事件掩码 async_events（如可读 EPOLLIN 或可写 EPOLLOUT）。
using AsyncCallback = std::function<void(int, Poco::Timespan, AsyncEventTimeoutType, const std::string &, uint32_t)>;
// 主动挂起回调函数类型。供任务在非 I/O 阻塞场景下显式调用，以主动挂起当前协程并让出执行权。
using SuspendCallback = std::function<void()>;

// 用于记录协程调用栈层级关系的元信息结构体（支持协程嵌套场景下的栈追踪或状态管理）：
struct CoroutineInfo
{
    // 指向当前正在运行的有栈协程对象。
    const StackfulCoroutine * coroutine = nullptr;
    // 指向父级协程信息节点的指针（形成链表结构）。
    const CoroutineInfo * parent_coroutine_info = nullptr;
};

/// Base class for a task that will be executed in a coroutine.
/// It has only one method - run, that takes 2 callbacks:
/// 1) async_callback - callback that should be called when this task tries to perform
///    some operation on a file descriptor (e.g. reading from socket) that can block this task execution.
/// 2) suspend_callback - callback that can be called to suspend current coroutine execution explicitly.
// 封装了需要在有栈协程内部实际运行的业务逻辑单元。
struct AsyncTask
{
public:
    // 接收 async_callback（用于注册文件描述符 I/O 等待并触发协程挂起）和 suspend_callback（用于无条件主动挂起协程），内部编写具体的任务执行逻辑。
    virtual void run(AsyncCallback async_callback, SuspendCallback suspend_callback) = 0;
    virtual ~AsyncTask() = default;
};

/// Base class for executing tasks inside a coroutine.
// 有栈协程生命周期与调度封装：管理 StackfulCoroutine 及其独立调用栈（CoroutineStack）的创建、恢复（resume）、重启（restart）和销毁（cancel）。
// 同步代码异步化的桥梁：当 AsyncTask::run 内部触发 async_callback 时，AsyncTaskExecutor 会记录需要监听的 fd 和事件，随后挂起协程（yield），使 resume() 方法立即返回；当外层调度器（如 PipelineExecutor）通过 epoll 检测到该 fd 就绪后，再次调用 resume()，协程会从上次挂起的精确指令位置恢复继续运行。
//
class AsyncTaskExecutor
{
public:
    /// operation_name_ is used as the name of the OpenTelemetry span covering one execution of the task
    // 构造函数。接管待执行的异步任务指针 task_，保存用于 OpenTelemetry 追踪的操作名称 operation_name_，记录当前线程的追踪上下文，并初始化协程栈与协程实例。
    AsyncTaskExecutor(std::unique_ptr<AsyncTask> task_, String operation_name_);

    /// Resume task execution. This method returns when task is completed or suspended.
    // 核心调度入口，用于启动或恢复协程的执行。
    void resume();

    /// Cancel task execution. Coroutine will be destroyed even if task wasn't finished.
    // 取消异步任务执行。
    void cancel();

    /// Restart task execution. Current coroutine will be destroyed
    /// and the new one will be created with the same task.
    /// The next resume() call will start the new task from the beginning
    // 重启异步任务。销毁当前的协程实例并重新创建一个全新的协程绑定同一个 task 对象，重置状态标志，使得下一次调用 resume() 时从任务的起点重新开始执行。
    void restart();

    bool isCancelled() const { return is_cancelled; }

    virtual ~AsyncTaskExecutor() = default;

// 统一跨平台（Linux epoll 与 macOS kqueue 兼容层）的 I/O 事件标志位：
#if defined(OS_LINUX) || defined(OS_DARWIN)
    /// EPOLLIN/EPOLLOUT/EPOLLERR come from <sys/epoll.h> on Linux and from the kqueue
    /// compatibility shim in <Common/Epoll.h> on macOS, so the values match the `Epoll` flags
    /// on both platforms.
    enum Event
    {
        // 表示关注文件描述符的可读事件。
        READ = EPOLLIN,
        // 表示关注文件描述符的可写事件。
        WRITE = EPOLLOUT,
        // 表示关注文件描述符的错误事件。
        ERROR = EPOLLERR,
    };
#else
    enum Event
    {
        READ = 1,
        WRITE = 2,
        ERROR = 4,
    };
#endif

protected:
    /// Method that is called in resume() before actual coroutine resuming.
    /// If it returns false, resume() will return immediately without actual coroutine resuming.
    // 纯虚钩子方法，在 resume() 真正切换进入协程之前调用。如果返回 false，resume() 会直接提前返回而不恢复协程（例如用于检查外部是否已取消或前置条件是否满足）。
    virtual bool checkBeforeTaskResume() = 0;

    /// Method that is called in resume() after coroutine resuming (when it was finished or suspended).
    // 纯虚钩子方法，在 resume() 退出协程执行（无论是任务完成还是中途挂起）之后立即调用，供子类更新自身的状态机（例如设置状态为“正在等待网络事件”或“已就绪”）。
    virtual void afterTaskResume() = 0;

    /// Method that is called on async event (when async callback is called) before coroutine is suspended.
    // 纯虚钩子方法，当任务内部调用 async_callback 准备挂起时触发。子类在此方法中保存引发阻塞的 fd、超时设置及事件掩码，以便外层调度器将其注册到 epoll 中。
    virtual void processAsyncEvent(int fd, Poco::Timespan timeout, AsyncEventTimeoutType timeout_type, const std::string & fd_description, uint32_t async_events) = 0;

    /// Method that is called when task is resumed after it was suspended on async event.
    // 当任务因异步事件挂起后再次被 resume() 唤醒时调用，用于清理上一轮保存的 fd 和等待事件状态。
    virtual void clearAsyncEvent() = 0;

    /// Process exception caught while task execution. It's called after coroutine resume if exception happened.
    // 异常处理虚方法。如果协程内部执行任务时抛出了异常并被捕获存入 exception，在 resume() 退出协程后会调用此方法。默认实现直接重新抛出该异常，子类可重写以进行自定义封装或日志记录。
    virtual void processException(std::exception_ptr e) { std::rethrow_exception(e); }

    /// Method that is called in cancel() before coroutine destruction.
    virtual void cancelBefore() { }
    /// Method that is called in cancel() after coroutine destruction.
    virtual void cancelAfter() { }

    /// Resume coroutine explicitly without mutex locking.
    /// Can be called in cancelBefore().
    void resumeUnlocked();

private:
    struct Routine;
    // 在 coroutine_stack 上构造并初始化一个新的 StackfulCoroutine 对象。
    void createCoroutine();
    void destroyCoroutine();

    CoroutineStack coroutine_stack;
    StackfulCoroutine coroutine;
    std::mutex coroutine_lock;
    std::exception_ptr exception;

    std::atomic_bool routine_is_finished = false;
    std::atomic_bool is_cancelled = false;

    std::unique_ptr<AsyncTask> task;

    const String operation_name;

    /// Spans created inside the task belong to the query trace.
    const OpenTelemetry::TracingContextOnThread parent_trace_context;
};
// 根据具体的超时类型（CONNECT、RECEIVE、SEND 或 NONE）、设定的超时时间 timeout 以及 Socket 描述信息 socket_description，格式化生成人类可读的详细超时错误提示字符串。
String getSocketTimeoutExceededMessageByTimeoutType(AsyncEventTimeoutType type, Poco::Timespan timeout, const String & socket_description);

}
