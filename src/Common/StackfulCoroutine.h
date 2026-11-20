#pragma once
/// BOOST_USE_ASAN, BOOST_USE_MSAN, BOOST_USE_TSAN and BOOST_USE_UCONTEXT are defined via CMake for sanitizer builds.
#include <base/defines.h>
#include <boost/context/fiber.hpp>

#include <Common/Exception.h>
#include <Common/FiberLocal.h>
#include <Common/SilkFiberScheduler.h>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Class wrapper for boost::context::fiber.
/// It tracks current executing coroutine for thread and
/// supports storing coroutine-specific data
/// that will be destroyed on coroutine destructor.
// boost::context::fiber 是 C++ 中一个非常重要的底层协程（Coroutine）实现。它允许你在一个操作系统线程中，创建多个拥有独立调用栈的执行流程，并且可以在任意合适的位置暂停和恢复执行。
// 它最核心的能力是：
// 保存当前函数的执行现场，切换到另一个执行现场，之后还能从原来的位置继续执行。
// StackfulCoroutine 是对底层 boost::context::fiber（有栈纤程/协程） 的安全封装类。它为上层的 AsyncTaskExecutor 提供了独立执行栈切换、协程级局部存储（Fiber-Local Storage）隔离以及安全的栈展开销毁机制。
// StackfulCoroutine 类的作用
// 有栈协程上下文切换封装：基于 boost::context::fiber，实现调用线程与协程独立栈之间的寄存器与指令指针切换（resume 与挂起回调 SuspendCallback），支持任意深度的函数调用栈挂起与恢复。
// 协程局部存储（FLS）隔离与切换：内置 FiberLocalStorage::Holder，在进入协程前和退出（挂起/结束）协程后自动交换当前线程的协程局部存储上下文，避免多个协程复用同一个 OS 线程时发生局部状态污染。
// 当前执行协程追踪与嵌套支持：通过线程局部引用 getCurrentCoroutine() 维护当前正在执行的协程指针，并在 resume() 和 unwind() 时保存并恢复 parent_coroutine，支持协程间的层级嵌套调用追踪。
// 安全的栈展开（Unwind）销毁保障：当一个处于挂起状态的协程被销毁时，保证在其局部变量析构（栈展开）期间，coroutine_locals 和 current_coroutine 依然处于活跃且正确绑定的状态，防止析构函数访问已释放的局部存储引发崩溃。
//
class StackfulCoroutine
{
private:
    // 底层有栈协程实现类型别名，封装了 Boost.Context 的单向续体（one-shot continuation）纤程对象。
    using Impl = boost::context::fiber;
    // 指向 StackfulCoroutine 实例的裸指针别名，用于追踪线程当前活跃的协程。
    using CoroutinePtr = StackfulCoroutine *;

public:
    template <typename StackAlloc, typename Fn>
    StackfulCoroutine(StackAlloc && salloc, Fn && fn)
        : impl(std::allocator_arg_t(), std::forward<StackAlloc>(salloc), RoutineImpl<Fn>(std::forward<Fn>(fn)))
        , coroutine_locals(FiberLocalStorage::create())
    {
        if (Silk::isInsideFiber())
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Stackful coroutines cannot be created inside silk fibers");
    }

    StackfulCoroutine() = default;

    ~StackfulCoroutine()
    {
        unwind();
    }

    StackfulCoroutine(StackfulCoroutine && other) = default;

    StackfulCoroutine & operator=(StackfulCoroutine && other) noexcept
    {
        if (this != &other)
        {
            unwind();
            impl = std::move(other.impl);
            coroutine_locals = std::move(other.coroutine_locals);
        }
        return *this;
    }

    StackfulCoroutine(const StackfulCoroutine &) = delete;
    StackfulCoroutine & operator =(const StackfulCoroutine &) = delete;

    explicit operator bool() const
    {
        return impl.operator bool();
    }

    void resume()
    {
        /// Update information about current executing coroutine.
        CoroutinePtr & current_coroutine = getCurrentCoroutine();
        CoroutinePtr parent_coroutine = current_coroutine;
        current_coroutine = this;
        FiberLocalStorage::swapCoroutineLocal(*coroutine_locals);
        impl = std::move(impl).resume();
        FiberLocalStorage::swapCoroutineLocal(*coroutine_locals);
        /// Restore parent coroutine.
        current_coroutine = parent_coroutine;
    }

    /// Defined in `StackfulCoroutine.cpp`: a static local in a header-defined function gives every
    /// shared object its own copy.
    static CoroutinePtr & getCurrentCoroutine();

private:
    template <typename Fn>
    struct RoutineImpl
    {
        struct SuspendCallback
        {
            Impl & impl;

            void operator()()
            {
                impl = std::move(impl).resume();
            }
        };

        explicit RoutineImpl(Fn && fn_) : fn(std::move(fn_))
        {
        }

        Impl operator()(Impl && sink)
        {
            SuspendCallback suspend_callback{sink};
            fn(suspend_callback);
            return std::move(sink);
        }

        Fn fn;
    };

    Impl && release()
    {
        return std::move(impl);
    }

    /// Destroying a coroutine that is suspended unwinds its stack: Called from the destructor body, while coroutine_locals is still alive.
    void unwind() noexcept
    {
        if (!impl)
            return;

        CoroutinePtr & current_coroutine = getCurrentCoroutine();
        CoroutinePtr parent_coroutine = current_coroutine;
        current_coroutine = this;
        FiberLocalStorage::swapCoroutineLocal(*coroutine_locals);
        {
            Impl to_destroy = std::move(impl);
        }
        FiberLocalStorage::swapCoroutineLocal(*coroutine_locals);
        current_coroutine = parent_coroutine;
    }

    Impl impl;
    FiberLocalStorage::Holder coroutine_locals;
};
