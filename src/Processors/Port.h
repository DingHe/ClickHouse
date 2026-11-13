#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <vector>

#include <Core/Block.h>
#include <Core/Block_fwd.h>
#include <Core/Defines.h>
#include <Processors/Chunk.h>
#include <Common/Exception.h>

namespace DB
{

class InputPort;
class OutputPort;
class IProcessor;

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}
// Port.h 定义了处理器（IProcessor）之间进行数据传输和状态交互的核心机制。它主要包含了三个核心类：Port（基类）、InputPort（输入端口）、OutputPort（输出端口），以及内部的State和UpdateInfo结构。
// Port.h 的核心作用
// 数据通道的抽象：充当算子（IProcessor）与算子之间传递数据块（Chunk）或异常（std::exception_ptr）的桥梁。
// 多线程无锁/轻量同步状态机：通过共享状态（State）和指针低位标记（Pointer Tagging），在多线程并发环境下安全地追踪端口的生命周期（如是否完成、是否需要数据、是否有数据）。
// 数据契约校验：携带统一的表头（Header），确保在数据推送（push）和拉取（pull）时，数据块的列数和结构与预期严格相符。
class Port
{
    friend void connect(OutputPort &, InputPort &, bool);
    friend void disconnect(OutputPort &, InputPort &);
    friend class IProcessor;

public:
    // 用于追踪端口状态变化的辅助结构，主要给调度器或监视器使用。
    struct UpdateInfo
    {
        // 记录发生变动的指针列表。
        using UpdateList = std::vector<void *>;

        UpdateList * update_list = nullptr;
        // 当前对象的唯一标识。
        void * id = nullptr;
        // 当前版本号。
        UInt64 version = 0;
        // 上一次同步时的版本号。
        UInt64 prev_version = 0;
        // 如果版本号等于前置版本号且存在更新列表，则将 id 推入列表，并将版本号自增。
        void inline ALWAYS_INLINE update()
        {
            if (version == prev_version && update_list)
                update_list->push_back(id);

            ++version;
        }
        // 将 prev_version 更新为当前的 version。
        void inline ALWAYS_INLINE trigger() { prev_version = version; }
    };

protected:
    /// Shared state of two connected ports.
    // 两个已连接端口（输入端和输出端）共享的状态机核心。
    class State
    {
    public:
        // 封装实际传输的数据内容。
        struct Data
        {
            /// Note: std::variant can be used. But move constructor for it can't be inlined.
            // 传输的数据块（包含多列数据）。
            Chunk chunk;
            // 如果执行中发生异常，用智能指针捕获并跨端口传递异常。
            std::exception_ptr exception;
            // 当 chunk 为空且没有异常时返回 true。
            bool isEmpty() const { return chunk.empty() && !exception; }
        };

    private:
        static std::uintptr_t getUInt(Data * data) { return reinterpret_cast<std::uintptr_t>(data); }
        static Data * getPtr(std::uintptr_t data) { return reinterpret_cast<Data *>(data); }

    public:

        /// Flags for Port state.
        /// Will store them in least pointer bits.

        /// Port was set finished or closed.
        // 端口已完成或关闭。
        static constexpr std::uintptr_t IS_FINISHED = 1;
        /// Block is not needed right now, but may be will be needed later.
        /// This allows to pause calculations if we are not sure that we need more data.
        // 下游当前需要数据（可用于暂停无用的计算）。
        static constexpr std::uintptr_t IS_NEEDED = 2;
        /// Check if port has data.
        // 端口当前包含未被消费的数据。
        static constexpr std::uintptr_t HAS_DATA = 4;
        // 低三位都是1
        static constexpr std::uintptr_t FLAGS_MASK = IS_FINISHED | IS_NEEDED | HAS_DATA;
        // 低三位都是0
        static constexpr std::uintptr_t PTR_MASK = ~FLAGS_MASK;

        /// Tiny smart ptr class for Data. Takes into account that ptr can have flags in least bits.
        // 一种微型的智能指针，专为管理 Data 设计，能够利用内存指针的低位（Least Significant Bits）存储状态标志位。
        class DataPtr
        {
        public:
            // 在堆上分配 Data，并检查内存对齐。
            DataPtr() : data(new Data())
            {
                if (unlikely((getUInt(data) & FLAGS_MASK) != 0))
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Not alignment memory for Port");
            }
            /// Pointer can store flags in case of exception in swap.
            // 析构函数，释放被遮罩掩码剥离后的有效指针。
            ~DataPtr() { delete getPtr(getUInt(data) & PTR_MASK); }

            DataPtr(DataPtr const &) : data(new Data()) {}
            DataPtr& operator=(DataPtr const &) = delete;

            Data * operator->() const { return data; }
            Data & operator*()  const { return *data; }

            Data * get() const { return data; }
            explicit operator bool() const { return data; }

            Data * release()
            {
                Data * result = nullptr;
                std::swap(result, data);
                return result;
            }
            // 实现指针标记（Pointer Tagging）与无锁原子状态切换（Lock-free Atomic State Swap）的核心底层方法。
            // 由于 ClickHouse 的端口指针（Data *）需要同时存储内存地址和状态标志位（如 IS_FINISHED, IS_NEEDED, HAS_DATA），
            // 它利用了 64 位系统指针的低位对齐空白（至少有低 3 位为 0），将标志位直接“压缩”拼接到指针的低位中。
            // 内存地址按 8 字节对齐后，低 3 位必然为 0，所以这 3 位可以拿来存储状态标志。
            // std::atomic<Data *> & value 目标原子指针变量（即共享状态 State::data）。它是多线程竞争的焦点，多个线程会通过 CAS（Compare-And-Swap）操作来争夺对它的修改权。
            // std::uintptr_t flags 期望写入的新标志位（例如 HAS_DATA 或 IS_FINISHED 等）。这些位将被合并到指针的低位中。
            // std::uintptr_t mask  掩码（Mask）。用于指示在原子交换时，需要清除或覆盖旧状态中的哪些标志位。
            // 1. 指针与状态的“打包”原子交换
            //在传统的并发编程中，更新数据指针和更新状态标志（如“是否有数据”、“是否已完成”）通常需要分别加锁。而 ClickHouse 利用 64 位系统指针低位对齐的空白，将内存地址指针（Data *）和状态标志位（Flags，如 HAS_DATA、IS_NEEDED）压缩在同一个 64 位整型指针中。swap 方法利用 CPU 的原子指令（CAS, Compare-And-Swap），能够同时原子性地修改指针和标志位。
            // 2. 实现上下游的无锁协作
            //对于上游线程（Push）：通过 swap 把装有新数据（Chunk）的指针推给原子变量，并打上 HAS_DATA 标记，通知下游“有数据了”。
            //对于下游线程（Pull）：通过 swap 把数据指针从原子变量中“交换”回本地，同时利用 mask 清除原子变量中的 HAS_DATA 标记，表示“数据已被消费”。

            uintptr_t ALWAYS_INLINE swap(std::atomic<Data *> & value, std::uintptr_t flags, std::uintptr_t mask) /// NOLINT
            {
                Data * expected = nullptr;
                // getUInt(data)：获取当前本地 DataPtr 对象内部指针的整数值（其低位可能带有旧的标志位）。
                // flags | ...：通过按位或操作，将传入的新标志位合并进去。
                // getPtr(...)：将合并后的整数重新转回 Data * 指针类型。
                Data * desired = getPtr(flags | getUInt(data));
                // 核心的 CAS 无锁循环
                // 尝试原子性地将 value 的值从 expected 替换为 desired。
                // 如果不匹配（说明在极短时间内有其他线程并发修改了 value），CAS 失败，compare_exchange_weak 会自动把 value 当前的最新值写回到 expected 中，并进入下一次循环重试。
                while (!value.compare_exchange_weak(expected, desired))
                    // getUInt(expected) & FLAGS_MASK：从最新失败的 expected 中提取出当前的标志位。
                    // & (~mask)：通过掩码的反码，清除那些需要被重置或消耗掉的旧标志位（例如拉取数据时清除 HAS_DATA）。
                    // | flags：赋予新操作要求的标志位。
                    // | getUInt(data)：保留本地对象的指针部分。重新计算出最新的 desired，准备进入下一轮 CAS 竞争。
                    desired = getPtr((getUInt(expected) & FLAGS_MASK & (~mask)) | flags | getUInt(data));

                /// It's not very safe. In case of exception after exchange and before assignment we will get leak.
                /// Don't know how to make it better.
                // 更新本地数据指针
                data = getPtr(getUInt(expected) & PTR_MASK);
                // 返回旧状态标志位
                return getUInt(expected) & FLAGS_MASK;
            }

        private:
            Data * data = nullptr;
        };

        /// Not finished, not needed, has not data.
        State() : data(new Data())
        {
            if (unlikely((getUInt(data) & FLAGS_MASK) != 0))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Not alignment memory for Port");
        }

        ~State()
        {
            Data * desired = nullptr;
            Data * expected = nullptr;

            while (!data.compare_exchange_weak(expected, desired));

            expected = getPtr(getUInt(expected) & PTR_MASK);
            delete expected;
        }
        // 上游算子（通过 OutputPort）向端口推送数据块（Chunk）或异常的核心无锁方法。它负责将本地准备好的数据安全地写入共享状态中，并更新状态标志位。
        // std::uintptr_t & flags 传出参数。用于返回执行原子交换瞬间，端口旧状态中所包含的状态标志位集合（如 HAS_DATA, IS_NEEDED, IS_FINISHED 等），供调用方检查。
        void ALWAYS_INLINE push(DataPtr & data_, std::uintptr_t & flags)
        {
            // 核心原子交换与状态更新
            // 调用本地 DataPtr 对象的 swap 方法，与共享状态的原子变量 data 进行无锁交互。
            flags = data_.swap(data, HAS_DATA, HAS_DATA);

            /// It's possible to push data into finished port. Will just ignore it.
            /// if (flags & IS_FINISHED)
            ///    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot push block to finished port.");

            /// It's possible to push data into port which is not needed now.
            /// if ((flags & IS_NEEDED) == 0)
            ///    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot push block to port which is not needed.");

            if (unlikely(flags & HAS_DATA))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot push block to port which already has data");
        }
        // 底层状态机中用于从端口拉取数据块（Chunk）的核心同步方法
        // 运行在多线程并发环境中，负责原子性地获取数据并修改端口状态。
        // ALWAYS_INLINE 编译器宏指令，强制内联该函数。因为 pull 是查询执行路径中的高频热点调用，内联可以消除函数调用的开销，提升执行效率。
        // DataPtr & data_  传入并传出参数。它是内部的本地数据指针（持有 Chunk 或异常）。通过引用传入后，会在函数内部与共享状态（原子变量）交换数据指针。
        // std::uintptr_t & flags  传出参数。用于返回操作发生前（或交换时）端口的状态标志位（如 HAS_DATA, IS_NEEDED, IS_FINISHED 等），供调用方（如 InputPort）检查并做进一步逻辑判断（例如判断上游是否已经结束 IS_FINISHED）。
        // bool set_not_needed = false  控制标志。默认为 false。如果传入 true，表示在拉取数据的同时，顺便将端口标记为“不再需要数据（Not Needed）”（常用于 LIMIT 算子已经满足行数要求、准备提前终止读取的场景）。
        void ALWAYS_INLINE pull(DataPtr & data_, std::uintptr_t & flags, bool set_not_needed = false)
        {
            // 默认包含 HAS_DATA，因为拉取数据的前提是端口有数据，拉取后需要清除“有数据”的标记。
            uintptr_t mask = HAS_DATA;
            // 如果 set_not_needed 为 true，则通过按位或操作（|=）将 IS_NEEDED 也加入掩码中。这意味着在原子交换时，不仅要清除“有数据”的状态，还要同时清除“需要数据”的状态（将 IS_NEEDED 标志清零）。
            if (set_not_needed)
                mask |= IS_NEEDED;
            // 核心原子交换操作
            // 返回值（赋给 flags）是交换前旧状态中所包含的标志位集合。
            flags = data_.swap(data, 0, mask);

            /// It's ok to check because this flag can be changed only by pulling thread.
            // 如果在拉取时发现状态中没有 IS_NEEDED 标志（即下游之前并没有声明需要数据），并且本次调用也没有主动设置 set_not_needed，说明违背了流水线契约，直接抛出逻辑错误（LOGICAL_ERROR）。
            if (unlikely((flags & IS_NEEDED) == 0) && !set_not_needed)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot pull block from port which is not needed");
            // 检查通过 swap 拿到的旧标志位中是否包含 HAS_DATA。如果包含 0（即根本没有数据），说明在没有数据的情况下强行调用了 pull，属于严重的逻辑错误，直接抛出异常。
            if (unlikely((flags & HAS_DATA) == 0))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot pull block from port which has no data");
        }
        // 原子性地设置/修改标志位。
        std::uintptr_t ALWAYS_INLINE setFlags(std::uintptr_t flags, std::uintptr_t mask)
        {
            Data * expected = nullptr;
            Data * desired = getPtr(flags);

            while (!data.compare_exchange_weak(expected, desired))
                desired = getPtr((getUInt(expected) & FLAGS_MASK & (~mask)) | flags | (getUInt(expected) & PTR_MASK));

            return getUInt(expected) & FLAGS_MASK;
        }

        std::uintptr_t ALWAYS_INLINE getFlags() const
        {
            return getUInt(data.load()) & FLAGS_MASK;
        }

    private:
        std::atomic<Data *> data;
    };

    const SharedHeader header;
    std::shared_ptr<State> state;

    /// This object is only used for data exchange between port and shared state.
    State::DataPtr data;

    IProcessor * processor = nullptr;

    /// If update_info was set, will call update() for it in case port's state have changed.
    UpdateInfo * update_info = nullptr;

public:
    using Data = State::Data;


    Port(SharedHeader header_) : header(std::move(header_)) { } // NOLINT(google-explicit-constructor)
    Port(Block && header_) : header(std::make_shared<const Block>(std::move(header_))) { } // NOLINT(google-explicit-constructor)
    Port(const Block & header_) : header(std::make_shared<const Block>(header_)) { } // NOLINT(google-explicit-constructor)
    Port(Block header_, IProcessor * processor_) : header(std::make_shared<const Block>(std::move(header_))), processor(processor_) { }

    void setUpdateInfo(UpdateInfo * info) { update_info = info; }
    bool hasUpdateInfo() const { return update_info != nullptr; }

    const Block & getHeader() const { return *header; }
    const SharedHeader & getSharedHeader() const { return header; }
    bool ALWAYS_INLINE isConnected() const { return state != nullptr; }

    /// Identity of the shared state of two connected ports (both sides return the same value).
    /// Lets diagnostics match an output port to its peer input port without dereferencing the
    /// peer processor, whose lifetime is not guaranteed during pipeline teardown. Opaque on
    /// purpose: only pointer identity is meaningful.
    const void * getConnectionId() const { return state.get(); }
    // 在对端口执行任何依赖连接的操作（如检查数据、读写数据等）之前，强制断言当前端口必须已经与对端建立连接。如果未连接，则直接抛出严重的逻辑错误异常。
    void ALWAYS_INLINE assumeConnected() const
    {
        if (unlikely(!isConnected()))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Port is not connected");
    }

    bool ALWAYS_INLINE hasData() const
    {
        assumeConnected();
        return state->getFlags() & State::HAS_DATA;
    }

    IProcessor & getProcessor()
    {
        if (!processor)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Port does not belong to Processor");
        return *processor;
    }

    const IProcessor & getProcessor() const
    {
        if (!processor)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Port does not belong to Processor");
        return *processor;
    }

protected:
    // 当端口的状态发生实质性变动（如拉取数据、修改标志位等）时，通知外部的 UpdateInfo 追踪器更新版本号，以便调度器或调试监视器感知到端口的变化。
    // update_info：这是一个指向 UpdateInfo 结构体的指针（定义在 Port 类中）。如果当前端口配置了状态更新追踪器（例如在特定的调试、性能剖析或图调度场景下），该指针不为空；在常规运行时，它通常为 nullptr。
    void inline ALWAYS_INLINE updateVersion()
    {
        if (likely(update_info))
            // 核心作用是：当端口的状态发生改变（例如有数据拉取或状态切换）时，通知外部的更新追踪器（UpdateInfo），以便调度器或监控系统能够感知到该端口的变更。
            update_info->update();
    }

    /// For processors_profile_log
    size_t rows = 0;
    size_t bytes = 0;
};

/// Invariants:
///   * If you close port, it isFinished().
///   * If port isFinished(), you can do nothing with it.
///   * If port is not needed, you can only setNeeded() or close() it.
///   * You can pull only if port hasData().
// 代表下游处理器接收数据的端口。
class InputPort : public Port
{
    friend void connect(OutputPort &, InputPort &, bool);
    friend void disconnect(OutputPort &, InputPort &);

private:
    // 连接的对端输出端口指针。
    OutputPort * output_port = nullptr;

    mutable bool is_finished = false;

public:
    using Port::Port;
    // 下游算子（通过输入端口）从上游拉取完整数据单元（Data，包含 Chunk 或异常）的核心对外接口方法。它负责完成版本更新、连接检查、底层状态拉取、结构校验以及本地统计计数。
    // bool set_not_needed = false 控制标志参数。默认为 false。如果传入 true，表示在拉取本次数据的同时，顺便将端口标记为“不再需要数据（Not Needed）”（常用于 LIMIT 算子已经满足行数要求、准备提前终止读取的场景）。
    Data ALWAYS_INLINE pullData(bool set_not_needed = false)
    {
        // 如果 set_not_needed 为 false（即正常的、需要继续拉取数据的操作），则调用 updateVersion() 方法。
        if (!set_not_needed)
            updateVersion();
        // 连通性断言检查
        // 强制断言当前输入端口必须已经与某个输出端口建立了连接（即 state 不为空）。如果未连接就调用拉取，会直接抛出逻辑异常。
        assumeConnected();
        // 调用底层共享状态机拉取数据
        std::uintptr_t flags = 0;
        // 传入本地的 data 指针缓冲区、flags 引用以及 set_not_needed 标志。
        state->pull(data, flags, set_not_needed);
        // 更新本地完成状态
        is_finished = flags & State::IS_FINISHED;
        // 数据结构契约校验（列数匹配检查）
        if (unlikely(!data->exception && data->chunk.getNumColumns() != header->columns()))
        {
            auto & chunk = data->chunk;

            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Invalid number of columns in chunk pulled from OutputPort. Expected {}, found {}\n"
                "Header: {}\n"
                "Chunk: {}\n",
                header->columns(),
                chunk.getNumColumns(),
                header->dumpStructure(),
                chunk.dumpStructure());
        }

        rows += data->chunk.getNumRows();
        bytes += data->chunk.bytes();
        // 移动并返回数据
        return std::move(*data);
    }

    Chunk ALWAYS_INLINE pull(bool set_not_needed = false)
    {
        auto pull_data = pullData(set_not_needed);

        if (pull_data.exception)
            std::rethrow_exception(pull_data.exception);

        return std::move(pull_data.chunk);
    }

    bool ALWAYS_INLINE isFinished() const
    {
        assumeConnected();

        if (is_finished)
            return true;

        auto flags = state->getFlags();

        is_finished = (flags & State::IS_FINISHED) && ((flags & State::HAS_DATA) == 0);

        return is_finished;
    }

    void ALWAYS_INLINE setNeeded()
    {
        assumeConnected();

        if ((state->setFlags(State::IS_NEEDED, State::IS_NEEDED) & State::IS_NEEDED) == 0)
            updateVersion();
    }

    void ALWAYS_INLINE setNotNeeded()
    {
        assumeConnected();
        state->setFlags(0, State::IS_NEEDED);
    }

    void ALWAYS_INLINE close()
    {
        assumeConnected();

        if ((state->setFlags(State::IS_FINISHED, State::IS_FINISHED) & State::IS_FINISHED) == 0)
            updateVersion();

        is_finished = true;
    }

    void ALWAYS_INLINE reopen()
    {
        assumeConnected();

        if (!isFinished())
            return;

        state->setFlags(0, State::IS_FINISHED);
        is_finished = false;
    }

    OutputPort & getOutputPort()
    {
        assumeConnected();
        return *output_port;
    }

    const OutputPort & getOutputPort() const
    {
        assumeConnected();
        return *output_port;
    }
};


/// Invariants:
///   * If you finish port, it isFinished().
///   * If port isFinished(), you can do nothing with it.
///   * If port not isNeeded(), you can only finish() it.
///   * You can push only if port doesn't hasData().
// 代表上游处理器发送数据的端口。
class OutputPort : public Port
{
    friend void connect(OutputPort &, InputPort &, bool);
    friend void disconnect(OutputPort &, InputPort &);

private:
    // 连接的对端输入端口指针。
    InputPort * input_port = nullptr;

public:
    using Port::Port;
    // 推送一个常规数据块 Chunk。
    void ALWAYS_INLINE push(Chunk chunk)
    {
        pushData({.chunk = std::move(chunk), .exception = {}});
    }
    // 向下游推送一个异常对象。
    void ALWAYS_INLINE pushException(std::exception_ptr exception)
    {
        pushData({.chunk = {}, .exception = exception});
    }
    // 底层推送实现。会校验 Chunk 的列数是否与表头匹配，更新累计行数/字节数，并通过状态机将数据推给对端。
    void ALWAYS_INLINE pushData(Data data_)
    {
        if (unlikely(!data_.exception && data_.chunk.getNumColumns() != header->columns()))
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Invalid number of columns in chunk pushed to OutputPort. Expected {}, found {}\n"
                "Header: {}\n"
                "Chunk: {}\n",
                header->columns(),
                data_.chunk.getNumColumns(),
                header->dumpStructure(),
                data_.chunk.dumpStructure());
        }

        updateVersion();

        assumeConnected();

        std::uintptr_t flags = 0;
        *data = std::move(data_);

        rows += data->chunk.getNumRows();
        bytes += data->chunk.bytes();

        state->push(data, flags);
    }

    void ALWAYS_INLINE finish()
    {
        assumeConnected();

        auto flags = state->setFlags(State::IS_FINISHED, State::IS_FINISHED);

        if ((flags & State::IS_FINISHED) == 0)
            updateVersion();
    }

    bool ALWAYS_INLINE isNeeded() const
    {
        assumeConnected();
        return state->getFlags() & State::IS_NEEDED;
    }

    bool ALWAYS_INLINE isFinished() const
    {
        assumeConnected();
        return state->getFlags() & State::IS_FINISHED;
    }

    bool ALWAYS_INLINE canPush() const
    {
        assumeConnected();
        auto flags = state->getFlags();
        return (flags & State::IS_NEEDED) && ((flags & State::HAS_DATA) == 0);
    }

    InputPort & getInputPort()
    {
        assumeConnected();
        return *input_port;
    }

    const InputPort & getInputPort() const
    {
        assumeConnected();
        return *input_port;
    }
};


using InputPorts = std::list<InputPort>;
using OutputPorts = std::list<OutputPort>;


void connect(OutputPort & output, InputPort & input, bool reconnect = false);
void disconnect(OutputPort & output, InputPort & input);

}
