#include <Processors/ISource.h>
#include <QueryPipeline/StreamLocalLimits.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

ISource::~ISource() = default;

ISource::ISource(SharedHeader header, bool enable_auto_progress)
    : IProcessor({}, {std::move(header)})
    , auto_progress(enable_auto_progress)
    , output(outputs.front())
{
}

void ISource::cancel(CancelReason) noexcept
{
    bool already_cancelled = is_cancelled.exchange(true, std::memory_order_acq_rel);
    if (already_cancelled)
        return;

    onCancel();
}

// prepare() 是数据源算子（ISource）的核心状态机准备方法。
// 调度器（Pipeline Executor）会周期性地调用该方法，以检查数据源当前的运行状态，并根据返回的 Status 枚举决定 下一步是调用 work()、等待端口可用、还是结束执行。
ISource::Status ISource::prepare()
{
	// 检查数据源是否已在本地标记为结束
    if (finished)
    {
		// 关闭输出端口，通知下游不会再有新数据产生
        output.finish();
        return Status::Finished;
    }

    /// Check can output.
	// 检查输出端口的下游接收状态
    if (output.isFinished())
        return Status::Finished;

    if (!output.canPush())
        return Status::PortFull;

    if (!has_input)
        return Status::Ready;
	// 向输出端口推送缓存的数据块
    output.pushData(std::move(current_chunk));
	// 表示缓存已被清空
    has_input = false;

    if (isCancelled())
    {
        output.finish();
        return Status::Finished;
    }

    if (got_exception)
    {
        output.finish();
        return Status::Finished;
    }

    /// Now, we pushed to output, and it must be full.
    return Status::PortFull;
}

void ISource::setStorageLimits(const std::shared_ptr<const StorageLimitsList> & storage_limits_)
{
    storage_limits = storage_limits_;
}

void ISource::progress(size_t read_rows, size_t read_bytes)
{
    //std::cerr << "========= Progress " << read_rows << " from " << getName() << std::endl << StackTrace().toString() << std::endl;
    read_progress_was_set = true;
    std::lock_guard lock(read_progress_mutex);
    read_progress.read_rows += read_rows;
    read_progress.read_bytes += read_bytes;
}

std::optional<ISource::ReadProgress> ISource::getReadProgress()
{
    std::lock_guard lock(read_progress_mutex);
    if (finished && read_progress.read_bytes == 0 && read_progress.total_rows_approx == 0)
        return {};

    ReadProgressCounters res_progress;
    std::swap(read_progress, res_progress);

    if (storage_limits)
        return ReadProgress{res_progress, *storage_limits};

    static StorageLimitsList empty_limits;
    return ReadProgress{res_progress, empty_limits};
}

void ISource::addTotalRowsApprox(size_t value)
{
    std::lock_guard lock(read_progress_mutex);
    read_progress.total_rows_approx += value;
}

void ISource::addTotalBytes(size_t value)
{
    std::lock_guard lock(read_progress_mutex);
    read_progress.total_bytes += value;
}
// ISource::work() 是数据源算子（ISource）的核心执行方法。当 prepare() 返回 Status::Ready 时，调度器（Pipeline Executor）会调用该方法。
// 主要职责是：执行实际的数据生成工作（调用底层表扫描或生成逻辑）、更新读取进度、捕获异常，并处理数据源的正常或异常终止。
void ISource::work()
{
    try
    {
		// 如果当前数据源已经处于结束状态，直接返回，避免重复执行。
        if (finished)
            return;
		// 重置进度已设置的标志，准备迎接本次生成的进度统计。
        read_progress_was_set = false;
		// 调用虚函数 tryGenerate() 尝试获取下一个数据块。由于返回类型是 std::optional<Chunk>：
        if (auto chunk = tryGenerate())
        {
			// 将生成的 Chunk 移动到本地缓存 current_chunk 中。
            current_chunk.chunk = std::move(*chunk);
            if (current_chunk.chunk)
            {
                has_input = true;
                if (auto_progress && !read_progress_was_set)
                    progress(current_chunk.chunk.getNumRows(), current_chunk.chunk.bytes());
            }
        }
        else
            finished = true;

        if (finished)
            onFinish();
    }
    catch (...)
    {
        got_exception = true;

        if (!std::exchange(finished, true))
            onFinish();

        throw;
    }
}

Chunk ISource::generate()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "generate is not implemented for {}", getName());
}

std::optional<Chunk> ISource::tryGenerate()
{
    auto chunk = generate();
    if (!chunk)
        return std::nullopt;

    return chunk;
}

}
