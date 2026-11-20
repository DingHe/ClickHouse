#pragma once

#include <Columns/IColumn_fwd.h>
#include <Common/CollectionOfDerived.h>
#include <Core/Types_fwd.h>

#include <memory>

namespace DB
{
// Chunk.h 定义了算子（IProcessor）之间在端口（Port）中实际流转的数据载体。整个文件除了核心的 Chunk 类外，还包含了用于附加元数据的 ChunkInfo 体系以及一组列格式转换辅助函数。
// Chunk（核心数据块载体）
// 轻量级数据容器：它是一组等长列（Columns）和行数（num_rows）的集合。与传统的 Block 相比，Chunk 不存储列名、数据类型和名称索引映射（这些元数据已被抽离到端口的 Header 中），因此极其轻量，极大降低了流水线传输时的元数据拷贝与维护开销。
// 严格的仅移动语义（Move-only）：禁止隐式拷贝构造和拷贝赋值，强制通过 std::move 在算子间零拷贝转移所有权；若确实需要复制，必须显式调用 clone()。
// 灵活的空状态语义：允许“有行数但无列”（例如 SELECT count() 只需传递行数）和“有列但 0 行”（例如过滤后无匹配行但需保留列结构），只有当“0 行且 0 列”时才被视为空（empty()）。

// 元数据抽象基类
class ChunkInfo
{
public:
    // 定义智能指针类型别名。
    using Ptr = std::shared_ptr<ChunkInfo>;

    ChunkInfo() = default;
    ChunkInfo(const ChunkInfo&) = default;
    ChunkInfo(ChunkInfo&&) = default;

    virtual Ptr clone() const = 0;
    // 虚函数，定义当两个 Chunk 合并（append）时，它们各自携带的 ChunkInfo 该如何合并。默认实现直接返回右侧的 right。
    virtual Ptr merge(const Ptr & right) const
    {
        return right; // merge left into right by default returns right
    }
    virtual ~ChunkInfo() = default;
};

// CRTP 克隆模板辅助类
template <typename Derived>
class ChunkInfoCloneable : public ChunkInfo
{
    // 利用奇异递归模板模式（CRTP），防止误用，确保只有 Derived 自身能构造。
    friend Derived;

private:
    ChunkInfoCloneable() = default;
    ChunkInfoCloneable(const ChunkInfoCloneable & other) = default;

public:
    // 自动通过 std::make_shared<Derived>(*static_cast<const Derived*>(this)) 实现多态克隆，免去每个具体 ChunkInfo 子类重复编写 clone() 样板代码。
    Ptr clone() const override
    {
        return std::static_pointer_cast<ChunkInfo>(std::make_shared<Derived>(*static_cast<const Derived*>(this)));
    }
};

/**
 * Chunk is a list of columns with the same length.
 * Chunk stores the number of rows in a separate field and supports invariant of equal column length.
 *
 * Chunk has move-only semantic. It's more lightweight than block cause doesn't store names, types and index_by_name.
 *
 * Chunk can have empty set of columns but non-zero number of rows. It helps when only the number of rows is needed.
 * Chunk can have columns with zero number of rows. It may happen, for example, if all rows were filtered.
 * Chunk is empty only if it has zero rows and empty list of columns.
 *
 * Any ChunkInfo may be attached to chunk.
 * It may be useful if additional info per chunk is needed. For example, bucket number for aggregated data.
**/

class Chunk
{
public:
    using ChunkInfoCollection = CollectionOfDerivedItems<ChunkInfo>;
    // 默认构造函数，创建一个 0 行 0 列的空 Chunk。
    Chunk() = default;
    Chunk(const Chunk & other) = delete;
    // 移动构造函数。将 other 的 columns、num_rows 和 chunk_infos 转移给当前对象，并将 other.num_rows 重置为 0。
    Chunk(Chunk && other) noexcept
        : columns(std::move(other.columns))
        , num_rows(other.num_rows)
        , chunk_infos(std::move(other.chunk_infos))
    {
        other.num_rows = 0;
    }
    // 构造函数，接收不可变列集合（Columns）和行数，并校验行数一致性。
    Chunk(Columns columns_, UInt64 num_rows_);
    // 构造函数，接收可变列集合（MutableColumns）和行数，将其转换为不可变列并校验一致性。
    Chunk(MutableColumns columns_, UInt64 num_rows_);

    Chunk & operator=(const Chunk & other) = delete;
    // 移动赋值运算符。接管 other 的所有资源，并将 other.num_rows 清零，返回 *this。
    Chunk & operator=(Chunk && other) noexcept
    {
        columns = std::move(other.columns);
        chunk_infos = std::move(other.chunk_infos);
        num_rows = other.num_rows;
        other.num_rows = 0;
        return *this;
    }

    Chunk clone() const;
    // 高效交换两个 Chunk 的 columns、num_rows 和 chunk_infos，无异常抛出。
    void swap(Chunk & other) noexcept
    {
        columns.swap(other.columns);
        std::swap(num_rows, other.num_rows);
        chunk_infos.swap(other.chunk_infos);
    }

    void clear()
    {
        num_rows = 0;
        columns.clear();
        chunk_infos.clear();
    }

    const Columns & getColumns() const { return columns; }
    void setColumns(Columns columns_, UInt64 num_rows_);
    void setColumns(MutableColumns columns_, UInt64 num_rows_);
    Columns detachColumns();
    MutableColumns mutateColumns();
    /** Get empty columns with the same types as in block. */
    MutableColumns cloneEmptyColumns() const;

    ChunkInfoCollection & getChunkInfos() { return chunk_infos; }
    const ChunkInfoCollection & getChunkInfos() const { return chunk_infos; }
    void setChunkInfos(ChunkInfoCollection chunk_infos_) { chunk_infos = std::move(chunk_infos_); }

    UInt64 getNumRows() const { return num_rows; }
    UInt64 getNumColumns() const { return columns.size(); }
    bool hasRows() const { return num_rows > 0; }
    bool hasColumns() const { return !columns.empty(); }
    bool empty() const { return !hasRows() && !hasColumns(); }
    explicit operator bool() const { return !empty(); }

    void addColumn(ColumnPtr column);
    void addColumn(size_t position, ColumnPtr column);
    void erase(size_t position);

    UInt64 bytes() const;
    UInt64 allocatedBytes() const;

    std::string dumpStructure() const;

    void append(const Chunk & chunk);
    void append(const Chunk & chunk, size_t from, size_t length); // append rows [from, from+length) of chunk

private:
    // 存储底层列数据指针的数组（std::vector<ColumnPtr>），所有列必须保持相同的行数（或为合法的常量/稀疏表示）。
    Columns columns;
    // 独立记录当前数据块的行数。即使 columns 为空，该字段依然可以独立记录有效行数。
    UInt64 num_rows = 0;
    // 存储附加在当前 Chunk 上的多态 ChunkInfo 集合（基于派生类类型索引）。
    ChunkInfoCollection chunk_infos;
    // 内部一致性校验方法，检查 columns 中每一列的实际大小是否与 num_rows 严格一致，不一致时抛出逻辑异常。
    void checkNumRowsIsConsistent();
};

using Chunks = std::vector<Chunk>;

/// AsyncInsert needs two kinds of information:
/// - offsets of different sub-chunks
/// - tokens of different sub-chunks, which are assigned by setting `insert_deduplication_token`.
// 异步插入元数据子类
class AsyncInsertInfo : public ChunkInfoCloneable<AsyncInsertInfo>
{
public:
    AsyncInsertInfo() = default;
    AsyncInsertInfo(const AsyncInsertInfo & other) = default;
    AsyncInsertInfo(const std::vector<size_t> & offsets_, const std::vector<String> & tokens_)
        : offsets(offsets_)
        , tokens(tokens_)
    {}
    // 记录合并批次中各个子数据块（sub-chunks）的行偏移位置。
    std::vector<size_t> offsets;
    // 记录各个子数据块对应的去重 Token（由 insert_deduplication_token 指定）。
    std::vector<String> tokens;
};

using AsyncInsertInfoPtr = std::shared_ptr<AsyncInsertInfo>;

/// Converts all columns to full serialization in chunk.
/// It's needed, when you have to access to the internals of the column,
/// or when you need to perform operation with two columns
/// and their structure must be equal (e.g. compareAt).
void convertToFullIfConst(Chunk & chunk);
void convertToFullIfSparse(Chunk & chunk);
void removeSpecialColumnRepresentations(Chunk & chunk);

/// Analog of materializeBlock: converts const columns to full and removes sparse/replicated representations.
void materializeChunk(Chunk & chunk);

/// Optimize ColumnReplicated columns memory layout.
void compactReplicatedColumns(Chunk & chunk);

/// Creates a chunk with the same columns but makes them constants with a default value and a specified number of rows.
Chunk cloneConstWithDefault(const Chunk & chunk, size_t num_rows);

}
