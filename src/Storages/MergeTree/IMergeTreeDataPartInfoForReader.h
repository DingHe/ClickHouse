#pragma once
#include <Interpreters/Context_fwd.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/ColumnsSubstreams.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ColumnSize.h>
#include <Core/NamesAndTypes.h>
#include <base/types.h>

namespace DB
{

class IDataPartStorage;
using DataPartStoragePtr = std::shared_ptr<const IDataPartStorage>;

class IMergeTreeDataPart;

struct MergeTreeSettings;
using MergeTreeSettingsPtr = std::shared_ptr<const MergeTreeSettings>;

class MergeTreeIndexGranularity;
struct MergeTreePartInfo;
struct MergeTreePartition;
struct MergeTreeDataPartChecksums;
struct MergeTreeIndexGranularityInfo;

class ISerialization;
using SerializationPtr = std::shared_ptr<const ISerialization>;
class SerializationInfoByName;

class AlterConversions;
using AlterConversionsPtr = std::shared_ptr<const AlterConversions>;

using Index = Columns;
using IndexPtr = std::shared_ptr<const Index>;

/**
 * A class which contains all information about a data part that is required
 * in order to use MergeTreeDataPartReader's.
 * It is a separate interface and not a simple struct because
 * otherwise it will need to copy all the information which might not
 * be even used (for example, an IndexGranularity class object is quite heavy).
 */
// 定义了 ClickHouse MergeTree 读取层用来获取数据分区（Data Part）元信息的纯虚抽象接口 IMergeTreeDataPartInfoForReader。
// IMergeTreeDataPartInfoForReader 类的核心作用
// 隔离底层 IMergeTreeDataPart 与读取器层（Reader）的强耦合：
// 完整的 IMergeTreeDataPart 对象包含大量生命周期管理、后台合并锁、状态机转换、TTL、权限与表引擎关联逻辑。读取器（IMergeTreeReader、MergeTreeRangeReader、MergeTreeReadTask）在读取数据时只需要获取分区的格式、列描述、稀疏主索引、索引粒度、序列化信息以及文件存储句柄，通过该接口可以将读取逻辑与具体 Part 实现彻底解耦。
// 支持多种 Part 来源（本地实体 Part vs 无状态 Worker 借用 Part）：
// 在本地查询或协调器节点上，派生类 LoadedMergeTreeDataPartInfoForReader 直接包装一个本地内存中的 IMergeTreeDataPart。
// 在存算分离 / 无状态计算节点（Stateless Worker）并行读取场景下，Worker 节点本地并没有加载完整的 IMergeTreeDataPart 对象，而是通过网络从协调器接收轻量级描述，实例化派生类 BorrowedMergeTreeDataPartInfoForReader 实现同一套接口（其中部分仅协调器可用的方法返回空或默认值）。
// 避免重量级元数据对象的无谓拷贝（按需懒加载/引用访问）：
// 正如类注释所述，如果将它设计成普通结构体（Struct），每次构建任务都必须深拷贝 MergeTreeIndexGranularity、主索引 Index、校验和等重量级结构，而很多读取路径根本用不到其中一部分信息。定义为纯虚接口后，实现类可以按需返回引用或内部懒加载，实现零拷贝高效访问。
class IMergeTreeDataPartInfoForReader : public WithContext
{
public:
    // 显式构造函数，将传入的 ContextPtr 传递给基类 WithContext 进行初始化。
    explicit IMergeTreeDataPartInfoForReader(ContextPtr context_) : WithContext(context_) {}

    virtual ~IMergeTreeDataPartInfoForReader() = default;
    // 判断当前分区是否为 Compact 格式（所有列的数据混存在单个 data.bin 文件中，通常用于小分区）。返回 true 时上层会创建 MergeTreeReaderCompact。
    virtual bool isCompactPart() const = 0;
    // 判断当前分区是否为 Wide 格式（每一列独立存储为各自的 .bin 和 .mrk2 文件）。
    // 返回 true 时上层会创建 MergeTreeReaderWide，且 IMergeTreeReader::getColumns() 会返回转换为 Nested 子列形式后的列清单。
    virtual bool isWidePart() const = 0;
    // 判断当前分区是否是一个物化投影（Projection）的子分区。用于生成格式化的条件缓存 Key（"父分区名:投影名"）以及决定是否启用父分区关联逻辑。
    // ProjectionPart（投影分区） 是 MergeTree 存储引擎实现 Projection（物化投影） 特性时，在物理存储层生成的子数据分区（Sub-Part）。
    // Projection 是 ClickHouse 提供的一种表内物化视图（Part-level Materialized View）机制，用来加速查询（尤其是非主键列过滤和高基数预聚合）：
    // 改变数据排序顺序（类似二级聚集索引）：主表按 ORDER BY (user_id, event_time) 排序，如果经常按 event_time 查询，可以定义一个按 ORDER BY (event_time, user_id) 排序的 Projection，避免全表扫描。
    // 预聚合（Pre-aggregation）：在写入数据时提前按指定维度计算 sum()、count()、uniq() 等聚合指标，查询时直接读取预聚合结果。
    // 普通物化视图（MATERIALIZED VIEW）会创建一张完全独立的物理表；而 Projection 则采用分区级绑定（Part-level）的存储设计——每个普通 Data Part 内部，都会为每个 Projection 单独生成一个子目录，这个子目录就是一个独立的 ProjectionPart。
    virtual bool isProjectionPart() const = 0;
    // 判断当前分区是否包含轻量级删除（Lightweight Delete）产生的隐藏系统列 _row_exists（用于标记哪些行已被 DELETE FROM 逻辑删除）。
    virtual bool hasLightweightDelete() const = 0;
    // 返回当前分区的名称字符串（例如 "all_1_5_1" 或投影名）。
    // 如果当前分区是投影分区（isProjectionPart() == true），返回其所属父分区的名称；否则返回空字符串 ""。用于读取池和 MergeTreeSelectProcessor 拼接带父级前缀的唯一分区标识。
    virtual const String & getPartName() const = 0;
    // 返回结构化的分区命名信息 MergeTreePartInfo（包含分区 ID partition_id、最小/最大数据块编号 min_block/max_block、合并层级 level 以及变更版本号 mutation）。
    virtual const MergeTreePartInfo & getPartInfo() const = 0;
    // 返回该分区所属的分区键键值对象 MergeTreePartition（用于填充 _partition_id、_partition_value 虚拟列以及分区级裁剪）。
    virtual const MergeTreePartition & getPartition() const = 0;
    // 返回当前分区包含的数据最小版本号（通常对应 min_block 或内部数据版本，用于多版本合并与可见性判断）。
    virtual Int64 getMinDataVersion() const = 0;
    // 返回当前分区包含的数据最大版本号（用于判断哪些历史 ALTER Mutation 已经合并进当前分区，哪些仍需在读取时动态执行 On-fly 转换）。
    virtual Int64 getMaxDataVersion() const = 0;
    // 返回当前分区驻留在内存中的稀疏主键索引（primary.idx 加载后的 Columns 数组指针）。
    // 用于主键二分查找定位 MarkRanges 以及在按主键有序读取时生成虚拟行边界（buildVirtualRowFromIndex）。
    virtual IndexPtr getIndexPtr() const = 0;
    // 返回抽象数据分区存储句柄 IDataPartStorage 的共享指针。
    // 通过它可以打开具体列文件流、查询文件是否存在、判断存储位于本地磁盘还是 S3/HDFS 远程对象存储（isStoredOnRemoteDisk()）。
    virtual DataPartStoragePtr getDataPartStorage() const = 0;
    // 返回当前分区实际存储的物理列名与数据类型有序列表。
    virtual const NamesAndTypesList & getColumns() const = 0;
    // 返回当前分区的完整列描述对象 ColumnsDescription（包含普通列、默认表达式、注释、编解码器 Codec 等信息）。
    virtual const ColumnsDescription & getColumnsDescription() const = 0;
    // 返回将扁平化的 Nested 独立数组列（如 n.a、n.b）重新聚合为完整 Nested(...) 复合类型后的列描述，用于处理 Nested 结构整体映射与缺失偏移借用。
    virtual const ColumnsDescription & getColumnsDescriptionWithCollectedNested() const = 0;
    // 返回当前分区中各列包含的物理子流清单（例如 Nullable 列的 .null 位图流、Array 的 .size0 偏移流、LowCardinality 的字典流等），用于精确判断某个子列或子流是否存在。
    virtual const ColumnsSubstreams & getColumnsSubstreams() const = 0;
    // 查找指定列名 column_name 在当前分区物理列列表中的下标位置（从 0 开始）；如果该列在当前分区中不存在（例如后续 ADD COLUMN 新增的列），返回 std::nullopt。
    virtual std::optional<size_t> getColumnPosition(const String & column_name) const = 0;

    /// Look up a (sub)column present in the part, if any.
    // ：尝试在当前分区中查找指定列或子列（如 tuple_col.field）；如果存在则返回其在该分区中的实际 NameAndTypePair（名称和物理类型），否则返回 std::nullopt。
    virtual std::optional<NameAndTypePair> tryGetColumn(const String & column_name) const = 0;
    // 判断指定的系统列（如轻量级删除掩码 _row_exists）在当前分区中是否已失效（例如全表恢复或掩码重置后旧物理文件已作废），失效时读取器会跳过读取其物理文件。
    virtual bool isSystemColumnInvalidated(const String & column_name) const = 0;
    // 从给定的候选列集合 available_columns 中，找出在当前分区中磁盘压缩字节数最小的列名。
    // 当查询不读取任何数据列（如 SELECT count() 或只读虚拟列）但又必须扫描物理行计数时，ClickHouse 会挑选体积最小的列（如 UInt8 或高压缩比列）来最小化磁盘 I/O。
    virtual String getColumnNameWithMinimumCompressedSize(const NamesAndTypesList & available_columns) const = 0;

    /// Name of the parent part when this is a projection part, empty otherwise.
    /// Used by the read pool/select processor to build a qualified part name.
    virtual String getParentPartName() const = 0;

    /// Per-column on-disk sizes, used for read-task sizing and the block size predictor.
    /// A borrowed part (stateless worker) has no size information: the scalar getters return zero
    /// (size predictor falls back to a default estimate) and `getColumnSizes` returns null. The map is
    /// returned by shared pointer, not by value, so hot callers (e.g. the per-block dataflow-statistics
    /// callback) reuse the part's cached map instead of copying it on every block.
    // 返回指定列 column_name 在当前分区中的存储大小统计 ColumnSize（包含压缩后字节数、未压缩字节数、Mark 字节数）。
    // 在无状态 Worker 借用分区场景下返回 0（上层预测器自动回退到默认估算值）。
    virtual ColumnSize getColumnSize(const String & column_name) const = 0;
    // 返回当前分区所有列的存储大小映射表的共享只读指针（在借用分区模式下返回 nullptr）。
    virtual std::shared_ptr<const std::unordered_map<String, ColumnSize>> getColumnSizes() const = 0;
    // 返回指定子列（如 map_col.keys 或 nested_col.id）的压缩与未压缩大小统计，用于精确估算只读取子列时的块大小和任务颗粒数。
    virtual ColumnSize getSubcolumnSize(const String & subcolumn_name) const = 0;

    /// MergeTree settings governing how the part is read.
    virtual MergeTreeSettingsPtr getStorageSettings() const = 0;

    /// The underlying concrete data part, or nullptr for a borrowed part (stateless worker).
    /// Only for the few coordinator-only features (patches, projections, index-read-tasks)
    /// that are never exercised on the stateless-worker read path. Hot-path/both-paths code
    /// must use the abstraction accessors above, not this.
    // 返回底层具体的 IMergeTreeDataPart 实体对象指针；如果是无状态 Worker 借用的远程分区（Borrowed Part），则返回 nullptr
    virtual std::shared_ptr<const IMergeTreeDataPart> getDataPart() const = 0;
    // 返回当前分区的 checksums.txt 校验和集合，用于验证底层文件完整性以及快速检查某列/某索引的文件是否存在于分区中。
    virtual const MergeTreeDataPartChecksums & getChecksums() const = 0;
    // 返回当前分区需要动态执行的 ALTER 变更转换规则（如历史列重命名映射、未物化的 DROP COLUMN 列表等）。
    virtual AlterConversionsPtr getAlterConversions() const = 0;
    // 返回当前分区包含的 Mark（数据颗粒 Granule）总数量。
    virtual size_t getMarksCount() const = 0;
    // 查询当前分区目录下指定文件（如某个 .bin 或 .mrk2 文件）的字节大小；
    // 如果该文件不存在则返回 0（常用于检查可选流文件是否存在以及设定读取边界）。
    virtual size_t getFileSizeOrZero(const std::string & file_name) const = 0;
    // 返回索引粒度的元配置信息 MergeTreeIndexGranularityInfo（包含是否为自适应粒度 is_adaptive、固定粒度行数 fixed_index_granularity、Mark 文件扩展名 .mrk/.mrk2/.mrk3 等）。
    virtual const MergeTreeIndexGranularityInfo & getIndexGranularityInfo() const = 0;
    // 返回当前分区具体的索引粒度对象 MergeTreeIndexGranularity，
    // 记录了每一个 Mark（Granule）实际包含的行数以及累计行数前缀和（支持根据 Mark 编号快速换算行号，以及判断是否存在末尾空 Mark hasFinalMark()）。
    virtual const MergeTreeIndexGranularity & getIndexGranularity() const = 0;
    // 根据传入的列名和数据类型，获取当前分区中该列对应的实际 ISerialization 序列化器
    virtual SerializationPtr getSerialization(const NameAndTypePair & column) const = 0;
    // 返回当前分区所有列的序列化特征描述字典（记录每列是否为稀疏列、空值比例、序列号版本等，写入在分区的 serialization.json 中）。
    virtual const SerializationInfoByName & getSerializationInfos() const = 0;
    // 返回当前分区所属 MergeTree 表的表名（主要用于错误日志、诊断信息和异常上下文输出）。
    virtual String getTableName() const = 0;
    // 唯一非 const 的业务方法。
    // 当读取器在读取或解压当前分区数据时遇到不可恢复的磁盘/数据损坏错误（如校验和不匹配、解压失败、意外 EOF），调用此方法将当前分区标记为损坏（Broken）
    virtual void reportBroken() = 0;
    // 返回当前分区存储的总数据行数。
    virtual size_t getRowCount() const = 0;
};

using MergeTreeDataPartInfoForReaderPtr = std::shared_ptr<IMergeTreeDataPartInfoForReader>;

}
