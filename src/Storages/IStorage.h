#pragma once

#include <Core/Names.h>
#include <Core/QueryProcessingStage.h>
#include <Databases/IDatabase.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Interpreters/CancellationCode.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Storages/CheckResults.h>
#include <Storages/ColumnDependency.h>
#include <Storages/ColumnSize.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/VirtualColumnsDescription.h>
#include <Storages/TableLockHolder.h>
#include <Storages/StorageSnapshot.h>
#include <Common/ActionLock.h>
#include <Common/RWLock.h>
#include <Common/TypePromotion.h>
#include <DataTypes/Serializations/SerializationInfo.h>

#include <expected>
#include <optional>
#include <list>

// IStorage 是所有表引擎（Table Engine）的基类（抽象接口类）。它在整个数据库架构中处于核心地位，定义了表在 ClickHouse 中的统一行为契约。
// IStorage 类的核心职责包括：
// 数据存储与访问管理：定义数据如何持久化、存储结构（如压缩、列式存储组织）、数据查找（SELECT）以及数据写入（INSERT）。
// 元数据管理：维护表的列定义、主键、索引、分区键、TTL、备注等元数据信息（通过 StorageInMemoryMetadata）。
// 并发控制与锁机制：提供表级别的各种锁（共享锁 lockForShare、修改锁 lockForAlter、排他锁 lockExclusively），保证多线程和分布式环境下的并发安全。
// 生命周期管理：控制表的启动 (startup)、关闭 (shutdown/flushAndShutdown)、删除 (drop)、重命名 (rename) 和清空 (truncate)。
// 功能特性开关（Capabilities）：通过大量的 supports...() 虚函数，向查询优化器和执行引擎声明当前表引擎支持哪些高级特性（如 PREWHERE、SAMPLE、FINAL、TTL、并行插入、事务、轻量级删除等）。
// DDL 与运维支持：支持 ALTER、分区操作 (alterPartition)、数据变异 (mutate / updateLightweight)、备份恢复 (backupData / restoreDataFromBackup) 以及后台优化 (optimize)。
namespace DB
{

using StorageActionBlockType = size_t;

class ASTCreateQuery;
class ASTInsertQuery;

struct Settings;

class AlterCommands;
class MutationCommands;
struct PartitionCommand;
using PartitionCommands = std::vector<PartitionCommand>;

class IProcessor;
using ProcessorPtr = std::shared_ptr<IProcessor>;
using Processors = std::list<ProcessorPtr>;

class Pipe;
class QueryPlan;
using QueryPlanPtr = std::unique_ptr<QueryPlan>;

class SinkToStorage;
using SinkToStoragePtr = std::shared_ptr<SinkToStorage>;

class QueryPipeline;

class IStoragePolicy;
using StoragePolicyPtr = std::shared_ptr<const IStoragePolicy>;

struct StreamLocalLimits;
class EnabledQuota;
struct SelectQueryInfo;

using NameDependencies = std::unordered_map<String, std::vector<String>>;
using DatabaseAndTableName = std::pair<String, String>;

class BackupEntriesCollector;
class RestorerFromBackup;

class ConditionSelectivityEstimator;
using ConditionSelectivityEstimatorPtr = std::shared_ptr<ConditionSelectivityEstimator>;

struct RangesInDataParts;

class ActionsDAG;

/** Storage. Describes the table. Responsible for
  * - storage of the table data;
  * - the definition in which files (or not in files) the data is stored;
  * - data lookups and appends;
  * - data storage structure (compression, etc.)
  * - concurrent access to data (locks, etc.)
  */
class IStorage : public std::enable_shared_from_this<IStorage>, public TypePromotion<IStorage>, public IHints<>
{
public:
    IStorage() = delete;
    /// Storage metadata can be set separately in setInMemoryMetadata method
    // 构造函数。
    // 传入表的唯一标识（数据库名+表名+UUID）以及初始的内存元数据。禁止了无参构造、拷贝构造和赋值操作。
    explicit IStorage(StorageID storage_id_, std::unique_ptr<StorageInMemoryMetadata> metadata_ = nullptr);

    IStorage(const IStorage &) = delete;
    IStorage & operator=(const IStorage &) = delete;

    /// The main name of the table type (e.g. Memory, MergeTree, CollapsingMergeTree).
    // 返回表引擎的主名称（例如 "Memory", "MergeTree", "CollapsingMergeTree" 等）
    virtual std::string getName() const = 0;

    /// The name of the table.
    // 获取当前表的 StorageID（包含数据库名和表名）
    StorageID getStorageID() const;
    // 返回该表包含的内部表（Inner Tables）的 ID 列表（例如某些复合引擎或带有子表的引擎）。默认返回空。
    virtual std::vector<StorageID> getInnerStorageIDs() const { return {}; }
    // 指示该表是否属于 MergeTree 家族
    virtual bool isMergeTree() const { return false; }
    // 是否属于数据湖
    virtual bool isDataLake() const { return false; }

    /// Returns true if the storage is an external database (MySQL, PostgreSQL, MongoDB, etc.)
    // 是否属于外部数据库（MySQL/PG等）
    virtual bool isExternalDatabase() const { return false; }

    /// Returns true if the storage is object storage (S3, Azure, GCS, HDFS, etc.)
    // 是否对象存储（S3等）
    virtual bool isObjectStorage() const { return false; }

    /// Returns true if the storage is a message queue (Kafka, RabbitMQ, NATS)
    // 是否消息队列（Kafka等）
    virtual bool isMessageQueue() const { return false; }

    /// Returns true if the storage continuously consumes from an external source in the background
    /// (Kafka, RabbitMQ, NATS, S3Queue/AzureQueue).
    // 是否流式存储
    virtual bool isStreamingStorage() const { return false; }

    /// Returns true if the storage receives data from a remote server or servers.
    // 是否远程表
    virtual bool isRemote() const { return false; }

    /// Returns true if the storage is a view of a table or another view.
    // 是否视图
    virtual bool isView() const { return false; }

    /// Returns true if the storage is dictionary
    // 是否字典
    virtual bool isDictionary() const { return false; }

    /// Returns true if the storage supports queries with the SAMPLE section.
    virtual bool supportsSampling() const;

    /// Returns true if the storage supports queries with the FINAL section.
    virtual bool supportsFinal() const { return false; }

    /// Returns true if the storage supports `SELECT ... FROM t STREAM` continuous reads.
    virtual bool supportsStreaming() const { return false; }

    /// Returns true if the storage supports insert queries with the PARTITION BY section.
    virtual bool supportsPartitionBy() const { return false; }

    /// Returns true if the storage supports queries with the TTL section.
    virtual bool supportsTTL() const { return false; }

    /// Returns true if the storage supports column statistics. Storages that reject the dedicated
    /// `ALTER TABLE ... ADD/DROP/MODIFY STATISTICS` commands must also reject the column-declaration
    /// spelling `ALTER TABLE ... ADD/MODIFY COLUMN c UInt64 STATISTICS(...)`, which is gated on this.
    virtual bool supportsStatistics() const { return false; }

    /// Returns true if the storage supports queries with the PREWHERE section.
    virtual bool supportsPrewhere() const { return false; }
    // 获取条件选择率估计器，用于优化查询执行计划中的过滤顺序。
    virtual ConditionSelectivityEstimatorPtr getConditionSelectivityEstimator(const RangesInDataParts &, const Names &, ContextPtr) const;

    /// Returns which columns supports PREWHERE, or empty std::nullopt if all columns is supported.
    /// This is needed for engines whose aggregates data from multiple tables, like Merge.
    virtual std::optional<NameSet> supportedPrewhereColumns() const { return std::nullopt; }

    /// Whether a subcolumn is admitted into PREWHERE whenever `supportedPrewhereColumns` admits
    /// its origin column. Subcolumn sets are open-ended (JSON paths), so the contract enumerates
    /// top-level names only; this tells whether the storage's PREWHERE machinery resolves a
    /// subcolumn of an admitted column. Wrappers delegating the read (`Merge`, `Buffer`,
    /// `MaterializedView`) re-derive the filter by name and forward the question to the
    /// underlying tables; storages evaluating PREWHERE in a format reader (e.g. data lakes over
    /// Parquet with column mapping) cannot resolve subcolumns there, hence the fail-safe default.
    virtual bool supportedPrewhereColumnsIncludeSubcolumns() const { return false; }

    /// Returns true if the storage supports optimization of moving conditions to PREWHERE section.
    virtual bool canMoveConditionsToPrewhere() const { return supportsPrewhere(); }

    /// Returns true if the storage replicates SELECT, INSERT and ALTER commands among replicas.
    virtual bool supportsReplication() const { return false; }

    /// Returns true if the storage supports parallel insert.
    /// If false, each INSERT query will call write() only once.
    /// Different INSERT queries may write in parallel regardless of this value.
    virtual bool supportsParallelInsert() const { return false; }

    /// Returns true if the storage supports deduplication of inserted data blocks.
    virtual bool supportsDeduplication() const { return false; }

    /// Returns true if the blocks shouldn't be pushed to associated views on insert.
    virtual bool noPushingToViewsOnInserts() const { return false; }

    /// Read query returns streams which automatically distribute data between themselves.
    /// So, it's impossible for one stream run out of data when there is data in other streams.
    /// Example is StorageSystemNumbers.
    virtual bool hasEvenlyDistributedRead() const { return false; }

    /// Returns true if the storage supports reading of subcolumns of complex types.
    virtual bool supportsSubcolumns() const { return false; }
    /// Returns true if storage supports optimizations of functions by reading subcolumns.
    virtual bool supportsOptimizationToSubcolumns() const { return supportsSubcolumns(); }
    /// Same, but restricted to tuple element access (`tupleElement(t, 'x')` -> reading `t.x`).
    /// A storage that cannot serve synthesised subcolumns such as `.null`/`.size0` as standalone
    /// inputs may enable this while keeping supportsOptimizationToSubcolumns() false.
    virtual bool supportsOptimizationToTupleElementSubcolumns() const { return supportsOptimizationToSubcolumns(); }

    /// Returns true if the storage supports transactions for SELECT, INSERT and ALTER queries.
    /// Storage may throw an exception later if some query kind is not fully supported.
    /// This method can return true for readonly engines that return the same rows for reading (such as SystemNumbers)
    virtual bool supportsTransactions() const { return false; }

    /// Returns true if the storage supports columns with dynamic structure (like JSON or Dynamic types).
    virtual bool supportsColumnsWithDynamicStructure() const { return false; }

    /// Returns true if a storage snapshot captured now can be read later and is guaranteed to return
    /// exactly the data that existed at capture time, even if the table is concurrently written or merged.
    /// Used for atomic `CREATE MATERIALIZED VIEW ... POPULATE`, which pins such a snapshot and populates
    /// the view from it (see InterpreterCreateQuery). True for the MergeTree family, which retains the
    /// pinned data parts for the lifetime of the snapshot.
    virtual bool supportsPinnedSnapshot() const { return false; }

    /// Requires squashing small blocks to large for optimal storage.
    /// This is true for most storages that store data on disk.
    virtual bool prefersLargeBlocks() const { return true; }

    /// Returns true if the storage is for system, which cannot be target of SHOW CREATE TABLE.
    // 是否系统表
    virtual bool isSystemStorage() const { return false; }

    /// Returns true if asynchronous inserts are enabled for table.
    virtual bool areAsynchronousInsertsEnabled() const { return false; }
    // 是否共享存储
    virtual bool isSharedStorage() const { return false; }

    /// Optional size information of each physical column.
    /// Used for query optimizations by the MergeTree family of storages and by Parquet reader.
    using ColumnSizeByName = std::unordered_map<std::string, ColumnSize>;
    // 获取每个物理列的大小信息（字节数），供 MergeTree 家族或 Parquet 读取器进行查询优化。
    virtual ColumnSizeByName getColumnSizes() const { return {}; }

    /// Same as parameterless overload but also includes sizes for the requested subcolumns.
    /// Computing exact subcolumn sizes can be expensive, so `calculate_subcolumn_sizes` (driven by
    /// `allow_calculating_subcolumns_sizes_for_merge_tree_reading` at call sites) selects between the
    /// exact size and the cheaper top-level column size as an approximation.
    /// The default implementation falls back to the parameterless version.
    virtual ColumnSizeByName getColumnSizes(const Names & /*columns*/, bool /*calculate_subcolumn_sizes*/) const { return getColumnSizes(); }

    /// Same as getColumnSizes() but may return nullopt in some specific engines like Merge/Alias
    virtual std::optional<ColumnSizeByName> tryGetColumnSizes() const { return getColumnSizes(); }

    /// Optional size information of each secondary index.
    /// Valid only for MergeTree family.
    using IndexSizeByName = std::unordered_map<std::string, IndexSize>;
    // 获取二级索引的大小信息（仅对 MergeTree 家族有效）
    virtual IndexSizeByName getSecondaryIndexSizes() const { return {}; }

    /// Get immutable version (snapshot) of storage metadata. Metadata object is
    /// multiversion, so it can be concurrently changed, but returned copy can be
    /// used without any locks.
    /// Pass query context to enable metadata caching in MergeTree.
    /// Pass nullptr when no query context is available.
    // 获取内存中表元数据的不可变快照（Snapshot）。多版本机制保证了获取快照时无需加锁。
    virtual StorageMetadataHandle getInMemoryMetadataPtr(ContextPtr /*context*/, bool /*bypass_metadata_cache*/) const
    {
        return metadata.get();
    }

    /// Update storage metadata. Used in ALTER or initialization of Storage.
    /// Metadata object is multiversion, so this method can be called without
    /// any locks.
    // 更新表的内存元数据，常用于 ALTER 操作或表初始化阶段。
    void setInMemoryMetadata(const StorageInMemoryMetadata & metadata_)
    {
        metadata.set(std::make_unique<StorageInMemoryMetadata>(metadata_));
    }
    // 仅更新元数据中的表注释（Comment）。
    void setInMemoryMetadataComment(const String & comment)
    {
        auto updated = std::make_unique<StorageInMemoryMetadata>(*metadata.get());
        updated->setComment(comment);
        metadata.set(std::move(updated));
    }
    // 继承自 IHints，返回所有已注册的列名或其他相关名称，用于拼写错误时提供 HINT 建议。
    VectorWithMemoryTracking<String> getAllRegisteredNames() const override;
    // 获取依赖于该表特定列的物化视图或其他衍生对象映射关系。
    NameDependencies getDependentViewsByColumn(ContextPtr context) const;

    /// Modify a CREATE TABLE query to make a variant which must be written to a backup.
    // 修改 CREATE TABLE 语句，使其生成适用于备份的变体。
    virtual void applyMetadataChangesToCreateQueryForBackup(const ASTPtr & create_query) const;

    /// Makes backup entries to backup the data of this storage.
    virtual void backupData(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> & partitions);

    /// Extracts data from the backup and put it to the storage.
    virtual void restoreDataFromBackup(RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> & partitions);

    /// Returns true if the storage supports backup/restore for specific partitions.
    virtual bool supportsBackupPartition() const { return false; }

    /// Called after all databases and tables on all replicas have been restored from backup.
    /// If this data does some background work that depends on contents of other tables, this is
    /// the place to kick off that work (and it should be paused when IStorage is created with
    /// is_restore_from_backup = true in StorageFactory::Arguments).
    virtual void finalizeRestoreFromBackup() {}

    /// Return true if storage can execute lightweight delete mutations.
    virtual bool supportsLightweightDelete() const { return false; }

    /// Returns true if storage can execute lightweight update.
    virtual std::expected<void, PreformattedMessage> supportsLightweightUpdate() const;

    /// Return true if storage has any projection.
    virtual bool hasProjection() const { return false; }

    /// Return true if storage can execute 'DELETE FROM' mutations. This is different from lightweight delete
    /// because those are internally translated into 'ALTER UDPATE' mutations.
    virtual bool supportsDelete() const { return false; }

    /// Returns true if storage can store columns in sparse serialization.
    virtual bool supportsSparseSerialization() const { return false; }

    /// Return true if the trivial count query could be optimized without reading the data at all
    /// in totalRows() or totalRowsByPartitionPredicate() methods or with optimized reading in read() method.
    /// 'storage_snapshot' may be nullptr.
    virtual bool supportsTrivialCountOptimization(const StorageSnapshotPtr & /*storage_snapshot*/, ContextPtr /*query_context*/) const
    {
        return false;
    }

    /// Returns hints for serialization of columns accorsing to statistics accumulated by storage.
    virtual SerializationInfoByName getSerializationHints() const { return SerializationInfoByName{{}}; }

    /// Same as getSerializationHints() but may return nullopt in some specific engines like Alias
    virtual std::optional<SerializationInfoByName> tryGetSerializationHints() const { return getSerializationHints(); }

    /// Add engine args that were inferred during storage creation to create query to avoid the same
    /// inference on server restart. For example - data format inference in File/URL/S3/etc engines.
    virtual void addInferredEngineArgsToCreateQuery(ASTs & /*args*/, const ContextPtr & /*context*/) const {}

private:
    // 存储当前表的唯一标识（包含数据库名称、表名和表 UUID）。
    StorageID storage_id TSA_GUARDED_BY(id_mutex);
    // 保护 storage_id 的互斥锁，用于多线程安全地访问或修改表的标识。
    mutable std::mutex id_mutex;

    /// Multiversion storage metadata. Allows to read/write storage metadata without locks.
    // 多版本的内存元数据指针。采用多版本并发控制（MVCC），允许在不加锁的情况下并发读取或更新表的元数据（如表结构、列定义等）。
    MultiVersionStorageMetadataPtr metadata;

protected:
    // 带超时机制的通用读写锁获取辅助方法
    RWLockImpl::LockHolder tryLockTimed(
        const RWLock & rwlock, RWLockImpl::Type type, const String & query_id, const Poco::Timespan & acquire_timeout) const;

public:
    /// Lock table for share. This lock must be acquired if you want to be sure,
    /// that table will be not dropped while you holding this lock. It's used in
    /// variety of cases starting from SELECT queries to background merges in
    /// MergeTree.
    // 为 SELECT 查询或后台 Merge 等操作加共享锁（Share Lock）。确保在持有锁期间表不会被 DROP。
    TableLockHolder lockForShare(const String & query_id, const Poco::Timespan & acquire_timeout);

    /// Similar to lockForShare, but returns a nullptr if the table is dropped while
    /// acquiring the lock instead of raising a TABLE_IS_DROPPED exception
    // 尝试加共享锁。如果在获取锁的过程中表被删除，则返回 nullptr 而不是抛出 TABLE_IS_DROPPED 异常。
    TableLockHolder tryLockForShare(const String & query_id, const Poco::Timespan & acquire_timeout);

    /// Lock table for alter. This lock must be acquired in ALTER queries to be
    /// sure, that we execute only one simultaneous alter. Doesn't affect share lock.
    using AlterLockHolder = std::unique_lock<std::timed_mutex>;
    // 为 ALTER 操作加锁，确保同一时间只有一个 ALTER 正在执行，且不影响共享锁（SELECT）。
    AlterLockHolder lockForAlter(const Poco::Timespan & acquire_timeout);
    // 为 DROP、TRUNCATE 等高危操作加排他锁（Exclusive Lock）。此锁极其重，会阻塞所有其他的 SELECT、MERGE、ALTER 线程。
    std::optional<AlterLockHolder> tryLockForAlter(const Poco::Timespan & acquire_timeout);

    /// Lock table exclusively. This lock must be acquired if you want to be
    /// sure, that no other thread (SELECT, merge, ALTER, etc.) doing something
    /// with table. For example it allows to wait all threads before DROP or
    /// truncate query.
    ///
    /// NOTE: You have to be 100% sure that you need this lock. It's extremely
    /// heavyweight and makes table irresponsive.
    TableExclusiveLockHolder lockExclusively(const String & query_id, const Poco::Timespan & acquire_timeout);

    /** Returns stage to which query is going to be processed in read() function.
      * (Normally, the function only reads the columns from the list, but in other cases,
      *  for example, the request can be partially processed on a remote server, or an aggregate projection.)
      *
      * SelectQueryInfo is required since the stage can depends on the query
      * (see Distributed() engine and optimize_skip_unused_shards,
      *  see also MergeTree engine and projection optimization).
      * And to store optimized cluster (after optimize_skip_unused_shards).
      * It will also store needed stuff for projection query pipeline.
      *
      * QueryProcessingStage::Enum required for Distributed over Distributed,
      * since it cannot return Complete for intermediate queries ever.
      */
    // 回在 read() 函数中查询将被处理到的阶段（如 FetchColumns、Complete 等），特别适用于分布式表或带有聚合投影的表。
    virtual QueryProcessingStage::Enum getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageSnapshotPtr &, SelectQueryInfo &) const
    {
        return QueryProcessingStage::FetchColumns;
    }

    /** Watch live changes to the table.
     * Accepts a list of columns to read, as well as a description of the query,
     *  from which information can be extracted about how to retrieve data
     *  (indexes, locks, etc.)
     * Returns a stream with which you can read data sequentially
     *  or multiple streams for parallel data reading.
     * The `processed_stage` info is also written to what stage the request was processed.
     * (Normally, the function only reads the columns from the list, but in other cases,
     *  for example, the request can be partially processed on a remote server.)
     *
     * context contains settings for one query.
     * Usually Storage does not care about these settings, since they are used in the interpreter.
     * But, for example, for distributed query processing, the settings are passed to the remote server.
     *
     * num_streams - a recommendation, how many streams to return,
     *  if the storage can return a different number of streams.
     *
     * It is guaranteed that the structure of the table will not change over the lifetime of the returned streams (that is, there will not be ALTER, RENAME and DROP).
     */
    // 用于“活查询”（Live View / Watch），实时监听表的变动并返回数据流。
    virtual Pipe watch(
        const Names & /*column_names*/,
        const SelectQueryInfo & /*query_info*/,
        ContextPtr /*context*/,
        QueryProcessingStage::Enum & /*processed_stage*/,
        size_t /*max_block_size*/,
        size_t /*num_streams*/);

    /// Returns true if FINAL modifier must be added to SELECT query depending on required columns.
    /// It's needed for ReplacingMergeTree wrappers such as MaterializedPostrgeSQL
    // 判断是否需要自动为 SELECT 查询追加 FINAL 修饰符（主要用于 ReplacingMergeTree 的包装引擎）。
    virtual bool needRewriteQueryWithFinal(const Names & /*column_names*/) const { return false; }

private:
    /** Read a set of columns from the table.
      * Accepts a list of columns to read, as well as a description of the query,
      *  from which information can be extracted about how to retrieve data
      *  (indexes, locks, etc.)
      * Returns a stream with which you can read data sequentially
      *  or multiple streams for parallel data reading.
      * The `processed_stage` must be the result of getQueryProcessingStage() function.
      *
      * context contains settings for one query.
      * Usually Storage does not care about these settings, since they are used in the interpreter.
      * But, for example, for distributed query processing, the settings are passed to the remote server.
      *
      * num_streams - a recommendation, how many streams to return,
      *  if the storage can return a different number of streams.
      *
      * metadata_snapshot is consistent snapshot of table metadata, it should be
      * passed in all parts of the returned pipeline. Storage metadata can be
      * changed during lifetime of the returned pipeline, but the snapshot is
      * guaranteed to be immutable.
      */
    // 核心读方法。根据指定的列名、查询信息、上下文、最大块大小和流数量，返回一个或多个数据读取流（Pipe），用于顺序或并行读取数据。
    virtual Pipe read(
        const Names & /*column_names*/,
        const StorageSnapshotPtr & /*storage_snapshot*/,
        SelectQueryInfo & /*query_info*/,
        ContextPtr /*context*/,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t /*max_block_size*/,
        size_t /*num_streams*/);

    /// Should we process blocks of data returned by the storage in parallel
    /// even when the storage returned only one stream of data for reading?
    /// It is beneficial, for example, when you read from a file quickly,
    /// but then do heavy computations on returned blocks.
    ///
    /// This is enabled by default, but in some cases shouldn't be done (for
    /// example it is disabled for all system tables, since it is pretty
    /// useless).
    // 指示是否在存储层只返回单个读取流后，在外部继续并行化处理输出块（系统表默认关闭）。
    virtual bool parallelizeOutputAfterReading(ContextPtr) const { return !isSystemStorage(); }

public:
    /// Returns an upper bound on the number of sources created for a read request.
    /// The default is conservative: a storage may create one source per requested stream.
    // 返回为读取请求创建的数据源数量的上限。
    virtual size_t getMaxReadStreams(size_t num_streams, ContextPtr) { return num_streams; }

    /// Other version of read which adds reading step to query plan.
    /// Default implementation creates ReadFromStorageStep and uses usual read.
    /// Can be called after `shutdown`, but not after `drop`.
    // 将“读取数据”这一步直接添加到 ClickHouse 的现代执行计划（QueryPlan）中。默认实现是创建 ReadFromStorageStep。
    virtual void read(
        QueryPlan & query_plan,
        const Names & /*column_names*/,
        const StorageSnapshotPtr & /*storage_snapshot*/,
        SelectQueryInfo & /*query_info*/,
        ContextPtr /*context*/,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t /*max_block_size*/,
        size_t /*num_streams*/);

    /** Writes the data to a table.
      * Receives a description of the query, which can contain information about the data write method.
      * Returns an object by which you can write data sequentially.
      *
      * metadata_snapshot is consistent snapshot of table metadata, it should be
      * passed in all parts of the returned streams. Storage metadata can be
      * changed during lifetime of the returned streams, but the snapshot is
      * guaranteed to be immutable.
      *
      * async_insert - set to true if the write is part of async insert flushing
      */
    // 核心写方法。接收 INSERT 查询信息和元数据快照，返回一个 SinkToStorage 对象，用于向表中顺序写入数据块。
    virtual SinkToStoragePtr write(
        const ASTPtr & /*query*/,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        ContextPtr /*context*/,
        bool /*async_insert*/);

    /** Checks on the initiator that the current user is allowed to insert into this table, in
      * addition to the `INSERT` privilege on the table name checked by the interpreter.
      *
      * Called when the storage is the destination of an `INSERT`, before the query is executed or
      * queued for asynchronous insertion. A storage whose `write` guards the write with an access
      * check of its own must repeat the check here: with `async_insert = 1` the sink is created
      * later, in a background flush, so a check done only in `write` neither reaches the user
      * (with `wait_for_async_insert = 0` the query has already returned success) nor happens with
      * the privileges the user had when the query was issued.
      */
    // 在发起端（Initiator）检查当前用户是否有权限向该表执行 INSERT（除了表名检查外，针对特定引擎的额外安全校验）。
    virtual void checkInsertIsAllowed(ContextPtr /*context*/) const {}

    /** Writes the data to a table in distributed manner.
      * It is supposed that implementation looks into SELECT part of the query and executes distributed
      * INSERT SELECT if it is possible with current storage as a receiver and query SELECT part as a producer.
      *
      * Returns query pipeline if distributed writing is possible, and nullptr otherwise.
      */
    // 尝试执行分布式 INSERT SELECT 写入。如果支持，返回对应的查询管道，否则返回 nullptr。
    virtual std::optional<QueryPipeline> distributedWrite(const ASTInsertQuery & /*query*/, ContextPtr /*context*/);

    /** Delete the table data. Called before deleting the directory with the data.
      * The method can be called only after detaching table from Context (when no queries are performed with table).
      * The table is not usable during and after call to this method.
      * If some queries may still use the table, then it must be called under exclusive lock.
      * If you do not need any action other than deleting the directory with data, you can leave this method blank.
      */
    // 删除表数据。在从 Context 中解耦表之后调用，负责物理删除数据目录等清理工作。
    virtual void drop() {}
    // 如果该表包含内部表（例如某些组合引擎），则同时删除内部表。
    virtual void dropInnerTableIfAny(bool /* sync */, ContextPtr /* context */) {}

    /// Return true if the storage supports TRUNCATE operation.
    /// Storages without their own data (e.g. View) return false.
    virtual bool supportsTruncate() const { return true; }

    /** Clear the table data and leave it empty.
      * Must be called under exclusive lock (lockExclusively).
      */
    // 清空表数据并保持表结构为空（必须在排他锁下执行）。
    virtual void truncate(
        const ASTPtr & /*query*/,
        const StorageMetadataPtr & /* metadata_snapshot */,
        ContextPtr /* context */,
        TableExclusiveLockHolder &);
    // 检查表是否可以被重命名。
    virtual void checkTableCanBeRenamed(const StorageID & /*new_name*/) const {}

    /** Rename the table.
      * Renaming a name in a file with metadata, the name in the list of tables in the RAM, is done separately.
      * In this function, you need to rename the directory with the data, if any.
      * Called when the table structure is locked for write.
      * Table UUID must remain unchanged, unless table moved between Ordinary and Atomic databases.
      */
    // 重命名表。不仅更新内存和元数据，还要在磁盘上重命名数据目录。
    virtual void rename(const String & /*new_path_to_table_data*/, const StorageID & new_table_id)
    {
        renameInMemory(new_table_id);
    }

    /**
     * Just updates names of database and table without moving any data on disk
     * Can be called directly only from DatabaseAtomic.
     */
    // 仅在内存中更新数据库名和表名，不移动磁盘数据（专供 DatabaseAtomic 使用）。
    virtual void renameInMemory(const StorageID & new_table_id);

    /** ALTER tables in the form of column changes that do not affect the change
      * to Storage or its parameters. Executes under alter lock (lockForAlter).
      */
    // 执行 ALTER 操作（如修改列、注释等不涉及底层大改动的结构变更）。
    virtual void alter(const AlterCommands & params, ContextPtr context, AlterLockHolder & alter_lock_holder);

    /// Updates metadata that can be changed by other processes
    /// Return true if external metadata exists and was updated.
    // 更新外部动态元数据（如果存在外部源变动）。
    virtual void updateExternalDynamicMetadataIfExists(ContextPtr /* context */) { }

    /** Checks that alter commands can be applied to storage. For example, columns can be modified,
      * or primary key can be changes, etc.
      */
    // 校验给定的 ALTER 命令是否可以安全应用到该存储引擎。
    virtual void checkAlterIsPossible(const AlterCommands & commands, ContextPtr context) const;

    /**
      * Checks that mutation commands can be applied to storage.
      */
    // 校验变异命令（Mutation）是否被支持并可应用。
    virtual void checkMutationIsPossible(const MutationCommands & commands, const Settings & settings) const;

    /** ALTER tables with regard to its partitions.
      * Should handle locks for each command on its own.
      */
    // 执行或校验表分区级别的 ALTER 操作（如 DROP PARTITION, DETACH PARTITION 等）。
    virtual Pipe alterPartition(
        const StorageMetadataPtr & /* metadata_snapshot */,
        const PartitionCommands & /* commands */,
        ContextPtr /* context */);

    /// Checks that partition commands can be applied to storage.
    virtual void checkAlterPartitionIsPossible(
        const PartitionCommands & commands,
        const StorageMetadataPtr & metadata_snapshot,
        const Settings & settings,
        ContextPtr context) const;

    /** Perform any background work. For example, combining parts in a MergeTree type table.
      * Returns whether any work has been done.
      */
    // 执行后台维护或优化工作（例如 MergeTree 中的合并数据片段、去重、清理等）。
    virtual bool optimize(
        const ASTPtr & /*query*/,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        const ASTPtr & /*partition*/,
        bool /*final*/,
        bool /*deduplicate*/,
        const Names & /* deduplicate_by_columns */,
        bool /*cleanup*/,
        ContextPtr /*context*/);

    /// Executes update query. More lightweight than mutation.
    virtual QueryPipeline updateLightweight(const MutationCommands & commands, ContextPtr context);

    /// Mutate the table contents
    virtual void mutate(const MutationCommands &, ContextPtr);

    virtual Pipe executeCommand(const String & command_name, const ASTPtr & args, ContextPtr context);

    /// Cancel a mutation.
    virtual CancellationCode killMutation(const String & /*mutation_id*/);

    virtual void waitForMutation(const String & /*mutation_id*/, bool /*wait_for_another_mutation*/);

    virtual void setMutationCSN(const String & /*mutation_id*/, UInt64 /*csn*/);

    /// Cancel a part move to shard.
    virtual CancellationCode killPartMoveToShard(const UUID & /*task_uuid*/);

    /** If the table have to do some complicated work on startup,
      *  that must be postponed after creation of table object
      *  (like launching some background threads),
      *  do it in this method.
      * You should call this method after creation of object.
      * By default, does nothing.
      * Cannot be called simultaneously by multiple threads.
      */
    virtual void startup() {}

    /**
      * If the storage requires some complicated work on destroying,
      * then you have two virtual methods:
      * - flushAndPrepareForShutdown()
      * - shutdown()
      *
      * @see shutdown()
      * @see flushAndPrepareForShutdown()
      */
    // 在 shutdown() 之前调用，将内存中的数据持久化到底层存储，并停止会影响其他表的后台工作（如物化视图刷新）。
    void flushAndShutdown(bool is_drop = false)
    {
        flushAndPrepareForShutdown();
        shutdown(is_drop);
    }

    /** If the table have to do some complicated work when destroying an object - do it in advance.
      * For example, if the table contains any threads for background work - ask them to complete and wait for completion.
      * By default, does nothing.
      * Can be called simultaneously from different threads, even after a call to drop().
      */
    // 销毁表对象前的收尾工作，主要用于请求并等待所有后台工作线程退出。
    virtual void shutdown(bool is_drop = false) { UNUSED(is_drop); } // NOLINT

    /// Called before shutdown() to flush data to underlying storage.
    /// Might be called multiple times; only the first call needs to be processed.
    /// Data in memory need to be persistent. Any background work that affects other tables
    /// (e.g. materialized view refreshes that create/drop tables) needs to be stopped.
    virtual void flushAndPrepareForShutdown() {}

    /// Asks table to stop executing some action identified by action_type
    /// If table does not support such type of lock, and empty lock is returned
    virtual ActionLock getActionLock(StorageActionBlockType /* action_type */)
    {
        return {};
    }

    /// Call when lock from previous method removed
    virtual void onActionLockRemove(StorageActionBlockType /* action_type */) {}

    /// Run exactly one unit of background activity now (without resuming further activity).
    /// No-op for tables without such activity.
    virtual void refreshBackgroundActivity() {}

    /// Abort the in-flight unit of background activity without blocking future ones, discarding its
    /// uncommitted result so it is retried later. No-op for tables without such activity.
    virtual void cancelBackgroundActivity() {}

    std::atomic<bool> is_dropped{false};
    std::atomic<bool> is_detached{false};
    std::atomic<bool> is_being_restarted{false};

    /** A list of tasks to check a validity of data.
      * Each IStorage implementation may interpret this task in its own way.
      * E.g. for some storages it's a list of files in filesystem, for others it can be a list of parts.
      * Also it may hold resources (e.g. locks) required during check.
      */
    struct DataValidationTasksBase
    {
        /// Number of entries left to check.
        /// It decreases after each call to checkDataNext().
        virtual size_t size() const = 0;
        virtual ~DataValidationTasksBase() = default;
    };

    using DataValidationTasksPtr = std::shared_ptr<DataValidationTasksBase>;

    /// Specifies to check all data / partition / part
    using CheckTaskFilter = std::variant<std::monostate, ASTPtr, String>;
    virtual DataValidationTasksPtr getCheckTaskList(const CheckTaskFilter & /* check_task_filter */, ContextPtr /* context */);

    /** Executes one task from the list.
      * If no tasks left - returns nullopt.
      * Note: Function `checkDataNext` is accessing `check_task_list` thread-safely,
      *   and can be called simultaneously for the same `getCheckTaskList` result
      *   to process different tasks in parallel.
      * Usage:
      *
      * auto check_task_list = storage.getCheckTaskList({}, context);
      * size_t total_tasks = check_task_list->size();
      * while (true)
      * {
      *     size_t tasks_left = check_task_list->size();
      *     std::cout << "Checking data: " << (total_tasks - tasks_left) << " / " << total_tasks << " tasks done." << std::endl;
      *     auto result = storage.checkDataNext(check_task_list);
      *     if (!result)
      *         break;
      *     doSomething(*result);
      * }
      */
    virtual std::optional<CheckResult> checkDataNext(DataValidationTasksPtr & check_task_list);

    /// Checks that table could be dropped right now
    /// Otherwise - throws an exception with detailed information.
    /// We do not use mutex because it is not very important that the size could change during the operation.
    virtual void checkTableCanBeDropped([[ maybe_unused ]] ContextPtr query_context) const {}
    /// Similar to above but checks for DETACH. It's only used for DICTIONARIES.
    virtual void checkTableCanBeDetached() const {}

    /// Size-only drop gate used by `CREATE OR REPLACE` to enforce
    /// `max_table_size_to_drop` before EXCHANGE. Narrower than
    /// `checkTableCanBeDropped` (no dictionary/view-dependency throws), so it
    /// can run on any storage engine. NOT a pure dry-run: the `MergeTreeData`
    /// override reaches `Context::checkCanBeDropped`, which removes the
    /// `force_drop_table` flag when it authorizes an over-limit drop. Callers
    /// must invoke it exactly once; a second call after the flag was consumed
    /// throws TABLE_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT.
    /// Default: no-op (engine has no on-disk data the size guard would care about).
    virtual void checkTableSizeBelowDropLimit([[ maybe_unused ]] ContextPtr query_context) const {}

    /// Returns true if Storage may store some data on disk.
    /// NOTE: may not be equivalent to !getDataPaths().empty()
    virtual bool storesDataOnDisk() const { return false; }

    /// Returns data paths if storage supports it, empty vector otherwise.
    virtual Strings getDataPaths() const { return {}; }

    /// Same as getDataPaths() but may return nullopt in some specific engines like Alias
    virtual std::optional<Strings> tryGetDataPaths() const { return getDataPaths(); }

    /// Returns storage policy if storage supports it.
    virtual StoragePolicyPtr getStoragePolicy() const { return {}; }

    /// Same as getStoragePolicy() but may return nullopt in some specific engines like Alias
    virtual std::optional<StoragePolicyPtr> tryGetStoragePolicy() const { return getStoragePolicy(); }

    /// Returns true if all disks of storage are read-only or write-once.
    /// NOTE: write-once also does not support INSERTs/merges/... for MergeTree
    virtual bool isStaticStorage() const;

    /// If it is possible to quickly determine exact number of rows in the table at this moment of time, then return it.
    /// Used for:
    /// - Simple count() optimization
    /// - For total_rows column in system.tables
    ///
    /// Does takes underlying Storage (if any) into account.
    virtual std::optional<UInt64> totalRows(ContextPtr) const { return {}; }

    /// Same as above but also take partition predicate into account.
    virtual std::optional<UInt64> totalRowsByPartitionPredicate(const ActionsDAG &, ContextPtr) const { return {}; }

    /// Aggregated `(num_rows, num_defaults)` for `column_name` across all visible parts,
    /// taken from per-part `SerializationInfo`. Returns nullopt when the storage cannot
    /// supply an exact count -- see `Storages/MergeTree/SparsityFilter.h` for the precise
    /// reliability rules. Default implementation returns nullopt.
    struct ColumnDefaultnessStats
    {
        UInt64 num_rows = 0;
        UInt64 num_defaults = 0;
    };
    virtual std::optional<ColumnDefaultnessStats>
    getColumnDefaultnessStats(const String & /*column_name*/, ContextPtr) const { return {}; }

    /// If it is possible to quickly determine exact number of bytes for the table on storage:
    /// - memory (approximated, resident)
    /// - disk (compressed)
    ///
    /// Used for:
    /// - For total_bytes column in system.tables
    //
    /// Does not takes underlying Storage (if any) into account
    /// (since for Buffer we still need to know how much bytes it uses).
    ///
    /// Memory part should be estimated as a resident memory size.
    /// In particular, alloctedBytes() is preferable over bytes()
    /// when considering in-memory blocks.
    virtual std::optional<UInt64> totalBytes(ContextPtr) const { return {}; }

    /// If it is possible to quickly determine exact number of uncompressed bytes for the table on storage:
    /// - disk (uncompressed)
    ///
    /// Used for:
    /// - For total_bytes_uncompressed column in system.tables
    ///
    /// Does not take underlying Storage (if any) into account
    virtual std::optional<UInt64> totalBytesUncompressed(const Settings &) const { return {}; }

    /// Number of rows INSERTed since server start.
    ///
    /// Does not take the underlying Storage (if any) into account.
    virtual std::optional<UInt64> lifetimeRows() const { return {}; }

    /// Same as lifetimeRows() but may return nullopt in some specific engines like Alias
    virtual std::optional<std::optional<UInt64>> tryLifetimeRows() const { return lifetimeRows(); }

    /// Number of bytes INSERTed since server start.
    ///
    /// Does not take the underlying Storage (if any) into account.
    virtual std::optional<UInt64> lifetimeBytes() const { return {}; }

    /// Same as lifetimeBytes() but may return nullopt in some specific engines like Alias
    virtual std::optional<std::optional<UInt64>> tryLifetimeBytes() const { return lifetimeBytes(); }

    /// Creates a storage snapshot from given metadata.
    virtual StorageSnapshotPtr getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr /*query_context*/) const
    {
        return std::make_shared<StorageSnapshot>(*this, metadata_snapshot);
    }

    /// Creates a storage snapshot but without holding a data specific to storage.
    virtual StorageSnapshotPtr getStorageSnapshotWithoutData(const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) const
    {
        return getStorageSnapshot(metadata_snapshot, query_context);
    }

    /// Re initialize disks in case the underlying storage policy changed
    virtual bool initializeDiskOnConfigChange(const std::set<String> & /*new_added_disks*/) { return true; }

    /// A helper to implement read()
    static void readFromPipe(
        QueryPlan & query_plan,
        Pipe pipe,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        std::shared_ptr<IStorage> storage_);

private:
    /// Lock required for alter queries (lockForAlter).
    /// Allows to execute only one simultaneous alter query.
    mutable std::timed_mutex alter_lock;

    /// Lock required for drop queries. Every thread that want to ensure, that
    /// table is not dropped have to table this lock for read (lockForShare).
    /// DROP-like queries take this lock for write (lockExclusively), to be sure
    /// that all table threads finished.
    mutable RWLock drop_lock = RWLockImpl::create();
};

}
