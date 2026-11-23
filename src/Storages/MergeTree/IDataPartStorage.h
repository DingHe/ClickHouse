#pragma once
#include <Core/NamesAndTypes.h>
#include <Disks/WriteMode.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteSettings.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/MergeTree/MergeTreeDataPartType.h>
#include <base/types.h>
#include <Common/TransactionID.h>

#include <memory>
#include <optional>

#include <boost/core/noncopyable.hpp>
#include <Poco/Timestamp.h>


namespace DB
{
struct ReadSettings;
class ReadBufferFromFileBase;
class ReadPipeline;
class WriteBufferFromFileBase;

struct IDiskTransaction;
using DiskTransactionPtr = std::shared_ptr<IDiskTransaction>;

struct CanRemoveDescription
{
    // 指示当前是否允许删除该分区的底层共享对象存储数据（在 S3 Zero-Copy Replication 模式下，若其他副本仍持有 ZooKeeper 共享锁，则不能删除物理 Blob）。
    bool can_remove_anything;
    // 即使允许清理分区，也不能删除的保留文件名集合（例如正被其他活跃分区硬链接共享的列文件）。
    NameSet files_not_to_remove;
};
// 在删除分区前动态查询共享数据清理许可。
using CanRemoveCallback = std::function<CanRemoveDescription()>;

// IDataPartStorage.h 定义了 ClickHouse MergeTree 存储引擎中单个数据分区（Data Part）物理存储层的抽象接口 IDataPartStorage 及其配套辅助类、迭代器、事务与克隆结构。
// IDataPartStorage 类的核心作用
// 屏蔽分区物理布局差异（Full 存储 vs Packed 归档存储）：
// 在标准磁盘目录下（DataPartStorageOnDiskFull），分区内的每一列、索引和校验和都是独立文件（如 id.bin、id.mrk2、primary.idx）。
// 在对象存储（如 S3/HDFS）小分区场景下（DataPartStorageOnDiskPacked），所有文件被打包存放在单个归档文件 data.packed 中，内部通过偏移量寻址以减少对象存储 API 请求次数。
// IDataPartStorage 向上层（IMergeTreeDataPart、各类 IMergeTreeReader、备份与复制模块）统一屏蔽这两种物理组织形式的差异。
// 解耦分区文件操作与底层 IDisk 接口：
// 上层分区代码不再直接持有或操作裸 IDisk 路径，而是通过 IDataPartStorage 完成文件读写、目录遍历、投影子分区获取、零拷贝硬链接/克隆（freeze/clonePart）、备份（backup）以及事务性写入（beginTransaction / commitTransaction）。
// 统一管理生命周期与并发状态：
// 支持临时目录写入后原子重命名发布（rename）、零拷贝复制下的共享 Blob 引用计数安全删除（remove / removeSharedRecursive），以及持久化只读保护（DataPartStorageHolder）。

// 用于遍历当前分区目录下的所有逻辑文件（无论是独立文件还是 Packed 归档内的逻辑成员）
class IDataPartStorageIterator
{
public:
    /// Iterate to the next file.
    // 将迭代器推进到下一个元素。
    virtual void next() = 0;

    /// Return `true` if the iterator points to a valid element.
    // 判断当前迭代器是否指向有效元素（未到达末尾）。
    virtual bool isValid() const = 0;

    /// Return `true` if the iterator points to a file.
    // 判断当前指向的元素是否为普通文件（而非子目录，如 .proj 目录）。
    virtual bool isFile() const = 0;

    /// Name of the file that the iterator currently points to.
    // 返回当前文件的文件名（不含目录前缀，如 "data.bin"）。
    virtual std::string name() const = 0;

    /// Path of the file that the iterator currently points to.
    // 返回当前文件相对于磁盘根路径的完整相对路径。
    virtual std::string path() const = 0;

    virtual ~IDataPartStorageIterator() = default;
};

using DataPartStorageIteratorPtr = std::unique_ptr<IDataPartStorageIterator>;

struct MergeTreeDataPartChecksums;

class IReservation;
using ReservationPtr = std::unique_ptr<IReservation>;

class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

class ISyncGuard;
using SyncGuardPtr = std::unique_ptr<ISyncGuard>;

class MergeTreeTransaction;
using MergeTreeTransactionPtr = std::shared_ptr<MergeTreeTransaction>;

class IBackupEntry;
using BackupEntryPtr = std::shared_ptr<const IBackupEntry>;
using BackupEntries = std::vector<std::pair<String, BackupEntryPtr>>;
struct BackupSettings;

struct WriteSettings;

class TemporaryFileOnDisk;

// 记录分区的硬链接来源信息（用于 Zero-Copy Replication 和 Mutation 共享跟踪）：
struct HardlinkedFiles
{
    /// Shared table uuid where hardlinks live
    // 源文件所属共享表的 UUID 标识。
    std::string source_table_shared_id;
    /// Hardlinked from part
    // 源文件所属的源分区名称。
    std::string source_part_name;
    /// Hardlinked files list
    // 从源分区硬链接过来的文件名集合。
    NameSet hardlinks_from_source_part;
};

/// This is an abstraction of storage for data part files.
/// Ideally, it is assumed to contain read-only methods from IDisk.
/// It is not fulfilled now, but let's try our best.
class IDataPartStorage : public boost::noncopyable
{
public:
    virtual ~IDataPartStorage() = default;
    // 返回当前分区存储的物理类型枚举（如 Full 独立文件存储或 Packed 打包归档存储）。
    virtual MergeTreeDataPartStorageType getType() const = 0;

    /// Methods to get path components of a data part.
    // 返回分区在操作系统上的完整绝对路径
    virtual std::string getFullPath() const = 0;         /// '/var/lib/clickhouse/data/database/table/moving/all_1_5_1'
    // 返回分区相对于所属 IDisk 挂载根目录的相对路径（如 'database/table/moving/all_1_5_1'）。
    virtual std::string getRelativePath() const = 0;     ///                          'database/table/moving/all_1_5_1'
    // 返回分区自身的目录名（如 'all_1_5_1'）。
    virtual std::string getPartDirectory() const = 0;    ///                                                'all_1_5_1'
    // 返回分区所在父目录的完整绝对路径（如 '/var/lib/clickhouse/data/database/table/moving'）。
    virtual std::string getFullRootPath() const = 0;     /// '/var/lib/clickhouse/data/database/table/moving'
    // 返回分区目录直接上级子目录名（普通分区返回 ''，处于分离目录时返回 'detached'，处于移动目录时返回 'moving'）。
    virtual std::string getParentDirectory() const = 0;  ///                                                '' (or 'detached' for 'detached/all_1_5_1')
    /// Can add it if needed                             ///                          'database/table/moving'
    /// virtual std::string getRelativeRootPath() const = 0;

    /// Get a storage for projection.
    // 获取指定名称的投影（Projection）子分区的可变存储对象；use_parent_transaction 控制是否继承父分区的磁盘事务上下文。
    virtual std::shared_ptr<IDataPartStorage> getProjection(const std::string & name, bool use_parent_transaction = true) = 0; // NOLINT
    // 获取投影子分区的存储对象但不立即执行磁盘初始化（默认直接调用 getProjection）。
    virtual std::shared_ptr<IDataPartStorage> getProjectionNoInitialize(const std::string & name, bool use_parent_transaction = true) // NOLINT
    {
        return getProjection(name, use_parent_transaction);
    }
    // const 重载版本，返回只读的投影子分区存储对象。
    virtual std::shared_ptr<const IDataPartStorage> getProjection(const std::string & name) const = 0;

    /// Part directory exists.
    // 判断当前分区目录本身在存储介质上是否存在。
    virtual bool exists() const = 0;

    /// File inside part directory exists. Specified path is relative to the part path.
    // 判断分区内指定相对路径的文件是否存在（在 Packed 模式下会查询归档目录索引）。
    virtual bool existsFile(const std::string & name) const = 0;
    // 判断分区内指定子目录是否存在。
    virtual bool existsDirectory(const std::string & name) const = 0;

    /// Modification time for part directory.
    // 返回分区目录的最后修改时间戳。
    virtual Poco::Timestamp getLastModified() const = 0;

    /// Iterate part directory. Iteration in subdirectory is not needed yet.
    // 返回遍历当前分区内所有文件的迭代器（IDataPartStorageIterator），不递归进入子目录。
    virtual DataPartStorageIteratorPtr iterate() const = 0;

    /// Get metadata for a file inside path dir.
    // 返回分区内指定文件的最后修改时间戳。
    virtual Poco::Timestamp getFileLastModified(const std::string & file_name) const = 0;
    // 返回分区内指定文件在磁盘/归档中的物理字节大小。
    virtual size_t getFileSize(const std::string & file_name) const = 0;
    /// Uncompressed size of a packed skip-index substream from the archive index, or nullopt if
    /// unknown (non-packed file, or v0 archive). Callers fall back to the compressed size.
    // 如果是 Packed 归档存储中的打包跳数索引子流，从归档索引中返回其未压缩字节大小；若未知（非 Packed 存储或旧版 v0 归档）则返回 std::nullopt，调用方回退使用压缩大小。
    virtual std::optional<UInt64> getPackedFileUncompressedSize(const std::string & /*file_name*/) const { return {}; }
    // 返回底层文件（或对象存储 Blob）的硬链接/共享引用计数。
    virtual UInt32 getRefCount(const std::string & file_name) const = 0;

    /// Get path on remote filesystem from file name on local filesystem.
    // 根据本地元数据文件名，解析出其在远程对象存储（如 S3/HDFS）上对应的实际远端对象路径列表。
    virtual std::vector<std::string> getRemotePaths(const std::string & file_name) const = 0;
    // 计算当前分区所有文件在磁盘/对象存储上占用的总物理字节大小。
    virtual UInt64 calculateTotalSizeOnDisk() const = 0;

    /// Open the file for read and return ReadBufferFromFileBase object.
    /// Convenience wrapper: calls prepareRead() + pipeline.build().
    // 打开分区内名为 name 的文件并返回读取缓冲区 ReadBufferFromFileBase（非虚便捷封装方法，内部通过创建 ReadPipeline 并调用 prepareRead() + pipeline.build() 实现）。
    std::unique_ptr<ReadBufferFromFileBase> readFile(
        const std::string & name,
        const ReadSettings & settings,
        std::optional<size_t> read_hint) const;

    /// Populate a ReadPipeline with the stages needed to read from this part storage.
    /// Every implementation must override this method.
    // 核心纯虚读取准备方法：根据存储类型（本地磁盘、远程对象存储、本地缓存层、Packed 归档偏移切片等）向 ReadPipeline 填充所需的读取阶段，所有实现类必须重写。
    virtual void prepareRead(
        const std::string & name,
        const ReadSettings & settings,
        std::optional<size_t> read_hint,
        ReadPipeline & pipeline) const = 0;
    // 安全读取封装：先检查 existsFile(name)，存在则调用 readFile(...) 打开并返回缓冲区，不存在则直接返回空指针，避免文件不存在时抛出异常。
    virtual std::unique_ptr<ReadBufferFromFileBase> readFileIfExists(
        const std::string & name,
        const ReadSettings & settings,
        std::optional<size_t> read_hint) const
    {
        if (existsFile(name))
            return readFile(name, settings, read_hint);
        return {};
    }

    struct ProjectionChecksums
    {
        // 投影（Projection）子分区名称（如 "proj_by_time.proj"）。
        const std::string & name;
        // 该投影子分区的校验和文件清单（传入 remove() 以避免在删除分区时对对象存储执行昂贵的递归目录列举 LIST 操作）。
        const MergeTreeDataPartChecksums & checksums;
    };

    /// Remove data part.
    /// can_remove_shared_data, names_not_to_remove are specific for DiskObjectStorage.
    /// projections, checksums are needed to avoid recursive listing
    // 完整分区清理核心方法：结合 can_remove_callback 判断共享数据可删性，直接利用 checksums 和 projections 列表精确删除文件（避免对远程对象存储执行缓慢的目录列举），
    // 并处理临时分区 is_temp 标记。
    virtual void remove(
        CanRemoveCallback && can_remove_callback,
        const MergeTreeDataPartChecksums & checksums,
        std::list<ProjectionChecksums> projections,
        bool is_temp,
        LoggerPtr log) = 0;

    /// Get a name like 'prefix_partdir_tryN' which does not exist in a root dir.
    /// TODO: remove it.
    // 在根目录下生成一个尚不存在的唯一相对路径名（形如 'prefix_partdir_tryN'），用于在分离损坏分区（detached/broken）时避免目标目录重名冲突。
    virtual std::optional<String> getRelativePathForPrefix(
        LoggerPtr log, const String & prefix, bool detached, bool broken) const = 0;

    /// Reset part directory, used for in-memory parts.
    /// TODO: remove it.
    // 重置分区的相对路径（主要用于内存表分区 In-Memory Part 的路径调整）。
    virtual void setRelativePath(const std::string & path) = 0;

    /// Some methods from IDisk. Needed to avoid getting internal IDisk interface.
    // 返回底层磁盘的类型名称（如 "local"、"s3"、"hdfs"）。
    virtual std::string getDiskName() const = 0;
    // 返回底层磁盘的类型名称（如 "local"、"s3"、"hdfs"）。
    virtual std::string getDiskType() const = 0;
    // 判断分区是否存储在远程对象存储/分布式文件系统上（默认 false，远程磁盘实现类重写返回 true，直接影响 MergeTreeReaderWide 是否开启远程异步预读）
    virtual bool isStoredOnRemoteDisk() const { return false; }
    // 如果远程磁盘配置了本地文件系统缓存（Filesystem Cache），返回对应的缓存名称；否则返回 std::nullopt。
    virtual std::optional<String> getCacheName() const { return std::nullopt; }
    // 判断底层磁盘是否支持基于对象存储引用计数的零拷贝复制（Zero-Copy Replication）。
    virtual bool supportZeroCopyReplication() const { return false; }
    // 判断底层存储是否支持多线程并发写入同一分区下的不同文件。
    virtual bool supportParallelWrite() const = 0;
    // 判断底层存储/磁盘当前是否处于损坏不可用状态。
    virtual bool isBroken() const = 0;
    // 判断底层存储当前是否处于只读模式。
    virtual bool isReadonly() const = 0;

    /// Get a path for internal disk if relevant. It is used mainly for logging.
    // 返回底层磁盘的挂载根路径（主要用于日志打印和排查）。
    virtual std::string getDiskPath() const = 0;

    /// Reserve space on the same disk.
    /// Probably we should try to remove it later.
    /// TODO: remove constness
    // 在当前分区所在的同一块磁盘上预留 bytes 字节的空间，失败时抛出异常（默认返回 nullptr）。
    virtual ReservationPtr reserve(UInt64 /*bytes*/) const  { return nullptr; }
    // 尝试在当前磁盘上预留 bytes 字节空间，空间不足时返回 nullptr 而不抛异常。
    virtual ReservationPtr tryReserve(UInt64 /*bytes*/) const  { return nullptr; }

    /// A leak of abstraction.
    /// Return some uniq string for file.
    /// Required for distinguish different copies of the same part on remote FS.
    virtual String getUniqueId() const = 0;

    /// Represents metadata which is required for fetching of part.
    // 用于在副本间拉取分区数据（DataPartsExchange）时描述待传输文件列表：
    struct ReplicatedFilesDescription
    {
        // 按需创建该文件读取流的工厂函数。
        using InputBufferGetter = std::function<std::unique_ptr<ReadBuffer>()>;
        // 包含 input_buffer_getter（流工厂）和 file_size（文件字节大小）。
        struct ReplicatedFileDescription
        {
            InputBufferGetter input_buffer_getter;
            size_t file_size{};
        };
        // 件名到文件传输描述的映射字典。
        std::map<String, ReplicatedFileDescription> files;

        /// Unique string that is used to distinguish different
        /// copies of the same part on remote disk
        // 件名到文件传输描述的映射字典。
        String unique_id;
    };
    // 专用于远程磁盘（Zero-Copy 模式下仅传输元数据小文件而不传输共享数据 Blob）的副本文件传输描述构建方法。
    virtual ReplicatedFilesDescription getReplicatedFilesDescription(const NameSet & file_names) const = 0;
    virtual ReplicatedFilesDescription getReplicatedFilesDescriptionForRemoteDisk(const NameSet & file_names) const = 0;

    /// Create a backup of a data part.
    /// This method adds a new entry to backup_entries.
    /// Also creates a new tmp_dir for internal disk (if disk is mentioned the first time).
    using TemporaryFilesOnDisks = std::map<DiskPtr, std::shared_ptr<TemporaryFileOnDisk>>;
    // 执行 BACKUP TABLE 时将当前分区（含普通分区或投影子分区）的所有文件封装为 IBackupEntry 条目追加到 backup_entries 中，按需创建临时硬链接目录以保证备份期间的快照一致性。
    virtual void backup(
        const MergeTreeDataPartChecksums & checksums,
        const NameSet & files_without_checksums,
        const String & path_in_backup,
        const BackupSettings & backup_settings,
        bool make_temporary_hard_links,
        BackupEntries & backup_entries,
        TemporaryFilesOnDisks * temp_dirs,
        bool is_projection_part,
        bool allow_backup_broken_projection) const = 0;

    /// Creates hardlinks into 'to/dir_path' for every file in data part.
    /// Some files can be copied instead of hardlinks. It's because of details of zero copy replication
    /// implementation which relies on paths of some blobs in S3. For example if we want to hardlink
    /// the whole part during mutation we shouldn't hardlink checksums.txt, because otherwise
    /// zero-copy locks for different parts will be on the same path in zookeeper.
    ///
    /// If `external_transaction` is provided, the disk operations (creating directories, hardlinking,
    /// etc) won't be applied immediately; instead, they'll be added to external_transaction, which the
    /// caller then needs to commit.

    struct ClonePartParams
    {
        MergeTreeTransactionPtr txn = NO_TRANSACTION_PTR;
        HardlinkedFiles * hardlinked_files = nullptr;
        bool copy_instead_of_hardlink = false;
        NameSet files_to_copy_instead_of_hardlinks = {};
        bool keep_metadata_version = false;
        bool make_source_readonly = false;
        DiskTransactionPtr external_transaction = nullptr;
        std::optional<int32_t> metadata_version_to_write = std::nullopt;
        NameSet invalidated_columns_to_write = {};
        /// fsync the cloned/frozen directories (the clone subtree plus the ancestor chain up to
        /// the disk root) so the new hardlink directory entries survive a power loss. Only honored
        /// by freeze() on a local disk, outside an external transaction.
        bool fsync_part_directory = false;
    };

    /// For packed storage the whole data.packed archive is rewritten (copied) during a clone whenever
    /// any file it contains must be copied instead of hardlinked, the metadata version is overwritten,
    /// or the version is dropped. When that happens none of the archive's logical members (of the part
    /// or its packed projections) are hardlinked from the source, so the caller must not record them as
    /// shared blobs. Full storage hardlinks members individually and has no such archive, so it never
    /// copies a whole archive.
    virtual bool cloneCopiesWholeArchive(const ClonePartParams & /*params*/) const { return false; }
    // 在同一块磁盘上的目标路径 to/dir_path 下快速冻结/克隆当前分区（用于 ALTER TABLE ... FREEZE 备份快照以及 Mutation 轻量级硬链接克隆），并在完成后回调 save_metadata_callback。
    virtual std::shared_ptr<IDataPartStorage> freeze(
        const std::string & to,
        const std::string & dir_path,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        std::function<void(const DiskPtr &)> save_metadata_callback,
        const ClonePartParams & params) const = 0;

    // 跨磁盘或向远程目标磁盘 dst_disk 执行分区冻结克隆。
    virtual std::shared_ptr<IDataPartStorage> freezeRemote(
    const std::string & to,
    const std::string & dir_path,
    const DiskPtr & dst_disk,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    std::function<void(const DiskPtr &)> save_metadata_callback,
    const ClonePartParams & params) const = 0;

    /// Make a full copy of a data part into 'to/dir_path' (possibly to a different disk).
    // 将当前分区完整物理拷贝到目标磁盘 disk 的 to/dir_path 目录下（支持 cancellation_hook 随时响应取消中断，用于跨盘 ALTER TABLE MOVE PART）。
    virtual std::shared_ptr<IDataPartStorage> clonePart(
        const std::string & to,
        const std::string & dir_path,
        const DiskPtr & disk,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        LoggerPtr log,
        const std::function<void()> & cancellation_hook
        ) const = 0;

    /// Change part's root. from_root should be a prefix path of current root path.
    /// Right now, this is needed for rename table query.
    // 修改分区内存对象记录的根路径前缀（将 from_root 替换为 to_root），主要用于执行 RENAME TABLE 时同步更新内存中分区的路径元数据。
    virtual void changeRootPath(const std::string & from_root, const std::string & to_root) = 0;
    // 创建当前分区所需的物理目录结构。
    virtual void createDirectories() = 0;
    // 在当前分区存储下创建一个新的投影子分区存储目录/结构。
    virtual void createProjection(const std::string & name) = 0;

    /// Hint for the preferred on-disk order of files. Packed storage uses it to lay out the
    /// single archive; storages that keep files separately can ignore it (default no-op).
    // 提示分区内文件的期望落盘顺序。Packed 存储利用此提示将相关列文件相邻排布在同一个归档文件中以提升顺序读局部性；Full 独立文件存储直接忽略（默认空实现）。
    virtual void setPreferredFileOrder(const Strings & /*file_names*/) {}
    // 以指定的缓冲区大小 buf_size、写入模式 mode（重写或追加）和写配置 settings 打开/创建文件，返回写入缓冲区 WriteBufferFromFileBase。
    virtual std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const String & name,
        size_t buf_size,
        WriteMode mode,
        const WriteSettings & settings) = 0;
    // 重载便捷方法，默认使用 WriteMode::Rewrite 覆盖写入模式调用上述 writeFile。
    std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const String & name,
        size_t buf_size,
        const WriteSettings & settings)
    {
        return writeFile(name, buf_size, WriteMode::Rewrite, settings);
    }

    /// A special const method to write transaction file.
    /// It's const, because file with transaction metadata
    /// can be modified after part creation.
    // 特殊的 const 写入方法，专门用于写入事务元数据文件（如 txn_version.txt）。因为事务提交/回滚信息可能在分区构建完成并变为只读后才修改，所以该方法声明为 const。
    virtual std::unique_ptr<WriteBufferFromFileBase> writeTransactionFile(const String & txn_file_name, WriteMode mode) const = 0;
    // 在分区中创建一个空文件（通常用于创建标记文件，如 default_compression_codec.txt 或状态哨兵文件）。
    virtual void createFile(const String & name) = 0;
    // 将分区内的文件 from_name 重命名/移动到 to_name（目标若存在可能失败）。
    virtual void moveFile(const String & from_name, const String & to_name) = 0;
    // 原子地用 from_name 替换目标文件 to_name。
    virtual void replaceFile(const String & from_name, const String & to_name) = 0;
    // 删除分区内的指定文件（若不存在会抛异常）。
    virtual void removeFile(const String & name) = 0;
    // 删除分区内的指定文件（若不存在会抛异常）。
    virtual void removeFileIfExists(const String & name) = 0;
    // 递归删除整个分区目录及其所有内容。
    virtual void removeRecursive() = 0;
    // 递归删除整个分区目录及其所有内容。
    virtual void removeSharedRecursive(bool keep_in_remote_fs) = 0;
    // 获取目录刷盘同步守卫（RAII 对象），在析构时对分区目录执行 fsync 确保目录元数据持久化。
    virtual SyncGuardPtr getDirectorySyncGuard() const { return nullptr; }
    // 从另一个源分区存储 source 的文件 from 创建硬链接到当前分区的 to 文件。
    virtual void createHardLinkFrom(const IDataPartStorage & source, const std::string & from, const std::string & to) = 0;
    // 从源分区存储 source 完整拷贝文件 from 到当前分区的 to 文件。
    virtual void copyFileFrom(const IDataPartStorage & source, const std::string & from, const std::string & to) = 0;

    /// Rename part.
    /// Ideally, new_root_path should be the same as current root (but it is not true).
    /// Examples are: 'all_1_2_1' -> 'detached/all_1_2_1'
    ///               'moving/tmp_all_1_2_1' -> 'all_1_2_1'
    // 重命名或移动整个分区（例如将临时构建目录 'moving/tmp_all_1_2_1' 原子提交为正式分区 'all_1_2_1'，或移入 'detached/all_1_2_1'），支持按需清理已存在的目标目录并对父目录执行 fsync。
    virtual void rename(
        std::string new_root_path,
        std::string new_part_dir,
        LoggerPtr log,
        bool remove_new_dir_if_exists,
        bool fsync_part_dir) = 0;

    /// Starts a transaction of mutable operations.
    // 开启一个分区修改事务（在 Packed 模式下开始在内存/缓冲区中收集待打包的多个逻辑文件；在远程对象存储下收集批量元数据操作）。
    virtual void beginTransaction() = 0;
    /// Commits a transaction of mutable operations.
    // 提交当前分区事务，将所有挂起的写操作/归档索引原子落盘生效。
    virtual void commitTransaction() = 0;

    /// Commits the accumulated operations and starts a fresh transaction, keeping the storage
    /// writable (commit->begin). Used mid-build to bound the volume of a single commit; a storage
    /// that accumulates no per-file operations may make it a no-op.
    // 检查点提交（默认实现为依次调用 commitTransaction() 和 beginTransaction()），在构建超大分区中途分段刷盘，防止单次事务积累过多内存或元数据操作。
    virtual void checkpointTransaction()
    {
        commitTransaction();
        beginTransaction();
    }
    /// Prepares transaction to commit.
    /// It may be flush of buffered data or similar.
    // 预提交事务，刷出所有缓冲区数据（Flush），为最终原子提交做准备。
    virtual void precommitTransaction() = 0;
    // 查询当前存储对象是否正处于一个尚未提交的活跃事务之中。
    virtual bool hasActiveTransaction() const = 0;

    /// Returns true if underlying filesystem is case-insensitive,
    /// e.g. file_name and FILE_NAME are the same files.
    // 返回底层文件系统是否大小写不敏感（如 macOS APFS 或 Windows NTFS 下 "file_name" 与 "FILE_NAME" 指向同一文件，用于防大小写冲突检查）。
    virtual bool isCaseInsensitive() const = 0;
};

using DataPartStoragePtr = std::shared_ptr<const IDataPartStorage>;
using MutableDataPartStoragePtr = std::shared_ptr<IDataPartStorage>;

/// A holder that encapsulates data part storage and
/// gives access to const storage from const methods
/// and to mutable storage from non-const methods.
class DataPartStorageHolder : public boost::noncopyable
{
public:
    explicit DataPartStorageHolder(MutableDataPartStoragePtr storage_)
        : storage(std::move(storage_))
    {
    }

    IDataPartStorage & getDataPartStorage() { return *storage; }
    const IDataPartStorage & getDataPartStorage() const { return *storage; }

    MutableDataPartStoragePtr getDataPartStoragePtr() { return storage; }
    DataPartStoragePtr getDataPartStoragePtr() const { return storage; }

private:
    // 持有底层可变的 IDataPartStorage 智能指针。
    MutableDataPartStoragePtr storage;
};

inline bool isFullPartStorage(const IDataPartStorage & storage)
{
    return storage.getType() == MergeTreeDataPartStorageType::Full;
}

inline bool isPackedPartStorage(const IDataPartStorage & storage)
{
    return storage.getType() == MergeTreeDataPartStorageType::Packed;
}

}
