#pragma once

#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <Interpreters/Context_fwd.h>
#include <Core/Defines.h>
#include <Core/Names.h>
#include <base/types.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/ThreadPool_fwd.h>
#include <Disks/DiskType.h>
#include <IO/ReadSettings.h>
#include <IO/WriteSettings.h>
#include <Disks/WriteMode.h>
#include <Disks/DirectoryIterator.h>

#include <memory>
#include <boost/noncopyable.hpp>
#include <Poco/Timestamp.h>
#include <filesystem>
#include <optional>
#include <sys/stat.h>
#include <atomic>
#include <mutex>

#include "config.h"


namespace fs = std::filesystem;

namespace Poco
{
    namespace Util
    {
        /// NOLINTNEXTLINE(cppcoreguidelines-virtual-class-destructor)
        class AbstractConfiguration;
    }
}
// IDisk.h 定义了 ClickHouse 存储抽象层的核心接口 IDisk（虚拟文件系统抽象基类），以及配套的空间预留接口 Space、预留凭据 IReservation 和同步守卫 ISyncGuard。
// IDisk 类的核心作用
// 屏蔽底层存储介质差异（虚拟文件系统 VFS）：
// 无论数据存放在本地 POSIX 文件系统（DiskLocal）、远程对象存储（S3/GCS/Azure/HDFS，DiskObjectStorage）、带本地 NVMe 缓存的对象存储（CachedObjectStorage）、透明加密盘（DiskEncrypted）还是纯内存盘（DiskMemory），上层存储引擎（MergeTree、Log 家族、备份恢复模块）均通过统一的 IDisk 接口进行文件和目录操作。
// 统一空间核算与配额预留（继承自 Space）：
// 承担磁盘总容量、可用容量、未预留容量统计，以及在写入或后台 Merge 前执行原子空间预留（reserve），防止高并发写入把磁盘写满。
namespace DB
{

class ReadPipeline;

#if USE_AWS_S3
namespace S3
{
class Client;
}
#endif

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;
using DisksMap = std::map<String, DiskPtr, std::less<>>;

class IReservation;
using ReservationPtr = std::unique_ptr<IReservation>;

class ReadBufferFromFileBase;
class WriteBufferFromFileBase;
class MMappedFileCache;
class IMetadataStorage;
using MetadataStoragePtr = std::shared_ptr<IMetadataStorage>;
struct IDiskTransaction;
using DiskTransactionPtr = std::shared_ptr<IDiskTransaction>;
struct RemoveRequest;
using RemoveBatchRequest = std::vector<RemoveRequest>;

class DiskObjectStorage;
using DiskObjectStoragePtr = std::shared_ptr<DiskObjectStorage>;
using DiskObjectStorageConstPtr = std::shared_ptr<const DiskObjectStorage>;

using ObjectAttributes = std::map<std::string, std::string>;

struct PartitionCommand;

/**
 * Constraints for disk space reservation to avoid filling up disks.
 */
// 空间预留安全阈值约束
struct ReservationConstraints
{
    /// Min free bytes that must remain on the disk after reservation
    // 执行空间预留后，磁盘上必须保留的最小可用字节数。
    UInt64 min_bytes = 0;
    /// Min free space ratio that must remain on the disk after reservation
    // 执行空间预留后，磁盘上必须保留的最小可用空间比例（0.0 ~ 1.0）。
    Float32 min_ratio = 0.0;

    ReservationConstraints(UInt64 min_bytes_, Float32 min_ratio_)
        : min_bytes(min_bytes_), min_ratio(min_ratio_) {}
};

/**
 * Provide interface for reservation.
 */
// 存储空间预留抽象基类
class Space : public std::enable_shared_from_this<Space>
{
public:
    /// Return the name of the space object.
    // 返回当前空间对象（磁盘或存储卷）的名称。
    virtual const String & getName() const = 0;

    /// Reserve the specified number of bytes.
    /// Returns valid reservation or nullptr when failure.
    // 尝试预留 bytes 字节空间，成功返回 IReservation 凭据，失败返回 nullptr。
    virtual ReservationPtr reserve(UInt64 bytes) = 0;

    /// Reserve the specified number of bytes with constraints.
    /// Returns valid reservation or nullptr when failure (including when constraints are not met).
    // 带保留空间安全阈值约束的空间预留，不满足阈值时同样返回 nullptr。
    virtual ReservationPtr reserve(UInt64 bytes, const ReservationConstraints & constraints) = 0;

    /// Whether this is a disk or a volume.
    // 标识当前对象是否为单块磁盘（IDisk 重写返回 true）。
    virtual bool isDisk() const { return false; }
    // 标识当前对象是否为存储卷（IVolume 重写返回 true）。
    virtual bool isVolume() const { return false; }

    virtual ~Space() = default;
};

using SpacePtr = std::shared_ptr<Space>;

/**
 * A guard, that should synchronize file's or directory's state
 * with storage device (e.g. fsync in POSIX) in its destructor.
 */
// RAII 刷盘同步守卫
// RAII 守卫接口，在析构函数中将文件或目录的元数据修改同步刷入底层物理设备（如 POSIX 下的 fsync）；远程对象存储实现为空操作。
class ISyncGuard
{
public:
    ISyncGuard() = default;
    virtual ~ISyncGuard() = default;
};

using SyncGuardPtr = std::unique_ptr<ISyncGuard>;

/**
 * A unit of storage persisting data and metadata.
 * Abstract underlying storage technology.
 * Responsible for:
 * - file management;
 * - space accounting and reservation.
 */
class IDisk : public Space
{
public:
    // 从配置文件节点初始化磁盘名称及通用参数。
    IDisk(const String & name_, const Poco::Util::AbstractConfiguration & config, const String & config_prefix);
    explicit IDisk(const String & name_);
    ~IDisk() override;

    /// This is a disk.
    // 标识本对象为磁盘。
    bool isDisk() const override { return true; }
    // 创建一个磁盘事务对象 IDiskTransaction，用于将多个写/删/硬链接操作打包原子提交（远程对象存储常用）。
    virtual DiskTransactionPtr createTransaction();

    /// Root path for all files stored on the disk.
    /// It's not required to be a local filesystem path.
    virtual const String & getPath() const = 0;

    /// Return disk name.
    const String & getName() const override { return name; }

    /// Total available space on the disk.
    virtual std::optional<UInt64> getTotalSpace() const = 0;

    /// Space currently available on the disk.
    virtual std::optional<UInt64> getAvailableSpace() const = 0;

    /// Space available for reservation (available space minus reserved space).
    virtual std::optional<UInt64> getUnreservedSpace() const = 0;

    /// Amount of bytes which should be kept free on the disk.
    virtual UInt64 getKeepingFreeSpace() const { return 0; }

    /// Return `true` if the specified file/directory exists.
    virtual bool existsFile(const String & path) const = 0;
    virtual bool existsDirectory(const String & path) const = 0;

    /// This method can be less efficient than the above.
    virtual bool existsFileOrDirectory(const String & path) const = 0;

    /// Return size of the specified file.
    virtual size_t getFileSize(const String & path) const = 0;

    /// Create directory.
    virtual void createDirectory(const String & path) = 0;

    /// Create directory and all parent directories if necessary.
    virtual void createDirectories(const String & path) = 0;

    /// Move directory from `from_path` to `to_path`.
    virtual void moveDirectory(const String & from_path, const String & to_path) = 0;

    /// Return iterator to the contents of the specified directory.
    virtual DirectoryIteratorPtr iterateDirectory(const String & path) const = 0;

    /// Return `true` if the specified directory is empty.
    virtual bool isDirectoryEmpty(const String & path) const;

    /// Create empty file at `path`.
    virtual void createFile(const String & path) = 0;

    /// Move the file from `from_path` to `to_path`.
    /// If a file with `to_path` path already exists, an exception will be thrown .
    virtual void moveFile(const String & from_path, const String & to_path) = 0;

    /// Move the file from `from_path` to `to_path`.
    /// If a file with `to_path` path already exists, it will be replaced.
    virtual void replaceFile(const String & from_path, const String & to_path) = 0;

    virtual void renameExchange(const std::string &, const std::string &)
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED, "Method `renameExchange()` not implemented for disk: {}", getDataSourceDescription().toString());
    }

    virtual bool renameExchangeIfSupported(const std::string &, const std::string &)
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Method `renameExchangeIfSupported()` not implemented for disk: {}",
            getDataSourceDescription().toString());
    }

    /// Recursively copy files from from_dir to to_dir. Create to_dir if not exists.
    virtual void copyDirectoryContent(
        const String & from_dir,
        const std::shared_ptr<IDisk> & to_disk,
        const String & to_dir,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        const std::function<void()> & cancellation_hook);

    /// Copy file `from_file_path` to `to_file_path` located at `to_disk`.
    virtual void copyFile( /// NOLINT
        const String & from_file_path,
        IDisk & to_disk,
        const String & to_file_path,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings = {},
        const std::function<void()> & cancellation_hook = {});

    /// List files at `path` and add their names to `file_names`
    virtual void listFiles(const String & path, std::vector<String> & file_names) const = 0;

    /// Open the file for read and return ReadBufferFromFileBase object.
    /// Convenience wrapper: calls prepareRead() + pipeline.build().
    std::unique_ptr<ReadBufferFromFileBase> readFile( /// NOLINT
        const String & path,
        const ReadSettings & settings,
        std::optional<size_t> read_hint = {}) const;

    /// Populate a ReadPipeline with the stages needed to read from this disk.
    /// Every disk implementation must override this method.
    virtual void prepareRead(
        const String & path,
        const ReadSettings & settings,
        std::optional<size_t> read_hint,
        ReadPipeline & pipeline) const = 0;

    /// Returns nullptr if the file does not exist, otherwise opens it for reading.
    /// This method can save a request. The default implementation will do a separate `exists` call.
    virtual std::unique_ptr<ReadBufferFromFileBase> readFileIfExists( /// NOLINT
        const String & path,
        const ReadSettings & settings,
        std::optional<size_t> read_hint = {}) const;

    /// Open the file for write and return WriteBufferFromFileBase object.
    virtual std::unique_ptr<WriteBufferFromFileBase> writeFile( /// NOLINT
        const String & path,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        WriteMode mode = WriteMode::Rewrite,
        const WriteSettings & settings = {}) = 0;

    /// Remove file. Throws exception if file doesn't exists or it's a directory.
    /// Return whether file was finally removed. (For remote disks it is not always removed).
    virtual void removeFile(const String & path) = 0;

    /// Remove file if it exists.
    virtual void removeFileIfExists(const String & path) = 0;

    /// Remove directory. Throws exception if it's not a directory or if directory is not empty.
    virtual void removeDirectory(const String & path) = 0;

    virtual void removeDirectoryIfExists(const String &)
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Method `removeDirectoryIfExists()` is not implemented for disk: {}",
            getDataSourceDescription().toString());
    }

    /// Remove file or directory with all children. Use with extra caution. Throws exception if file doesn't exists.
    virtual void removeRecursive(const String & path) = 0;

    /// Remove file or directory with all children. Use with extra caution. Throws exception if file doesn't exists.
    /// Differs from removeRecursive for S3/HDFS disks
    /// Limits the number of removing files in batches to prevent high memory consumption.
    virtual void removeRecursiveWithLimit(const String & path) { removeRecursive(path); }

    /// Remove file. Throws exception if file doesn't exists or if directory is not empty.
    /// Differs from removeFile for S3/HDFS disks
    /// Second bool param is a flag to remove (true) or keep (false) shared data on S3
    virtual void removeSharedFile(const String & path, bool /* keep_shared_data */) { removeFile(path); }

    /// Remove file or directory with all children. Use with extra caution. Throws exception if file doesn't exists.
    /// Differs from removeRecursive for S3/HDFS disks
    /// Second bool param is a flag to remove (false) or keep (true) shared data on S3.
    /// Third param determines which files cannot be removed even if second is true.
    virtual void removeSharedRecursive(const String & path, bool /* keep_all_shared_data */, const NameSet & /* file_names_remove_metadata_only */) { removeRecursive(path); }

    /// Remove file or directory if it exists.
    /// Differs from removeFileIfExists for S3/HDFS disks
    /// Second bool param is a flag to remove (true) or keep (false) shared data on S3
    virtual void removeSharedFileIfExists(const String & path, bool /* keep_shared_data */) { removeFileIfExists(path); }

    /// Returns the path to a blob representing a specified file.
    /// The meaning of the returned path depends on disk's type.
    /// E.g. for DiskLocal it's the absolute path to the file and for DiskObjectStorage it's
    /// StoredObject::remote_path for each stored object combined with the name of the objects' namespace.
    virtual Strings getBlobPath(const String & path) const = 0;

    /// Returns whether the blob paths this disk uses are randomly generated.
    virtual bool areBlobPathsRandom() const = 0;

    using WriteBlobFunction = std::function<size_t(const Strings & blob_path, WriteMode mode, const std::optional<ObjectAttributes> & object_attributes)>;

    /// Write a file using a custom function to write a blob representing the file.
    /// This method is alternative to writeFile(), the difference is that for example for DiskObjectStorage
    /// writeFile() calls IObjectStorage::writeObject() to write an object to the object storage while
    /// this method allows to specify a callback for that.
    virtual void writeFileUsingBlobWritingFunction(const String & path, WriteMode mode, WriteBlobFunction && write_blob_function) = 0;

    /// Reads a file from an encrypted disk without decrypting it (only for encrypted disks).
    virtual std::unique_ptr<ReadBufferFromFileBase> readEncryptedFile(const String & path, const ReadSettings & settings) const;

    /// Writes an already encrypted file to the disk (only for encrypted disks).
    virtual std::unique_ptr<WriteBufferFromFileBase> writeEncryptedFile(
        const String & path, size_t buf_size, WriteMode mode, const WriteSettings & settings) const;

    /// Returns the size of an encrypted file (only for encrypted disks).
    virtual size_t getEncryptedFileSize(const String & path) const;
    virtual size_t getEncryptedFileSize(size_t unencrypted_size) const;

    /// Returns IV of an encrypted file (only for encrypted disks).
    virtual UInt128 getEncryptedFileIV(const String & path) const;

    virtual const String & getCacheName() const { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "There is no cache"); }

    virtual bool supportsCache() const { return false; }

    virtual NameSet getCacheLayersNames() const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Method `getCacheLayersNames()` is not implemented for disk: {}",
            getDataSourceDescription().toString());
    }

    /// Returns a list of storage objects (contains path, size, ...).
    /// (A list is returned because for Log family engines there might
    /// be multiple files in remote fs for single clickhouse file.
    virtual StoredObjects getStorageObjects(const String &) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Method `getStorageObjects()` not implemented for disk: {}",
            getDataSourceDescription().toString());
    }

    virtual std::optional<StoredObjects> getStorageObjectsIfExist(const String & path) const
    {
        if (existsFile(path))
            return getStorageObjects(path);
        return std::nullopt;
    }

    /// For one local path there might be multiple remote paths in case of Log family engines.
    struct LocalPathWithObjectStoragePaths
    {
        std::string local_path;
        StoredObjects objects;

        LocalPathWithObjectStoragePaths(
            const std::string & local_path_,
            StoredObjects && objects_)
            : local_path(local_path_)
            , objects(std::move(objects_))
        {}
    };

    /// Batch request to remove multiple files.
    /// May be much faster for blob storage.
    /// Second bool param is a flag to remove (true) or keep (false) shared data on S3.
    /// Third param determines which files cannot be removed even if second is true.
    virtual void removeSharedFiles(const RemoveBatchRequest & files, bool keep_all_batch_data, const NameSet & file_names_remove_metadata_only);

    /// Set last modified time to file or directory at `path`.
    virtual void setLastModified(const String & path, const Poco::Timestamp & timestamp) = 0;

    /// Get last modified time of file or directory at `path`.
    virtual Poco::Timestamp getLastModified(const String & path) const = 0;

    /// Get last changed time of file or directory at `path`.
    /// Meaning is the same as stat.mt_ctime (e.g. different from getLastModified()).
    virtual time_t getLastChanged(const String & path) const = 0;

    /// Set file at `path` as read-only.
    virtual void setReadOnly(const String & path) = 0;

    /// Create hardlink from `src_path` to `dst_path`.
    virtual void createHardLink(const String & src_path, const String & dst_path) = 0;

    virtual bool isSymlinkSupported() const { return false; }
    virtual bool isSymlink(const String &) const
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED, "Method isSymlink is not implemented for disk type: {}", getDataSourceDescription().toString());
    }

    virtual bool isSymlinkNoThrow(const String &) const
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Method isSymlinkNothrow is not implemented for disk type: {}",
            getDataSourceDescription().toString());
    }

    virtual void createDirectorySymlink(const String &, const String &)
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Method createDirectorySymlink is not implemented for disk type: {}",
            getDataSourceDescription().toString());
    }

    virtual String readSymlink(const fs::path &) const
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Method readSymlink is not implemented for disk type: {}",
            getDataSourceDescription().toString());
    }

    virtual bool equivalent(const String &, const String &) const
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED, "Method equivalent is not implemented for disk type: {}", getDataSourceDescription().toString());
    }

    virtual bool equivalentNoThrow(const String &, const String &) const
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED, "Method equivalent is not implemented for disk type: {}", getDataSourceDescription().toString());
    }

    /// Truncate file to specified size.
    virtual void truncateFile(const String & path, size_t size);

    /// Return data source description
    virtual DataSourceDescription getDataSourceDescription() const = 0;

    /// Involves network interaction.
    virtual bool isRemote() const = 0;

    /// Whether this disk support zero-copy replication.
    /// Overrode in remote fs disks.
    virtual bool supportZeroCopyReplication() const = 0;

    /// Whether this disk support parallel write
    /// Overrode in remote fs disks.
    virtual bool supportParallelWrite() const { return false; }

    virtual bool isReadOnly() const { return false; }

    /// If the disk is plain object storage.
    virtual bool isPlain() const { return false; }

    virtual bool isWriteOnce() const { return false; }

    virtual bool supportsHardLinks() const { return true; }

    /// Check if disk is broken. Broken disks will have 0 space and cannot be used.
    virtual bool isBroken() const { return false; }

    /// Invoked when Global Context is shutdown.
    virtual void shutdown() {}

    /// Performs access check and custom action on disk startup.
    // 磁盘启动入口：若未跳过检查则调用 checkAccess() 验证读写权限，随后调用 startupImpl()。
    void startup(bool skip_access_check);

    /// Performs custom action on disk startup.
    virtual void startupImpl() {}

    /// If the state can be changed under the hood and become outdated in memory, perform a reload if necessary.
    /// but don't do it more frequently than the specified parameter.
    /// Note: for performance reasons, it's allowed to assume that only some subset of changes are possible
    /// (those that MergeTree tables can make).
    // 当底层存储状态可能被外部进程/其他节点修改导致内存元数据过期时，按最小间隔参数触发状态刷新（默认空实现）。
    virtual void refresh(UInt64 /* not_sooner_than_milliseconds */)
    {
        /// The default no-op implementation when the state in memory cannot be out of sync of the actual state.
    }

    /// Return some uniq string for file, overrode for IDiskRemote
    /// Required for distinguish different copies of the same part on remote disk
    virtual String getUniqueId(const String & path) const { return path; }

    /// Check file exists and ClickHouse has an access to it
    /// Overrode in remote FS disks (s3/hdfs)
    /// Required for remote disk to ensure that the replica has access to data written by other node
    virtual bool checkUniqueId(const String & id) const { return existsFile(id); }

    /// Returns guard, that insures synchronization of directory metadata with storage device.
    virtual SyncGuardPtr getDirectorySyncGuard(const String & path) const;

    /// Applies new settings for disk in runtime.
    // 支持在运行时热加载修改后的磁盘配置参数。
    virtual void applyNewSettings(const Poco::Util::AbstractConfiguration & config, ContextPtr context, const String & config_prefix, const DisksMap & map);

    /// Quite leaky abstraction. Some disks can use additional disk to store
    /// some parts of metadata. In general case we have only one disk itself and
    /// return pointer to it.
    ///
    /// Actually it's a part of IDiskRemote implementation but we have so
    /// complex hierarchy of disks (with decorators), so we cannot even
    /// dynamic_cast some pointer to IDisk to pointer to IDiskRemote.
    virtual MetadataStoragePtr getMetadataStorage()
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Method getMetadataStorage is not implemented for disk type: {}",
            getDataSourceDescription().toString());
    }

    /// Very similar case as for getMetadataDiskIfExistsOrSelf(). If disk has "metadata"
    /// it will return mapping for each required path: path -> metadata as string.
    /// Only for IDiskRemote.
    virtual std::unordered_map<String, String> getSerializedMetadata(const std::vector<String> & /* paths */) const { return {}; }

    /// Return reference count for remote FS.
    /// You can ask -- why we have zero and what does it mean? For some unknown reason
    /// the decision was made to take 0 as "no references exist", but only file itself left.
    /// With normal file system we will get 1 in this case:
    /// $ stat clickhouse
    ///  File: clickhouse
    ///  Size: 3014014920      Blocks: 5886760    IO Block: 4096   regular file
    ///  Device: 10301h/66305d   Inode: 3109907     Links: 1
    /// Why we have always zero by default? Because normal filesystem
    /// manages hardlinks by itself. So you can always remove hardlink and all
    /// other alive hardlinks will not be removed.
    virtual UInt32 getRefCount(const String &) const { return 0; }

    /// Revision is an incremental counter of disk operation.
    /// Revision currently exisis only in DiskS3.
    /// It is used to save current state during backup and restore that state from backup.
    /// This method sets current disk revision if it lower than required.
    virtual void syncRevision(UInt64) {}
    /// Return current disk revision.
    virtual UInt64 getRevision() const { return 0; }

    virtual ObjectStoragePtr getObjectStorage()
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Method getObjectStorage is not implemented for disk type: {}",
            getDataSourceDescription().toString());
    }

    virtual bool supportsStat() const { return false; }
    virtual struct stat stat(const String & /*path*/) const { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Disk does not support stat"); }

    virtual bool supportsChmod() const { return false; }
    virtual void chmod(const String & /*path*/, mode_t /*mode*/) { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Disk does not support chmod"); }

    /// Was disk created to be used without storage configuration?
    bool isCustomDisk() const { return custom_disk_settings_hash != 0; }
    UInt128 getCustomDiskSettings() const { return custom_disk_settings_hash; }
    void markDiskAsCustom(UInt128 settings_hash) { custom_disk_settings_hash = settings_hash; }

    virtual DiskPtr getDelegateDiskIfExists() const { return nullptr; }

#if USE_AWS_S3
    virtual std::shared_ptr<const S3::Client> getS3StorageClient() const
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Method getS3StorageClient is not implemented for disk type: {}",
            getDataSourceDescription().toString());
    }

    virtual std::shared_ptr<const S3::Client> tryGetS3StorageClient() const { return nullptr; }
#endif

    bool isCaseInsensitive();

protected:
    // 磁盘在配置文件中定义的唯一名称（如 "default"、"s3_disk"），构造后不可更改
    const String name;

    /// Base implementation of the function copy().
    /// It just opens two files, reads data by portions from the first file, and writes it to the second one.
    /// A derived class may override copy() to provide a faster implementation.
    void copyThroughBuffers(
        const String & from_path,
        const std::shared_ptr<IDisk> & to_disk,
        const String & to_path,
        const ReadSettings & read_settings,
        WriteSettings write_settings,
        const std::function<void()> & cancellation_hook);

    virtual void checkAccessImpl(const String & path);

private:
    // 专用于 copyDirectoryContent 跨盘/跨目录并发拷贝多个文件时的内部线程池（按需懒初始化）。
    std::unique_ptr<ThreadPool> copying_thread_pool;
    // 0 means the disk is not custom, the disk is predefined in the config
    // 自定义磁盘（未在 config.xml 中预定义，而是通过 SQL SETTINGS disk = disk(...) 动态创建的临时磁盘）的配置哈希值；0 表示配置文件预定义磁盘。
    UInt128 custom_disk_settings_hash = 0;

    /// True if underlying filesystem is case-insensitive,
    /// e.g. file_name and FILE_NAME are the same files.
    // 原子布尔标志，缓存底层文件系统是否大小写不敏感（如 macOS APFS / Windows NTFS）。
    std::atomic_bool is_case_insensitive = false;
    // 原子布尔标志，记录是否已经完成过大小写敏感性探测（用于双检锁避免重复探测）。
    std::atomic_bool is_case_sensitivity_checked = false;
    // 互斥锁，保护首次执行大小写敏感性探测时的并发安全。
    std::mutex case_sensitivity_check_mutex;

    /// Check access to the disk.
    void checkAccess();
};

using Disks = std::vector<DiskPtr>;

/**
 * Information about reserved size on particular disk.
 */
// 已预留磁盘空间凭据
class IReservation : boost::noncopyable
{
public:
    /// Get reservation size.
    // 获取当前预留凭据锁定的字节数。
    virtual UInt64 getSize() const = 0;

    /// Space available for reservation
    /// (with this reservation already take into account).
    // 获取所在磁盘扣除本次及所有其他预留后剩余的未预留可用字节数。
    virtual std::optional<UInt64> getUnreservedSpace() const = 0;

    /// Get i-th disk where reservation take place.
    // 获取第 i 块发生预留的磁盘智能指针。
    virtual DiskPtr getDisk(size_t i = 0) const = 0; /// NOLINT

    /// Get all disks, used in reservation
    // 获取本次预留涉及的所有磁盘列表。
    virtual Disks getDisks() const = 0;

    /// Changes amount of reserved space.
    // 动态调整已预留的空间大小（例如写入实际大小小于预估大小时释放多余预留）。
    virtual void update(UInt64 new_size) = 0;

    /// Unreserves reserved space.
    virtual ~IReservation() = default;
};

/// Return full path to a file on disk.
// 将 disk->getPath() 与相对路径 path 拼接为完整绝对路径。
inline String fullPath(const DiskPtr & disk, const String & path)
{
    return fs::path(disk->getPath()) / path;
}

/// Return parent path for the specified path.
// 返回指定路径的父目录路径（末尾带 /）。
inline String parentPath(const String & path)
{
    if (path == "/")
        return "/";
    if (path.ends_with('/'))
        return fs::path(path).parent_path().parent_path() / "";
    return fs::path(path).parent_path() / "";
}

/// Return file name for the specified path.
// 提取路径末尾的文件名部分。
inline String fileName(const String & path)
{
    return fs::path(path).filename();
}

/// Return directory path for the specified path.
// 提取文件所属的目录路径（末尾带 /）。
inline String directoryPath(const String & path)
{
    return fs::path(path).parent_path() / "";
}


}

// 可以直接传入 ClickHouse 的 fmt 日志和异常格式化字符串中。
template <>
struct fmt::formatter<fs::path> : fmt::formatter<std::string>
{
    template <typename FormatCtx>
    auto format(const fs::path & path, FormatCtx & ctx) const
    {
        return fmt::formatter<std::string>::format(path.string(), ctx);
    }
};
