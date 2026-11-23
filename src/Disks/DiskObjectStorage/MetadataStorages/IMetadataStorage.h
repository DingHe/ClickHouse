#pragma once

#include <memory>
#include <optional>
#include <vector>
#include <unordered_map>
#include <Poco/Timestamp.h>
#include <IO/ReadSettings.h>
#include <IO/WriteSettings.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Disks/DirectoryIterator.h>
#include <Disks/WriteMode.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <Disks/DiskObjectStorage/Replication/ClusterConfiguration.h>
#include <Disks/DiskObjectStorage/Replication/Location.h>
#include <Disks/DiskCommitTransactionOptions.h>
#include <Disks/DiskType.h>
#include <Common/ErrorCodes.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

/// Tries to provide some "transactions" interface, which allow
/// to execute (commit) operations simultaneously. We don't provide
/// any snapshot isolation here, so no read operations in transactions
/// interface. This transaction is more like "batch operation" than real "transaction".
///
/// But for better usability we can get MetadataStorage interface and use some read methods.
// IMetadataTransaction.h 定义了 ClickHouse 对象存储磁盘（如 DiskObjectStorage）元数据层的两个核心抽象接口：元数据事务接口 IMetadataTransaction 和 元数据存储接口 IMetadataStorage。
// IMetadataTransaction（元数据批量操作/事务接口）
// 在对象存储磁盘中，数据体存放在远端 Blob 存储（如 S3），而目录树、逻辑文件路径、文件属性、Blob 映射关系以及引用计数存放在元数据层。IMetadataTransaction 负责收集并批量提交（或尝试提交）一组元数据变更操作。
//
class IMetadataTransaction : private boost::noncopyable
{
public:
    // 提交事务中累积的所有元数据操作；失败时抛出异常。
    virtual void commit(const TransactionCommitOptionsVariant & options) = 0;
    // 尝试提交元数据事务并返回提交结果变体
    virtual TransactionCommitOutcomeVariant tryCommit(const TransactionCommitOptionsVariant & options) = 0;

    /// General purpose methods

    /// Write metadata string to file
    // 参数：path（元数据文件路径）；data（写入的字符串内容）。
    // 将字符串元数据写入指定文件
    virtual void writeStringToFile(const std::string & /* path */, const std::string & /* data */)
    {
        throwNotImplemented();
    }

    /// Writes the data inline with the metadata
    // 将小文件数据直接内联存储在元数据中，无需创建独立远端 Blob
    virtual void writeInlineDataToFile(const std::string & /* path */, const std::string & /* data */)
    {
        throwNotImplemented();
    }
    // 设置文件或目录的最后修改时间
    virtual void setLastModified(const std::string & /* path */, const Poco::Timestamp & /* timestamp */)
    {
        throwNotImplemented();
    }
    // 返回当前元数据事务/存储是否支持修改权限
    virtual bool supportsChmod() const = 0;
    // 修改指定路径的权限属性
    virtual void chmod(const String & /* path */, mode_t /* mode */)
    {
        throwNotImplemented();
    }
    // 将指定文件设为只读
    virtual void setReadOnly(const std::string & /* path */)
    {
        throwNotImplemented();
    }
    // 解除/删除文件元数据，并按参数决定是否移除底层对象
    virtual void unlinkFile(const std::string & /* path */, bool /* if_exists */, bool /* should_remove_objects */)
    {
        throwNotImplemented();
    }
    // 创建单级目录元数据
    virtual void createDirectory(const std::string & /* path */)
    {
        throwNotImplemented();
    }
    // 递归创建目录及缺失的父目录元数据
    virtual void createDirectoryRecursive(const std::string & /* path */)
    {
        throwNotImplemented();
    }
    // 删除空目录元数据
    virtual void removeDirectory(const std::string & /* path */)
    {
        throwNotImplemented();
    }
    // 递归删除目录时，用于判断指定相对路径对应的底层对象是否应当被一并删除的谓词回调。
    using ShouldRemoveObjectsPredicate = std::function<bool(const std::string & relative_path)>;
    // 递归删除目录/文件元数据，并通过谓词决定哪些底层对象需要删除
    virtual void removeRecursive(const std::string & /* path */, const ShouldRemoveObjectsPredicate & /* should_remove_objects */)
    {
        throwNotImplemented();
    }
    // 在元数据层建立硬链接映射（共享底层 Blob）
    virtual void createHardLink(const std::string & /* path_from */, const std::string & /* path_to */)
    {
        throwNotImplemented();
    }
    // 移动/重命名文件元数据
    virtual void moveFile(const std::string & /* path_from */, const std::string & /* path_to */)
    {
        throwNotImplemented();
    }
    // 移动/重命名目录元数据
    virtual void moveDirectory(const std::string & /* path_from */, const std::string & /* path_to */)
    {
        throwNotImplemented();
    }
    // 用 path_from 替换 path_to 的元数据
    virtual void replaceFile(const std::string & /* path_from */, const std::string & /* path_to */)
    {
        throwNotImplemented();
    }

    /// Metadata related methods

    /// Generate blob name for passed absolute local path.
    /// Path can be generated either independently or based on `path`.
    // 为指定路径生成对象存储中的键（ObjectStorageKey，可基于路径或随机生成）。
    virtual ObjectStorageKey generateObjectKeyForPath(const std::string & path) = 0;
    // 记录 Blob 在哪些位置尚待复制；若 missing_locations 非空且未实现则抛出 NOT_IMPLEMENTED。
    virtual void recordBlobsReplication(const StoredObject & /*blob*/, const Locations & missing_locations)
    {
        if (!missing_locations.empty())
            throwNotImplemented();
    }
    // 获取当前事务中已提交登记待删除的 StoredObjects 列表。
    virtual StoredObjects getSubmittedForRemovalBlobs() = 0;

    /// Create metadata file on paths with content consisting of objects
    // 在 path 创建元数据文件，将其关联到指定的 objects。
    virtual void createMetadataFile(const std::string & path, const StoredObjects & objects) = 0;

    /// Add to new blob to metadata file (way to implement appends).
    // 向已有元数据文件追加新的 Blob 记录
    virtual void addBlobToMetadata(const std::string & /* path */, const StoredObject & /* object */)
    {
        throwNotImplemented();
    }
    // 在元数据层截断文件到指定大小；默认抛出 NOT_IMPLEMENTED。
    virtual void truncateFile(const std::string & /* path */, size_t /* size */)
    {
        throwNotImplemented();
    }

    /// Increment the reference count of a data blob shared between metadata files.
    // 递增共享 Blob 的引用计数
    virtual void incrementBlobRefCount(const std::string & /* blob */)
    {
        throwNotImplemented();
    }

    /// Decrement the reference count of a data blob; at zero the blob becomes eligible for removal.
    // 递减共享 Blob 的引用计数（归零时具备回收资格）
    virtual void decrementBlobRefCount(const std::string & /* blob */)
    {
        throwNotImplemented();
    }

    /// Register a data blob for background removal, atomically with the commit.
    // 随事务提交将远端 Blob 注册为待后台删除对象
    virtual void submitBlobForRemoval(const std::string & /* remote_path */)
    {
        throwNotImplemented();
    }

    virtual ~IMetadataTransaction() = default;

protected:
    [[noreturn]] static void throwNotImplemented()
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Operation is not implemented");
    }
};

using MetadataTransactionPtr = std::shared_ptr<IMetadataTransaction>;

/// Metadata storage for remote disks like DiskObjectStorage.
/// Support some subset of Disk operations, allow to read/write only
/// small amounts of data (strings).
class IMetadataStorage : private boost::noncopyable
{
public:
    // 创建一个新的元数据事务实例
    virtual MetadataTransactionPtr createTransaction() = 0;

    /// Get metadata root path.
    // 返回元数据存储的根路径。
    virtual const std::string & getPath() const = 0;
    // 返回元数据存储的具体类型枚举值（MetadataStorageType）
    virtual MetadataStorageType getType() const = 0;
    // 若使用 ZooKeeper/Keeper 存储元数据，返回对应的 ZK 集群名称（否则返回空字符串）。
    virtual std::string getZooKeeperName() const { return ""; }
    // 若使用 ZooKeeper/Keeper 存储元数据，返回元数据在 ZK 中的根路径（否则返回空字符串）。
    virtual std::string getZooKeeperPath() const { return ""; }

    /// Returns true if empty file can be created without any blobs in the corresponding object storage.
    /// E.g. metadata storage can store the empty list of blobs corresponding to a file without actually storing any blobs.
    /// But if the metadata storage just relies on for example local FS to store data under logical path, then a file has to be created even if it's empty.
    // 返回是否允许创建无对应底层 Blob 的空文件元数据。
    virtual bool supportsEmptyFilesWithoutBlobs() const { return false; }

    /// Whether small file content can be stored inside the metadata itself, with no backing blob
    /// (see `WriteSettings::inline_file_max_bytes`).
    // 返回是否支持将小文件数据内联保存在元数据中
    virtual bool supportsInlineData() const { return false; }

    /// Returns true if underlying blob ids generator uses random.
    // 返回是否支持将小文件数据内联保存在元数据中
    virtual bool areBlobPathsRandom() const = 0;

    /// ==== General purpose methods. Define properties of object storage file based on metadata files ====
    // 判断指定逻辑文件是否存在
    virtual bool existsFile(const std::string & path) const = 0;
    // 判断指定逻辑目录是否存在
    virtual bool existsDirectory(const std::string & path) const = 0;
    // 判断指定路径处是否存在文件或目录。
    virtual bool existsFileOrDirectory(const std::string & path) const = 0;
    // 返回指定逻辑文件的字节大小。
    virtual uint64_t getFileSize(const std::string & path) const = 0;
    // 若文件存在则返回字节大小
    virtual std::optional<uint64_t> getFileSizeIfExists(const std::string & path) const
    {
        if (existsFile(path))
            return getFileSize(path);
        return std::nullopt;
    }

    virtual Poco::Timestamp getLastModified(const std::string & path) const = 0;

    virtual std::optional<Poco::Timestamp> getLastModifiedIfExists(const std::string & path) const
    {
        if (existsFileOrDirectory(path))
            return getLastModified(path);
        return std::nullopt;
    }

    virtual time_t getLastChanged(const std::string & /* path */) const
    {
        throwNotImplemented();
    }

    virtual bool supportsChmod() const = 0;

    virtual bool supportsStat() const = 0;
    virtual struct stat stat(const String & /* path */) const
    {
        throwNotImplemented();
    }
    // 列举指定目录下的条目名称列表。
    virtual std::vector<std::string> listDirectory(const std::string & path) const = 0;
    // 返回遍历指定目录的迭代器
    virtual DirectoryIteratorPtr iterateDirectory(const std::string & path) const = 0;

    virtual bool isDirectoryEmpty(const std::string & path) const
    {
        return !iterateDirectory(path)->isValid();
    }

    virtual uint32_t getHardlinkCount(const std::string & path) const = 0;

    /// Read metadata file to string from path
    // 将指定元数据文件内容读取为字符串
    virtual std::string readFileToString(const std::string & /* path */) const
    {
        throwNotImplemented();
    }

    /// Read inline data for file to string from path
    // 读取指定文件的内联数据为字符串
    virtual std::string readInlineDataToString(const std::string & /* path */) const
    {
        throwNotImplemented();
    }

    virtual void startup() {}
    virtual void shutdown()
    {
        /// This method is overridden for specific metadata implementations in ClickHouse Cloud.
    }

    /// If the state can be changed under the hood and become outdated in memory, perform a reload if necessary,
    /// but don't do it more frequently than the specified parameter.
    /// Note: for performance reasons, it's allowed to assume that only some subset of changes are possible
    /// (those that MergeTree tables can make).
    virtual void refresh(UInt64 /* not_sooner_than_milliseconds */)
    {
        /// The default no-op implementation when the state in memory cannot be out of sync of the actual state.
    }

    virtual ~IMetadataStorage() = default;

    /// ==== More specific methods. Previous were almost general purpose. ====

    /// Read multiple metadata files into strings and return mapping from file_path -> metadata
    virtual std::unordered_map<std::string, std::string> getSerializedMetadata(const std::vector<String> & /* file_paths */) const
    {
        throwNotImplemented();
    }

    /// Return object information (absolute_path, bytes_size, ...) for metadata path.
    /// object_storage_path is absolute.
    virtual StoredObjects getStorageObjects(const std::string & path) const = 0;

    virtual std::optional<StoredObjects> getStorageObjectsIfExist(const std::string & path) const
    {
        if (existsFile(path))
            return getStorageObjects(path);
        return std::nullopt;
    }

    virtual bool isReadOnly() const = 0;

    /// True if transactions apply operations immediately instead of accumulating them until commit.
    virtual bool appliesOperationsEagerly() const
    {
        return false;
    }

    /// The generator of object storage keys for new blobs, if this storage owns one.
    virtual ObjectStorageKeyGeneratorPtr getKeyGenerator() const
    {
        return nullptr;
    }

    virtual bool isTransactional() const
    {
        return false;
    }

    virtual bool isPlain() const
    {
        return false;
    }

    virtual bool isWriteOnce() const
    {
        return false;
    }
    // 待删除存储对象到其所在位置集合的映射。
    using BlobsToRemove = std::unordered_map<StoredObject, LocationSet>;
    virtual BlobsToRemove getBlobsToRemove(const ClusterConfigurationPtr & /*cluster*/, int64_t /*max_count*/) { return {}; }
    virtual int64_t recordAsRemoved(const StoredObjects & /*blobs*/) { return 0; }
    virtual bool hasPendingRemovalBlobs(const StoredObjects & /*blobs*/) const { return false; }
    virtual int64_t getDeadBlobsQueueEstimate() { return 0; }
    // 包含 StoredObject blob、Location from、Location to，描述一次跨位置 Blob 复制任务。
    struct BlobsReplication
    {
        StoredObject blob;
        Location from;
        Location to;
    };
    // 待复制任务列表。
    using BlobsToReplicate = std::vector<BlobsReplication>;
    virtual BlobsToReplicate getBlobsToReplicate(const ClusterConfigurationPtr & /*cluster*/, int64_t /*max_count*/) { return {}; }
    virtual int64_t recordAsReplicated(const BlobsToReplicate & /*blobs*/) { return 0; }
    virtual bool hasUnreplicatedBlobs(const Location & /*location_to_check*/) { return false; }
    virtual int64_t getMissingBlobsQueueEstimate() { return 0; }

    /// Re-read paths or their full subtrees from disk and update cache.
    /// Can return serialized description of cache update which can be used to populate cache on other nodes.
    virtual void updateCache(const std::vector<std::string> & /* paths */, bool /* recursive */, bool /* enforce_fresh */,
        std::string * /* serialized_cache_update_description */) {}
    /// Allows to apply cache update from serialized description.
    virtual void updateCacheFromSerializedDescription(const std::string & /* serialized_cache_update_description */) {}
    virtual void invalidateCache(const std::string & /* path */) {}

    /// Clear all cache content.
    virtual void dropCache() {}

    /// Apply configuration changes.
    virtual void applyNewSettings(
        const Poco::Util::AbstractConfiguration & /* config */,
        const std::string & /* config_prefix */,
        ContextPtr /* context */) {}

    /// True if write with Append mode supported.
    virtual bool supportWritingWithAppend() const { return false; }

protected:
    [[noreturn]] static void throwNotImplemented()
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Operation is not implemented");
    }
};

using MetadataStoragePtr = std::shared_ptr<IMetadataStorage>;

}
