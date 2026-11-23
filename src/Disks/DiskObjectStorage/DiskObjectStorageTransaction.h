#pragma once

#include <Disks/DiskObjectStorage/DiskObjectStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <Disks/DiskObjectStorage/Replication/ObjectStorageRouter.h>
#include <Disks/DiskObjectStorage/Replication/ClusterConfiguration.h>
#include <Disks/IDiskTransaction.h>

#include <Common/ThreadPool_fwd.h>

#include <memory>

namespace DB
{

/// Disk object storage transaction, actually implement some part of disk object storage
/// logic. Works on top of non atomic operations with blobs and possibly atomic implementation
/// of metadata storage.
///
/// Commit works like:
/// 1. Execute all accumulated operations in loop.
/// 2. Commit metadata transaction.
// 对象存储（Blob Store）本身通常不具备 POSIX 目录重命名、硬链接或多文件原子事务能力，因此 DiskObjectStorage 将数据对象（Blobs）与路径/目录/引用计数元数据（IMetadataStorage）分离。
// DiskObjectStorageTransaction 负责协调这两层状态：
// 分阶段协调提交（两步提交流程）：
// 第一步：循环执行累积在 operations_to_execute 中的所有元数据回调操作（op(metadata_transaction)），填充或驱动底层 metadata_transaction。
// 第二步：提交底层 metadata_transaction，使整批文件与目录变更在同一时刻对外可见。
// 区分“即时生效（Eager）”与“延迟排队（Queued）”的元数据存储：
// 某些元数据存储实现（如本地元数据/内存状态）可能在操作加入时立即应用，而某些事务型元数据后端（如 Keeper/ZooKeeper 或事务封装）则需要将操作排队并在提交阶段统一执行。addOperation 封装了这一差异。
// 跟踪已写入 Blob 与后台垃圾回收（BlobKillerThread）：
// 记录事务中各存储位置（Location）新写入的 StoredObject 集合（written_blobs），并在回滚（undo）或删除过期数据时协同后台清理线程回收无用对象，支持按需等待删除完成（wait_blob_removal）。
struct DiskObjectStorageTransaction : public IDiskTransaction, public std::enable_shared_from_this<DiskObjectStorageTransaction>
{
    void waitBlobRemoval(const StoredObjects & blobs) const;

protected:
    // 指向集群配置信息，描述副本/节点及存储位置拓扑关系，用于在多副本或跨位置对象存储操作中解析节点与位置信息。
    const ClusterConfigurationPtr cluster;
    // 指向底层元数据存储接口（IMetadataStorage），负责维护逻辑文件路径到远端对象存储 Blob 的映射关系、目录结构及 Blob 引用计数。
    const MetadataStoragePtr metadata_storage;
    // 对象存储路由器，根据存储位置（Location）或路由策略选择与访问对应的底层对象存储实例（IObjectStorage）。
    const ObjectStorageRouterPtr object_storages;
    // 后台异步删除无用 Blob 的线程管理器指针，用于卸载或延迟清理不再被引用的远端对象，降低前台写/删/回滚路径的延迟。
    const BlobKillerThreadPtr blob_killer;
    /// Thread pool used by `copyFile` to dispatch `copyObjectToAnotherObjectStorage`
    /// calls in parallel. Owned by `DiskObjectStorage` and shared across transactions.
    // 由 DiskObjectStorage 拥有并在多个事务间共享的线程池，供 copyFile 在并行执行跨对象存储拷贝（copyObjectToAnotherObjectStorage）时调度任务。
    const std::shared_ptr<ThreadPool> copy_object_pool;
    // 控制标志位，指示在相关清理/回滚场景中是否需要同步阻塞等待 Blob 删除操作完成。
    const bool wait_blob_removal;
    // 当前磁盘事务绑定的底层元数据事务句柄，所有元数据变更最终都在该元数据事务上下文中累积并提交。
    MetadataTransactionPtr metadata_transaction;
    // 待执行的元数据操作函数队列。每个元素是一个接收 MetadataTransactionPtr 的可调用对象，在提交阶段按顺序执行。
    std::vector<std::function<void(MetadataTransactionPtr tx)>> operations_to_execute;
    // 按存储位置 Location 分组记录本次事务中已经写入/生成的 StoredObjects 列表，便于回滚时清理孤儿对象或追踪已落盘 Blob。
    std::unordered_map<Location, StoredObjects> written_blobs;

    /// Execute the operation right away if the metadata storage applies operations eagerly,
    /// otherwise queue it until commit.
    void addOperation(std::function<void(MetadataTransactionPtr tx)> op);

public:
    /// Record locations still missing the blob, for blobs without a per-file metadata node.
    void recordBlobReplication(const StoredObject & object, const Locations & missing_locations);

    /// Register a blob for background removal, committed atomically with this transaction.
    void submitBlobForRemoval(const std::string & remote_path);

    /// The metadata transaction this disk transaction executes against.
    MetadataTransactionPtr getMetadataTransaction() const { return metadata_transaction; }

    DiskObjectStorageTransaction(
        ClusterConfigurationPtr cluster_,
        MetadataStoragePtr metadata_storage_,
        ObjectStorageRouterPtr object_storages_,
        BlobKillerThreadPtr blob_killer_,
        std::shared_ptr<ThreadPool> copy_object_pool_,
        bool wait_blob_removal_);

    void commit() override;
    TransactionCommitOutcomeVariant tryCommit(const TransactionCommitOptionsVariant & options) override;
    void undo() noexcept override;

    void createDirectory(const std::string & path) override;

    void createDirectories(const std::string & path) override;

    void moveDirectory(const std::string & from_path, const std::string & to_path) override;

    void moveFile(const String & from_path, const String & to_path) override;

    void replaceFile(const std::string & from_path, const std::string & to_path) override;

    void createFile(const String & path) override;

    void truncateFile(const String & path, size_t size) override;

    void incrementBlobRefCount(const std::string & blob) override;

    void decrementBlobRefCount(const std::string & blob) override;

    void copyFile(const std::string & from_file_path, const std::string & to_file_path, const ReadSettings & read_settings, const WriteSettings &) override;

    /// writeFile is a difficult function for transactions.
    /// Now it's almost noop because metadata added to transaction in finalize method
    /// of write buffer. Autocommit means that transaction will be immediately committed
    /// after returned buffer will be finalized.
    std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const std::string & path,
        size_t buf_size,
        WriteMode mode,
        const WriteSettings & settings) override;
    std::unique_ptr<WriteBufferFromFileBase> writeFileWithAutoCommit(
        const std::string & path,
        size_t buf_size,
        WriteMode mode,
        const WriteSettings & settings) override;

    /// Write a file using a custom function to write an object to the disk's object storage.
    void writeFileUsingBlobWritingFunction(const String & path, WriteMode mode, WriteBlobFunction && write_blob_function) override;

    void removeFile(const std::string & path) override;
    void removeFileIfExists(const std::string & path) override;
    void removeDirectory(const std::string & path) override;
    void removeRecursive(const std::string & path) override;

    void removeSharedFile(const std::string & path, bool keep_shared_data) override;
    void removeSharedRecursive(const std::string & path, bool keep_all_shared_data, const NameSet & file_names_remove_metadata_only) override;
    void removeSharedFileIfExists(const std::string & path, bool keep_shared_data) override;
    void removeSharedFiles(const RemoveBatchRequest & files, bool keep_all_batch_data, const NameSet & file_names_remove_metadata_only) override;

    void setLastModified(const std::string & path, const Poco::Timestamp & timestamp) override;
    void chmod(const String & path, mode_t mode) override;
    void setReadOnly(const std::string & path) override;
    void createHardLink(const std::string & src_path, const std::string & dst_path) override;

protected:
    /// Shared between `DiskObjectStorageTransaction::copyFile` and
    /// `MultipleDisksObjectStorageTransaction::copyFile`. Reads source blobs from the
    /// passed-in source triple and writes them onto this transaction's destination
    /// (`metadata_transaction`, `object_storages`, `written_blobs`, `operations_to_execute`).
    void copyFileImpl(
        const MetadataStoragePtr & src_metadata_storage,
        const ClusterConfigurationPtr & src_cluster,
        const ObjectStorageRouterPtr & src_object_storages,
        const std::string & from_file_path,
        const std::string & to_file_path,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings);

private:
    std::unique_ptr<WriteBufferFromFileBase> writeFileImpl( /// NOLINT
        bool autocommit,
        const std::string & path,
        size_t buf_size,
        WriteMode mode,
        const WriteSettings & settings);
};

/// Only needed for S3 server side object copy
struct MultipleDisksObjectStorageTransaction final : public DiskObjectStorageTransaction
{
    // 源端集群配置指针，用于读取源对象存储中的文件/Blob 信息。
    ClusterConfigurationPtr source_cluster;
    // 源端元数据存储指针，用于查询源文件路径对应的底层 Blob 列表及属性。
    MetadataStoragePtr source_metadata_storage;
    // 源端对象存储路由器指针，用于定位和读取源端对象存储中的 Blob 数据。
    ObjectStorageRouterPtr source_object_storages;

    MultipleDisksObjectStorageTransaction(
        ClusterConfigurationPtr source_cluster_,
        MetadataStoragePtr source_metadata_storage_,
        ObjectStorageRouterPtr source_object_storages_,
        ClusterConfigurationPtr destination_cluster_,
        MetadataStoragePtr destination_metadata_storage_,
        ObjectStorageRouterPtr destination_object_storages_,
        std::shared_ptr<ThreadPool> copy_object_pool_);

    void copyFile(const std::string & from_file_path, const std::string & to_file_path, const ReadSettings & read_settings, const WriteSettings &) override;
};

using DiskObjectStorageTransactionPtr = std::shared_ptr<DiskObjectStorageTransaction>;

}
