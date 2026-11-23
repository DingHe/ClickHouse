#pragma once

#include <Disks/DiskObjectStorage/Replication/Location.h>
#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/DiskObjectStorageMetadata.h>
#include <Disks/DiskObjectStorage/MetadataStorages/MetadataOperationsHolder.h>
#include <Disks/DiskObjectStorage/MetadataStorages/Local/MetadataStorageFromDiskTransactionOperations.h>
#include <Disks/DiskObjectStorage/MetadataStorages/MetadataStorageTransactionState.h>
#include <Disks/DiskObjectStorage/MetadataStorages/InMemoryRemovalQueue.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <Disks/IDisk.h>

#include <Common/ObjectStorageKeyGenerator.h>
#include <Common/SharedMutex.h>

#include <base/defines.h>

namespace DB
{

/// Stores metadata on a separate disk
/// (used for object storages, like S3 and related).
// 定义了 ClickHouse 对象存储磁盘（如 DiskObjectStorage）的本地磁盘元数据存储实现类 MetadataStorageFromDisk 及其对应的本地磁盘元数据事务类 MetadataStorageFromDiskTransaction。
// MetadataStorageFromDisk（基于独立磁盘的本地元数据存储）
// 在对象存储（如 S3、GCS、Azure Blob）模式下，数据对象存放在远端对象存储中，而目录树、逻辑文件路径、文件大小、底层 Blob 映射列表以及“待删除死 Blob 队列”则保存在一块独立的磁盘（通常为本地 SSD/NVMe 盘，即 disk 成员）上。
// 读写锁并发控制：使用 SharedMutex metadata_mutex 保护元数据读写，允许多个读线程并发查询元数据，写事务提交时独占修改。
class MetadataStorageFromDisk final : public IMetadataStorage
{
private:
    friend class MetadataStorageFromDiskTransaction;
    // 读写锁，协调并发元数据读取（共享锁）与元数据事务变更（独占锁）。
    mutable SharedMutex metadata_mutex;
    // 承载逻辑目录与元数据文件的底层磁盘指针（通常为本地盘）。
    const DiskPtr disk;
    // 用于兼容历史或特定对象键布局的路径前缀字符串。
    const std::string compatible_key_prefix;
    // 对象存储键生成器，负责为新建 Blob 生成唯一/随机 Key。
    const ObjectStorageKeyGeneratorPtr key_generator;
    // 互斥锁，保护内存待删除队列 objects_to_remove
    mutable std::mutex removed_objects_mutex;
    // 内存中的待删除死 Blob 队列，管理因文件删除/覆盖而无人引用的远端对象。
    InMemoryRemovalQueue objects_to_remove TSA_GUARDED_BY(removed_objects_mutex);
    // 元数据存储内部系统子目录名，用于存放内部状态文件。
    static constexpr std::string_view SYSTEM_METADATA_DIR = ".metadata";
    // 待删除 Blob 队列的持久化追加日志路径。
    static constexpr std::string_view REMOVAL_LOG_FILE = ".metadata/blobs_to_remove.log";
    // 删除日志二进制格式版本号。
    enum RemovalLogVersion : UInt32
    {
        V0 = 0, /// initial binary format
    };
    // 当前使用的删除日志版本。
    static constexpr RemovalLogVersion REMOVAL_LOG_CURRENT_VERSION = RemovalLogVersion::V0;
    // 日志记录类型枚举，分别表示“新增待删除 Blob”与“已确认删除 Blob”。
    enum RemovalLogEntryType : UInt8
    {
        ADD = 0,
        REMOVED = 1,
    };

    /// Persistence for the removal queue. Returns true if compaction is needed (version mismatch or truncated entries).
    bool loadRemovalLog() TSA_REQUIRES(removed_objects_mutex);
    void appendToRemovalLog(RemovalLogEntryType entry_type, const StoredObjects & blobs) TSA_REQUIRES(removed_objects_mutex);
    void compactRemovalLog() TSA_REQUIRES(removed_objects_mutex);

    /// Number of REMOVED entries in the log that haven't been compacted yet.
    size_t removal_log_stale_entries TSA_GUARDED_BY(removed_objects_mutex) = 0;

    bool persist_removal_queue;
    size_t removal_log_compaction_threshold;
    LoggerPtr log;

public:
    MetadataStorageFromDisk(DiskPtr disk_, String compatible_key_prefix_, ObjectStorageKeyGeneratorPtr key_generator_, bool persist_removal_queue_, size_t removal_log_compaction_threshold_);

    MetadataTransactionPtr createTransaction() override;

    ObjectStorageKeyGeneratorPtr getKeyGenerator() const override { return key_generator; }

    bool supportWritingWithAppend() const override;

    const std::string & getPath() const override;

    MetadataStorageType getType() const override { return MetadataStorageType::Local; }

    /// Metadata on disk for an empty file can store empty list of blobs and size=0
    bool supportsEmptyFilesWithoutBlobs() const override { return true; }
    bool areBlobPathsRandom() const override { return true; }

    bool existsFile(const std::string & path) const override;
    bool existsDirectory(const std::string & path) const override;
    bool existsFileOrDirectory(const std::string & path) const override;

    uint64_t getFileSize(const String & path) const override;

    Poco::Timestamp getLastModified(const std::string & path) const override;

    time_t getLastChanged(const std::string & path) const override;

    bool supportsChmod() const override { return disk->supportsChmod(); }

    bool supportsStat() const override { return disk->supportsStat(); }

    struct stat stat(const String & path) const override { return disk->stat(path); }

    std::vector<std::string> listDirectory(const std::string & path) const override;

    DirectoryIteratorPtr iterateDirectory(const std::string & path) const override;

    std::string readFileToString(const std::string & path) const override;

    std::string readInlineDataToString(const std::string & path) const override;

    std::unordered_map<String, String> getSerializedMetadata(const std::vector<String> & file_paths) const override;

    uint32_t getHardlinkCount(const std::string & path) const override;
    // 返回承载元数据的底层 DiskPtr。
    DiskPtr getDisk() const { return disk; }

    StoredObjects getStorageObjects(const std::string & path) const override;

    DiskObjectStorageMetadataPtr readMetadata(const std::string & path) const;

    DiskObjectStorageMetadataPtr readMetadataUnlocked(const std::string & path, std::unique_lock<SharedMutex> & lock) const;
    DiskObjectStorageMetadataPtr readMetadataUnlocked(const std::string & path, std::shared_lock<SharedMutex> & lock) const;

    bool isReadOnly() const override { return disk->isReadOnly(); }

    void startup() override;

    BlobsToRemove getBlobsToRemove(const ClusterConfigurationPtr & cluster, int64_t max_count) override;
    int64_t recordAsRemoved(const StoredObjects & blobs) override;
    bool hasPendingRemovalBlobs(const StoredObjects & blobs) const override;
    int64_t getDeadBlobsQueueEstimate() override;
};
// 基于磁盘元数据操作的事务类
// 收集单次元数据变更过程中的操作对象（MetadataOperationsHolder operations）以及因删除/替换而失效的 Blob 列表（objects_to_remove）；在 commit / tryCommit 时执行这些元数据操作，并将失效 Blob 纳入待清理队列。
class MetadataStorageFromDiskTransaction final : public IMetadataTransaction
{
private:
    MetadataStorageFromDisk & metadata_storage;

    /// We collect all removed in transaction blobs here. After successful tx commit
    /// these blobs will be scheduled for background removal (into in-memory queue of outdated blobs).
    StoredObjects objects_to_remove;
    MetadataOperationsHolder operations;

public:
    explicit MetadataStorageFromDiskTransaction(MetadataStorageFromDisk & metadata_storage_);

    void commit(const TransactionCommitOptionsVariant & options) override;
    TransactionCommitOutcomeVariant tryCommit(const TransactionCommitOptionsVariant &options) override;

    void writeStringToFile(const std::string & path, const std::string & data) override;

    void writeInlineDataToFile(const std::string & path, const std::string & data) override;

    void createMetadataFile(const std::string & path, const StoredObjects & objects) override;

    void addBlobToMetadata(const std::string & path, const StoredObject & object) override;

    void setLastModified(const std::string & path, const Poco::Timestamp & timestamp) override;

    bool supportsChmod() const override { return metadata_storage.supportsChmod(); }

    void chmod(const String & path, mode_t mode) override;

    void setReadOnly(const std::string & path) override;

    void unlinkFile(const std::string & path, bool if_exists, bool should_remove_objects) override;

    void createDirectory(const std::string & path) override;

    void createDirectoryRecursive(const std::string & path) override;

    void removeDirectory(const std::string & path) override;

    void removeRecursive(const std::string & path, const ShouldRemoveObjectsPredicate & should_remove_objects) override;

    void createHardLink(const std::string & path_from, const std::string & path_to) override;

    void moveFile(const std::string & path_from, const std::string & path_to) override;

    void moveDirectory(const std::string & path_from, const std::string & path_to) override;

    void replaceFile(const std::string & path_from, const std::string & path_to) override;

    void truncateFile(const std::string & src_path, size_t target_size) override;

    ObjectStorageKey generateObjectKeyForPath(const std::string & path) override;
    StoredObjects getSubmittedForRemovalBlobs() override;
};

}
