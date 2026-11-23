#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/Local/MetadataStorageFromDisk.h>
#include <Disks/DiskObjectStorage/MetadataStorages/Local/MetadataStorageFromDiskTransactionOperations.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Common/logger_useful.h>

#include <memory>
#include <shared_mutex>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MetadataStorageFromDisk::MetadataStorageFromDisk(DiskPtr disk_, std::string compatible_key_prefix_, ObjectStorageKeyGeneratorPtr key_generator_, bool persist_removal_queue_, size_t removal_log_compaction_threshold_)
    : disk(disk_)
    , compatible_key_prefix(std::move(compatible_key_prefix_))
    , key_generator(std::move(key_generator_))
    , persist_removal_queue(persist_removal_queue_)
    , removal_log_compaction_threshold(removal_log_compaction_threshold_)
    , log(getLogger("MetadataStorageFromDisk"))
{
}

const std::string & MetadataStorageFromDisk::getPath() const
{
    return disk->getPath();
}

bool MetadataStorageFromDisk::existsFile(const std::string & path) const
{
    return disk->existsFile(path);
}

bool MetadataStorageFromDisk::existsDirectory(const std::string & path) const
{
    return disk->existsDirectory(path);
}

bool MetadataStorageFromDisk::existsFileOrDirectory(const std::string & path) const
{
    return disk->existsFileOrDirectory(path);
}

Poco::Timestamp MetadataStorageFromDisk::getLastModified(const std::string & path) const
{
    return disk->getLastModified(path);
}

time_t MetadataStorageFromDisk::getLastChanged(const std::string & path) const
{
    return disk->getLastChanged(path);
}

uint64_t MetadataStorageFromDisk::getFileSize(const String & path) const
{
    return getTotalSize(readMetadata(path)->objects);
}

std::vector<std::string> MetadataStorageFromDisk::listDirectory(const std::string & path) const
{
    std::vector<std::string> result_files;
    disk->listFiles(path, result_files);
    return result_files;
}

DirectoryIteratorPtr MetadataStorageFromDisk::iterateDirectory(const std::string & path) const
{
    return disk->iterateDirectory(path);
}

std::string MetadataStorageFromDisk::readFileToString(const std::string & path) const
{
    auto buf = disk->readFile(path, ReadSettings{});
    std::string result;
    readStringUntilEOF(result, *buf);
    return result;
}

std::string MetadataStorageFromDisk::readInlineDataToString(const std::string & path) const
{
    return readMetadata(path)->inline_data;
}

DiskObjectStorageMetadataPtr MetadataStorageFromDisk::readMetadataUnlocked(const std::string & path, std::shared_lock<SharedMutex> &) const
{
    // 在堆上构造一个新的 DiskObjectStorageMetadata 元数据对象，传入 compatible_key_prefix（兼容前缀配置）和当前目标 path，初始化元数据的路径上下文与默认字段。
    auto metadata = std::make_unique<DiskObjectStorageMetadata>(compatible_key_prefix, path);
    // 从底层元数据磁盘读取 path 处保存的原始元数据文本/字节内容，存入局部字符串 str（内部会再次通过 disk 读取该元数据文件）。
    auto str = readFileToString(path);
    // 解析 str 中的序列化元数据内容（如关联的远端对象路径、字节大小、版本或附加属性等），填充到 metadata 对象各个字段中。
    metadata->deserializeFromString(str);
    return metadata;
}

DiskObjectStorageMetadataPtr MetadataStorageFromDisk::readMetadataUnlocked(const std::string & path, std::unique_lock<SharedMutex> &) const
{
    auto metadata = std::make_unique<DiskObjectStorageMetadata>(compatible_key_prefix, path);
    auto str = readFileToString(path);
    metadata->deserializeFromString(str);
    return metadata;
}

DiskObjectStorageMetadataPtr MetadataStorageFromDisk::readMetadata(const std::string & path) const
{
    std::shared_lock lock(metadata_mutex);
    return readMetadataUnlocked(path, lock);
}

std::unordered_map<String, String> MetadataStorageFromDisk::getSerializedMetadata(const std::vector<String> & file_paths) const
{
    std::shared_lock lock(metadata_mutex);
    std::unordered_map<String, String> metadatas;

    for (const auto & path : file_paths)
    {
        auto metadata = readMetadataUnlocked(path, lock);
        metadata->ref_count = 0;
        WriteBufferFromOwnString buf;
        metadata->serialize(buf);
        metadatas[path] = buf.str();
    }

    return metadatas;
}
// 实例化并返回 MetadataStorageFromDiskTransaction，将路径创建、硬链接、移动、删除及引用变更纳入事务批处理与后续 objects_to_remove 收集。
MetadataTransactionPtr MetadataStorageFromDisk::createTransaction()
{
    return std::make_shared<MetadataStorageFromDiskTransaction>(*this);
}

bool MetadataStorageFromDisk::supportWritingWithAppend() const
{
    return true;
}

StoredObjects MetadataStorageFromDisk::getStorageObjects(const std::string & path) const
{
    return readMetadata(path)->objects;
}

uint32_t MetadataStorageFromDisk::getHardlinkCount(const std::string & path) const
{
    return readMetadata(path)->ref_count;
}

void MetadataStorageFromDisk::startup()
{
    // 持久化开关短路检查：
    // 如果 persist_removal_queue 为 false（未开启删除队列持久化），则无需创建系统元数据目录或读取日志文件，直接返回。
    // 内存队列 objects_to_remove 保持为空，后续运行期间仅在内存中维护待删除对象。
    if (!persist_removal_queue)
        return;
    // 确保系统元数据目录存在
    if (!disk->existsDirectory(String(SYSTEM_METADATA_DIR)))
        disk->createDirectory(String(SYSTEM_METADATA_DIR));

    std::lock_guard guard(removed_objects_mutex);
    // 加载持久化删除日志并重建内存队列
    // 调用私有成员 loadRemovalLog() 读取 .metadata/blobs_to_remove.log 中的历史记录（如 ADD 和 REMOVED 条目），在 objects_to_remove 中恢复进程退出前尚未清理完成的死 Blob 集合。
    // 返回布尔值 needs_compaction：若日志文件格式版本号不匹配、文件末尾存在因崩溃导致的截断记录或损坏条目，则返回 true。
    bool needs_compaction = loadRemovalLog();
    // 如果 loadRemovalLog() 指出需要压缩，立即调用 compactRemovalLog() 重写/清理日志文件（剔除无效/截断尾部或已移除条目，仅保留有效待删状态），使磁盘上的日志与恢复后的内存队列保持干净一致。
    if (needs_compaction)
        compactRemovalLog();

    LOG_INFO(log, "Loaded {} blobs pending removal from {}", objects_to_remove.size(), REMOVAL_LOG_FILE);
}

bool MetadataStorageFromDisk::loadRemovalLog()
{
    // 1. 打开删除日志文件并处理首次启动/无文件场景
    const auto path = std::string(REMOVAL_LOG_FILE);
    // 尝试以默认读取配置打开删除日志文件。
    auto buf = disk->readFileIfExists(path, ReadSettings{});
    if (!buf)
        return false;

    bool needs_compaction = false;

    /// Read version header.
    // 2. 读取并校验二进制文件头版本号
    UInt32 version = 0;
    try
    {
        // 从读取缓冲区 *buf 按小端序读取 4 字节版本号头。
        readBinaryLittleEndian(version, *buf);
    }
    catch (...)
    {
        LOG_WARNING(log, "Truncated version header in {}, starting with empty queue: {}", REMOVAL_LOG_FILE, getCurrentExceptionMessage(false));
        return true;
    }
    // 向前兼容性保护：如果日志文件的 version 大于当前代码支持的 REMOVAL_LOG_CURRENT_VERSION，
    // 说明该日志可能由更高版本写入且包含未知格式，抛出 ErrorCodes::LOGICAL_ERROR 逻辑错误异常，防止误解析损坏数据。
    if (version > REMOVAL_LOG_CURRENT_VERSION)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported removal log version: {}", version);

    if (version != REMOVAL_LOG_CURRENT_VERSION)
        needs_compaction = true;
    // 3. 循环解析每条日志记录并更新内存队列状态
    while (!buf->eof())
    {
        try
        {
            /// Binary format (V0): entry_type (UInt8) | remote_path (binary string) | local_path (binary string) | bytes_size (UInt64 LE)
            // 只要输入流 *buf 尚未到达文件末尾（!buf->eof()），就持续循环读取下一条记录，并用独立的 try 块包裹单条记录的解析过程，实现单条截断容错。
            // 首先读取 1 字节无符号整数 entry_type（操作类型）。
            UInt8 entry_type = 0;
            readBinaryLittleEndian(entry_type, *buf);

            StoredObject object;
            // 远端对象存储中的路径/Key，变长二进制字符串
            readStringBinary(object.remote_path, *buf);
            // 元数据/本地关联路径，变长二进制字符串
            readStringBinary(object.local_path, *buf);
            // 对象字节大小，64 位小端无符号整数
            readBinaryLittleEndian(object.bytes_size, *buf);
            // 当 entry_type 为 ADD 时，表示该 Blob 曾被标记为待删除且尚未确认移除：
            // 调用 objects_to_remove.submitForRemoval({std::move(object)}) 将其重新加入内存待清理队列（使用移动语义避免多余拷贝）。
            if (entry_type == RemovalLogEntryType::ADD)
            {
                objects_to_remove.submitForRemoval({std::move(object)});
            }
            // 当 entry_type 为 REMOVED 时，表示该 Blob 此前已完成删除确认：
            // 调用 objects_to_remove.markAsRemoved({object}) 确保内存队列中不再保留该对象为待删除状态；
            else if (entry_type == RemovalLogEntryType::REMOVED)
            {
                objects_to_remove.markAsRemoved({object});
                ++removal_log_stale_entries;
            }
            else
            {
                LOG_WARNING(log, "Skipping unknown entry type '{}' in {}", static_cast<UInt16>(entry_type), REMOVAL_LOG_FILE);
            }
        }
        catch (...)
        {
            /// Truncated entry from a partial write (e.g. crash mid-append). Stop reading — everything
            /// loaded so far is valid. Compaction will rewrite the file cleanly.
            LOG_WARNING(log, "Truncated entry in {}, stopping: {}", REMOVAL_LOG_FILE, getCurrentExceptionMessage(false));
            needs_compaction = true;
            break;
        }
    }

    return needs_compaction;
}

void MetadataStorageFromDisk::appendToRemovalLog(RemovalLogEntryType entry_type, const StoredObjects & blobs)
{
    bool file_exists = disk->existsFile(String(REMOVAL_LOG_FILE));
    auto buf = disk->writeFile(String(REMOVAL_LOG_FILE), /* buf_size */ DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append, /* settings */ {});

    /// Write version header if this is a new file.
    /// An empty or corrupt file cannot exist here because loadRemovalLog on startup
    /// detects such cases and compactRemovalLog rewrites the file with a valid header.
    if (!file_exists)
    {
        UInt32 version = REMOVAL_LOG_CURRENT_VERSION;
        writeBinaryLittleEndian(version, *buf);
    }

    for (const auto & blob : blobs)
    {
        UInt8 type = entry_type;
        writeBinaryLittleEndian(type, *buf);
        writeStringBinary(blob.remote_path, *buf);
        writeStringBinary(blob.local_path, *buf);
        writeBinaryLittleEndian(blob.bytes_size, *buf);
    }
    buf->finalize();
    buf->sync();
}

void MetadataStorageFromDisk::compactRemovalLog()
{
    // 1. 创建临时日志文件与写入二进制版本号头
    static constexpr std::string_view REMOVAL_LOG_TMP_FILE = ".metadata/blobs_to_remove.log.tmp";

    auto buf = disk->writeFile(String(REMOVAL_LOG_TMP_FILE), /* buf_size */ DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, /* settings */ {});
    // 将当前日志格式版本（REMOVAL_LOG_CURRENT_VERSION）赋给 32 位无符号整数 version，并通过 writeBinaryLittleEndian 按小端字节序写入输出缓冲区 *buf 的文件头部。
    UInt32 version = REMOVAL_LOG_CURRENT_VERSION;
    writeBinaryLittleEndian(version, *buf);
    // 2. 获取当前全部待删除 Blob 并序列化为 ADD 条目
    // 取出当前内存中仍处于待删除队列里的全部 StoredObject 对象集合
    auto all_blobs = objects_to_remove.takeFirst(0);
    for (const auto & blob : all_blobs)
    {
        // 严格按照 loadRemovalLog() 所读取的 V0 二进制格式顺序写出一条完整记录：
        UInt8 type = RemovalLogEntryType::ADD;
        writeBinaryLittleEndian(type, *buf);
        writeStringBinary(blob.remote_path, *buf);
        writeStringBinary(blob.local_path, *buf);
        writeBinaryLittleEndian(blob.bytes_size, *buf);
    }
    // 3. 刷盘落盘、原子替换正式文件与重置过期计数
    buf->finalize();
    buf->sync();
    // 将已刷盘的临时文件 ".metadata/blobs_to_remove.log.tmp" 原子替换为正式日志文件 ".metadata/blobs_to_remove.log"，完成新紧凑日志的发布。
    disk->replaceFile(String(REMOVAL_LOG_TMP_FILE), String(REMOVAL_LOG_FILE));
    removal_log_stale_entries = 0;
}

IMetadataStorage::BlobsToRemove MetadataStorageFromDisk::getBlobsToRemove(const ClusterConfigurationPtr & cluster, int64_t max_count)
{
    std::lock_guard guard(removed_objects_mutex);

    BlobsToRemove blobs_to_remove;
    for (const auto & blob : objects_to_remove.takeFirst(max_count))
        blobs_to_remove[blob] = {cluster->getLocalLocation()};

    return blobs_to_remove;
}

int64_t MetadataStorageFromDisk::recordAsRemoved(const StoredObjects & blobs)
{
    std::lock_guard guard(removed_objects_mutex);

    /// Persist REMOVED entries to WAL before erasing from in-memory queue,
    /// so that on WAL failure the blobs remain tracked and will be retried.
    if (persist_removal_queue && !blobs.empty())
        appendToRemovalLog(RemovalLogEntryType::REMOVED, blobs);

    /// Erase from in-memory queue before compaction, so that `compactRemovalLog`
    /// (which snapshots the queue via `takeFirst(0)`) does not re-add these blobs
    /// as ADD entries in the compacted file — otherwise a restart after compaction
    /// would resurrect them as zombie pending-removal entries.
    auto removed_count = objects_to_remove.markAsRemoved(blobs);

    if (persist_removal_queue && !blobs.empty())
    {
        removal_log_stale_entries += blobs.size();
        if (removal_log_stale_entries >= removal_log_compaction_threshold)
            compactRemovalLog();
    }

    return removed_count;
}

bool MetadataStorageFromDisk::hasPendingRemovalBlobs(const StoredObjects & blobs) const
{
    std::lock_guard guard(removed_objects_mutex);
    return objects_to_remove.containsAny(blobs);
}

int64_t MetadataStorageFromDisk::getDeadBlobsQueueEstimate()
{
    std::lock_guard guard(removed_objects_mutex);
    return std::ssize(objects_to_remove);
}

MetadataStorageFromDiskTransaction::MetadataStorageFromDiskTransaction(MetadataStorageFromDisk & metadata_storage_)
    : metadata_storage(metadata_storage_)
{
}

void MetadataStorageFromDiskTransaction::commit(const TransactionCommitOptionsVariant & options)
{
    if (!std::holds_alternative<NoCommitOptions>(options))
        throwNotImplemented();

    {
        std::unique_lock lock(metadata_storage.metadata_mutex);
        operations.commit();
    }

    operations.finalize();

    if (!objects_to_remove.empty())
    {
        std::lock_guard guard(metadata_storage.removed_objects_mutex);
        if (metadata_storage.persist_removal_queue)
        {
            try
            {
                metadata_storage.appendToRemovalLog(MetadataStorageFromDisk::RemovalLogEntryType::ADD, objects_to_remove);
            }
            catch (...)
            {
                LOG_ERROR(metadata_storage.log, "Failed to persist removal queue entry: {}. "
                    "Blobs are tracked in memory but may become orphaned on crash.",
                    getCurrentExceptionMessage(false));
            }
        }

        metadata_storage.objects_to_remove.submitForRemoval(objects_to_remove);
    }
}

TransactionCommitOutcomeVariant MetadataStorageFromDiskTransaction::tryCommit(const TransactionCommitOptionsVariant & options)
{
    if (!std::holds_alternative<NoCommitOptions>(options))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Metadata storage from disk supports only tryCommit without options");

    commit(NoCommitOptions{});
    return true;
}

void MetadataStorageFromDiskTransaction::writeStringToFile(
     const std::string & path,
     const std::string & data)
{
    operations.addOperation(std::make_unique<WriteFileOperation>(path, data, *metadata_storage.getDisk()));
}

void MetadataStorageFromDiskTransaction::writeInlineDataToFile(
     const std::string & path,
     const std::string & data)
{
    operations.addOperation(std::make_unique<WriteInlineDataOperation>(path, data, metadata_storage.compatible_key_prefix, *metadata_storage.getDisk()));
}

void MetadataStorageFromDiskTransaction::setLastModified(const std::string & path, const Poco::Timestamp & timestamp)
{
    operations.addOperation(std::make_unique<SetLastModifiedOperation>(path, timestamp, *metadata_storage.getDisk()));
}

void MetadataStorageFromDiskTransaction::chmod(const String & path, mode_t mode)
{
    operations.addOperation(std::make_unique<ChmodOperation>(path, mode, *metadata_storage.getDisk()));
}

void MetadataStorageFromDiskTransaction::unlinkFile(const std::string & path, bool if_exists, bool should_remove_objects)
{
    operations.addOperation(std::make_unique<UnlinkFileOperation>(path, if_exists, should_remove_objects, metadata_storage.compatible_key_prefix, *metadata_storage.getDisk(), objects_to_remove));
}

void MetadataStorageFromDiskTransaction::removeRecursive(const std::string & path, const ShouldRemoveObjectsPredicate & should_remove_objects)
{
    operations.addOperation(std::make_unique<RemoveRecursiveOperation>(path, should_remove_objects, metadata_storage.compatible_key_prefix, *metadata_storage.getDisk(), objects_to_remove));
}

void MetadataStorageFromDiskTransaction::createHardLink(const std::string & path_from, const std::string & path_to)
{
    operations.addOperation(std::make_unique<CreateHardlinkOperation>(path_from, path_to, metadata_storage.compatible_key_prefix, *metadata_storage.disk));
}

void MetadataStorageFromDiskTransaction::createDirectory(const std::string & path)
{
    operations.addOperation(std::make_unique<CreateDirectoryOperation>(path, *metadata_storage.getDisk()));
}

void MetadataStorageFromDiskTransaction::createDirectoryRecursive(const std::string & path)
{
    operations.addOperation(std::make_unique<CreateDirectoryRecursiveOperation>(path, *metadata_storage.getDisk()));
}

void MetadataStorageFromDiskTransaction::removeDirectory(const std::string & path)
{
    operations.addOperation(std::make_unique<RemoveDirectoryOperation>(path, *metadata_storage.getDisk()));
}

void MetadataStorageFromDiskTransaction::moveFile(const std::string & path_from, const std::string & path_to)
{
    operations.addOperation(std::make_unique<MoveFileOperation>(path_from, path_to, *metadata_storage.getDisk()));
}

void MetadataStorageFromDiskTransaction::moveDirectory(const std::string & path_from, const std::string & path_to)
{
    operations.addOperation(std::make_unique<MoveDirectoryOperation>(path_from, path_to, *metadata_storage.getDisk()));
}

void MetadataStorageFromDiskTransaction::replaceFile(const std::string & path_from, const std::string & path_to)
{
    operations.addOperation(std::make_unique<ReplaceFileOperation>(path_from, path_to, metadata_storage.compatible_key_prefix, *metadata_storage.getDisk(), objects_to_remove));
}

void MetadataStorageFromDiskTransaction::setReadOnly(const std::string & path)
{
    operations.addOperation(std::make_unique<SetReadonlyFileOperation>(path, metadata_storage.compatible_key_prefix, *metadata_storage.getDisk()));
}

void MetadataStorageFromDiskTransaction::createMetadataFile(const std::string & path, const StoredObjects & objects)
{
    operations.addOperation(std::make_unique<RewriteFileOperation>(path, objects, metadata_storage.compatible_key_prefix, *metadata_storage.disk, objects_to_remove));
}

void MetadataStorageFromDiskTransaction::addBlobToMetadata(const std::string & path, const StoredObject & object)
{
    operations.addOperation(std::make_unique<AddBlobOperation>(path, object, metadata_storage.compatible_key_prefix, *metadata_storage.disk));
}

void MetadataStorageFromDiskTransaction::truncateFile(const std::string & src_path, size_t target_size)
{
    operations.addOperation(std::make_unique<TruncateMetadataFileOperation>(src_path, target_size, metadata_storage.compatible_key_prefix, *metadata_storage.getDisk(), objects_to_remove));
}

ObjectStorageKey MetadataStorageFromDiskTransaction::generateObjectKeyForPath(const std::string & /*path*/)
{
    return metadata_storage.key_generator->generate();
}

StoredObjects MetadataStorageFromDiskTransaction::getSubmittedForRemovalBlobs()
{
    return objects_to_remove;
}

}
