#pragma once

#include <Disks/IDiskTransaction.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Common/logger_useful.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

/// Fake disk transaction implementation.
/// Just execute all operations immediately, commit is noop operation.
/// No support for atomicity and rollback.
// 定义了 ClickHouse 存储层中用于本地磁盘等非事务型存储介质的“伪事务”适配器类 FakeDiskTransaction
// 接口适配与无痛多态（Adapter Pattern）：
// 上层存储引擎（如 DataPartStorageOnDiskBase、分区克隆与合并流程）为了同时兼容需要批量元数据事务的对象存储（S3/HDFS）和普通本地磁盘（DiskLocal），
// 统一通过 disk->createTransaction() 获取 IDiskTransaction 接口编写逻辑。
// FakeDiskTransaction 作为 IDisk::createTransaction() 的默认返回值，将 IDiskTransaction 接口的方法逐一直接转发给底层 IDisk 实例，使上层代码无需区分当前磁盘是否真正支持事务。
// 正如类注释所述，该类不维护任何内存命令队列，所有文件/目录写删操作在调用瞬间立即在底层磁盘上真实执行；commit() 和 undo() 均为空操作（No-op），
// 不提供跨多文件的事务原子性和失败自动回滚能力（本地盘上依赖目录原子 rename 及异常清理逻辑保证完整性）。
struct FakeDiskTransaction final : public IDiskTransaction
{
public:
    explicit FakeDiskTransaction(IDisk & disk_)
        : disk(disk_)
    {
        LOG_DEBUG(getLogger("FakeDiskTransaction"), "Creating FakeDiskTransaction for disk {}", disk.getName());
    }

    void commit() override {}
    void undo() noexcept override {}

    void createDirectory(const std::string & path) override
    {
        disk.createDirectory(path);
    }

    void createDirectories(const std::string & path) override
    {
        disk.createDirectories(path);
    }

    void createFile(const std::string & path) override
    {
        disk.createFile(path);
    }

    void moveDirectory(const std::string & from_path, const std::string & to_path) override
    {
        disk.moveDirectory(from_path, to_path);
    }

    void moveFile(const String & from_path, const String & to_path) override
    {
        disk.moveFile(from_path, to_path);
    }

    void replaceFile(const std::string & from_path, const std::string & to_path) override
    {
        disk.replaceFile(from_path, to_path);
    }

    void copyFile(const std::string & from_file_path, const std::string & to_file_path, const ReadSettings & read_settings, const WriteSettings & write_settings) override
    {
        disk.copyFile(from_file_path, disk, to_file_path, read_settings, write_settings);
    }

    std::unique_ptr<WriteBufferFromFileBase> writeFileWithAutoCommit(
        const std::string & path,
        size_t buf_size,
        WriteMode mode,
        const WriteSettings & settings) override
    {
        return disk.writeFile(path, buf_size, mode, settings);
    }

    std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const std::string & path,
        size_t buf_size,
        WriteMode mode,
        const WriteSettings & settings) override
    {
        return disk.writeFile(path, buf_size, mode, settings);
    }

    void writeFileUsingBlobWritingFunction(const String & path, WriteMode mode, WriteBlobFunction && write_blob_function) override
    {
        disk.writeFileUsingBlobWritingFunction(path, mode, std::move(write_blob_function));
    }

    void removeFile(const std::string & path) override
    {
        disk.removeFile(path);
    }

    void removeFileIfExists(const std::string & path) override
    {
        disk.removeFileIfExists(path);
    }

    void removeDirectory(const std::string & path) override
    {
        disk.removeDirectory(path);
    }

    void removeRecursive(const std::string & path) override
    {
        disk.removeRecursive(path);
    }

    void removeSharedFile(const std::string & path, bool keep_shared_data) override
    {
        disk.removeSharedFile(path, keep_shared_data);
    }

    void removeSharedRecursive(const std::string & path, bool keep_all_shared_data, const NameSet & file_names_remove_metadata_only) override
    {
        disk.removeSharedRecursive(path, keep_all_shared_data, file_names_remove_metadata_only);
    }

    void removeSharedFileIfExists(const std::string & path, bool keep_shared_data) override
    {
        disk.removeSharedFileIfExists(path, keep_shared_data);
    }

    void removeSharedFiles(const RemoveBatchRequest & files, bool keep_all_batch_data, const NameSet & file_names_remove_metadata_only) override
    {
        disk.removeSharedFiles(files, keep_all_batch_data, file_names_remove_metadata_only);
    }

    void setLastModified(const std::string & path, const Poco::Timestamp & timestamp) override
    {
        disk.setLastModified(path, timestamp);
    }

    void chmod(const String & path, mode_t mode) override
    {
        disk.chmod(path, mode);
    }

    void setReadOnly(const std::string & path) override
    {
        disk.setReadOnly(path);
    }

    void createHardLink(const std::string & src_path, const std::string & dst_path) override
    {
        disk.createHardLink(src_path, dst_path);
    }

    void truncateFile(const std::string & /* src_path */, size_t) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Operation `truncateFile` is not implemented");
    }

private:
    IDisk & disk;
};

}
