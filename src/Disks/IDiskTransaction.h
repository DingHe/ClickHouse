#pragma once

#include <string>
#include <vector>
#include <boost/noncopyable.hpp>
#include <Disks/IDisk.h>
#include <sys/types.h>
#include <Disks/DiskCommitTransactionOptions.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

struct RemoveRequest
{
    // 待删除文件相对于磁盘根目录的相对路径
    std::string path; /// Relative path.
    // 容错标志；为 true 时若文件不存在则静默忽略，为 false 时若文件不存在则抛出异常。
    bool if_exists = false;

    explicit RemoveRequest(std::string path_, bool if_exists_ = false)
        : path(std::move(path_)), if_exists(std::move(if_exists_))
    {
    }
};

using RemoveBatchRequest = std::vector<RemoveRequest>;

/// Simple interface batch execution of write disk operations.
/// Method are almost equal to disk methods.
// 定义了 ClickHouse 存储层用于批量收集与原子提交磁盘写操作的事务接口 IDiskTransaction，以及批量删除请求结构体 RemoveRequest / RemoveBatchRequest。
// IDiskTransaction 的核心作用
// 批量化执行磁盘写操作（Operation Batching）：
// 在 S3、HDFS 等远程对象存储磁盘（DiskObjectStorage）上，每个文件的创建、移动、硬链接或删除都需要更新底层元数据存储（如 ZooKeeper/Keeper 或本地 RocksDB 元数据）。IDiskTransaction 提供了一套与 IDisk 几乎一致的写入接口，调用方在事务内执行的所有修改都会先缓存在内存操作队列中，最后通过一次 commit() 批量提交，极大减少网络往返（RTT）和元数据锁竞争。
// 保证多文件修改的原子性与异常回滚（Atomicity & Rollback）：
// 写入一个 Data Part 或执行一次 freeze 克隆往往涉及数十上百个文件。通过 IDiskTransaction，所有元数据变更要么全部原子生效（commit / tryCommit），要么在发生异常或取消时通过 undo() 彻底撤销并清理已上传的孤儿 Blob，避免留下残缺的脏目录状态。
//
struct IDiskTransaction : private boost::noncopyable
{
public:
    // 原子提交事务中积累的所有磁盘写入、目录创建、硬链接和删除操作；提交失败时抛出异常。
    virtual void commit() = 0;

    // 尝试以指定选项（如绑定外部 Keeper 会话执行联合原子提交）提交事务，返回提交结果变体 TransactionCommitOutcomeVariant；默认实现抛出 NOT_IMPLEMENTED 异常。
    virtual TransactionCommitOutcomeVariant tryCommit(const TransactionCommitOptionsVariant &)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Commit with ZK connection not implemented");
    }
    // 无异常回滚（noexcept）。撤销事务中所有未提交的元数据修改，并清理本次事务期间已经上传到对象存储的临时/孤儿数据 Blob，确保不产生状态污染或存储泄漏。
    virtual void undo() noexcept = 0;

    virtual ~IDiskTransaction() = default;

    /// Create directory.
    virtual void createDirectory(const std::string & path) = 0;

    /// Create directory and all parent directories if necessary.
    virtual void createDirectories(const std::string & path) = 0;

    /// Move directory from `from_path` to `to_path`.
    virtual void moveDirectory(const std::string & from_path, const std::string & to_path) = 0;

    virtual void moveFile(const String & from_path, const String & to_path) = 0;

    virtual void createFile(const String & path) = 0;

    /// Move the file from `from_path` to `to_path`.
    /// If a file with `to_path` path already exists, it will be replaced.
    virtual void replaceFile(const std::string & from_path, const std::string & to_path) = 0;

    /// Only copy of several files supported now. Disk interface support copy to another disk
    /// but it's impossible to implement correctly in transactions because other disk can
    /// use different metadata storage.
    /// TODO: maybe remove it at all, we don't want copies
    virtual void copyFile(
        const std::string & from_file_path,
        const std::string & to_file_path,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings) = 0;

    /// Open the file for write and return WriteBufferFromFileBase object.
    virtual std::unique_ptr<WriteBufferFromFileBase> writeFileWithAutoCommit(
        const std::string & path,
        size_t buf_size,
        WriteMode mode,
        const WriteSettings & settings) = 0;

    /// Open the file for write and return WriteBufferFromFileBase object.
    virtual std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const std::string & path,
        size_t buf_size,
        WriteMode mode,
        const WriteSettings & settings) = 0;
    // 用于自定义写入远端对象存储 Blob 的回调函数。
    using WriteBlobFunction = std::function<size_t(const Strings & blob_path, WriteMode mode, const std::optional<ObjectAttributes> & object_attributes)>;

    /// Write a file using a custom function to write an object to the disk's object storage.
    virtual void writeFileUsingBlobWritingFunction(const String & path, WriteMode mode, WriteBlobFunction && write_blob_function) = 0;

    /// Remove file. Throws exception if file doesn't exists or it's a directory.
    virtual void removeFile(const std::string & path) = 0;

    /// Remove file if it exists.
    virtual void removeFileIfExists(const std::string & path) = 0;

    /// Remove directory. Throws exception if it's not a directory or if directory is not empty.
    virtual void removeDirectory(const std::string & path) = 0;

    /// Remove file or directory with all children. Use with extra caution. Throws exception if file doesn't exists.
    virtual void removeRecursive(const std::string & path) = 0;

    /// Remove file. Throws exception if file doesn't exists or if directory is not empty.
    /// Differs from removeFile for S3/HDFS disks
    /// Second bool param is a flag to remove (true) or keep (false) shared data on S3
    virtual void removeSharedFile(const std::string & path, bool /* keep_shared_data */) = 0;

    /// Remove file or directory with all children. Use with extra caution. Throws exception if file doesn't exists.
    /// Differs from removeRecursive for S3/HDFS disks
    /// Second bool param is a flag to remove (false) or keep (true) shared data on S3.
    /// Third param determines which files cannot be removed even if second is true.
    virtual void removeSharedRecursive(const std::string & path, bool /* keep_all_shared_data */, const NameSet & /* file_names_remove_metadata_only */) = 0;

    /// Remove file or directory if it exists.
    /// Differs from removeFileIfExists for S3/HDFS disks
    /// Second bool param is a flag to remove (true) or keep (false) shared data on S3
    virtual void removeSharedFileIfExists(const std::string & path, bool /* keep_shared_data */) = 0;

    /// Batch request to remove multiple files.
    /// May be much faster for blob storage.
    /// Second bool param is a flag to remove (true) or keep (false) shared data on S3.
    /// Third param determines which files cannot be removed even if second is true.
    virtual void removeSharedFiles(const RemoveBatchRequest & files, bool keep_all_batch_data, const NameSet & file_names_remove_metadata_only) = 0;

    /// Set last modified time to file or directory at `path`.
    virtual void setLastModified(const std::string & path, const Poco::Timestamp & timestamp) = 0;

    /// Just chmod.
    virtual void chmod(const String & path, mode_t mode) = 0;

    /// Set file at `path` as read-only.
    virtual void setReadOnly(const std::string & path) = 0;

    /// Create hardlink from `src_path` to `dst_path`.
    virtual void createHardLink(const std::string & src_path, const std::string & dst_path) = 0;

    /// Truncate file to the target size.
    virtual void truncateFile(const std::string & src_path, size_t size) = 0;

    /// Increment the reference count of a data blob shared between metadata files.
    virtual void incrementBlobRefCount(const std::string & /* blob */)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Blob reference counting is not implemented for this disk transaction");
    }

    /// Decrement the reference count of a data blob; at zero the blob becomes eligible for removal.
    virtual void decrementBlobRefCount(const std::string & /* blob */)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Blob reference counting is not implemented for this disk transaction");
    }
};

using DiskTransactionPtr = std::shared_ptr<IDiskTransaction>;

}
