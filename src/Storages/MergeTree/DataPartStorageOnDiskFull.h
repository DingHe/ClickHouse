#pragma once
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/DataPartStorageOnDiskBase.h>

namespace DB
{

/// A storage for data part that stores files on filesystem as is.
// DataPartStorageOnDiskFull 类的核心作用
// 按原样存储分区文件的完整存储后端
// 正如类注释所述，它将 Data Part 的各个文件（如列数据文件、索引/标记文件、校验文件、投影子目录等）直接以独立文件/目录形式原样存放在底层存储（Volume / Disk）的分区目录下，不将多个逻辑文件打包进单一归档文件。
// 统一本地磁盘与对象存储磁盘的分区读写及事务视图：
// 依托基类 DataPartStorageOnDiskBase 提供的卷（volume）、根路径（root_path）、分区目录（part_dir）及活跃事务句柄（transaction），实现分区文件创建、写入、移动、替换、删除、硬链接、拷贝、迭代与读取流水线准备；当处于活跃事务（transaction != nullptr）时，读写与变更会结合事务状态处理。
// 投影（Projection）子存储管理与 Cloud 事务扩展：
// 支持创建和获取隶属于当前 Data Part 的投影（Projection）子存储视图，并可在 CLICKHOUSE_CLOUD 编译宏开启时提供 tryCommitTransaction、undoTransaction 及辅助信息序列化/反序列化钩子。
class DataPartStorageOnDiskFull final : public DataPartStorageOnDiskBase
{
public:
    DataPartStorageOnDiskFull(VolumePtr volume_, std::string root_path_, std::string part_dir_);
    // 返回分区存储类型枚举 MergeTreeDataPartStorageType::Full。
    MergeTreeDataPartStorageType getType() const override { return MergeTreeDataPartStorageType::Full; }
    // 获取指定投影对应的可变存储视图 MutableDataPartStoragePtr，按 use_parent_transaction 决定是否绑定当前事务。
    MutableDataPartStoragePtr getProjection(const std::string & name, bool use_parent_transaction = true) override; // NOLINT
    DataPartStoragePtr getProjection(const std::string & name) const override;
    // 判断当前 Data Part 存储目录是否存在。
    bool exists() const override;

    bool existsDirectory(const std::string & name) const override;
    // 返回遍历当前分区存储中文件/条目的迭代器 DataPartStorageIteratorPtr。
    DataPartStorageIteratorPtr iterate() const override;
    // 返回指定文件的最后修改时间戳。
    Poco::Timestamp getFileLastModified(const String & file_name) const override;
    // 返回打包文件的未压缩大小；因为 Full 存储不将文件打包进归档，因此返回 std::nullopt。
    std::optional<UInt64> getPackedFileUncompressedSize(const std::string & file_name) const override;
    UInt32 getRefCount(const std::string & file_name) const override;
    std::vector<std::string> getRemotePaths(const std::string & file_name) const override;
    String getUniqueId() const override;
    // 在当前存储分区下创建投影子目录结构。
    void createProjection(const std::string & name) override;

    std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const String & name,
        size_t buf_size,
        WriteMode mode,
        const WriteSettings & settings) override;

    void createFile(const String & name) override;
    void moveFile(const String & from_name, const String & to_name) override;
    void replaceFile(const String & from_name, const String & to_name) override;

    void removeFile(const String & name) override;
    void removeFileIfExists(const String & name) override;

    void createHardLinkFrom(const IDataPartStorage & source, const std::string & from, const std::string & to) override;
    void copyFileFrom(const IDataPartStorage & source, const std::string & from, const std::string & to) override;
    // 在当前存储上开启新的磁盘事务，初始化 transaction。
    void beginTransaction() override;
    // 提交当前活跃事务并重置事务状态。
    void commitTransaction() override;
    void precommitTransaction() override {}
    // 返回当前是否存在未提交的活跃事务。
    bool hasActiveTransaction() const override { return transaction != nullptr; }
#if CLICKHOUSE_CLOUD
    TransactionCommitOutcomeVariant tryCommitTransaction(const TransactionCommitOptionsVariant & options) override;
    void undoTransaction() override;
    void serializeAuxiliaryInfo(WriteBuffer &) const override {}
    void deserializeAuxiliaryInfo(ReadBuffer &) override {}
#endif

private:
    DataPartStorageOnDiskFull(VolumePtr volume_, std::string root_path_, std::string part_dir_, DiskTransactionPtr transaction_);
    MutableDataPartStoragePtr create(VolumePtr volume_, std::string root_path_, std::string part_dir_, bool initialize_) const override;

    NameSet getActualFileNamesOnDisk(const NameSet & file_names) const override { return file_names; }

    bool existsFileImpl(const std::string & name) const override;
    size_t getFileSizeImpl(const std::string & file_name) const override;
    void prepareReadImpl(
        const std::string & name,
        const ReadSettings & settings,
        std::optional<size_t> read_hint,
        ReadPipeline & pipeline) const override;
    std::unique_ptr<ReadBufferFromFileBase> readFileIfExistsImpl(
        const std::string & name,
        const ReadSettings & settings,
        std::optional<size_t> read_hint) const override;
};

}
