#pragma once

#include <Storages/MergeTree/MergeTreeDataPartType.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <IO/PackedFilesWriter.h>
#include <IO/PackedFilesReader.h>
#include <IO/ReadPipeline.h>

namespace DB
{

/// A storage for data part that stores all files of part in one archive
/// (except projections and several files listed below).
// DataPartStorageOnDiskPacked 类的核心作用
// 将 Data Part 大部分文件打包进单一归档文件（data.packed）的存储实现（MergeTreeDataPartStorageType::Packed）：
// 与 DataPartStorageOnDiskFull 将每个文件原样散落存放不同，DataPartStorageOnDiskPacked 将分区内绝大多数文件合并写入单个归档文件 data<ARCHIVE_EXTENSION>（即 data.packed），以减少小文件数量、降低对象存储请求次数与元数据开销。
// 例外文件与投影：投影（Projection）子目录以及 files_written_separately 集合中列出的控制/元数据文件（如 delete-on-destroy.txt、txn_version.txt、invalidated_system_columns.txt）会单独写入/存放，不混入打包归档。
// 归档读写与事务预提交协调（PackedFilesReader + PackedFilesWriter）：
// 写入/变更通常在事务内进行：通过 PackedFilesWriter 缓冲归档内逻辑文件与顺序（preferred_file_order），在 precommitTransaction() 时完成写入器收尾（finalizeWriter()）并将数据刷入事务；读取时通过 PackedFilesReader 按索引定位偏移与大小，并支持内嵌归档（如归档内的 skp_idx.packed）的读取路由。

class DataPartStorageOnDiskPacked final : public DataPartStorageOnDiskBase
{
public:
    // 打包归档文件的扩展名常量（通常为 .packed）。
    static constexpr auto DATA_FILE_EXTENSION = PackedFilesIO::ARCHIVE_EXTENSION;
    // 分区主打包归档文件名（即 "data.packed"）。
    inline static const String DATA_FILE_NAME = String("data") + DATA_FILE_EXTENSION;
    // 必须独立于 data.packed 单独写入/管理的文件名集合，涉及分区销毁标记、事务版本与失效系统列信息。
    inline static const std::unordered_set<String> files_written_separately =
    {
        "delete-on-destroy.txt",
        "txn_version.txt",
        "invalidated_system_columns.txt",
    };

    DataPartStorageOnDiskPacked(
        VolumePtr volume_,
        std::string root_path_,
        std::string part_dir_,
        const ReadSettings & read_settings_,
        bool initialize_ = true);

    MergeTreeDataPartStorageType getType() const override { return MergeTreeDataPartStorageType::Packed; }

    MutableDataPartStoragePtr getProjection(const std::string & name, bool use_parent_transaction = true) override; // NOLINT
    MutableDataPartStoragePtr getProjectionNoInitialize(const std::string & name, bool use_parent_transaction = true) override; // NOLINT
    DataPartStoragePtr getProjection(const std::string & name) const override;

    bool exists() const override;
    bool existsDirectory(const std::string & file_name) const override;

    DataPartStorageIteratorPtr iterate() const override;
    Poco::Timestamp getFileLastModified(const String &) const override;
    PackedFilesIO::FileOffset getFileOffsetAndSize(const std::string & file_name) const;
    std::optional<UInt64> getPackedFileUncompressedSize(const std::string & file_name) const override;
    String getActualFileNameOnDisk(const String & file_name) const;
    UInt32 getRefCount(const std::string & file_name) const override;
    std::vector<std::string> getRemotePaths(const std::string & file_name) const override;
    String getUniqueId() const override;

    void rename(
        std::string new_root_path,
        std::string new_part_dir,
        LoggerPtr log,
        bool remove_new_dir_if_exists,
        bool fsync_part_dir) override;

    void createProjection(const std::string & name) override;

    void changeRootPath(const std::string & from_root, const std::string & to_root) override;

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

    void removeRecursive() override;
    void removeSharedRecursive(bool keep_in_remote_fs) override;

    void createHardLinkFrom(const IDataPartStorage & source, const std::string & from, const std::string & to) override;
    void copyFileFrom(const IDataPartStorage & source, const std::string & from, const std::string & to) override;

    void beginTransaction() override;
    void commitTransaction() override;

    void setPreferredFileOrder(const Strings & file_names) override { preferred_file_order = file_names; }

    /// Finalizes writer and writes buffered data into transaction.
    void precommitTransaction() override;

#if CLICKHOUSE_CLOUD
    TransactionCommitOutcomeVariant tryCommitTransaction(const TransactionCommitOptionsVariant & options) override;
    void undoTransaction() override;
#endif

    bool cloneCopiesWholeArchive(const ClonePartParams & params) const override;

    MutableDataPartStoragePtr freeze(
        const std::string & to,
        const std::string & dir_path,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        std::function<void(const DiskPtr &)> save_metadata_callback,
        const ClonePartParams & params) const override;

    MutableDataPartStoragePtr freezeRemote(
        const std::string & to,
        const std::string & dir_path,
        const DiskPtr & dst_disk,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        std::function<void(const DiskPtr &)> save_metadata_callback,
        const ClonePartParams & params) const override;

#if CLICKHOUSE_CLOUD
    void serializeAuxiliaryInfo(WriteBuffer &) const override;
    void deserializeAuxiliaryInfo(ReadBuffer &) override;
#endif

private:
    /// Reader is initialized from index of archive if file
    /// with archive exists or after finalization of writer.
    // 归档读取器实例（可选包装）。
    // 当归档文件已存在或在写入器收尾后初始化，用于依据归档索引定位并读取归档内逻辑文件。
    std::optional<PackedFilesReader> reader;

    /// Writer is initialized after creating a disk transaction.
    // 归档写入器实例（可选包装）。通常在开启磁盘事务并初始化写入上下文后构建，用于在事务阶段收集归档内文件写入与元数据。
    std::optional<PackedFilesWriter> writer;
    // 标识当前事务是否已经执行过 precommitTransaction()（归档写入器已完成收尾并纳入事务），防止重复预提交或状态错乱。
    bool is_precommitted = false;
    // 写入归档时偏好的逻辑文件排列顺序列表，用于优化顺序读取或特定布局下的归档组织。
    Strings preferred_file_order;

    String getRelativeDataPath() const;
    bool isWrittenSeparately(const String & file_name) const;

    /// True if any of the given files lives inside data.packed (rather than being written separately),
    /// which forces the whole archive to be copied rather than hardlinked during a clone/freeze.
    bool anyArchivedFileRequestedForCopy(const NameSet & files_to_copy_instead_of_hardlinks) const;

    /// Native file access hooks for the base storage. On packed-part storage skp_idx.packed is a
    /// virtual file inside data.packed, so the base file-read overlay (which reads a standalone
    /// skp_idx.packed) is disabled here via getArchiveReaderForFile below; these hooks serve both the
    /// packed index substreams (through the inner-archive composition) and the native part files.
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

    /// Override the base-class disk probe: on packed-part storage skp_idx.packed is a virtual
    /// file inside data.packed and can't be opened via disk->readFile against its part-relative
    /// path. We route the inner-archive header read through the outer reader instead. The archive
    /// helpers (copy/filter/seedFrom/hasSkipIndicesPackedArchive) rely on this returning the inner
    /// reader for packed source parts.
    std::shared_ptr<const PackedFilesReader> getSkipIndicesPackedReader() const override;

    /// Disable the base file-read overlay for packed storage: its standalone-archive read
    /// composition can't reach a skp_idx.packed that lives inside data.packed. The *Impl hooks above
    /// serve the index substreams instead (via the inner-archive composition).
    std::shared_ptr<const PackedFilesReader> getArchiveReaderForFile(const std::string &) const override { return nullptr; }

    void resetReader(const ReadSettings & read_settings);
    void resetWriterFromTransaction();
    void finalizeWriter();

    /// Modifying of files is possible only with transaction.

    template <typename Op>
    void executeUnaryWriteOperation(const String & file_name, Op && op);

    template <typename Op>
    void executeBinaryWriteOperation(const String & from_name, const String & to_name, Op && op);

    /// Private constructor to create a storage for projection with shared transaction.
    DataPartStorageOnDiskPacked(
        VolumePtr volume_,
        std::string root_path_,
        std::string part_dir_,
        DiskTransactionPtr transaction_,
        const ReadSettings & read_settings_,
        bool initialize = true);

    MutableDataPartStoragePtr create(VolumePtr volume_, std::string root_path_, std::string part_dir_, bool initialize_) const override;
    NameSet getActualFileNamesOnDisk(const NameSet & file_names) const override;
};

}
