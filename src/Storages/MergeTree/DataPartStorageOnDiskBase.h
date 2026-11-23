#pragma once
#include <IO/PackedFilesReader.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Disks/IDisk.h>
#include <Disks/IVolume.h>
#include <memory>
#include <mutex>
#include <string>

namespace DB
{

class IVolume;
using VolumePtr = std::shared_ptr<IVolume>;
class PackedFilesWriter;
// 定义了 ClickHouse MergeTree 存储引擎中所有基于磁盘（IDisk / IVolume）的分区存储实现的抽象中间基类
// 统一封装基于 IVolume / IDisk 的通用物理文件与目录生命周期
// 实现了路径拼装（全路径、相对路径、父目录）、磁盘状态查询（本地盘 vs 远程对象存储、缓存名称、空间预留 reserve）、分区目录创建、原子重命名（rename）、安全清理（remove）、备份（backup）以及分区克隆/冻结（freeze、freezeRemote、clonePart）等所有磁盘存储共有的核心基础设施。
// 统一事务执行分发器：
// 持有 DiskTransactionPtr transaction，通过模板辅助方法 executeWriteOperation 将写操作透明路由到活跃的磁盘事务（用于对象存储批量原子提交）或直接提交给底层 IDisk。
// 提供透明的打包跳数索引覆盖层（skp_idx.packed Overlay）：
// 为了减少二级跳数索引（Skip Indices）在磁盘上产生过多小文件，ClickHouse 支持将多个 skp_idx_* 文件合并打包进 skp_idx.packed 归档中。
class DataPartStorageOnDiskBase : public IDataPartStorage
{
public:
    DataPartStorageOnDiskBase(VolumePtr volume_, std::string root_path_, std::string part_dir_);

    std::string getFullPath() const override;
    std::string getRelativePath() const override;
    std::string getPartDirectory() const override;
    std::string getFullRootPath() const override;
    std::string getParentDirectory() const override;

    Poco::Timestamp getLastModified() const override;
    UInt64 calculateTotalSizeOnDisk() const override;

    /// Returns path to place detached part in or nullopt if we don't need to detach part (if it already exists and has the same content)
    std::optional<String> getRelativePathForPrefix(LoggerPtr log, const String & prefix, bool detached, bool broken) const override;

    /// Returns true if detached part already exists and has the same content (compares checksums.txt and the list of files)
    bool looksLikeBrokenDetachedPartHasTheSameContent(const String & detached_part_path, std::optional<String> & original_checksums_content,
                                                      std::optional<Strings> & original_files_list) const;

    void setRelativePath(const std::string & path) override;

    std::string getDiskName() const override;
    std::string getDiskType() const override;
    bool isStoredOnRemoteDisk() const override;
    std::optional<String> getCacheName() const override;
    bool supportZeroCopyReplication() const override;
    bool supportParallelWrite() const override;
    bool isBroken() const override;
    bool isReadonly() const override;
    std::string getDiskPath() const override;
    ReservationPtr reserve(UInt64 bytes) const override;
    ReservationPtr tryReserve(UInt64 bytes) const override;
    DiskPtr getDisk() const;

    /// File reads resolve the per-part skp_idx.packed overlay first: a "skp_idx_..." name that is a
    /// member of the archive is served from it, anything else (incl. all non-index files) falls
    /// through to the storage's native access (existsFileImpl etc.). The overlay lives here, in the
    /// base that owns the packed-index format, and is `final` so no storage can bypass it: a storage
    /// only fills in native access via the *Impl hooks. Storage-agnostic -- a storage that keeps no
    /// standalone skp_idx.packed (e.g. packed part storage) simply has no reader, so the *Impl path
    /// serves the substreams instead. Keeps the archive out of the read pipeline and the callers.
    bool existsFile(const std::string & name) const final;
    size_t getFileSize(const std::string & file_name) const final;
    // 构建读取管道（ReadPipeline）的核心入口。prepareRead 先通过 getArchiveReaderForFile 判断是否为 skp_idx.packed 内的虚拟索引流；
    // 若是则直接添加偏移切片读取阶段，否则下沉调用子类的 prepareReadImpl（Full 模式打开独立文件，Packed 模式切片读取 data.packed）。
    void prepareRead(
        const std::string & name,
        const ReadSettings & settings,
        std::optional<size_t> read_hint,
        ReadPipeline & pipeline) const final;
    std::unique_ptr<ReadBufferFromFileBase> readFileIfExists(
        const std::string & name,
        const ReadSettings & settings,
        std::optional<size_t> read_hint) const final;

    /// True iff @name resolves to a virtual file inside this part's skp_idx.packed archive (and
    /// not a standalone file on disk). Lets external callers distinguish per-file substreams
    /// from packed ones without touching the archive reader directly.
    bool isFileInPackedSkipIndicesArchive(const std::string & name) const;

    /// True iff this part has a packed skip-index archive (skp_idx.packed). Cheaper than calling
    /// isFileInPackedSkipIndicesArchive with a sentinel and clearer at call sites that just need
    /// to know whether the archive must be rebuilt/excluded from hardlinks.
    bool hasSkipIndicesPackedArchive() const;

    /// Copy the named virtual files from this storage's skp_idx.packed archive into @target so
    /// the writer can ship a complete archive after also writing fresh recalc'd entries. Used by
    /// mutations to preserve surviving in-archive indices that aren't being recomputed when the
    /// source archive cannot be hardlinked (because the writer is about to write into the same
    /// file name in the new part). Names not present in the archive are skipped silently; this
    /// matches the contract of dropped_skip_index_archive_file_names where probing for absent extensions is
    /// expected.
    void copyPackedSkipIndicesFilesInto(
        const NameSet & file_names,
        PackedFilesWriter & target,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings) const;

    /// Rewrite this storage's skp_idx.packed into a fresh archive on @new_storage, dropping any
    /// virtual file whose name is in @dropped_skip_index_archive_file_names (exact match). Callers must
    /// pre-resolve the full in-archive substream filenames; passing only an index-name prefix
    /// would over-match when two indices share a prefix (e.g. "a" and "a.b" with
    /// escape_index_filenames=0). If every entry would be dropped, no archive is written and the
    /// corresponding checksum entry is removed instead. Used by MutateSomePartColumnsTask::prepare
    /// when DROP INDEX targets an in-archive index and there's no writer pipeline to rebuild
    /// from data.
    void filterPackedSkipIndicesArchiveTo(
        const NameSet & dropped_skip_index_archive_file_names,
        IDataPartStorage & new_storage,
        const WriteSettings & write_settings,
        const ReadSettings & read_settings,
        MergeTreeDataPartChecksums & checksums,
        bool sync) const;

    ReplicatedFilesDescription getReplicatedFilesDescription(const NameSet & file_names) const override;
    ReplicatedFilesDescription getReplicatedFilesDescriptionForRemoteDisk(const NameSet & file_names) const override;

    void backup(
        const MergeTreeDataPartChecksums & checksums,
        const NameSet & files_without_checksums,
        const String & path_in_backup,
        const BackupSettings & backup_settings,
        bool make_temporary_hard_links,
        BackupEntries & backup_entries,
        TemporaryFilesOnDisks * temp_dirs,
        bool is_projection_part,
        bool allow_backup_broken_projection) const override;

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

    MutableDataPartStoragePtr clonePart(
        const std::string & to,
        const std::string & dir_path,
        const DiskPtr & dst_disk,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        LoggerPtr log,
        const std::function<void()> & cancellation_hook
        ) const override;

    void rename(
        std::string new_root_path,
        std::string new_part_dir,
        LoggerPtr log,
        bool remove_new_dir_if_exists,
        bool fsync_part_dir) override;

    void remove(
        CanRemoveCallback && can_remove_callback,
        const MergeTreeDataPartChecksums & checksums,
        std::list<ProjectionChecksums> projections,
        bool is_temp,
        LoggerPtr log) override;

    void changeRootPath(const std::string & from_root, const std::string & to_root) override;
    void createDirectories() override;

    std::unique_ptr<WriteBufferFromFileBase> writeTransactionFile(const String & txn_file_name, WriteMode mode) const override;

    void removeRecursive() override;
    void removeSharedRecursive(bool keep_in_remote_fs) override;

    SyncGuardPtr getDirectorySyncGuard() const override;
    bool hasActiveTransaction() const override;

    bool isCaseInsensitive() const override;

protected:

    DataPartStorageOnDiskBase(VolumePtr volume_, std::string root_path_, std::string part_dir_, DiskTransactionPtr transaction_);
    virtual MutableDataPartStoragePtr create(VolumePtr volume_, std::string root_path_, std::string part_dir_, bool initialize_) const = 0;

    /// Lazily load the per-part skp_idx.packed archive (if any), reading it as a standalone disk
    /// file. Subsequent calls return the cached reader, or nullptr when there is no such file --
    /// including on storages that don't keep skp_idx.packed standalone (e.g. packed part storage,
    /// where index substreams live in data.packed and are served by the *Impl hooks instead). Used
    /// by the file-read overlay above and by the public archive helpers.
    ///
    /// Returns a shared owning handle, not a raw pointer: callers dereference the reader after the
    /// internal mutex is released, while a concurrent resetReader/seed can replace or drop the
    /// cached reader. Holding a shared_ptr for the duration of use keeps the object alive and
    /// avoids a use-after-free on the cached archive index.
    ///
    /// Virtual so packed part storage can route the probe through its outer data.packed reader:
    /// there skp_idx.packed is a virtual member of data.packed rather than a standalone disk file,
    /// so the disk-probe default always misses.
    virtual std::shared_ptr<const PackedFilesReader> getSkipIndicesPackedReader() const;

    /// Cheap pre-filtered lookup for the file-read overlay: returns the archive reader only when
    /// @name is a "skp_idx_..." substream that the archive actually contains, else nullptr. The
    /// prefix gate keeps unrelated files (checksums.txt, count.txt, columns.txt, ...) from loading
    /// or probing skp_idx.packed at all -- avoiding extra metadata I/O on remote/Keeper disks and
    /// keeping a bad/future-version archive from blocking reads of unrelated files.
    ///
    /// Virtual so packed part storage can disable the base file-read overlay by returning nullptr:
    /// its skp_idx.packed lives inside data.packed, so the overlay's standalone-archive read
    /// composition is invalid there. Packed serves index substreams through its *Impl hooks instead.
    virtual std::shared_ptr<const PackedFilesReader> getArchiveReaderForFile(const std::string & name) const;

    /// Copy a single archive member into @target, reading it through this storage's readFile
    /// overlay. Shared by copyPackedSkipIndicesFilesInto and filterPackedSkipIndicesArchiveTo.
    void copyArchiveEntryTo(
        const PackedFilesReader & source_archive,
        const String & file_name,
        PackedFilesWriter & target,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings) const;

public:
    /// Pre-populate the cached PackedFilesReader from an in-memory index produced by the
    /// writer's PackedFilesWriter::finalize. This lets the overlay (existsFile / getFileSize)
    /// answer queries about packed substreams BEFORE the archive file is fully committed on
    /// disk, which matters on object-storage disks where the file isn't visible until the
    /// underlying multipart upload finishes (the writer only calls preFinalize at fillChecksums
    /// time; the actual finalize happens later). After the file is committed, on-disk reads
    /// would work too, but the in-memory index is always cheaper and equally authoritative.
    void seedSkipIndicesPackedReader(const PackedFilesIO::Index & index) const;

    /// Seed this storage's PackedFilesReader from `source`'s archive index. Used when the
    /// unchanged skp_idx.packed is hardlinked from `source` into this (new) part: the hardlink
    /// shares the source's bytes, so the source's index applies verbatim. No-op if `source` has
    /// no packed archive. See the seed rationale above.
    void seedSkipIndicesPackedReaderFrom(const IDataPartStorage & source) const;
protected:
    // 指向存储卷（IVolume）的共享指针，通过 volume->getDisk() 获取当前分区所在的底层 IDisk 实例（本地磁盘或 S3/HDFS 磁盘）。
    VolumePtr volume;
    // 分区所在父目录相对于磁盘挂载根的路径（例如 "database/table" 或 "database/table/moving"）。
    std::string root_path;
    // 当前分区自身的目录名称（例如 "all_1_5_1"）。root_path 与 part_dir 拼接即构成完整的分区相对路径。
    std::string part_dir;
    // 底层磁盘事务指针（在 S3/Keeper 磁盘模式下用于收集批量元数据修改操作，实现原子提交；普通本地盘模式下通常为空）。
    DiskTransactionPtr transaction;
    // 标识当前存储实例的 transaction 是否是从外部父分区（如创建 Projection 子分区时传入 use_parent_transaction = true）共享借用的。若为 true，子分区不应独自提交或回滚该共享事务。
    bool has_shared_transaction = false;

    /// Cached probe state for skp_idx.packed. probed=false means we haven't checked the disk yet;
    /// probed=true with reader=null means we checked and the archive isn't present.
    // 互斥锁，用于保护多线程并发探测和懒加载 skp_idx.packed 归档索引时的状态一致性。
    mutable std::mutex skip_indices_packed_mutex;
    // 懒加载探测标志位（受 skip_indices_packed_mutex 保护）。false 表示尚未检查磁盘是否存在 skp_idx.packed；
    // true 表示已探测过（配合 skip_indices_packed_reader == nullptr 可缓存“文件不存在”的负向结果，避免反复访问磁盘/S3 探测）。
    mutable bool skip_indices_packed_probed TSA_GUARDED_BY(skip_indices_packed_mutex) = false;
    // 缓存的 skp_idx.packed 归档索引读取器共享指针（受 skip_indices_packed_mutex 保护），保存归档内所有跳数索引虚拟文件的 (offset, size) 目录表。
    mutable std::shared_ptr<const PackedFilesReader> skip_indices_packed_reader TSA_GUARDED_BY(skip_indices_packed_mutex);

    template <typename Op>
    void executeWriteOperation(Op && op)
    {
        if (transaction)
            op(*transaction);
        else
            op(*volume->getDisk());
    }

private:
    void clearDirectory(
        const std::string & dir,
        const CanRemoveDescription & can_remove_description,
        const MergeTreeDataPartChecksums & checksums,
        bool is_temp,
        LoggerPtr log);

    /// For names of expected data part files returns the actual names
    /// of files in filesystem to which data of these files is written.
    /// Actual file name may be the same as expected
    /// or be the name of the file with packed data.
    virtual NameSet getActualFileNamesOnDisk(const NameSet & file_names) const = 0;

    /// Native file access for the concrete storage (disk files for Full storage; data.packed
    /// members for Packed storage), without the skp_idx.packed overlay. The `final`
    /// existsFile/getFileSize/prepareRead/readFileIfExists add the overlay and delegate here.
    virtual bool existsFileImpl(const std::string & name) const = 0;

    virtual size_t getFileSizeImpl(const std::string & file_name) const = 0;
    // 如果 name 不是 skp_idx.packed 中的成员（例如所有普通数据列 .bin、Mark 文件 .mrk2、主索引 primary.idx 以及独立存储的跳数索引），则调用纯虚钩子 prepareReadImpl(...) 交由具体子类处理
    virtual void prepareReadImpl(
        const std::string & name,
        const ReadSettings & settings,
        std::optional<size_t> read_hint,
        ReadPipeline & pipeline) const = 0;
    virtual std::unique_ptr<ReadBufferFromFileBase> readFileIfExistsImpl(
        const std::string & name,
        const ReadSettings & settings,
        std::optional<size_t> read_hint) const = 0;

    /// Returns the destination path for the part directory while copying a detached part.
    String getPartDirForPrefix(const String & prefix, bool detached, int try_no) const;
};

/// Make a freeze/clone hardlink directory tree durable: fsync `clone_dir_path` and every
/// subdirectory below it (children first), then fsync every ancestor directory from its
/// immediate parent up to and including the disk root. fsync(dir) persists the entries inside
/// dir, not dir's own entry in its parent, so the parent chain must be synced too. Shared by
/// both freeze overrides (Full and Packed). A no-op on remote/object disks
/// (getDirectorySyncGuard returns nullptr there).
void fsyncFrozenCloneTree(IDisk & disk, const std::string & clone_dir_path);

}
