#include <string_view>
#include <Backups/BackupEntryFromImmutableFile.h>
#include <Backups/BackupEntryWrappedWith.h>
#include <Backups/BackupSettings.h>
#include <Disks/IDiskTransaction.h>
#include <Disks/SingleDiskVolume.h>
#include <Common/Jemalloc.h>
#include <Common/JemallocMergeTreeArena.h>
#include <Disks/TemporaryFileOnDisk.h>
#include <IO/Expect404ResponseScope.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/PackedFilesReader.h>
#include <IO/PackedFilesWriter.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadPipeline.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/copyData.h>
#include <Interpreters/Context.h>
#include <Interpreters/MergeTreeTransaction/VersionMetadataOnDisk.h>
#include <Storages/MergeTree/Backup.h>
#include <Storages/MergeTree/DataPartStorageOnDiskBase.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/MergeTree/MergeTreeIndicesSerialization.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>

#include <fmt/ranges.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int DIRECTORY_ALREADY_EXISTS;
    extern const int NOT_ENOUGH_SPACE;
    extern const int LOGICAL_ERROR;
    extern const int FILE_DOESNT_EXIST;
    extern const int CORRUPTED_DATA;
}

namespace
{
    /// fsync `dir_path` and every subdirectory below it (children first), so that the directory
    /// entries created by a freeze/hardlink clone become durable. Used only for local disks
    /// (on remote/object disks getDirectorySyncGuard() returns nullptr and this is a no-op).
    void syncDirectoryTree(IDisk & disk, const std::string & dir_path)
    {
        for (auto it = disk.iterateDirectory(dir_path); it->isValid(); it->next())
        {
            if (disk.existsDirectory(it->path()))
                syncDirectoryTree(disk, it->path());
        }
        /// Children are synced first; this guard fsyncs `dir_path` itself on destruction.
        SyncGuardPtr guard = disk.getDirectorySyncGuard(dir_path);
    }
}

void fsyncFrozenCloneTree(IDisk & disk, const std::string & clone_dir_path)
{
    /// Subtree first (children before parents), then the ancestor chain up to the disk root ("").
    syncDirectoryTree(disk, clone_dir_path);

    fs::path dir = fs::path(clone_dir_path).parent_path();
    while (true)
    {
        SyncGuardPtr guard = disk.getDirectorySyncGuard(dir.string());
        guard.reset();
        if (dir.empty())
            break;
        dir = dir.parent_path();
    }
}

std::unique_ptr<ReadBufferFromFileBase> IDataPartStorage::readFile(
    const std::string & name,
    const ReadSettings & settings,
    std::optional<size_t> read_hint) const
{
    ReadPipeline pipeline;
    prepareRead(name, settings, read_hint, pipeline);
    return pipeline.build();
}

DataPartStorageOnDiskBase::DataPartStorageOnDiskBase(VolumePtr volume_, std::string root_path_, std::string part_dir_)
    : volume(std::move(volume_)), root_path(std::move(root_path_)), part_dir(std::move(part_dir_))
{
}

DataPartStorageOnDiskBase::DataPartStorageOnDiskBase(
    VolumePtr volume_, std::string root_path_, std::string part_dir_, DiskTransactionPtr transaction_)
    : volume(std::move(volume_))
    , root_path(std::move(root_path_))
    , part_dir(std::move(part_dir_))
    , transaction(std::move(transaction_))
    , has_shared_transaction(transaction != nullptr)
{
}

DiskPtr DataPartStorageOnDiskBase::getDisk() const
{
    return volume->getDisk();
}

std::string DataPartStorageOnDiskBase::getFullPath() const
{
    return fs::path(volume->getDisk()->getPath()) / root_path / part_dir / "";
}

std::string DataPartStorageOnDiskBase::getRelativePath() const
{
    return fs::path(root_path) / part_dir / "";
}

std::string DataPartStorageOnDiskBase::getParentDirectory() const
{
    /// Cut last "/" if it exists (it shouldn't). Otherwise fs::path behave differently.
    fs::path part_dir_without_slash = part_dir.ends_with("/") ? part_dir.substr(0, part_dir.size() - 1) : part_dir;

    if (part_dir_without_slash.has_parent_path())
        return part_dir_without_slash.parent_path();
    return "";
}

std::optional<String> DataPartStorageOnDiskBase::getRelativePathForPrefix(LoggerPtr log, const String & prefix, bool detached, bool broken) const
{
    chassert(!broken || detached);
    String res;

    auto full_relative_path = fs::path(root_path);
    if (detached)
        full_relative_path /= MergeTreeData::DETACHED_DIR_NAME;

    std::optional<String> original_checksums_content;
    std::optional<Strings> original_files_list;

    for (int try_no = 0; try_no < 10; ++try_no)
    {
        res = getPartDirForPrefix(prefix, detached, try_no);

        if (!volume->getDisk()->existsDirectory(full_relative_path / res))
            return res;

        /// If part with compacted storage is broken then we probably
        /// cannot read the single file with data and check its content.
        if (broken
            && isFullPartStorage(*this)
            && looksLikeBrokenDetachedPartHasTheSameContent(res, original_checksums_content, original_files_list))
        {
            LOG_WARNING(log, "Directory {} (to detach to) already exists, "
                        "but its content looks similar to content of the broken part which we are going to detach. "
                        "Assuming it was already cloned to detached, will not do it again to avoid redundant copies of broken part.", res);
            return {};
        }

        LOG_WARNING(log, "Directory {} (to detach to) already exists. Will detach to directory with '_tryN' suffix.", res);
    }

    return res;
}

String DataPartStorageOnDiskBase::getPartDirForPrefix(const String & prefix, bool detached, int try_no) const
{
    /// This function joins `prefix` and the part name and an attempt number returning something like "<prefix>_<part_name>_<tryN>".
    String res = prefix;
    if (!prefix.empty() && !prefix.ends_with("_"))
        res += "_";

    /// During RESTORE temporary part directories are created with names like "tmp_restore_all_2_2_0-XXXXXXXX".
    /// To detach such a directory we need to rename it replacing "tmp_restore_" with a specified prefix,
    /// and a random suffix with an attempt number.
    String part_name;
    if (detached && part_dir.starts_with("tmp_restore_"))
    {
        part_name = part_dir.substr(strlen("tmp_restore_"));
        size_t endpos = part_name.find('-');
        if (endpos != String::npos)
            part_name.erase(endpos, String::npos);
    }

    if (!part_name.empty())
        res += part_name;
    else
        res += part_dir;

    if (try_no)
        res += DetachedPartInfo::TRY_N_SUFFIX + DB::toString(try_no);

    return res;
}

bool DataPartStorageOnDiskBase::looksLikeBrokenDetachedPartHasTheSameContent(const String & detached_part_path,
                                                                         std::optional<String> & original_checksums_content,
                                                                         std::optional<Strings> & original_files_list) const
{
    /// We cannot know for sure that content of detached part is the same,
    /// but in most cases it's enough to compare checksums.txt and list of files.

    if (!existsFile("checksums.txt"))
        return false;

    auto storage_from_detached = create(volume, fs::path(root_path) / MergeTreeData::DETACHED_DIR_NAME, detached_part_path, /*initialize=*/ true);
    if (!storage_from_detached->existsFile("checksums.txt"))
        return false;

    if (!original_checksums_content)
    {
        auto in = storage_from_detached->readFile("checksums.txt", /* settings */ {}, /* read_hint */ {});
        original_checksums_content.emplace();
        readStringUntilEOF(*original_checksums_content, *in);
    }

    if (original_checksums_content->empty())
        return false;

    String detached_checksums_content;
    {
        auto in = readFile("checksums.txt", /* settings */ {}, /* read_hint */ {});
        readStringUntilEOF(detached_checksums_content, *in);
    }

    if (original_checksums_content != detached_checksums_content)
        return false;

    if (!original_files_list)
    {
        original_files_list.emplace();
        for (auto it = iterate(); it->isValid(); it->next())
            original_files_list->emplace_back(it->name());
        std::sort(original_files_list->begin(), original_files_list->end());
    }

    Strings detached_files_list;
    for (auto it = storage_from_detached->iterate(); it->isValid(); it->next())
        detached_files_list.emplace_back(it->name());
    std::sort(detached_files_list.begin(), detached_files_list.end());

    return original_files_list == detached_files_list;
}

void DataPartStorageOnDiskBase::setRelativePath(const std::string & path)
{
    part_dir = path;
    /// Unlike rename/changeRootPath, this can repoint the storage at an unrelated directory whose
    /// archive has a different index, so the cached reader must be dropped. The reset is safe for
    /// concurrent readers because the reader is shared-owned: a reader still using it keeps it
    /// alive until done.
    {
        std::lock_guard lock(skip_indices_packed_mutex);
        skip_indices_packed_probed = false;
        skip_indices_packed_reader.reset();
    }
}

std::string DataPartStorageOnDiskBase::getPartDirectory() const
{
    return part_dir;
}

std::string DataPartStorageOnDiskBase::getFullRootPath() const
{
    return fs::path(volume->getDisk()->getPath()) / root_path / "";
}

Poco::Timestamp DataPartStorageOnDiskBase::getLastModified() const
{
    return volume->getDisk()->getLastModified(fs::path(root_path) / part_dir);
}

static UInt64 calculateTotalSizeOnDiskImpl(const DiskPtr & disk, const String & from)
{
    if (disk->existsFile(from))
        return disk->getFileSize(from);

    std::vector<std::string> files;
    disk->listFiles(from, files);

    UInt64 res = 0;
    for (const auto & file : files)
        res += calculateTotalSizeOnDiskImpl(disk, fs::path(from) / file);

    return res;
}

UInt64 DataPartStorageOnDiskBase::calculateTotalSizeOnDisk() const
{
    return calculateTotalSizeOnDiskImpl(volume->getDisk(), fs::path(root_path) / part_dir);
}

std::string DataPartStorageOnDiskBase::getDiskName() const
{
    return volume->getDisk()->getName();
}

std::string DataPartStorageOnDiskBase::getDiskType() const
{
    return volume->getDisk()->getDataSourceDescription().name();
}

bool DataPartStorageOnDiskBase::isStoredOnRemoteDisk() const
{
    return volume->getDisk()->isRemote();
}

std::optional<String> DataPartStorageOnDiskBase::getCacheName() const
{
    if (volume->getDisk()->supportsCache())
        return volume->getDisk()->getCacheName();
    return std::nullopt;
}

bool DataPartStorageOnDiskBase::supportZeroCopyReplication() const
{
    return volume->getDisk()->supportZeroCopyReplication();
}

bool DataPartStorageOnDiskBase::supportParallelWrite() const
{
    return volume->getDisk()->supportParallelWrite();
}

bool DataPartStorageOnDiskBase::isBroken() const
{
    return volume->getDisk()->isBroken();
}

bool DataPartStorageOnDiskBase::isReadonly() const
{
    return volume->getDisk()->isReadOnly() || volume->getDisk()->isWriteOnce();
}

std::string DataPartStorageOnDiskBase::getDiskPath() const
{
    return volume->getDisk()->getPath();
}

ReservationPtr DataPartStorageOnDiskBase::reserve(UInt64 bytes) const
{
    auto res = volume->reserve(bytes);
    if (!res)
        throw Exception(ErrorCodes::NOT_ENOUGH_SPACE, "Cannot reserve {}, not enough space", ReadableSize(bytes));

    return res;
}

ReservationPtr DataPartStorageOnDiskBase::tryReserve(UInt64 bytes) const
{
    return volume->reserve(bytes);
}

IDataPartStorage::ReplicatedFilesDescription
DataPartStorageOnDiskBase::getReplicatedFilesDescription(const NameSet & file_names) const
{
    ReplicatedFilesDescription description;
    auto relative_path = fs::path(root_path) / part_dir;
    auto disk = volume->getDisk();
    auto read_settings = getReadSettings();

    auto actual_file_names = getActualFileNamesOnDisk(file_names);
    for (const auto & name : actual_file_names)
    {
        auto path = relative_path / name;
        size_t file_size = disk->getFileSize(path);

        auto & file_desc = description.files[name];

        file_desc.file_size = file_size;
        file_desc.input_buffer_getter = [disk, path, file_size, read_settings]
        {
            return disk->readFile(path, read_settings.adjustBufferSize(file_size), file_size);
        };
    }

    return description;
}

IDataPartStorage::ReplicatedFilesDescription
DataPartStorageOnDiskBase::getReplicatedFilesDescriptionForRemoteDisk(const NameSet & file_names) const
{
    ReplicatedFilesDescription description;
    auto relative_path = fs::path(root_path) / part_dir;

    auto disk = volume->getDisk();
    if (!disk->supportZeroCopyReplication())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Disk {} doesn't support zero-copy replication", disk->getName());

    description.unique_id = getUniqueId();

    Names paths;
    auto actual_file_names = getActualFileNamesOnDisk(file_names);

    for (const auto & name : actual_file_names)
    {
        /// Just some additional checks
        auto metadata_full_file_path = fs::path(getFullPath()) / name;
        if (!fs::exists(metadata_full_file_path))
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Remote metadata '{}' is not exists", name);
        if (!fs::is_regular_file(metadata_full_file_path))
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Remote metadata '{}' is not a file", name);

        paths.emplace_back(relative_path / name);
    }

    auto serialized_metadata = disk->getSerializedMetadata(paths);
    for (const auto & name : actual_file_names)
    {
        auto & file_desc = description.files[name];
        const auto & metadata_str = serialized_metadata.at(relative_path / name);

        file_desc.file_size = metadata_str.size();
        file_desc.input_buffer_getter = [metadata_str]
        {
            return std::make_unique<ReadBufferFromString>(metadata_str);
        };
    }

    return description;
}

void DataPartStorageOnDiskBase::backup(
    const MergeTreeDataPartChecksums & checksums,
    const NameSet & files_without_checksums,
    const String & path_in_backup,
    const BackupSettings & backup_settings,
    bool make_temporary_hard_links,
    BackupEntries & backup_entries,
    TemporaryFilesOnDisks * temp_dirs,
    bool is_projection_part,
    bool allow_backup_broken_projection) const
{
    fs::path part_path_on_disk = fs::path{root_path} / part_dir;
    fs::path part_path_in_backup = fs::path{path_in_backup} / part_dir;

    auto disk = volume->getDisk();

    fs::path temp_part_dir;
    std::shared_ptr<TemporaryFileOnDisk> temp_dir_owner;
    if (make_temporary_hard_links)
    {
        chassert(temp_dirs);
        auto temp_dir_it = temp_dirs->find(disk);
        if (temp_dir_it == temp_dirs->end())
            temp_dir_it = temp_dirs->emplace(disk, std::make_shared<TemporaryFileOnDisk>(disk, "tmp/")).first;

        temp_dir_owner = temp_dir_it->second;
        fs::path temp_dir = temp_dir_owner->getRelativePath();
        temp_part_dir = temp_dir / part_path_in_backup.relative_path();
        disk->createDirectories(temp_part_dir);
    }

    /// For example,
    /// part_path_in_backup = /data/test/table/0_1_1_0
    /// part_path_on_disk = store/f57/f5728353-44bb-4575-85e8-28deb893657a/0_1_1_0
    /// tmp_part_dir = tmp/1aaaaaa/data/test/table/0_1_1_0
    /// Or, for projections:
    /// part_path_in_backup = /data/test/table/0_1_1_0/prjmax.proj
    /// part_path_on_disk = store/f57/f5728353-44bb-4575-85e8-28deb893657a/0_1_1_0/prjmax.proj
    /// tmp_part_dir = tmp/1aaaaaa/data/test/table/0_1_1_0/prjmax.proj

    auto files_to_backup = files_without_checksums;
    for (const auto & [name, _] : checksums.files)
    {
        if (!name.ends_with(".proj"))
            files_to_backup.insert(name);
    }

    files_to_backup = getActualFileNamesOnDisk(files_to_backup);

    bool copy_encrypted = !backup_settings.decrypt_files_from_encrypted_disks;
    bool allow_checksums_from_remote_paths = backup_settings.allow_checksums_from_remote_paths;

    auto backup_file = [&](const String & filepath)
    {
        auto filepath_on_disk = part_path_on_disk / filepath;
        auto filepath_in_backup = part_path_in_backup / filepath;

        if (is_projection_part && allow_backup_broken_projection && !disk->existsFile(filepath_on_disk))
            return;

        if (make_temporary_hard_links)
        {
            String hardlink_filepath = temp_part_dir / filepath;
            disk->createHardLink(filepath_on_disk, hardlink_filepath);
            filepath_on_disk = hardlink_filepath;
        }

        std::optional<UInt64> file_size;
        std::optional<UInt128> file_hash;

        auto it = checksums.files.find(filepath);
        if (it != checksums.files.end())
        {
            file_size = it->second.file_size;
            file_hash = it->second.file_hash;
        }

        BackupEntryPtr backup_entry = std::make_unique<BackupEntryFromImmutableFile>(
            disk, filepath_on_disk, copy_encrypted, file_size, file_hash, allow_checksums_from_remote_paths);

        if (temp_dir_owner)
            backup_entry = wrapBackupEntryWith(std::move(backup_entry), temp_dir_owner);

        backup_entries.emplace_back(filepath_in_backup, std::move(backup_entry));
    };

    auto * log = &Poco::Logger::get("DataPartStorageOnDiskBase::backup");

    for (const auto & filepath : files_to_backup)
    {
        if (is_projection_part && allow_backup_broken_projection)
        {
            try
            {
                backup_file(filepath);
            }
            catch (Exception & e)
            {
                if (e.code() != ErrorCodes::FILE_DOESNT_EXIST)
                    throw;

                LOG_ERROR(log, "Cannot backup file {} of projection part {}. Will try to ignore it", filepath, part_dir);
                continue;
            }
        }
        else
        {
            backup_file(filepath);
        }
    }
}
// 用于将当前 Data Part 存储目录克隆/冻结到目标位置（常见于 ALTER TABLE ... FREEZE 备份快照或 DETACH 分区分离流程）
// 返回新建的可变分区存储智能指针 MutableDataPartStoragePtr。
// const std::string & to：目标冻结/克隆根目录的相对路径（相对于 disk 根目录，例如 shadow/<backup_name>/store/<uuid> 或 detached 所在路径前缀）。
// const std::string & dir_path：目标分区在 to 下的子目录路径（通常为分区目录名，如 all_1_1_0）。最终克隆出的分区目录路径为 fs::path(to) / dir_path。
// const ReadSettings & read_settings：克隆/备份读取源文件或数据时的读取参数配置。
// const WriteSettings & write_settings：写入目标克隆文件及系统列文件时的写入参数配置。
// std::function<void(const DiskPtr &)> save_metadata_callback：可选的元数据保存回调函数，在文件克隆完成后、清理旧/临时事务元数据文件之前执行，接收目标 DiskPtr 作为参数。
// const ClonePartParams & params：克隆控制参数结构体
MutableDataPartStoragePtr DataPartStorageOnDiskBase::freeze(
    const std::string & to,
    const std::string & dir_path,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    std::function<void(const DiskPtr &)> save_metadata_callback,
    const ClonePartParams & params) const
{
    // 1. 获取目标磁盘与创建目标目录
    // 从当前存储所属的 volume 获取底层磁盘 DiskPtr（disk），后续所有目录操作、备份克隆、元数据清理及新存储构造均基于此磁盘。
    auto disk = volume->getDisk();
    // 若提供了 params.external_transaction，则在事务中调用 createDirectories(to)；否则直接通过 disk->createDirectories(to) 创建。
    if (params.external_transaction)
        params.external_transaction->createDirectories(to);
    else
        disk->createDirectories(to);
    // 2. 执行分区文件克隆（硬链接或拷贝）与元数据回调
    // 将当前 Data Part 存储（源磁盘为 disk，源相对路径为 getRelativePath()）克隆到目标路径 fs::path(to) / dir_path（目标磁盘同样为 disk）：
    Backup(
        disk,
        disk,
        getRelativePath(),
        fs::path(to) / dir_path,
        read_settings,
        write_settings,
        params.make_source_readonly,
        /* max_level= */ {},
        params.copy_instead_of_hardlink,
        params.files_to_copy_instead_of_hardlinks,
        params.external_transaction);

    if (save_metadata_callback)
        save_metadata_callback(disk);

    /// Also remove any leftover `txn_version.txt.tmp`: leaving it without the main file makes the
    /// cloned/frozen part load as a rolled-back transaction (see `VersionMetadataOnDisk::loadMetadata`)
    /// and get discarded as `Outdated`. Remove the temporary file before the main file so the cleanup
    /// is fail-closed: a failure between the two removals leaves a valid `txn_version.txt` rather than
    /// the dangerous tmp-only state.
    // 3. 清理残留事务/销毁标记文件与写入失效系统列文件
    if (params.external_transaction)
    {
        // 移除销毁标记文件，防止冻结/分离出来的分区在销毁时误触发源生命周期清理。
        params.external_transaction->removeFileIfExists(fs::path(to) / dir_path / "delete-on-destroy.txt");
        // 先移除临时事务版本文件（如 txn_version.txt.tmp）。注释明确说明原因：若只剩 .tmp 而无主文件，加载时会被视为已回滚事务进而被丢弃为 Outdated；先删 .tmp 再删主文件属于 Fail-Closed（故障安全收敛） 设计，
        // 即使中间出错也只会留下合法的事务版本文件，不会陷入危险的仅剩 .tmp 状态。
        params.external_transaction->removeFileIfExists(fs::path(to) / dir_path / VersionMetadata::TMP_TXN_VERSION_METADATA_FILE_NAME);
        // 移除事务版本主文件（如 txn_version.txt）。
        params.external_transaction->removeFileIfExists(fs::path(to) / dir_path / VersionMetadata::TXN_VERSION_METADATA_FILE_NAME);
        if (!params.keep_metadata_version)
            params.external_transaction->removeFileIfExists(fs::path(to) / dir_path / IMergeTreeDataPart::METADATA_VERSION_FILE_NAME);
        IMergeTreeDataPart::writeInvalidatedSystemColumnsFile(*params.external_transaction, fs::path(to) / dir_path, params.invalidated_columns_to_write, write_settings);
    }
    else
    {
    // 非事务分支（else）：当没有 params.external_transaction 时，直接在 disk 上执行上述完全相同的 5 步操作（最后一步传入 *disk 作为写入目标）。
        disk->removeFileIfExists(fs::path(to) / dir_path / "delete-on-destroy.txt");
        disk->removeFileIfExists(fs::path(to) / dir_path / VersionMetadata::TMP_TXN_VERSION_METADATA_FILE_NAME);
        disk->removeFileIfExists(fs::path(to) / dir_path / VersionMetadata::TXN_VERSION_METADATA_FILE_NAME);
        if (!params.keep_metadata_version)
            disk->removeFileIfExists(fs::path(to) / dir_path / IMergeTreeDataPart::METADATA_VERSION_FILE_NAME);
        IMergeTreeDataPart::writeInvalidatedSystemColumnsFile(*disk, fs::path(to) / dir_path, params.invalidated_columns_to_write, write_settings);
    }

    /// Make the hardlink clone durable (the Backup loop above fsyncs nothing). This runs
    /// synchronously before freeze returns, so a caller that afterwards makes a destructive change
    /// (e.g. DETACH commits a covering empty part and drops the source) sees the clone already on
    /// disk. See the commit message / #111382 for the full rationale.
    // 4. 同步刷盘冻结目录树（本地非事务场景）
    // params.fsync_part_directory 为 true（要求同步刷盘）；
    // !params.external_transaction（当前不在未提交的外部事务中）；
    // !disk->isRemote()（目标磁盘不是远端对象存储磁盘）。
    if (params.fsync_part_directory && !params.external_transaction && !disk->isRemote())
        fsyncFrozenCloneTree(*disk, fs::path(to) / dir_path);

    /// The SingleDiskVolume and the DataPartStorageOnDiskFull built by `create` are stored on the
    /// frozen part for its whole lifetime; route them into the dedicated MergeTree arena, like the
    /// builder-owned storage path.
    // 5. 切换 jemalloc Arena 构造新存储对象并返回
    ScopedJemallocThreadArena mergetree_arena_scope(JemallocMergeTreeArena::getArenaIndex());
    auto single_disk_volume = std::make_shared<SingleDiskVolume>(disk->getName(), disk, 0);

    /// Do not initialize storage in case of DETACH because part may be broken.
    bool to_detached = dir_path.starts_with(std::string_view((fs::path(MergeTreeData::DETACHED_DIR_NAME) / "").string()));
    // 实例化冻结/克隆后的分区存储对象：
    auto frozen_storage = create(single_disk_volume, to, dir_path, /*initialize=*/ !to_detached && !params.external_transaction);

    return frozen_storage;
}

MutableDataPartStoragePtr DataPartStorageOnDiskBase::freezeRemote(
    const std::string & to,
    const std::string & dir_path,
    const DiskPtr & dst_disk,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    std::function<void(const DiskPtr &)> save_metadata_callback,
    const ClonePartParams & params) const
{
    auto src_disk = volume->getDisk();
    if (params.external_transaction)
        params.external_transaction->createDirectories(to);
    else
        dst_disk->createDirectories(to);

    /// freezeRemote() using copy instead of hardlinks for all files
    /// In this case, files_to_copy_intead_of_hardlinks is set by empty
    Backup(
        src_disk,
        dst_disk,
        getRelativePath(),
        fs::path(to) / dir_path,
        read_settings,
        write_settings,
        params.make_source_readonly,
        /* max_level= */ {},
        true,
        /* files_to_copy_intead_of_hardlinks= */ {},
        params.external_transaction);

    /// The save_metadata_callback function acts on the target dist.
    if (save_metadata_callback)
        save_metadata_callback(dst_disk);

    /// Also remove any leftover `txn_version.txt.tmp`: leaving it without the main file makes the
    /// cloned/frozen part load as a rolled-back transaction (see `VersionMetadataOnDisk::loadMetadata`)
    /// and get discarded as `Outdated`. Remove the temporary file before the main file so the cleanup
    /// is fail-closed: a failure between the two removals leaves a valid `txn_version.txt` rather than
    /// the dangerous tmp-only state.
    if (params.external_transaction)
    {
        params.external_transaction->removeFileIfExists(fs::path(to) / dir_path / "delete-on-destroy.txt");
        params.external_transaction->removeFileIfExists(fs::path(to) / dir_path / VersionMetadata::TMP_TXN_VERSION_METADATA_FILE_NAME);
        params.external_transaction->removeFileIfExists(fs::path(to) / dir_path / VersionMetadata::TXN_VERSION_METADATA_FILE_NAME);
        if (!params.keep_metadata_version)
            params.external_transaction->removeFileIfExists(fs::path(to) / dir_path / IMergeTreeDataPart::METADATA_VERSION_FILE_NAME);
        IMergeTreeDataPart::writeInvalidatedSystemColumnsFile(*params.external_transaction, fs::path(to) / dir_path, params.invalidated_columns_to_write, write_settings);
    }
    else
    {
        dst_disk->removeFileIfExists(fs::path(to) / dir_path / "delete-on-destroy.txt");
        dst_disk->removeFileIfExists(fs::path(to) / dir_path / VersionMetadata::TMP_TXN_VERSION_METADATA_FILE_NAME);
        dst_disk->removeFileIfExists(fs::path(to) / dir_path / VersionMetadata::TXN_VERSION_METADATA_FILE_NAME);
        if (!params.keep_metadata_version)
            dst_disk->removeFileIfExists(fs::path(to) / dir_path / IMergeTreeDataPart::METADATA_VERSION_FILE_NAME);
        IMergeTreeDataPart::writeInvalidatedSystemColumnsFile(*dst_disk, fs::path(to) / dir_path, params.invalidated_columns_to_write, write_settings);
    }

    /// The SingleDiskVolume and the DataPartStorageOnDiskFull built by `create` are stored on the
    /// frozen part for its whole lifetime; route them into the dedicated MergeTree arena.
    ScopedJemallocThreadArena mergetree_arena_scope(JemallocMergeTreeArena::getArenaIndex());
    auto single_disk_volume = std::make_shared<SingleDiskVolume>(dst_disk->getName(), dst_disk, 0);

    /// Do not initialize storage in case of DETACH because part may be broken.
    bool to_detached = dir_path.starts_with(std::string_view((fs::path(MergeTreeData::DETACHED_DIR_NAME) / "").string()));
    auto frozen_storage = create(single_disk_volume, to, dir_path, /*initialize=*/ !to_detached && !params.external_transaction);

    return frozen_storage;
}

MutableDataPartStoragePtr DataPartStorageOnDiskBase::clonePart(
    const std::string & to,
    const std::string & dir_path,
    const DiskPtr & dst_disk,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    LoggerPtr log,
    const std::function<void()> & cancellation_hook) const
{
    String path_to_clone = fs::path(to) / dir_path / "";
    auto src_disk = volume->getDisk();

    if (dst_disk->existsDirectory(path_to_clone))
    {
        throw Exception(ErrorCodes::DIRECTORY_ALREADY_EXISTS,
                        "Cannot clone part {} from '{}' to '{}': path '{}' already exists",
                        dir_path, getRelativePath(), path_to_clone, fullPath(dst_disk, path_to_clone));
    }

    try
    {
        dst_disk->createDirectories(to);
        src_disk->copyDirectoryContent(getRelativePath(), dst_disk, path_to_clone, read_settings, write_settings, cancellation_hook);
    }
    catch (...)
    {
        /// It's safe to remove it recursively (even with zero-copy-replication)
        /// because we've just did full copy through copyDirectoryContent
        LOG_WARNING(log, "Removing directory {} after failed attempt to move a data part", path_to_clone);
        dst_disk->removeRecursive(path_to_clone);
        throw;
    }

    /// The SingleDiskVolume and the DataPartStorageOnDiskFull built by `create` are stored on the
    /// cloned part for its whole lifetime; route them into the dedicated MergeTree arena.
    ScopedJemallocThreadArena mergetree_arena_scope(JemallocMergeTreeArena::getArenaIndex());
    auto single_disk_volume = std::make_shared<SingleDiskVolume>(dst_disk->getName(), dst_disk, 0);
    return create(single_disk_volume, to, dir_path, /*initialize=*/ true);
}

void DataPartStorageOnDiskBase::rename(
    std::string new_root_path,
    std::string new_part_dir,
    LoggerPtr log,
    bool remove_new_dir_if_exists,
    bool fsync_part_dir)
{
    if (new_root_path.ends_with('/'))
        new_root_path.pop_back();
    if (new_part_dir.ends_with('/'))
        new_part_dir.pop_back();

    String to = fs::path(new_root_path) / new_part_dir / "";

    if (volume->getDisk()->existsDirectory(to))
    {
        /// FIXME it should be logical error
        if (remove_new_dir_if_exists)
        {
            Names files;
            volume->getDisk()->listFiles(to, files);

            if (log)
                LOG_WARNING(log,
                    "Part directory {} already exists and contains {} files. Removing it.",
                    fullPath(volume->getDisk(), to), files.size());

            /// Do not remove blobs if they exist
            executeWriteOperation([&](auto & disk) { disk.removeSharedRecursive(to, true, {}); });
        }
        else
        {
            throw Exception(
                ErrorCodes::DIRECTORY_ALREADY_EXISTS,
                "Part directory {} already exists",
                fullPath(volume->getDisk(), to));
        }
    }

    String from = getRelativePath();

    /// Why?
    executeWriteOperation([&](auto & disk)
    {
        disk.setLastModified(from, Poco::Timestamp::fromEpochTime(time(nullptr)));
        disk.moveDirectory(from, to);

        /// Only after moveDirectory() since before the directory does not exist.
        if (fsync_part_dir)
        {
            /// Sync child before parent so a parent dentry never points at a not-yet-synced
            /// child: the moved dir, then the parent(s) holding the renamed dentry.
            { SyncGuardPtr to_sync_guard = volume->getDisk()->getDirectorySyncGuard(to); }

            /// parent_path() twice: step past the trailing slash on `to`/`from`, then to the container.
            const String to_parent = fs::path(to).parent_path().parent_path().string();
            const String from_parent = fs::path(from).parent_path().parent_path().string();

            if (!to_parent.empty())
                { SyncGuardPtr to_parent_sync_guard = volume->getDisk()->getDirectorySyncGuard(to_parent); }

            if (!from_parent.empty() && from_parent != to_parent)
                { SyncGuardPtr from_parent_sync_guard = volume->getDisk()->getDirectorySyncGuard(from_parent); }
        }
    });

    part_dir = new_part_dir;
    root_path = new_root_path;

    /// A successfully-loaded reader stays valid: its archive index is path-independent and reads
    /// resolve the archive's current location through readFile, so keep it (dropping it could also
    /// race a concurrent query holding it). But a cached *miss* may be stale: a probe that ran while
    /// this rename was in progress could have built packed_path from the old location and found no
    /// file. Clear that stale miss (only when there is no reader) so the next access re-probes the
    /// new path.
    {
        std::lock_guard lock(skip_indices_packed_mutex);
        if (!skip_indices_packed_reader)
            skip_indices_packed_probed = false;
    }
}

void DataPartStorageOnDiskBase::remove(
    CanRemoveCallback && can_remove_callback,
    const MergeTreeDataPartChecksums & checksums,
    std::list<ProjectionChecksums> projections,
    bool is_temp,
    LoggerPtr log)
{
    /// NOTE We rename part to delete_tmp_<relative_path> instead of delete_tmp_<name> to avoid race condition
    /// when we try to remove two parts with the same name, but different relative paths,
    /// for example all_1_2_1 (in Deleting state) and tmp_merge_all_1_2_1 (in Temporary state).
    fs::path from = fs::path(root_path) / part_dir;
    // TODO directory delete_tmp_<name> is never removed if server crashes before returning from this function

    /// Cut last "/" if it exists (it shouldn't). Otherwise fs::path behave differently.
    fs::path part_dir_without_slash = part_dir.ends_with("/") ? part_dir.substr(0, part_dir.size() - 1) : part_dir;

    /// NOTE relative_path can contain not only part name itself, but also some prefix like
    /// "moving/all_1_1_1" or "detached/all_2_3_5". We should handle this case more properly.

    /// File might be already renamed on previous try
    bool has_delete_prefix = part_dir_without_slash.filename().string().starts_with("delete_tmp_");
    std::optional<CanRemoveDescription> can_remove_description;
    auto disk = volume->getDisk();
    fs::path to = fs::path(root_path) / part_dir_without_slash;

    if (!has_delete_prefix)
    {
        auto parent_path = getParentDirectory();
        if (!parent_path.empty())
        {
            if (parent_path == MergeTreeData::DETACHED_DIR_NAME)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Trying to remove detached part {} with path {} in remove function. It shouldn't happen",
                    part_dir,
                    root_path);

            part_dir_without_slash = fs::path(parent_path) / ("delete_tmp_" + std::string{part_dir_without_slash.filename()});
        }
        else
        {
            part_dir_without_slash = ("delete_tmp_" + std::string{part_dir_without_slash.filename()});
        }

        to = fs::path(root_path) / part_dir_without_slash;

        if (disk->existsDirectory(to))
        {
            LOG_WARNING(log, "Directory {} (to which part must be renamed before removing) already exists. "
                        "Most likely this is due to unclean restart or race condition. Removing it.", fullPath(disk, to));
            try
            {
                can_remove_description.emplace(can_remove_callback());
                disk->removeSharedRecursive(
                    fs::path(to) / "", !can_remove_description->can_remove_anything, can_remove_description->files_not_to_remove);
            }
            catch (const fs::filesystem_error & e)
            {
                if (e.code() == std::errc::no_such_file_or_directory)
                {
                    /// If the directory was already removed (e.g. by clearOldTemporaryDirectories), nothing to do.
                }
                else
                    throw;
            }
            catch (...)
            {
                LOG_ERROR(
                    log, "Cannot recursively remove directory {}. Exception: {}", fullPath(disk, to), getCurrentExceptionMessage(false));
                throw;
            }
        }

        if (!disk->existsDirectory(from))
        {
            LOG_WARNING(log, "Directory {} (part to remove) doesn't exist or one of nested files has gone. Most likely this is due to manual removing. This should be discouraged. Ignoring.", fullPath(disk, from));
            /// We will never touch this part again, so unlocking it from zero-copy
            if (!can_remove_description)
                can_remove_description.emplace(can_remove_callback());
            return;
        }

        /// Evaluate can_remove_callback before moving the directory so zero-copy reference checks
        /// use the current (existing) path. We intentionally don't update part_dir to avoid races.
        if (!can_remove_description)
            can_remove_description.emplace(can_remove_callback());

        try
        {
            disk->moveDirectory(from, to);
            /// NOTE: we intentionally don't update part_dir here because it would cause a data race
            /// with concurrent readers (e.g. system.parts table queries calling getFullPath()).
            /// The part is being removed anyway, so the path doesn't need to be updated.
        }
        catch (const Exception & e)
        {
            if (e.code() == ErrorCodes::FILE_DOESNT_EXIST)
            {
                LOG_WARNING(log, "Directory {} (part to remove) doesn't exist or one of nested files has gone. Most likely this is due to manual removing. This should be discouraged. Ignoring.", fullPath(disk, from));
                return;
            }
            throw;
        }
        catch (const fs::filesystem_error & e)
        {
            if (e.code() == std::errc::no_such_file_or_directory)
            {
                LOG_WARNING(log, "Directory {} (part to remove) doesn't exist or one of nested files has gone. "
                          "Most likely this is due to manual removing. This should be discouraged. Ignoring.", fullPath(disk, from));
                return;
            }
            throw;
        }
    }

    if (!can_remove_description)
        can_remove_description.emplace(can_remove_callback());

    // Record existing projection directories so we don't remove them twice
    std::unordered_set<String> projection_directories;
    std::string proj_suffix = ".proj";
    for (const auto & projection : projections)
    {
        std::string proj_dir_name = projection.name + proj_suffix;
        projection_directories.emplace(proj_dir_name);

        NameSet files_not_to_remove_for_projection;
        for (const auto & file_name : can_remove_description->files_not_to_remove)
            if (file_name.starts_with(proj_dir_name))
                files_not_to_remove_for_projection.emplace(fs::path(file_name).filename());

        if (!files_not_to_remove_for_projection.empty())
            LOG_DEBUG(
                log, "Will not remove files [{}] for projection {}", fmt::join(files_not_to_remove_for_projection, ", "), projection.name);

        CanRemoveDescription proj_description
        {
            can_remove_description->can_remove_anything,
            std::move(files_not_to_remove_for_projection),
        };

        clearDirectory(fs::path(to) / proj_dir_name, proj_description, projection.checksums, is_temp, log);
    }

    /// It is possible that we are removing the part which have a written but not loaded projection.
    /// Such a part can appear server was restarted after DROP PROJECTION but before old part was removed.
    /// In this case, the old part will load only projections from metadata.
    /// See test 01701_clear_projection_and_part.
    for (const auto & [name, _] : checksums.files)
    {
        if (endsWith(name, proj_suffix) && !projection_directories.contains(name))
        {
            static constexpr auto checksums_name = "checksums.txt";
            auto projection_storage = create(volume, to, name, /*initialize=*/ true);

            /// If we have a directory with suffix '.proj' it is likely a projection.
            /// Try to load checksums for it (to avoid recursive removing fallback).
            if (projection_storage->existsFile(checksums_name))
            {
                try
                {
                    MergeTreeDataPartChecksums tmp_checksums;
                    auto in = projection_storage->readFile(checksums_name, {}, {});
                    tmp_checksums.read(*in);

                    clearDirectory(fs::path(to) / name, *can_remove_description, tmp_checksums, is_temp, log);
                }
                catch (...)
                {
                    LOG_ERROR(log, "Cannot load checksums from {}", fs::path(projection_storage->getRelativePath()) / checksums_name);
                }
            }
        }
    }

    clearDirectory(to, *can_remove_description, checksums, is_temp, log);
}

void DataPartStorageOnDiskBase::clearDirectory(
    const std::string & dir,
    const CanRemoveDescription & can_remove_description,
    const MergeTreeDataPartChecksums & checksums,
    bool is_temp,
    LoggerPtr log)
{
    auto disk = volume->getDisk();
    auto [can_remove_shared_data, names_not_to_remove] = can_remove_description;
    names_not_to_remove = getActualFileNamesOnDisk(names_not_to_remove);

    /// It does not make sense to try fast path for incomplete temporary parts, because some files are probably absent.
    /// Sometimes we add something to checksums.files before actually writing checksums and columns on disk.
    /// Also sometimes we write checksums.txt and columns.txt in arbitrary order, so this check becomes complex...
    bool incomplete_temporary_part = is_temp && (!disk->existsFile(fs::path(dir) / "checksums.txt") || !disk->existsFile(fs::path(dir) / "columns.txt"));
    if (checksums.empty() || incomplete_temporary_part)
    {
        /// If the part is not completely written, we cannot use fast path by listing files.
        try
        {
            disk->removeSharedRecursive(fs::path(dir) / "", !can_remove_shared_data, names_not_to_remove);
        }
        catch (const fs::filesystem_error & e)
        {
            if (e.code() == std::errc::no_such_file_or_directory)
            {
                /// If the directory was already removed (e.g. by clearOldTemporaryDirectories), nothing to do.
            }
            else
                throw;
        }
        return;
    }

    try
    {
        NameSet names_to_remove = {"checksums.txt", "columns.txt"};
        for (const auto & [file, _] : checksums.files)
            if (!endsWith(file, ".proj"))
                names_to_remove.emplace(file);

        names_to_remove = getActualFileNamesOnDisk(names_to_remove);

        /// Remove each expected file in directory, then remove directory itself.
        RemoveBatchRequest request;
        for (const auto & file : names_to_remove)
            request.emplace_back(fs::path(dir) / file);
        request.emplace_back(fs::path(dir) / "default_compression_codec.txt", true);
        request.emplace_back(fs::path(dir) / "delete-on-destroy.txt", true);
        request.emplace_back(fs::path(dir) / VersionMetadata::TXN_VERSION_METADATA_FILE_NAME, true);
        /// A leftover `txn_version.txt.tmp` would otherwise be missed here and leave the directory
        /// non-empty, forcing the slow recursive-removal fallback below.
        request.emplace_back(fs::path(dir) / VersionMetadata::TMP_TXN_VERSION_METADATA_FILE_NAME, true);
        request.emplace_back(fs::path(dir) / "metadata_version.txt", true);
        request.emplace_back(fs::path(dir) / IMergeTreeDataPart::COLUMNS_SUBSTREAMS_FILE_NAME, true);
        request.emplace_back(fs::path(dir) / IMergeTreeDataPart::INVALIDATED_SYSTEM_COLUMNS_FILE_NAME, true);

        disk->removeSharedFiles(request, !can_remove_shared_data, names_not_to_remove);
        disk->removeDirectory(dir);
    }
    catch (...)
    {
        /// Recursive directory removal does many excessive "stat" syscalls under the hood.

        LOG_ERROR(log, "Cannot quickly remove directory {} by removing files; fallback to recursive removal. Reason: {}", fullPath(disk, dir), getCurrentExceptionMessage(false));
        try
        {
            disk->removeSharedRecursive(fs::path(dir) / "", !can_remove_shared_data, names_not_to_remove);
        }
        catch (const fs::filesystem_error & e)
        {
            if (e.code() == std::errc::no_such_file_or_directory)
            {
                /// If the directory was already removed (e.g. by clearOldTemporaryDirectories), nothing to do.
            }
            else
                throw;
        }
    }
}

void DataPartStorageOnDiskBase::changeRootPath(const std::string & from_root, const std::string & to_root)
{
    /// This is a very dumb implementation, here for root path like
    /// "some/current/path/to/part" and change like
    /// "some/current" -> "other/different", we just replace prefix to make new root like
    /// "other/different/path/to/part".
    /// Here we expect that actual move was done by somebody else.

    size_t prefix_size = from_root.size();
    if (prefix_size > 0 && from_root.back() == '/')
        --prefix_size;

    if (prefix_size > root_path.size()
        || std::string_view(from_root).substr(0, prefix_size) != std::string_view(root_path).substr(0, prefix_size))
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot change part root to {} because it is not a prefix of current root {}",
            from_root, root_path);

    size_t dst_size = to_root.size();
    if (dst_size > 0 && to_root.back() == '/')
        --dst_size;

    /// `root_path` is part-lifetime metadata of this (arena-owned) storage, so build its new value in
    /// the dedicated arena instead of the caller's default arena. Parent-part commit calls this for
    /// every projection storage, so otherwise the projection path escapes back to the default arena.
    {
        ScopedJemallocThreadArena mergetree_arena_scope(JemallocMergeTreeArena::getArenaIndex());
        root_path = to_root.substr(0, dst_size) + root_path.substr(prefix_size);
    }

    /// See rename: keep a successfully-loaded (path-independent) reader, but clear a stale cached
    /// miss so the next access re-probes the new path.
    {
        std::lock_guard lock(skip_indices_packed_mutex);
        if (!skip_indices_packed_reader)
            skip_indices_packed_probed = false;
    }
}

SyncGuardPtr DataPartStorageOnDiskBase::getDirectorySyncGuard() const
{
    return volume->getDisk()->getDirectorySyncGuard(fs::path(root_path) / part_dir);
}

std::unique_ptr<WriteBufferFromFileBase> DataPartStorageOnDiskBase::writeTransactionFile(const String & txn_file_name, WriteMode mode) const
{
    return volume->getDisk()->writeFile(fs::path(root_path) / part_dir / txn_file_name, 256, mode);
}

void DataPartStorageOnDiskBase::removeRecursive()
{
    executeWriteOperation([&](auto & disk) { disk.removeRecursive(fs::path(root_path) / part_dir); });
}

void DataPartStorageOnDiskBase::removeSharedRecursive(bool keep_in_remote_fs)
{
    executeWriteOperation([&](auto & disk) { disk.removeSharedRecursive(fs::path(root_path) / part_dir, keep_in_remote_fs, {}); });
}

void DataPartStorageOnDiskBase::createDirectories()
{
    executeWriteOperation([&](auto & disk) { disk.createDirectories(fs::path(root_path) / part_dir); });
}

bool DataPartStorageOnDiskBase::hasActiveTransaction() const
{
    return transaction != nullptr;
}

bool DataPartStorageOnDiskBase::isCaseInsensitive() const
{
    return getDisk()->isCaseInsensitive();
}
// 盘分区存储基类中用于快速判断并获取 skp_idx.packed（打包跳数索引归档）读取器的受保护（protected）虚方法。
// const std::string & name：请求查询或读取的分区内逻辑文件名（例如 "user_id.bin"、"checksums.txt"，或二级跳数索引子流文件名 "skp_idx_my_idx.idx2"）。
std::shared_ptr<const PackedFilesReader> DataPartStorageOnDiskBase::getArchiveReaderForFile(const std::string & name) const
{
    /// Prefix gate: only "skp_idx_..." names can be archive members, so unrelated files never load
    /// or probe skp_idx.packed.
    // 在触碰磁盘、缓存或互斥锁之前，先在纯内存中进行一次极低开销的字符串前缀检查（判断 name 是否符合跳数索引文件的命名规范，即以 "skp_idx_" 开头）。
    // 核心设计动机（性能与隔离性优化）：
    // 避免多余的元数据 I/O：一个分区读取过程中会高频查询大量普通文件（如 checksums.txt、columns.txt、primary.idx 以及几十上百个列 .bin/.mrk2 文件）。在 S3/HDFS 等远程对象存储或 ZooKeeper/Keeper 磁盘上，
    // 探测 skp_idx.packed 是否存在会产生额外的网络元数据请求。通过这行前缀门控，所有普通文件在内存中直接短路返回 nullptr，完全不会触发对 skp_idx.packed 的探测或加载。
    if (!looksLikePackedSkipIndexFile(name))
        return nullptr;
    auto reader = getSkipIndicesPackedReader();
    // 3. 校验目标文件是否真实存在于归档目录中
    return (reader && reader->exists(name)) ? reader : nullptr;
}
// const std::string & name 待查询的逻辑文件名（相对于当前 Data Part 存储目录的相对路径，例如某列的 .bin、.mrk2 数据/标记文件，或分区元信息文件）。
bool DataPartStorageOnDiskBase::existsFile(const std::string & name) const
{
    // 查询当前 Data Part 是否存在包含逻辑文件 name 的归档读取器（如将多个小文件合并打包后的归档索引）。
    if (getArchiveReaderForFile(name))
        return true;
    return existsFileImpl(name);
}

size_t DataPartStorageOnDiskBase::getFileSize(const std::string & file_name) const
{
    if (auto reader = getArchiveReaderForFile(file_name))
        return reader->getFileSize(file_name);
    return getFileSizeImpl(file_name);
}
// 磁盘型分区存储（包括 Full 和 Packed 两种存储布局）构建底层文件读取管道（ReadPipeline）的统一入口方法
// 核心职责是实现“打包跳数索引覆盖层（skp_idx.packed Overlay）”的透明分流：优先检查目标文件是否位于分区级的跳数索引归档包内，若命中则构建归档切片读取源；否则下沉交给具体子类的 prepareReadImpl 处理常规列文件。
void DataPartStorageOnDiskBase::prepareRead(
    // 请求读取的分区内逻辑文件名（例如 "user_id.bin"、"checksums.txt"，或者二级跳数索引子流文件名 "skp_idx_my_idx.idx2"）。
    const std::string & name,
    // 读取配置参数集合，包含缓冲区大小、是否启用本地文件系统缓存、异步预读配置、IO 优先级及校验和选项等。
    const ReadSettings & settings,
    // 读取大小提示（可选值）。
    // 在 MergeTreeReaderWide 等场景下，通常传入从 .mrk 索引算出的目标读取区间长度，帮助底层缓冲区精确分配内存或发起指定字节范围（Range）的远程对象存储请求
    std::optional<size_t> read_hint,
    ReadPipeline & pipeline) const
{
    // 1. 打包跳数索引归档（skp_idx.packed）探测与拦截
    // 调用受保护的虚方法 getArchiveReaderForFile(name) 检查 name 是否是存储在 skp_idx.packed 归档文件中的虚拟跳数索引文件。
    if (auto reader = getArchiveReaderForFile(name))
    {
        /// Members of skp_idx.packed skip the disk's normal pipeline (filesystem cache, async
        /// prefetch) and read through PackedFilesReader::readFile, which opens the archive via the
        /// underlying disk and wraps the result with ReadBufferFromFileView at the right offset.
        /// The archive's current location is captured here, so the reader holds no path of its own.
        // 2. 命中归档后的路径快照与读取源构造
        // 获取当前分区所在的底层 IDisk 智能指针（本地磁盘或远程 S3/HDFS 磁盘）。
        auto disk = volume->getDisk();
        // 动态拼接出当前 skp_idx.packed 归档文件在 disk 上的完整相对路径（例如 "database/table/all_1_1_0/skp_idx.packed"）。
        String archive_path = fs::path(root_path) / part_dir / String(SKIP_INDICES_PACKED_FILENAME);
        // 3. 组装 ReadPipeline::BufferCreator 工厂闭包并设为管道数据源
        ReadPipeline::BufferCreator creator =
            [reader, disk, archive_path, name, read_hint](const StoredObject &, const ReadSettings & s, bool, bool)
            {
                return reader->readFile(disk, archive_path, name, s, read_hint);
            };
        pipeline.setSource(std::move(creator), StoredObjects{StoredObject{}}, settings);
        return;
    }
    prepareReadImpl(name, settings, read_hint, pipeline);
}

std::unique_ptr<ReadBufferFromFileBase> DataPartStorageOnDiskBase::readFileIfExists(
    const std::string & name,
    const ReadSettings & settings,
    std::optional<size_t> read_hint) const
{
    if (auto reader = getArchiveReaderForFile(name))
        return reader->readFile(
            volume->getDisk(),
            fs::path(root_path) / part_dir / String(SKIP_INDICES_PACKED_FILENAME),
            name, settings, read_hint);
    return readFileIfExistsImpl(name, settings, read_hint);
}
// 磁盘分区存储基类中用于线程安全地懒加载（Lazy Load）并缓存分区级打包跳数索引归档（skp_idx.packed）读取器的受保护虚方法。
std::shared_ptr<const PackedFilesReader> DataPartStorageOnDiskBase::getSkipIndicesPackedReader() const
{
    // 加互斥锁，确保同一时刻只有一个线程执行磁盘探测与 skp_idx.packed 归档头部解析，其余并发查询线程在此等待结果。
    std::lock_guard lock(skip_indices_packed_mutex);
    // 正向缓存命中：若 probed == true 且 reader != nullptr，直接返回已解析好的归档读取器。
    if (skip_indices_packed_probed)
        return skip_indices_packed_reader;
    // 在当前线程上下文中设置 ZooKeeper/Keeper 协调组件标签（RAII 守卫，函数退出时自动恢复）
    // 当底层 disk 是带有 Keeper 元数据映射的远程对象存储（如 DiskObjectStorage）时，该标签用于追踪并统计是哪个模块发起了元数据查询。
    auto component_guard = Coordination::setCurrentComponent("DataPartStorageOnDiskBase::getSkipIndicesPackedReader");
    // 实时拼接出当前分区目录下 skp_idx.packed 文件的相对路径（例如 "database/table/all_1_1_0/skp_idx.packed"）。
    // 之所以每次动态拼接而非缓存固定路径，是因为分区在构建期间会经历 rename 移动。
    const String packed_path = fs::path(root_path) / part_dir / String(SKIP_INDICES_PACKED_FILENAME);
    // 获取当前分区所属存储卷对应的底层 IDisk 智能指针。
    auto disk = volume->getDisk();
    // 3. 探测文件存在性并处理并发竞态（TOCTOU 防护）
    if (disk->existsFile(packed_path))
    {
        /// On shared storage another replica may delete or relocate skp_idx.packed between the
        /// existence check above and the open below, so the open fails with a "no such key" error.
        /// Expect404ResponseScope keeps that 404 from being logged or counted as a DiskS3NoSuchKeyError
        /// (which fails stress tests); catching alone is not enough. If the archive is still present
        /// it is a genuine read error -> rethrow. Otherwise it was removed or moved: do NOT cache a
        /// miss (return without setting probed), so the next access re-probes against the current
        /// path -- after a rename the archive lives at the new path and must not be lost. A part that
        /// genuinely has no archive is cached as a miss via the existsFile-false branch above, so this
        /// does not loop.
        Expect404ResponseScope scope;
        try
        {
            // 构造 PackedFilesReader，它会打开 packed_path 文件并读取归档末尾的索引目录表（记录每个虚拟跳数索引子流的偏移量和大小）。
            skip_indices_packed_reader = std::make_shared<PackedFilesReader>(disk, packed_path, ReadSettings{});
        }
        catch (const Exception &)
        {
            if (disk->existsFile(packed_path))
                throw;
            return nullptr;
        }
    }
    // 4. 标记探测完成并返回结果
    skip_indices_packed_probed = true;
    return skip_indices_packed_reader;
}

void DataPartStorageOnDiskBase::seedSkipIndicesPackedReader(const PackedFilesIO::Index & index) const
{
    std::lock_guard lock(skip_indices_packed_mutex);
    skip_indices_packed_reader = std::make_shared<PackedFilesReader>(index);
    skip_indices_packed_probed = true;
}

void DataPartStorageOnDiskBase::seedSkipIndicesPackedReaderFrom(const IDataPartStorage & source) const
{
    const auto * source_disk = dynamic_cast<const DataPartStorageOnDiskBase *>(&source);
    if (!source_disk)
        return;

    /// Same-class access to the protected probe is allowed; this also triggers the source's lazy
    /// load if it hasn't been read yet.
    auto source_archive = source_disk->getSkipIndicesPackedReader();
    if (!source_archive)
        return;

    seedSkipIndicesPackedReader(source_archive->getIndex());
}

bool DataPartStorageOnDiskBase::isFileInPackedSkipIndicesArchive(const std::string & name) const
{
    auto reader = getSkipIndicesPackedReader();
    return reader != nullptr && reader->exists(name);
}

bool DataPartStorageOnDiskBase::hasSkipIndicesPackedArchive() const
{
    return getSkipIndicesPackedReader() != nullptr;
}

void DataPartStorageOnDiskBase::copyArchiveEntryTo(
    const PackedFilesReader & source_archive,
    const String & file_name,
    PackedFilesWriter & target,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings) const
{
    /// Route the read through this storage's readFile (the overlay), not source_archive.readFile,
    /// so a storage where skp_idx.packed isn't a flat disk file still composes the read correctly.
    const auto file_size = source_archive.getFileSize(file_name);
    auto src = readFile(file_name, read_settings, file_size);
    auto dst = target.writeFile(file_name, write_settings);
    copyData(*src, *dst);
    dst->finalize();
    /// Carry over the uncompressed size so the rewritten archive keeps v1 accounting.
    if (auto uncompressed = source_archive.getFileUncompressedSize(file_name))
        target.setUncompressedSize(file_name, *uncompressed);
}

void DataPartStorageOnDiskBase::copyPackedSkipIndicesFilesInto(
    const NameSet & file_names,
    PackedFilesWriter & target,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings) const
{
    if (file_names.empty())
        return;

    auto source_archive = getSkipIndicesPackedReader();
    if (!source_archive)
        return;

    for (const auto & file_name : file_names)
    {
        if (source_archive->exists(file_name))
            copyArchiveEntryTo(*source_archive, file_name, target, read_settings, write_settings);
    }
}

void DataPartStorageOnDiskBase::filterPackedSkipIndicesArchiveTo(
    const NameSet & dropped_skip_index_archive_file_names,
    IDataPartStorage & new_storage,
    const WriteSettings & write_settings,
    const ReadSettings & read_settings,
    MergeTreeDataPartChecksums & checksums,
    bool sync) const
{
    auto source_archive = getSkipIndicesPackedReader();
    if (!source_archive)
        return;

    const String packed_filename{SKIP_INDICES_PACKED_FILENAME};
    /// Drop the inherited archive entry up front; we either re-add it below with the new
    /// contents or, if all virtual files were dropped, leave it removed so the new part
    /// reflects "no skip-index archive at all".
    checksums.remove(packed_filename);

    PackedFilesWriter writer;
    bool any_kept = false;

    for (const auto & file_name : source_archive->getFileNames())
    {
        /// Exact match: dropped_skip_index_archive_file_names lists the full virtual filenames inside the
        /// archive, not name prefixes. This is what prevents an index named "a" from also
        /// dropping files belonging to "a.b" when escape_index_filenames is off.
        if (dropped_skip_index_archive_file_names.contains(file_name))
            continue;

        any_kept = true;
        copyArchiveEntryTo(*source_archive, file_name, writer, read_settings, write_settings);
    }

    if (!any_kept)
        return;

    auto out = new_storage.writeFile(packed_filename, DBMS_DEFAULT_BUFFER_SIZE, write_settings);
    HashingWriteBuffer hashing(*out);
    auto [packed_index, _] = writer.finalize(hashing, {}, PackedFilesIO::VERSION_WITH_UNCOMPRESSED_SIZE);
    hashing.finalize();

    auto & checksum = checksums.files[packed_filename];
    checksum.file_size = hashing.count();
    checksum.file_hash = hashing.getHash();

    out->finalize();
    /// Match the rest of the mutated part's durability: with need_sync=true the caller fsyncs
    /// other on-disk artifacts (checksums.txt, column streams), so the rewritten archive must
    /// reach the device too. Otherwise a crash between finalize and checksum publication could
    /// leave the new part referencing an archive whose contents weren't flushed.
    if (sync)
        out->sync();

    /// Seed new_storage's reader from the in-memory index, mirroring fillSkipIndicesChecksums.
    /// On object-storage disks with a non-fake transaction (Keeper metadata) the archive's
    /// metadata isn't committed until the part transaction commits, which happens after
    /// finalizeMutatedPart. With columns_and_secondary_indices_sizes_lazy_calculation=0 the
    /// size accounting runs inside finalizeMutatedPart, before that commit; without this seed
    /// getSkipIndicesPackedReader probes the disk, misses the not-yet-committed archive, caches
    /// nullptr, and the surviving packed index's size is latched at zero.
    if (auto * disk_storage = dynamic_cast<DataPartStorageOnDiskBase *>(&new_storage))
        disk_storage->seedSkipIndicesPackedReader(packed_index);
}

}
