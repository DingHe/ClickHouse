#pragma once

#include <IO/PackedFilesIO.h>
#include <IO/ReadSettings.h>
#include <Core/Names.h>
#include <base/types.h>
#include <IO/ReadBuffer.h>

namespace DB
{

class IDisk;
class ReadBufferFromFileBase;
using DiskPtr = std::shared_ptr<IDisk>;

/// Class that allows to read files from archive written by PackedFilesWriter.
///
/// Holds only the archive index (file name -> offset+size within the archive). The index is
/// path-independent: it stays valid when the part is renamed or moved, because the offsets inside
/// the archive do not change. The disk and the archive's current path are supplied by the caller
/// to readFile, never cached here, so a concurrent rename cannot leave the reader pointing at a
/// stale location, and the reader can be shared without locking.
// 定义了 ClickHouse I/O 层中用于读取由 PackedFilesWriter 生成的归档文件（如 skp_idx.packed 或 data.packed）的读取器类 PackedFilesReader。
// PackedFilesReader 类的核心作用
// 解析并驻留归档文件内部索引（PackedFilesIO::Index）：
// PackedFilesWriter 将多个逻辑小文件顺序合并写入单个归档大文件，并在归档末尾写入目录索引区。PackedFilesReader 负责解析该目录索引并在内存中保存“逻辑文件名 $\rightarrow$ 归档内 (offset, size)”的不可变映射表。
// 路径无关与无锁并发共享设计（Path-Independent & Lock-Free）：
// 正如类注释所述，PackedFilesReader 只保存归档内部的相对偏移索引，不缓存底层 DiskPtr 或归档文件路径。
// 原因在于 ClickHouse 的分区在生命周期中会频繁发生原子移动或重命名（例如从 tmp_all_1_1_0 重命名为 all_1_1_0，或移动到 detached/），但归档文件内部各成员的相对字节偏移量永远不变。通过由调用方在每次 readFile 时实时传入当前的 disk 和 data_file_name，
// 彻底消除了并发重命名导致路径过期（Stale Path）的竞态风险，同时使该对象成为纯不可变（Immutable）数据结构，可在多线程间无锁共享。
// 提供虚拟文件的元数据查询与切片流读取：
// 向上层（如 DataPartStorageOnDiskBase 和 DataPartStorageOnDiskPacked）提供文件存在性检查、大小查询以及按偏移范围裁剪的 ReadBufferFromFileBase 视图流，使上层读取器无需感知底层文件其实打包在一个大文件中。
class PackedFilesReader
{
public:
    /// Constructor that loads the index from an archive file on disk. The disk and path are used
    /// only to read the index here; they are not retained.
    // disk：归档文件所在的底层存储磁盘智能指针（本地盘或 S3/HDFS 远程盘）。
    // data_file_name：归档文件在 disk 上的相对路径（例如 "database/table/all_1_1_0/skp_idx.packed"）。
    // read_settings：读取归档尾部索引区时使用的 I/O 读取配置。
    // 作用：从磁盘打开归档文件，定位并解析文件末尾的索引区域，初始化 index 成员。构造完成后不会保留 disk 和 data_file_name 的引用。
    PackedFilesReader(const DiskPtr & disk, const String & data_file_name, const ReadSettings & read_settings);

    /// Constructor that initializes the index by the provided one.
    // 已构建好的归档索引对象，按值传入以便移动构造
    explicit PackedFilesReader(PackedFilesIO::Index index_);

    /// Common read operations which return data from index.
    // 在内存 index 中查找 file_name 是否存在，存在返回 true，否则返回 false（纯内存查找，无磁盘 I/O）。
    bool exists(const std::string & file_name) const;
    // 查询并返回指定逻辑文件在归档中的物理存储字节大小（size）；如果文件不存在通常会抛出异常。
    size_t getFileSize(const String & file_name) const;
    /// Recorded uncompressed size, or nullopt if not recorded (v0). Throws if file is absent.
    // 返回归档索引中记录的该逻辑文件的未压缩字节大小
    std::optional<UInt64> getFileUncompressedSize(const String & file_name) const;
    // 获取指定逻辑文件在归档大文件中的完整位置结构体 PackedFilesIO::FileOffset（包含起始偏移量 offset、物理大小 size 等信息）。
    PackedFilesIO::FileOffset getFileOffsetAndSize(const String & file_name) const;
    // 遍历内存 index，返回当前归档文件内包含的所有逻辑成员文件名列表（std::vector<String>），常用于目录遍历（iterate()）或复制/过滤归档内容。
    Names getFileNames() const;

    /// Returns read buffer to read requested file as a part of the archive file. The archive's
    /// current location (disk + path) is supplied by the caller, so a relocated part reads from
    /// the right place without the reader caching a path.
    // file_name：要读取的归档内部逻辑成员文件名（如 "skp_idx_my_idx.idx2"）。
    std::unique_ptr<ReadBufferFromFileBase> readFile(
        const DiskPtr & disk,
        const String & data_file_name,
        const String & file_name,
        const ReadSettings & settings,
        std::optional<size_t> read_hint) const;

    const PackedFilesIO::Index & getIndex() const { return index; }

    static PackedFilesIO::Index readIndex(ReadBuffer & in);

private:
    /// Index of archive: immutable and path-independent.
    // 归档文件的目录索引映射表
    // 内部记录了归档内所有虚拟成员文件的元数据（包括文件在归档中的起始字节偏移量 offset、物理存储字节数 size，以及可选的未压缩字节大小 uncompressed_size）。
    const PackedFilesIO::Index index;
};

}
