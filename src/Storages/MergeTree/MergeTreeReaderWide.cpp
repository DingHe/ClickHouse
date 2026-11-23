#include <Storages/MergeTree/MergeTreeReaderWide.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnSparse.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeNested.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/DeserializationPrefixesCache.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeDataPartWide.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>
#include <IO/SharedThreadPools.h>
#include <Compression/CachedCompressedReadBuffer.h>

namespace DB
{

namespace
{
    constexpr auto DATA_FILE_EXTENSION = ".bin";
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeReaderWide::MergeTreeReaderWide(
    MergeTreeDataPartInfoForReaderPtr data_part_info_,
    NamesAndTypesList columns_,
    const VirtualFields & virtual_fields_,
    const StorageSnapshotPtr & storage_snapshot_,
    const MergeTreeSettingsPtr & storage_settings_,
    UncompressedCache * uncompressed_cache_,
    MarkCache * mark_cache_,
    DeserializationPrefixesCache * deserialization_prefixes_cache_,
    MarkRanges mark_ranges_,
    MergeTreeReaderSettings settings_,
    ValueSizeMap avg_value_size_hints_,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback_,
    clockid_t clock_type_)
    : IMergeTreeReader(
        data_part_info_,
        columns_,
        virtual_fields_,
        storage_snapshot_,
        storage_settings_,
        uncompressed_cache_,
        mark_cache_,
        mark_ranges_,
        settings_,
        avg_value_size_hints_)
    , deserialization_prefixes_cache(deserialization_prefixes_cache_)
    , profile_callback(profile_callback_)
    , clock_type(clock_type_)
    , read_without_marks(
        settings.can_read_part_without_marks
        && all_mark_ranges.isOneRangeForWholePart(data_part_info_for_read->getMarksCount()))
{
    try
    {
        for (size_t i = 0; i < columns_to_read.size(); ++i)
        {
            /// Column was dropped by a pending mutation or invalidated. Don't read stale data;
            if (!isColumnDroppedByPendingMutation(i) && !isSystemColumnInvalidated(i))
                addStreams(columns_to_read[i], serializations[i]);
        }
    }
    catch (...)
    {
        if (!isRetryableException(std::current_exception()))
            data_part_info_for_read->reportBroken();
        throw;
    }
}

void MergeTreeReaderWide::prefetchBeginOfRange(Priority priority)
{
    prefetched_streams.clear();

    if (all_mark_ranges.getNumberOfMarks() == 0)
        return;

    try
    {
        /// Start prefetches for all columns. But don't deserialize prefixes, because it can be a heavy operation
        /// (for example for JSON column) and starting prefetches for all subcolumns here can consume a lot of memory.
        prefetchForAllColumns(priority, columns_to_read.size(), all_mark_ranges.front().begin, false, /*deserialize_prefixes=*/false);
        prefetched_from_mark = all_mark_ranges.front().begin;
        /// Arguments explanation:
        /// Current prefetch is done for read tasks before they can be picked by reading threads in IMergeTreeReadPool::getTask method.
        /// 1. columns_to_read.size() == requested_columns.size() == readRows::res_columns.size().
        /// 2. continue_reading == false, as we haven't read anything yet.
    }
    catch (...)
    {
        if (!isRetryableException(std::current_exception()))
            data_part_info_for_read->reportBroken();
        throw;
    }
}
// ClickHouse 宽表读取器（MergeTreeReaderWide）在真正逐列反序列化数据之前，批量向底层文件系统发起异步预读（Prefetch）请求的核心方法。
// 在宽表格式（Wide Part）中，每一列都有独立的 .bin 数据文件。如果串行读取 50 列，每读一列都要等待一次磁盘或远程对象存储（如 S3/HDFS）的 I/O 往返延迟。
// 该方法通过提前遍历所有待读取列并下发异步预取指令，让底层 I/O 线程池能够并行加载所有列的数据块。
// Priority priority：I/O 调度优先级（封装了读取任务的优先级数值）。底层异步读取线程池（如 AsynchronousReadBufferFromRemoteFS）会根据该优先级来调度不同查询或后台合并任务的预读请求顺序。
// size_t num_columns：需要预读的列总数（通常等于 columns_to_read.size()）。
// size_t from_mark：计划开始读取的起始 Mark（颗粒 Granule）索引编号，用于定位每列 .bin 文件中的读取偏移量。
// bool continue_reading：是否紧接上一次读取结束的位置继续向后读取：
// bool deserialize_prefixes：布尔开关，指示在预取各列主体数据流之前，是否先预取并反序列化复杂类型的公共头部前缀（如 LowCardinality 的字典流、Tuple/Nested 的外层结构前缀）
// Mark（标记） 是连接稀疏主键索引（primary.idx）与底层列压缩数据文件（.bin）之间的地址定位指针。
// ClickHouse 的数据按主键排序后，每间隔 index_granularity 行（默认 8192 行）划分为一个逻辑数据颗粒，称为 Granule。为了节省内存，稀疏主索引 primary.idx 只记录每个 Granule 起始行的主键值，并不直接存储磁盘文件字节偏移量。
// 原因在于：
// 列式独立压缩：在 Wide 格式下，每一列单独存为一个 .bin 文件，且 .bin 文件由多个变长的压缩块（Compressed Block，默认约 64KB～1MB）拼接而成。不同列的数据压缩率不同，无法通过行号直接推算出磁盘位置。
// 解耦索引与物理存储：通过引入独立的 Mark 文件（.mrk / .mrk2 / .mrk3），主索引只需要给出目标颗粒的编号（即 mark_number，第几个 Granule），再由 Mark 文件将其翻译为物理磁盘坐标。
// 在典型的宽表（Wide Part）下，每个列都有自己对应的 .mrk2 文件（如 user_id.mrk2）。文件由一条条固定长度的 Mark 记录顺序排列而成，第 $i$ 条 Mark 记录对应第 $i$ 个 Granule 的起始位置。
void MergeTreeReaderWide::prefetchForAllColumns(
    Priority priority,
    size_t num_columns,
    size_t from_mark,
    bool continue_reading,
    bool deserialize_prefixes)
{
    // 1. 判断当前存储介质是否启用预读
    // 根据当前 Data Part 所在的存储介质类型（远程对象存储 vs 本地磁盘），从读取配置 settings.read_settings 中选择对应的预读开关：
    bool do_prefetch = data_part_info_for_read->getDataPartStorage()->isStoredOnRemoteDisk()
        ? settings.read_settings.remote_fs_settings.prefetch
        : settings.read_settings.local_fs_settings.prefetch;
    // 2. 准入条件检查与并发预读数量限制
    // 开关与空范围检查：如果对应存储介质未开启预读（!do_prefetch），或者当前分配到的总待读 Mark 数为 0（无数据可读），直接 return 跳过。
    if (!do_prefetch || all_mark_ranges.getNumberOfMarks() == 0)
        return;
    // 如果配置了单次最大并发文件系统预读数限制（settings.filesystem_prefetches_limit > 0），且当前查询涉及的列数 num_columns 超过了该阈值（例如一张宽表有上千列 SELECT *），则放弃批量预读直接 return。
    // 设计动机：同时为上千列创建异步读取缓冲区并向 S3/内核发起上千个并发请求，会导致内存瞬间暴涨或打满远程存储连接池；超过阈值时退化为按需同步读取更稳妥。
    if (settings.filesystem_prefetches_limit && num_columns > settings.filesystem_prefetches_limit)
        return;
    // 当 deserialize_prefixes 为 true 时，先对所有包含公共前缀的复杂列（例如同一 Nested 或 Tuple 下的多个子列、带有共享字典的 LowCardinality 列）发起前缀流的预取并完成前缀反序列化。
    if (deserialize_prefixes)
        deserializePrefixForAllColumnsWithPrefetch(num_columns, from_mark, priority);

    /// Request reading of data in advance,
    /// so if reading can be asynchronous, it will also be performed in parallel for all columns.
    // 4. 遍历所有列发起并行异步预读
    for (size_t pos = 0; pos < num_columns; ++pos)
    {
        // 过滤无效列：如果第 pos 列已被未合并的 DROP COLUMN Mutation 删除（isColumnDroppedByPendingMutation(pos)），或该系统列已失效（isSystemColumnInvalidated(pos)），
        // 说明后续 readRows 根本不会去读它的磁盘文件，因此直接 continue 跳过，避免浪费多余的磁盘 I/O。
        if (isColumnDroppedByPendingMutation(pos) || isSystemColumnInvalidated(pos))
            continue;

        try
        {
            auto & cache = caches[columns_to_read[pos].getNameInStorage()];
            // 调用单列预读
            // 根据该列的序列化器 serializations[pos] 遍历其关联的所有底层文件流（例如普通列只有 .bin 流；Nullable 有 .null.bin 和 .bin 两个流；Array 有 .size0.bin 和 .bin 两个流），定位到 from_mark 对应的压缩块偏移量，
            // 并向底层 ReadBuffer 发送异步预读指令（将任务提交给 I/O 线程池后台加载，当前线程不阻塞等待数据就绪，立刻继续循环下一列）
            prefetchForColumn(
                priority, columns_to_read[pos], serializations[pos], from_mark, continue_reading, cache);
        }
        catch (Exception & e)
        {
            /// Better diagnostics.
            e.addMessage("(while reading column " + columns_to_read[pos].name + ")");
            throw;
        }
    }
}
// ClickHouse 宽表格式（Wide Part，即每一列单独存储为一个 .bin 数据文件和 .mrk/.mrk2 索引文件）的核心数据读取实现。
// 负责按颗粒（Granule/Mark）和行数，并发预读、反序列化多列数据，并处理列缺失、缓存复用以及损坏分区诊断。
// size_t from_mark：起始读取的 Mark（数据颗粒 Granule）索引编号。当需要重新寻址（Seek）时，读取器根据该编号从 .mrk 文件中查找对应列在 .bin 文件中的压缩块偏移量与解压后偏移量。
// bool continue_reading：是否紧接上一次读取结束的位置继续向后读取： true：忽略 from_mark 的 seek 操作，直接从当前流缓冲区指针继续往后顺序读（消除连续范围读取时的冗余寻址开销）。false：主动执行 seek 定位到 from_mark 处开始读取（通常在首次读取或跳过了不连续的 Mark 区间时触发）。
// size_t max_rows_to_read：本次调用允许读取的最大行数上限（由上层 MergeTreeRangeReader 结合 Granule 边界和期望块大小计算得出）。
// MutableColumns & res_columns：输出列容器数组（std::vector<MutableColumnPtr> 的引用），长度与 columns_to_read 一一对应。读取到的数据会直接追加（Append）到对应位置的列对象末尾；若元素为 nullptr，方法内部会自动创建新列；若某列在当前 Part 中缺失或已被删除，对应位置会被置为 nullptr 交由后续 fillMissingColumns 补全
// 返回值 size_t：本次实际读取到的行数。
size_t MergeTreeReaderWide::readRows(
    size_t from_mark, bool continue_reading, size_t max_rows_to_read,
    MutableColumns & res_columns)
{
    // 1. 初始化与校验预取缓存有效性
    // 记录本批次中所有列读取到的最大行数。
    size_t read_rows = 0;
    // 预取状态检查
    if (prefetched_from_mark != -1 && static_cast<size_t>(prefetched_from_mark) != from_mark)
    {
        prefetched_streams.clear();
        prefetched_from_mark = -1;
    }
    // 2. 列数校验与零列快速路径
    try
    {
        size_t num_columns = res_columns.size();
        // 校验传入的 res_columns 数组长度是否与 columns_to_read 的大小严格一致，不一致则抛出逻辑异常。
        checkNumberOfColumns(num_columns);
        // 当查询只需要虚拟列（如 SELECT _part FROM table）或纯计数（如 SELECT count() FROM table 且无过滤条件）时，
        // 不需要从磁盘读取任何物理列，num_columns 为 0。此时直接返回 max_rows_to_read，跳过一切磁盘 I/O。
        if (num_columns == 0)
            return max_rows_to_read;
        // 3. 批量异步预读与公共前缀反序列化
        // 在逐列解码之前，一次性遍历所有待读取列，向底层 MergeTreeReaderStream（尤其是 S3/HDFS 等远程对象存储或异步本地文件读取器）发起批量异步预读（Prefetch）请求，
        // 使多列 I/O 能够并发重叠执行，而不是串行等待每一列的磁盘响应。
        prefetchForAllColumns(Priority{}, num_columns, from_mark, continue_reading, /*deserialize_prefixes=*/ true);
        // 针对复杂类型或子列（如 Array、Tuple、Map、Nullable、LowCardinality），
        // 在读取主体数据前统一反序列化它们的公共头部前缀（例如 LowCardinality 的字典流或嵌套类型的外层结构前缀），确保后续读取子列时状态就绪。
        deserializePrefixForAllColumns(num_columns, from_mark);

        for (size_t pos = 0; pos < num_columns; ++pos)
        {
            // 跳过已删除或失效的列：
            if (isColumnDroppedByPendingMutation(pos) || isSystemColumnInvalidated(pos))
            {
                res_columns[pos] = nullptr;
                continue;
            }
            // 获取/创建输出列容器
            const auto & column_to_read = columns_to_read[pos];

            /// The column may already be present (we append the values to the end) or empty; either way it is
            /// uniquely owned here, so we read into it directly without cloning.
            // 如果 res_columns[pos] 为空（首轮读取），调用数据类型和序列化器的 createColumn 创建一个新的空可变列对象
            auto & column = res_columns[pos];
            if (!column)
                column = column_to_read.type->createColumn(*serializations[pos]);

            try
            {
                // 记录读取前该列已有的行数
                size_t column_size_before_reading = column->size();
                // 当同一张表同时读取同一个父列的多个子列（例如读取 tuple_col.a 和 tuple_col.b，或 nested_col.x 和 nested_col.y）时，
                // 第一个子列读取共享数据流（如 offsets 或父 Tuple）后会存入这两个缓存中，第二个子列直接从内存缓存复用，避免对同一底层文件流重复读取与反序列化。
                auto & cache = caches[column_to_read.getNameInStorage()];
                auto & deserialize_states_cache = deserialize_states_caches[column_to_read.getNameInStorage()];
                // 核心单列读取函数
                readData(
                    column_to_read,
                    serializations[pos],
                    *column,
                    from_mark,
                    continue_reading,
                    max_rows_to_read,
                    cache,
                    deserialize_states_cache);

                /// For elements of Nested, column_size_before_reading may be greater than column size
                ///  if offsets are not empty and were already read, but elements are empty.

                if (!column->empty())
                    read_rows = std::max(read_rows, column->size() - column_size_before_reading);
            }
            catch (Exception & e)
            {
                /// Better diagnostics.
                e.addMessage("(while reading column " + column_to_read.name + ")");
                throw;
            }
            // 如果要求读取的行数 max_rows_to_read > 0，但读完后 column 依然为空（说明该列在当前历史 Data Part 中物理文件不存在，例如 ALTER TABLE ADD COLUMN 新增的列）
            if (column->empty() && max_rows_to_read > 0)
                res_columns[pos] = nullptr;
        }

        prefetched_streams.clear();
        caches.clear();

        /// NOTE: positions for all streams must be kept in sync.
        /// In particular, even if for some streams there are no rows to be read,
        /// you must ensure that no seeks are skipped and at this point they all point to to_mark.
    }
    catch (...)
    {
        if (!isRetryableException(std::current_exception()))
            data_part_info_for_read->reportBroken();

        /// Better diagnostics.
        try
        {
            rethrow_exception(std::current_exception());
        }
        catch (Exception & e)
        {
            e.addMessage(getMessageForDiagnosticOfBrokenPart(from_mark, max_rows_to_read));
        }

        throw;
    }

    return read_rows;
}

void MergeTreeReaderWide::addStreams(
    const NameAndTypePair & name_and_type,
    const SerializationPtr & serialization)
{
    bool has_any_stream = false;
    bool has_all_streams = true;

    ISerialization::StreamCallback callback = [&] (const ISerialization::SubstreamPath & substream_path)
    {
        /// Don't create streams for ephemeral subcolumns that don't store any real data.
        if (ISerialization::isEphemeralSubcolumn(substream_path, substream_path.size()))
            return;

        auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, ".bin", data_part_info_for_read->getChecksums(), storage_settings);

        /** If data file is missing then we will not try to open it.
          * It is necessary since it allows to add new column to structure of the table without creating new files for old parts.
          */
        if (!stream_name)
        {
            has_all_streams = false;
            return;
        }

        if (streams.contains(*stream_name))
        {
            has_any_stream = true;
            return;
        }

        addStream(substream_path, *stream_name);
        has_any_stream = true;
    };

    serialization->enumerateStreams(callback);

    if (has_any_stream && !has_all_streams)
        partially_read_columns.insert(name_and_type.name);
}

MergeTreeReaderWide::FileStreams::iterator MergeTreeReaderWide::addStream(const ISerialization::SubstreamPath & substream_path, const String & stream_name)
{
    auto context = data_part_info_for_read->getContext();
    auto * load_marks_threadpool = settings.load_marks_asynchronously ? &context->getLoadMarksThreadpool() : nullptr;
    size_t num_marks_in_part = data_part_info_for_read->getMarksCount();

    auto marks_loader = std::make_shared<MergeTreeMarksLoader>(
        data_part_info_for_read,
        mark_cache,
        data_part_info_for_read->getIndexGranularityInfo().getMarksFilePath(stream_name),
        num_marks_in_part,
        data_part_info_for_read->getIndexGranularityInfo(),
        settings.save_marks_in_cache,
        settings.read_settings,
        load_marks_threadpool,
        /*num_columns_in_mark=*/ 1,
        settings.use_streaming_marks_compression);

    auto stream_settings = settings;
    stream_settings.is_low_cardinality_dictionary = ISerialization::isLowCardinalityDictionarySubcolumn(substream_path);
    stream_settings.is_metadata_file = ISerialization::isMetadataStream(substream_path);
    stream_settings.is_single_value_per_part = ISerialization::isSingleValuePerPartStream(substream_path);

    auto create_stream = [&]<typename Stream>()
    {
        return std::make_unique<Stream>(
            data_part_info_for_read->getDataPartStorage(), stream_name, DATA_FILE_EXTENSION,
            num_marks_in_part, all_mark_ranges, stream_settings,
            uncompressed_cache, data_part_info_for_read->getFileSizeOrZero(stream_name + DATA_FILE_EXTENSION),
            std::move(marks_loader), profile_callback, clock_type);
    };

    if (read_without_marks)
        return streams.emplace(stream_name, create_stream.operator()<MergeTreeReaderStreamSingleColumnWholePart>()).first;

    marks_loader->startAsyncLoad();
    return streams.emplace(stream_name, create_stream.operator()<MergeTreeReaderStreamSingleColumn>()).first;
}

ReadBuffer * MergeTreeReaderWide::getStream(
    bool seek_to_start,
    const ISerialization::SubstreamPath & substream_path,
    const MergeTreeDataPartChecksums & checksums,
    const NameAndTypePair & name_and_type,
    size_t from_mark,
    bool seek_to_mark,
    ISerialization::SubstreamsCache & cache)
{
    /// If substream have already been read.
    if (cache.contains(ISerialization::getSubcolumnNameForStream(substream_path)))
        return nullptr;

    auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, ".bin", checksums, storage_settings);
    if (!stream_name)
    {
        /// We allow missing streams only for columns/subcolumns that are not present in this part.
        auto column = data_part_info_for_read->getColumnsDescription().tryGetColumn(GetColumnsOptions::AllPhysical, name_and_type.getNameInStorage());
        if (column && (!name_and_type.isSubcolumn() || column->type->hasSubcolumn(name_and_type.getSubcolumnName())))
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Stream {} for column {} with type {} is not found",
                ISerialization::getFileNameForStream(
                    name_and_type.name, substream_path, ISerialization::StreamFileNameSettings(*storage_settings)),
                    name_and_type.name,
                    name_and_type.type->getName());
        }

        return nullptr;
    }

    auto it = streams.find(*stream_name);
    if (it == streams.end())
    {
        /// If we didn't create requested stream, but file with this path exists, create a stream for it.
        /// It may happen during reading of columns with dynamic subcolumns, because all streams are known
        /// only after deserializing of binary bulk prefix.

        it = addStream(substream_path, *stream_name);
    }

    MergeTreeReaderStream & stream = *it->second;
    stream.adjustRightMark(last_mark_to_read);

    if (seek_to_start)
        stream.seekToStart();
    else if (seek_to_mark)
        stream.seekToMark(from_mark);

    return stream.getDataBuffer();
}

void MergeTreeReaderWide::deserializePrefix(
    const SerializationPtr & serialization,
    const NameAndTypePair & name_and_type,
    size_t from_mark,
    DeserializeBinaryBulkStateMap & deserialize_state_map,
    ISerialization::SubstreamsCache & cache,
    ISerialization::SubstreamsDeserializeStatesCache & deserialize_states_cache,
    ISerialization::StreamCallback prefixes_prefetch_callback)
{
    const auto & name = name_and_type.name;
    if (!deserialize_state_map.contains(name))
    {
        ISerialization::DeserializeBinaryBulkSettings deserialize_settings;
        deserialize_settings.object_and_dynamic_read_statistics = true;
        deserialize_settings.prefixes_prefetch_callback = prefixes_prefetch_callback;
        deserialize_settings.data_part_type = MergeTreeDataPartType::Wide;
        deserialize_settings.prefixes_deserialization_thread_pool = settings.use_prefixes_deserialization_thread_pool ? &getMergeTreePrefixesDeserializationThreadPool().get() : nullptr;
        deserialize_settings.getter = [&](const ISerialization::SubstreamPath & substream_path)
        {
            auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, ".bin", data_part_info_for_read->getChecksums(), storage_settings);
            /// This stream could be prefetched in prefetchBeginOfRange, but here we
            /// have to seek the stream to the start of file to deserialize the prefix.
            /// If we do not read from the first mark, we should remove this stream from
            /// prefetched_streams to prefetch it again starting from the current mark
            /// after prefix is deserialized.
            if (stream_name && from_mark != 0)
                prefetched_streams.erase(*stream_name);

            return getStream(/* seek_to_start = */true, substream_path, data_part_info_for_read->getChecksums(), name_and_type, 0, /* seek_to_mark = */false, cache);
        };
        deserialize_settings.seek_to_start_callback = [&](const ISerialization::SubstreamPath & substream_path)
        {
            auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, ".bin", data_part_info_for_read->getChecksums(), storage_settings);
            if (!stream_name)
                return;

            if (from_mark != 0)
                prefetched_streams.erase(*stream_name);

            auto it = streams.find(*stream_name);
            if (it == streams.end())
                it = addStream(substream_path, *stream_name);

            it->second->adjustRightMark(last_mark_to_read);
            it->second->seekToStart();
        };
        /// Add streams for newly discovered dynamic subcolumns to start async marks loading beforehand if needed.
        deserialize_settings.dynamic_subcolumns_callback = [&](const ISerialization::SubstreamPath & substream_path)
        {
            /// Don't create streams for ephemeral subcolumns that don't store any real data.
            if (ISerialization::isEphemeralSubcolumn(substream_path, substream_path.size()))
                return;

            auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, ".bin", data_part_info_for_read->getChecksums(), storage_settings);
            if (stream_name && !streams.contains(*stream_name))
                addStream(substream_path, *stream_name);
        };
        deserialize_settings.release_stream_callback = [&](const ISerialization::SubstreamPath & substream_path)
        {
            auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, ".bin", data_part_info_for_read->getChecksums(), storage_settings);
            if (stream_name)
                streams.erase(*stream_name);
        };
        deserialize_settings.check_stream_exists_callback = [&](const ISerialization::SubstreamPath & substream_path) -> bool
        {
            auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(
                name_and_type, substream_path, ".bin",
                data_part_info_for_read->getChecksums(), storage_settings);
            return stream_name.has_value();
        };
        deserialize_settings.release_all_prefixes_streams = settings.read_only_column_sample;
        deserialize_settings.has_uniform_marks_callback =
            [&](const ISerialization::SubstreamPath & substream_path,
                size_t max_transitions) -> bool
        {
            /// Wide parts with a final mark have a trailing position after the
            /// suffix, so a single per-part dictionary shows up as <= 2 distinct
            /// positions in the dictionary stream (data mark + final mark).
            /// Without a final mark, the same check must be stricter: a true
            /// single-dictionary part has only one distinct position, while a part
            /// with one dictionary in the main stream and another in the suffix can
            /// still look like "2 positions".
            /// This is only a necessary condition; `SerializationLowCardinality`
            /// still checks the `DictionaryKeys` stream reaches EOF after the
            /// first dictionary.
            const bool has_final_mark = data_part_info_for_read->getIndexGranularity().hasFinalMark();
            const size_t allowed_distinct_marks = has_final_mark || max_transitions == 0
                ? max_transitions
                : max_transitions - 1;

            auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(
                name_and_type, substream_path, ".bin",
                data_part_info_for_read->getChecksums(), storage_settings);
            if (!stream_name)
                return false;

            auto it = streams.find(*stream_name);
            if (it == streams.end())
                return false;

            return it->second->hasAtMostNDistinctMarks(allowed_distinct_marks);
        };
        serialization->deserializeBinaryBulkStatePrefix(deserialize_settings, deserialize_state_map[name], &deserialize_states_cache);
    }
}

void MergeTreeReaderWide::deserializePrefixForAllColumnsImpl(size_t num_columns, size_t from_mark, StreamCallbackGetter prefixes_prefetch_callback_getter)
{
    /// Check if we already deserialized prefixes.
    if (!deserialize_binary_bulk_state_map.empty())
        return;

    auto deserialize = [&]()
    {
        DeserializeBinaryBulkStateMap deserialize_state_map;
        for (size_t pos = 0; pos < num_columns; ++pos)
        {
            if (isColumnDroppedByPendingMutation(pos) || isSystemColumnInvalidated(pos))
                continue;

            try
            {
                auto & cache = caches[columns_to_read[pos].getNameInStorage()];
                auto & deserialize_states_cache = deserialize_states_caches[columns_to_read[pos].getNameInStorage()];
                deserializePrefix(
                    serializations[pos],
                    columns_to_read[pos],
                    from_mark,
                    deserialize_state_map,
                    cache,
                    deserialize_states_cache,
                    prefixes_prefetch_callback_getter ? prefixes_prefetch_callback_getter(columns_to_read[pos]) : ISerialization::StreamCallback{});
            }
            catch (Exception & e)
            {
                /// Better diagnostics.
                e.addMessage("(while reading prefix of column " + columns_to_read[pos].name + ")");
                throw;
            }
        }

        return deserialize_state_map;
    };

    /// If we have cache, deserialize through it.
    if (deserialization_prefixes_cache && settings.use_deserialization_prefixes_cache)
        deserialize_binary_bulk_state_map = deserialization_prefixes_cache->getOrSet(deserialize);
    else
        deserialize_binary_bulk_state_map = deserialize();
}

void MergeTreeReaderWide::deserializePrefixForAllColumns(size_t num_columns, size_t from_mark)
{
    deserializePrefixForAllColumnsImpl(num_columns, from_mark, {});
}

void MergeTreeReaderWide::deserializePrefixForAllColumnsWithPrefetch(size_t num_columns, size_t from_mark, Priority priority)
{
    auto prefixes_prefetch_callback_getter = [&](const NameAndTypePair & name_and_type)
    {
        return [&](const ISerialization::SubstreamPath & substream_path)
        {
            auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, ".bin", data_part_info_for_read->getChecksums(), storage_settings);
            if (stream_name && !prefetched_streams.contains(*stream_name))
            {
                if (ReadBuffer * buf = getStream(/* seek_to_start = */true, substream_path, data_part_info_for_read->getChecksums(), name_and_type, 0, /* seek_to_mark = */false, caches[name_and_type.getNameInStorage()]))
                {
                    buf->prefetch(priority);
                    prefetched_streams.insert(*stream_name);
                }
            }
        };
    };

    deserializePrefixForAllColumnsImpl(num_columns, from_mark, prefixes_prefetch_callback_getter);
}

void MergeTreeReaderWide::prefetchForColumn(
    Priority priority,
    const NameAndTypePair & name_and_type,
    const SerializationPtr & serialization,
    size_t from_mark,
    bool continue_reading,
    ISerialization::SubstreamsCache & cache)
{
    auto callback = [&](const ISerialization::SubstreamPath & substream_path)
    {
        /// Skip ephemeral subcolumns that don't store any real data.
        if (ISerialization::isEphemeralSubcolumn(substream_path, substream_path.size()))
            return;

        /// Skip substreams that don't need to be prefetched.
        if (!ISerialization::isPrefetchNeededForSubstream(substream_path, substream_path.size(), settings.prefetch_json_shared_data_substreams))
            return;

        auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, ".bin", data_part_info_for_read->getChecksums(), storage_settings);

        if (stream_name && !prefetched_streams.contains(*stream_name))
        {
            bool seek_to_mark = !continue_reading && !read_without_marks;
            if (ReadBuffer * buf = getStream(false, substream_path, data_part_info_for_read->getChecksums(), name_and_type, from_mark, seek_to_mark, cache))
            {
                buf->prefetch(priority);
                prefetched_streams.insert(*stream_name);
            }
        }
    };

    /// If we already deserialized prefixes, we can use deserialization state during streams enumeration to enumerate dynamic subcolumns.
    if (!deserialize_binary_bulk_state_map.empty())
    {
        auto data = ISerialization::SubstreamData(serialization).withType(name_and_type.type).withDeserializeState(deserialize_binary_bulk_state_map[name_and_type.name]);
        ISerialization::EnumerateStreamsSettings settings;
        serialization->enumerateStreams(settings, callback, data);
    }
    else
    {
        serialization->enumerateStreams(callback);
    }
}


void MergeTreeReaderWide::readData(
    const NameAndTypePair & name_and_type,
    const SerializationPtr & serialization,
    IColumn & column,
    size_t from_mark,
    bool continue_reading,
    size_t max_rows_to_read,
    ISerialization::SubstreamsCache & cache,
    ISerialization::SubstreamsDeserializeStatesCache & deserialize_states_cache)
{
    ISerialization::DeserializeBinaryBulkSettings deserialize_settings;
    deserialize_settings.data_part_type = MergeTreeDataPartType::Wide;

    deserializePrefix(serialization, name_and_type, from_mark, deserialize_binary_bulk_state_map, cache, deserialize_states_cache, {});

    deserialize_settings.getter = [&](const ISerialization::SubstreamPath & substream_path)
    {
        auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, ".bin", data_part_info_for_read->getChecksums(), storage_settings);
        bool was_prefetched = stream_name && prefetched_streams.contains(*stream_name);
        bool seek_to_mark = !was_prefetched && !continue_reading && !read_without_marks;

        return getStream(
            /* seek_to_start = */false, substream_path,
            data_part_info_for_read->getChecksums(),
            name_and_type, from_mark, seek_to_mark, cache);
    };

    deserialize_settings.seek_stream_to_mark_callback = [&](const ISerialization::SubstreamPath & substream_path, const MarkInCompressedFile & mark)
    {
        auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, ".bin", data_part_info_for_read->getChecksums(), storage_settings);
        if (!stream_name)
            return;

        streams[*stream_name]->seekToMark(mark);
    };

    /// Seek a substream's stream to the current granule's mark. Needed by serializations that read a
    /// value not implied by the ongoing range position (e.g. a per-part value broadcast to every
    /// granule): the data getter above seeks only conditionally (skipped on prefetch/continue_reading),
    /// so such a stream can be left at a sibling substream's offset. With `read_without_marks` the
    /// whole part is one range read from the start and the stream has no marks to seek to, so skip it.
    deserialize_settings.seek_stream_to_current_mark_callback = [&](const ISerialization::SubstreamPath & substream_path)
    {
        if (read_without_marks)
            return;

        auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, ".bin", data_part_info_for_read->getChecksums(), storage_settings);
        if (!stream_name)
            return;

        auto it = streams.find(*stream_name);
        if (it != streams.end())
            it->second->seekToMark(from_mark);
    };

    deserialize_settings.get_avg_value_size_hint_callback
        = [&](const ISerialization::SubstreamPath & substream_path) -> double
    {
        auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, ".bin", data_part_info_for_read->getChecksums(), storage_settings);
        if (!stream_name)
            return 0.0;

        return avg_value_size_hints[*stream_name];
    };

    deserialize_settings.update_avg_value_size_hint_callback
        = [&](const ISerialization::SubstreamPath & substream_path, const IColumn & column_)
    {
        auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, ".bin", data_part_info_for_read->getChecksums(), storage_settings);
        if (!stream_name)
            return;

        IDataType::updateAvgValueSizeHint(column_, avg_value_size_hints[*stream_name]);
    };

    deserialize_settings.continuous_reading = continue_reading;
    auto & deserialize_state = deserialize_binary_bulk_state_map[name_and_type.name];

    serialization->deserializeBinaryBulkWithMultipleStreams(
        column, max_rows_to_read, deserialize_settings, deserialize_state, &cache);
}

std::unordered_map<String, std::vector<String>> MergeTreeReaderWide::getAllColumnsSubstreams()
{
    /// We need to read prefixes to be able to collect all streams (because of dynamic structure of some columns).
    deserializePrefixForAllColumns(columns_to_read.size(), 0);
    std::unordered_map<String, std::vector<String>> column_to_streams;
    for (size_t i = 0; i < columns_to_read.size(); ++i)
    {
        const auto & name_and_type = columns_to_read[i];
        const auto & serialization = serializations[i];

        ISerialization::StreamCallback callback = [&] (const ISerialization::SubstreamPath & substream_path)
        {
            if (ISerialization::isEphemeralSubcolumn(substream_path, substream_path.size()))
                return;

            if (auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, ".bin", data_part_info_for_read->getChecksums(), storage_settings))
                column_to_streams[name_and_type.name].push_back(*stream_name);
        };

        auto data = ISerialization::SubstreamData(serialization).withType(name_and_type.type).withDeserializeState(deserialize_binary_bulk_state_map[name_and_type.name]);
        ISerialization::EnumerateStreamsSettings settings;
        serialization->enumerateStreams(settings, callback, data);
    }

    return column_to_streams;
}

}
