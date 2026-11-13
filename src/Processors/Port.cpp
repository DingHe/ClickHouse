#include <Processors/Port.h>
#include <Processors/IProcessor.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
// 责将上游输出端口（OutputPort）与下游输入端口（InputPort）进行互联的核心全局函数。它建立了两个算子（Processor）之间的数据传输通道。
// OutputPort & output 传人引用参数。代表数据流的上游输出端口（负责向外推送数据块）。
// InputPort & input 传人引用参数。代表数据流的下游输入端口（负责从上游拉取数据块）。
// bool reconnect 控制标志。默认为 false。如果传入 true，表示允许在端口已经连接的情况下进行“重新连接”（Reconnection），跳过重复连接的报错检查。
void connect(OutputPort & output, InputPort & input, bool reconnect)
{
    // 如果 reconnect 为 false（即不允许重连），并且当前 input 端口的 state 已经存在（说明它已经被连接到某个输出端口上了），则属于非法操作。
    if (!reconnect && input.state)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Port is already connected, (header: [{}])", input.header->dumpStructure());

    if (!reconnect && output.state)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Port is already connected, (header: [{}])", output.header->dumpStructure());
    // 获取上下游处理器的名称
    // 检查上游端口和下游端口是否绑定了所属的处理器（processor 指针是否非空）
    auto out_name = output.processor ? output.getProcessor().getName() : "null";
    auto in_name = input.processor ? input.getProcessor().getName() : "null";
    // 表头兼容性断言检查
    // 严格校验上游输出端口的表头（output.getHeader()）与下游输入端口的表头（input.getHeader()）是否兼容（即列名、数据类型必须严格匹配）。
    assertCompatibleHeader(output.getHeader(), input.getHeader(), fmt::format("function connect between {} and {}", out_name, in_name));
    // 建立双向指针引用
    input.output_port = &output;
    output.input_port = &input;
    // 共享状态机（State）的创建与绑定
    // 至关重要的一步：通过让上下游持有同一个 std::shared_ptr<Port::State>，它们在后续的多线程并发中就可以通过这个共享状态原子性地交换数据（push / pull）和状态标志位（HAS_DATA, IS_NEEDED 等），从而实现完全无锁的高效数据传输通道。
    input.state = std::make_shared<Port::State>();
    output.state = input.state;
}

void disconnect(OutputPort & output, InputPort & input)
{
    if (output.input_port != &input || input.output_port != &output)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot disconnect ports that are not connected to each other");

    /// Disconnect from pipeline edges
    input.update_info = nullptr;
    output.update_info = nullptr;

    /// Disconnect from each other
    input.output_port = nullptr;
    output.input_port = nullptr;

    /// Reset shared State on both sides
    input.state.reset();
    output.state.reset();
}

}
