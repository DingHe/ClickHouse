#pragma once
#include <Processors/ISource.h>


namespace DB
{

// 主要用于没有数据需要读取的情况。
// 例如 SELECT * FROM t WHERE 1 = 0;
class NullSource final : public ISource
{
public:
    explicit NullSource(SharedHeader header) : ISource(std::move(header)) {}
    String getName() const override { return "NullSource"; }

protected:
    Chunk generate() override { return Chunk(); }
};

}
