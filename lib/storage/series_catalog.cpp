#include "series_catalog.hpp"

#include <algorithm>

namespace timestar {

namespace {
constexpr uint8_t kMaxValueType = static_cast<uint8_t>(TSMValueType::Integer);  // Float,Boolean,String,Integer
}

void CatalogEntry::canonicalize() {
    std::sort(tags.begin(), tags.end());
}

void CatalogEntry::encodeInto(std::string& out) const {
    codec::putStr(out, measurement);
    codec::putU32(out, static_cast<uint32_t>(tags.size()));
    for (const auto& [key, value] : tags) {
        codec::putStr(out, key);
        codec::putStr(out, value);
    }
    codec::putStr(out, field);
    codec::putU8(out, static_cast<uint8_t>(valueType));
}

std::optional<CatalogEntry> CatalogEntry::decode(codec::Reader& reader) {
    CatalogEntry entry;
    entry.measurement = reader.str();

    const uint32_t tagCount = reader.u32();
    if (!reader.ok())
        return std::nullopt;
    // Each tag needs at least two u32 length prefixes (8 bytes); reject an
    // absurd count from corrupt input before reserving.
    if (tagCount > reader.remaining() / 8)
        return std::nullopt;
    entry.tags.reserve(tagCount);
    for (uint32_t i = 0; i < tagCount; ++i) {
        std::string key = reader.str();
        std::string value = reader.str();
        entry.tags.emplace_back(std::move(key), std::move(value));
    }

    entry.field = reader.str();
    const uint8_t valueType = reader.u8();
    if (!reader.ok() || valueType > kMaxValueType)
        return std::nullopt;
    entry.valueType = static_cast<TSMValueType>(valueType);

    // Enforce canonical form on decode: tags strictly increasing (sorted, no
    // duplicate keys). A non-canonical encoding is rejected rather than silently
    // accepted, so equal series always have byte-identical encodings.
    for (size_t i = 1; i < entry.tags.size(); ++i) {
        if (!(entry.tags[i - 1] < entry.tags[i]))
            return std::nullopt;
    }
    return entry;
}

void CatalogRecord::encodeInto(std::string& out) const {
    seriesId.appendTo(out);  // fixed 16 bytes
    entry.encodeInto(out);
}

std::string CatalogRecord::encode() const {
    std::string out;
    encodeInto(out);
    return out;
}

std::optional<CatalogRecord> CatalogRecord::decode(codec::Reader& reader) {
    const std::string idBytes = reader.bytes(16);
    if (!reader.ok())
        return std::nullopt;
    auto entry = CatalogEntry::decode(reader);
    if (!entry)
        return std::nullopt;
    CatalogRecord record;
    record.seriesId = SeriesId128::fromBytes(idBytes);
    record.entry = std::move(*entry);
    return record;
}

std::optional<CatalogRecord> CatalogRecord::decode(std::span<const char> data, size_t& consumed) {
    codec::Reader reader(data);
    auto record = decode(reader);
    if (!record || !reader.ok())
        return std::nullopt;
    consumed = reader.consumed();
    return record;
}

SeriesCatalog::GetOrCreateResult SeriesCatalog::getOrCreate(const SeriesId128& id, CatalogEntry entry) {
    if (auto it = entries_.find(id); it != entries_.end())
        return {&it->second, false};
    entry.canonicalize();
    auto [it, inserted] = entries_.emplace(id, std::move(entry));
    return {&it->second, true};
}

const CatalogEntry* SeriesCatalog::find(const SeriesId128& id) const {
    auto it = entries_.find(id);
    return it == entries_.end() ? nullptr : &it->second;
}

bool SeriesCatalog::apply(const CatalogRecord& record) {
    auto [it, inserted] = entries_.emplace(record.seriesId, record.entry);
    if (inserted)
        return true;
    return it->second == record.entry;  // idempotent replay OK; conflicting redefinition rejected
}

std::string SeriesCatalog::snapshot() const {
    std::vector<const SeriesId128*> ids;
    ids.reserve(entries_.size());
    for (const auto& [id, entry] : entries_)
        ids.push_back(&id);
    // Deterministic order: sort by the 16 raw id bytes.
    std::sort(ids.begin(), ids.end(),
              [](const SeriesId128* a, const SeriesId128* b) { return a->getRawData() < b->getRawData(); });

    std::string out;
    codec::putU32(out, static_cast<uint32_t>(ids.size()));
    for (const SeriesId128* id : ids) {
        id->appendTo(out);
        entries_.at(*id).encodeInto(out);
    }
    return out;
}

std::optional<SeriesCatalog> SeriesCatalog::loadSnapshot(std::span<const char> data) {
    codec::Reader reader(data);
    const uint32_t count = reader.u32();
    if (!reader.ok())
        return std::nullopt;

    SeriesCatalog catalog;
    for (uint32_t i = 0; i < count; ++i) {
        auto record = CatalogRecord::decode(reader);
        if (!record)
            return std::nullopt;
        if (!catalog.apply(*record))
            return std::nullopt;  // conflicting duplicate id in the snapshot
    }
    // A trailing byte after the declared count is corruption.
    if (!reader.exhausted())
        return std::nullopt;
    return catalog;
}

}  // namespace timestar
