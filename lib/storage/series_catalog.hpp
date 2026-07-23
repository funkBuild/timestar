#pragma once

#include "../core/series_id.hpp"
#include "../utils/byte_codec.hpp"
#include "tsm.hpp"  // TSMValueType

#include <optional>
#include <span>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace timestar {

// The durable identity of a series behind its SeriesId128 (plan "TSM and catalog
// requirements", ADR 0002 sec 1): measurement, ordered tags, field, value type.
// Tags are stored in canonical (key-then-value sorted, de-duplicated) order so a
// series always serialises identically and two encodings compare equal iff the
// series is the same.
struct CatalogEntry {
    std::string measurement;
    std::vector<std::pair<std::string, std::string>> tags;  // canonical order
    std::string field;
    TSMValueType valueType = TSMValueType::Float;

    // Sort tags by (key, value). Idempotent. Does not remove duplicate keys --
    // a duplicate tag key is malformed input and is rejected on decode.
    void canonicalize();

    void encodeInto(std::string& out) const;
    // Decode from a bounds-checked cursor. Returns nullopt on a reader failure,
    // an out-of-range value type, or non-canonical / duplicate tag ordering.
    [[nodiscard]] static std::optional<CatalogEntry> decode(codec::Reader& reader);

    friend bool operator==(const CatalogEntry&, const CatalogEntry&) = default;
};

// A catalog-creation record: the payload of a JournalRecordKind::CatalogCreate
// journal record. Ties the SeriesId128 to its definition so the two can never
// diverge durably (they travel in the same journal).
struct CatalogRecord {
    SeriesId128 seriesId;
    CatalogEntry entry;

    void encodeInto(std::string& out) const;
    [[nodiscard]] std::string encode() const;
    [[nodiscard]] static std::optional<CatalogRecord> decode(codec::Reader& reader);
    [[nodiscard]] static std::optional<CatalogRecord> decode(std::span<const char> data, size_t& consumed);
};

// In-memory durable series catalog: SeriesId128 -> CatalogEntry. Populated on the
// write path by getOrCreate (which reports first-seen series so the caller
// journals a CatalogRecord), and rebuilt by replaying CatalogRecords (journal
// replay) or installing a snapshot. This is the authoritative source; the
// NativeIndex 0x05 series-metadata keys are derived from it.
class SeriesCatalog {
public:
    struct GetOrCreateResult {
        const CatalogEntry* entry;  // never null
        bool created;               // true => caller must journal a CatalogRecord for (id, *entry)
    };

    // Look up or insert. On first sight the entry is canonicalized and stored.
    GetOrCreateResult getOrCreate(const SeriesId128& id, CatalogEntry entry);

    [[nodiscard]] const CatalogEntry* find(const SeriesId128& id) const;
    [[nodiscard]] size_t size() const noexcept { return entries_.size(); }

    // Apply a decoded record during replay/install. First definition wins;
    // re-applying the identical record is idempotent (true); a conflicting
    // redefinition of an existing id is a corruption signal (false).
    bool apply(const CatalogRecord& record);

    // Deterministic snapshot: [u32 count][records sorted by SeriesId128 bytes].
    // Sorting makes the snapshot byte-identical for equal catalogs (anti-entropy).
    [[nodiscard]] std::string snapshot() const;
    // Install a snapshot into a fresh catalog. Returns nullopt on any decode
    // failure, a count mismatch, or a conflicting duplicate id.
    [[nodiscard]] static std::optional<SeriesCatalog> loadSnapshot(std::span<const char> data);

private:
    std::unordered_map<SeriesId128, CatalogEntry, SeriesId128::Hash> entries_;
};

}  // namespace timestar
