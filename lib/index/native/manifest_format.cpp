#include "manifest_format.hpp"

#include "crc32.hpp"

#include <algorithm>
#include <limits>
#include <ranges>
#include <unordered_set>
#include <utility>

namespace timestar::index {
namespace {

enum class RecordType : uint8_t { Snapshot = 0, AddFile = 1, RemoveFile = 2 };

uint32_t decodeFixed32(const char* value) {
    return static_cast<uint32_t>(static_cast<uint8_t>(value[0])) |
           (static_cast<uint32_t>(static_cast<uint8_t>(value[1])) << 8) |
           (static_cast<uint32_t>(static_cast<uint8_t>(value[2])) << 16) |
           (static_cast<uint32_t>(static_cast<uint8_t>(value[3])) << 24);
}

uint64_t decodeFixed64(const char* value) {
    uint64_t result = 0;
    for (int byte = 0; byte < 8; ++byte)
        result |= static_cast<uint64_t>(static_cast<uint8_t>(value[byte])) << (byte * 8);
    return result;
}

class Cursor {
public:
    explicit Cursor(std::string_view input) : input_(input) {}

    bool readByte(uint8_t& value) {
        if (remaining() < 1)
            return false;
        value = static_cast<uint8_t>(input_[position_++]);
        return true;
    }

    bool readFixed32(uint32_t& value) {
        if (remaining() < 4)
            return false;
        value = decodeFixed32(input_.data() + position_);
        position_ += 4;
        return true;
    }

    bool readFixed64(uint64_t& value) {
        if (remaining() < 8)
            return false;
        value = decodeFixed64(input_.data() + position_);
        position_ += 8;
        return true;
    }

    bool readString(size_t length, std::string& value) {
        if (length > remaining())
            return false;
        value.assign(input_.data() + position_, length);
        position_ += length;
        return true;
    }

    [[nodiscard]] size_t remaining() const noexcept { return input_.size() - position_; }

private:
    std::string_view input_;
    size_t position_ = 0;
};

bool decodeFileRecord(Cursor& cursor, ManifestFileRecord& file, std::string& issue) {
    uint32_t encodedLevel = 0;
    uint32_t minKeyLength = 0;
    uint32_t maxKeyLength = 0;
    if (!cursor.readFixed64(file.fileNumber) || !cursor.readFixed32(encodedLevel) ||
        !cursor.readFixed64(file.fileSize) || !cursor.readFixed64(file.entryCount) ||
        !cursor.readFixed32(minKeyLength) || !cursor.readString(minKeyLength, file.minKey) ||
        !cursor.readFixed32(maxKeyLength) || !cursor.readString(maxKeyLength, file.maxKey)) {
        issue = "truncated SSTable metadata";
        return false;
    }
    if (file.fileNumber == 0) {
        issue = "SSTable file number must be positive";
        return false;
    }
    if (encodedLevel > static_cast<uint32_t>(std::numeric_limits<int>::max())) {
        issue = "SSTable level exceeds the supported range";
        return false;
    }
    file.level = static_cast<int>(encodedLevel);

    return true;
}

bool decodeSnapshotFiles(Cursor cursor, uint32_t fileCount, bool timestampsPresent,
                         std::vector<ManifestFileRecord>& files, std::string& issue) {
    files.clear();
    files.reserve(std::min<size_t>(fileCount, cursor.remaining() / (timestampsPresent ? 44 : 36)));
    for (uint32_t index = 0; index < fileCount; ++index) {
        ManifestFileRecord file;
        if (!decodeFileRecord(cursor, file, issue))
            return false;
        if (timestampsPresent && !cursor.readFixed64(file.writeTimestamp)) {
            issue = "truncated SSTable write timestamp";
            return false;
        }
        files.push_back(std::move(file));
    }
    if (cursor.remaining() != 0) {
        issue = "snapshot record has trailing bytes";
        return false;
    }
    return true;
}

bool validateFileSet(const ManifestDecodeResult& result, std::string& issue) {
    std::unordered_set<uint64_t> seen;
    uint64_t maximumFileNumber = 0;
    for (const auto& file : result.files) {
        if (!seen.insert(file.fileNumber).second) {
            issue = "duplicate active SSTable file number " + std::to_string(file.fileNumber);
            return false;
        }
        maximumFileNumber = std::max(maximumFileNumber, file.fileNumber);
    }
    if (result.nextFileNumber <= maximumFileNumber) {
        issue = "next SSTable file number does not exceed the active file set";
        return false;
    }
    return true;
}

bool applyRecord(std::string_view bytes, ManifestDecodeResult& state, std::string& issue) {
    Cursor cursor(bytes);
    uint8_t encodedType = 0;
    if (!cursor.readByte(encodedType)) {
        issue = "empty manifest record";
        return false;
    }

    const auto type = static_cast<RecordType>(encodedType);
    if (type == RecordType::Snapshot) {
        uint64_t nextFileNumber = 0;
        uint32_t fileCount = 0;
        if (!cursor.readFixed64(nextFileNumber) || !cursor.readFixed32(fileCount)) {
            issue = "truncated snapshot header";
            return false;
        }
        if (nextFileNumber == 0) {
            issue = "next SSTable file number must be positive";
            return false;
        }

        ManifestDecodeResult candidate;
        auto decodeCandidate = [&](bool timestampsPresent, std::string& candidateIssue) {
            candidate = state;
            candidate.nextFileNumber = nextFileNumber;
            if (!decodeSnapshotFiles(cursor, fileCount, timestampsPresent, candidate.files, candidateIssue))
                return false;
            if (candidate.files.size() != fileCount) {
                candidateIssue = "snapshot file count does not match its records";
                return false;
            }
            return validateFileSet(candidate, candidateIssue);
        };

        std::string timestampedIssue;
        if (!decodeCandidate(true, timestampedIssue)) {
            std::string legacyIssue;
            if (!decodeCandidate(false, legacyIssue)) {
                issue = "invalid snapshot metadata with timestamps (" + timestampedIssue + ") or without timestamps (" +
                        legacyIssue + ")";
                return false;
            }
        }

        state.nextFileNumber = candidate.nextFileNumber;
        state.files = std::move(candidate.files);
        return true;
    }

    if (type == RecordType::AddFile) {
        ManifestFileRecord file;
        // AddFile records written before the timestamp extension are valid in
        // both disk formats. Exact trailing-byte validation below limits the
        // optional suffix to either zero or eight bytes.
        if (!decodeFileRecord(cursor, file, issue))
            return false;
        if (cursor.remaining() == 8 && !cursor.readFixed64(file.writeTimestamp)) {
            issue = "truncated SSTable write timestamp";
            return false;
        }
        if (cursor.remaining() != 0) {
            issue = "add-file record has trailing bytes";
            return false;
        }
        if (std::ranges::any_of(state.files,
                                [&file](const auto& current) { return current.fileNumber == file.fileNumber; })) {
            issue = "duplicate active SSTable file number " + std::to_string(file.fileNumber);
            return false;
        }
        if (file.fileNumber == std::numeric_limits<uint64_t>::max()) {
            issue = "SSTable file number cannot be incremented";
            return false;
        }
        state.nextFileNumber = std::max(state.nextFileNumber, file.fileNumber + 1);
        state.files.push_back(std::move(file));
        return true;
    }

    if (type == RecordType::RemoveFile) {
        uint64_t fileNumber = 0;
        if (!cursor.readFixed64(fileNumber) || cursor.remaining() != 0) {
            issue = "remove-file record must contain exactly one file number";
            return false;
        }
        if (fileNumber == 0) {
            issue = "removed SSTable file number must be positive";
            return false;
        }
        std::erase_if(state.files, [fileNumber](const auto& file) { return file.fileNumber == fileNumber; });
        return true;
    }

    issue = "unknown manifest record type " + std::to_string(encodedType);
    return false;
}

ManifestDecodeResult fail(ManifestDecodeResult result, ManifestDecodeStatus status, size_t offset, std::string issue) {
    result.status = status;
    result.issueOffset = offset;
    result.issue = std::move(issue);
    return result;
}

}  // namespace

ManifestDecodeResult decodeManifest(std::string_view contents) {
    ManifestDecodeResult result;
    if (contents.empty()) {
        result.status = ManifestDecodeStatus::Empty;
        result.issue = "manifest is empty";
        return result;
    }

    size_t position = 0;
    if (contents.size() >= 4 && decodeFixed32(contents.data()) == MANIFEST_MAGIC) {
        result.format = ManifestDiskFormat::CrcV2;
        if (contents.size() < MANIFEST_HEADER_SIZE)
            return fail(std::move(result), ManifestDecodeStatus::Fatal, 0, "truncated v2 manifest header");
        const uint32_t version = decodeFixed32(contents.data() + 4);
        if (version != MANIFEST_VERSION) {
            return fail(std::move(result), ManifestDecodeStatus::Fatal, 4,
                        "unsupported manifest version " + std::to_string(version));
        }
        position = MANIFEST_HEADER_SIZE;
    }

    while (position < contents.size()) {
        const size_t frameOffset = position;
        const size_t headerSize = result.format == ManifestDiskFormat::CrcV2 ? 8 : 4;
        if (contents.size() - position < headerSize) {
            const auto status =
                result.validRecordCount == 0 ? ManifestDecodeStatus::Fatal : ManifestDecodeStatus::RecoverableTail;
            return fail(std::move(result), status, frameOffset, "truncated manifest frame header");
        }

        const uint32_t recordLength = decodeFixed32(contents.data() + position);
        const uint32_t storedCrc =
            result.format == ManifestDiskFormat::CrcV2 ? decodeFixed32(contents.data() + position + 4) : 0;
        position += headerSize;
        if (recordLength > contents.size() - position) {
            const auto status =
                result.validRecordCount == 0 ? ManifestDecodeStatus::Fatal : ManifestDecodeStatus::RecoverableTail;
            return fail(std::move(result), status, frameOffset, "truncated manifest record payload");
        }

        const std::string_view record = contents.substr(position, recordLength);
        if (result.format == ManifestDiskFormat::CrcV2 && CRC32::compute(record.data(), record.size()) != storedCrc) {
            const auto status =
                result.validRecordCount == 0 ? ManifestDecodeStatus::Fatal : ManifestDecodeStatus::RecoverableTail;
            return fail(std::move(result), status, frameOffset, "manifest record CRC mismatch");
        }

        std::string issue;
        if (!applyRecord(record, result, issue))
            return fail(std::move(result), ManifestDecodeStatus::Fatal, frameOffset, std::move(issue));

        ++result.validRecordCount;
        position += recordLength;
    }

    if (result.validRecordCount == 0)
        return fail(std::move(result), ManifestDecodeStatus::Fatal, position, "manifest contains no records");

    result.status = ManifestDecodeStatus::Complete;
    return result;
}

}  // namespace timestar::index
