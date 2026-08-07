#pragma once

#include <cstdint>
#include <cstddef>
#include <cstdio>
#include <memory>
#include <string>
#include <vector>

namespace llm {

// ===========================================================================
// Compact binary tick format for the replay/backtest pipeline.
//
//   * BinaryTick is a fixed 24-byte, trivially-copyable POD so it packs
//     densely into cache lines and can be fread/mmap'd in bulk with zero
//     per-tick parsing.
//   * Prices are stored as float (full pip/point fidelity for the strategy's
//     four-decimal rounding; ~7 significant digits) to keep the 24-byte
//     footprint; the strategy layer widens to double before math.
//   * Files are endian-native ("fast binary"); conversion is a one-time
//     offline step from CSV, so portability is not a hot-path concern.
// ===========================================================================
struct BinaryTick {
    int64_t  ts_ms   = 0;    // epoch millisecond timestamp (offset 0)
    float    bid     = 0.0f; // best bid  (or last price for index feeds)
    float    ask     = 0.0f; // best ask
    float    volume  = 0.0f; // traded volume at this tick
    uint32_t flags   = 0;    // bit0: valid
    // --- 8 + 4 + 4 + 4 + 4 = 24 bytes -----------------------------------

    bool     is_valid() const { return (flags & 1u) != 0; }
    void     set_valid(bool v) { flags = v ? (flags | 1u) : (flags & ~1u); }
    double   mid() const { return (static_cast<double>(bid) + static_cast<double>(ask)) * 0.5; }
    double   spread() const { return static_cast<double>(ask) - static_cast<double>(bid); }
};
static_assert(sizeof(BinaryTick) == 24,
              "BinaryTick must be exactly 24 bytes for cache-packed streaming");
static_assert(alignof(BinaryTick) <= 8, "BinaryTick alignment must be cache-friendly");

// ===========================================================================
// CSV -> binary converter.
//
// Input CSV lines (header optional):
//     timestamp,bid,ask,volume
//     1723050000123,64321.5,64322.1,1.25
//     ...
//
// timestamp is epoch milliseconds; an epoch-seconds value (< 1e12) is auto-
// scaled to milliseconds. Rows with non-numeric or non-positive prices are
// skipped (the converter never fails the whole file on one bad row).
// Output is a flat array of BinaryTick records (no header).
// ===========================================================================
class CsvToBinaryConverter {
public:
    struct Options {
        char   delimiter   = ',';
        bool   has_header  = true;
        size_t skip_rows   = 0;  // extra leading rows to ignore after header
    };

    // Returns the number of BinaryTick records written, or -1 on I/O error
    // (with `err` populated when non-null).
    static long convert(const std::string& csv_path,
                        const std::string& bin_path,
                        const Options&     opt,
                        std::string*       err = nullptr);

    // Convenience overload using default Options (comma, header).
    static long convert(const std::string& csv_path,
                        const std::string& bin_path,
                        std::string*       err = nullptr);
};

// ===========================================================================
// Zero-copy mmap reader for .bin tick files.
//
// Maps the whole file read-only via mmap() (POSIX) or MapViewOfFile (Windows)
// and exposes a pointer to the BinaryTick array — no allocation, no per-tick
// parsing on the replay loop. The mapping is created in open() and released
// in close()/the destructor.
// ===========================================================================
class MmapTickReader {
public:
    explicit MmapTickReader(std::string path);
    ~MmapTickReader();

    MmapTickReader(const MmapTickReader&) = delete;
    MmapTickReader& operator=(const MmapTickReader&) = delete;

    // Maps the file; returns false with `err` set on failure (missing file,
    // size not a multiple of sizeof(BinaryTick), empty file).
    bool open(std::string* err = nullptr);

    const BinaryTick* data() const { return base_; }
    size_t            count() const { return count_; }
    bool              is_open() const { return base_ != nullptr; }

    const BinaryTick& operator[](size_t i) const { return base_[i]; }

    // Last tick whose ts_ms <= ts; returns false when ts < first tick.
    bool find_at_or_before(int64_t ts, BinaryTick& out) const;

    void close();

private:
    std::string  path_;
    const BinaryTick* base_  = nullptr;
    size_t       count_      = 0;
    int64_t      file_len_   = 0;

    // Platform mapping handles.
    void* mapping_ = nullptr;   // void*: mmap ptr reused as base_; kept separate
    void* view_    = nullptr;   // Windows MapViewOfFile handle
    void* file_    = nullptr;   // Windows HANDLE
};

}  // namespace llm
