#include "backtest/tick_binary.hpp"

#include <cstdlib>
#include <cstring>
#include <fstream>
#include <sstream>

#ifdef _WIN32
#include <windows.h>
#else
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace llm {

// ===========================================================================
// CsvToBinaryConverter
// ===========================================================================
namespace {

// Tokenize one CSV line into exactly the (timestamp,bid,ask,volume) fields.
// Returns true and out[0..4] populated only when the row is well-formed.
bool parse_tick_line(const std::string& line, char delim,
                     int64_t& ts, double& bid, double& ask, double& vol,
                     bool& parse_ok) {
    std::vector<std::string> cols;
    std::string cur;
    for (char c : line) {
        if (c == delim) { cols.push_back(cur); cur.clear(); }
        else cur.push_back(c);
    }
    cols.push_back(cur);
    if (cols.size() < 4) { parse_ok = false; return false; }

    try {
        double raw_ts = std::stod(cols[0]);
        ts    = static_cast<int64_t>(raw_ts > 1e12 ? raw_ts : raw_ts * 1000.0);
        bid   = std::stod(cols[1]);
        ask   = std::stod(cols[2]);
        vol   = cols.size() >= 4 && !cols[3].empty() ? std::stod(cols[3]) : 0.0;
    } catch (...) {
        parse_ok = false;
        return false;
    }
    parse_ok = true;
    return bid > 0.0 && ask > 0.0;  // prices must be positive to be usable
}

}  // namespace

long CsvToBinaryConverter::convert(const std::string& csv_path,
                                   const std::string& bin_path,
                                   const Options& opt, std::string* err) {
    std::ifstream in(csv_path, std::ios::in);
    if (!in.is_open()) {
        if (err) *err = "cannot open CSV for read: " + csv_path;
        return -1;
    }
    std::ofstream out(bin_path, std::ios::binary | std::ios::out | std::ios::trunc);
    if (!out.is_open()) {
        if (err) *err = "cannot open binary for write: " + bin_path;
        return -1;
    }

    size_t skip = opt.has_header ? 1u + opt.skip_rows : opt.skip_rows;
    std::string line;
    long written = 0;
    size_t line_no = 0;
    while (std::getline(in, line)) {
        ++line_no;
        if (line.empty()) continue;
        if (skip > 0) { --skip; continue; }

        // Tolerate a trailing CR.
        if (!line.empty() && line.back() == '\r') line.pop_back();

        int64_t ts; double bid, ask, vol; bool ok = false;
        if (!parse_tick_line(line, opt.delimiter, ts, bid, ask, vol, ok)) {
            continue;  // malformed or non-positive row -> skip
        }
        BinaryTick t;
        t.ts_ms  = ts;
        t.bid    = static_cast<float>(bid);
        t.ask    = static_cast<float>(ask);
        t.volume = static_cast<float>(vol);
        t.flags  = 0;
        t.set_valid(true);
        out.write(reinterpret_cast<const char*>(&t), sizeof(BinaryTick));
        ++written;
    }
    if (written == 0) {
        if (err) *err = "no valid rows converted (check CSV columns/delimiter)";
        return -1;
    }
    return written;
}

long CsvToBinaryConverter::convert(const std::string& csv_path,
                                   const std::string& bin_path,
                                   std::string* err) {
    return convert(csv_path, bin_path, Options{}, err);
}

// ===========================================================================
// MmapTickReader
// ===========================================================================
MmapTickReader::MmapTickReader(std::string path) : path_(std::move(path)) {}

MmapTickReader::~MmapTickReader() { close(); }

bool MmapTickReader::open(std::string* err) {
    if (base_) return true;  // already mapped

#ifdef _WIN32
    HANDLE hFile = CreateFileA(path_.c_str(), GENERIC_READ, FILE_SHARE_READ,
                               nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL,
                               nullptr);
    if (hFile == INVALID_HANDLE_VALUE) {
        if (err) *err = "open failed: " + path_;
        return false;
    }
    LARGE_INTEGER sz{};
    if (!GetFileSizeEx(hFile, &sz) || sz.QuadPart == 0) {
        if (err) *err = "empty or unreadable file: " + path_;
        CloseHandle(hFile);
        return false;
    }
    const int64_t len = sz.QuadPart;
    if (len % sizeof(BinaryTick) != 0) {
        if (err) *err = "file size not a multiple of BinaryTick: " + path_;
        CloseHandle(hFile);
        return false;
    }
    HANDLE map = CreateFileMappingA(hFile, nullptr, PAGE_READONLY, 0, 0, nullptr);
    if (!map) {
        if (err) *err = "CreateFileMapping failed: " + path_;
        CloseHandle(hFile);
        return false;
    }
    void* view = MapViewOfFile(map, FILE_MAP_READ, 0, 0, 0);
    if (!view) {
        if (err) *err = "MapViewOfFile failed: " + path_;
        CloseHandle(map);
        CloseHandle(hFile);
        return false;
    }
    // Keep map + file handles alive until close(); the view stays valid.
    file_    = hFile;
    mapping_ = map;
    view_    = view;
    base_    = static_cast<const BinaryTick*>(view);
    count_   = static_cast<size_t>(len / sizeof(BinaryTick));
    file_len_= len;
    return true;
#else
    const int fd = ::open(path_.c_str(), O_RDONLY);
    if (fd < 0) {
        if (err) *err = "open failed: " + path_;
        return false;
    }
    struct stat st;
    if (::fstat(fd, &st) != 0 || st.st_size == 0) {
        if (err) *err = "empty or unreadable file: " + path_;
        ::close(fd);
        return false;
    }
    const int64_t len = static_cast<int64_t>(st.st_size);
    if (len % sizeof(BinaryTick) != 0) {
        if (err) *err = "file size not a multiple of BinaryTick: " + path_;
        ::close(fd);
        return false;
    }
    void* p = ::mmap(nullptr, static_cast<size_t>(len), PROT_READ,
                     MAP_PRIVATE, fd, 0);
    ::close(fd);
    if (p == MAP_FAILED) {
        if (err) *err = "mmap failed: " + path_;
        return false;
    }
    mapping_ = p;
    base_    = static_cast<const BinaryTick*>(p);
    count_   = static_cast<size_t>(len / sizeof(BinaryTick));
    file_len_= len;
    return true;
#endif
}

void MmapTickReader::close() {
#ifdef _WIN32
    if (view_)    { UnmapViewOfFile(view_);    view_    = nullptr; }
    if (mapping_) { CloseHandle(mapping_);       mapping_ = nullptr; }
    if (file_)    { CloseHandle(file_);          file_    = nullptr; }
    base_ = nullptr;
#else
    if (base_) { ::munmap(const_cast<BinaryTick*>(base_),
                          static_cast<size_t>(file_len_)); }
    base_    = nullptr;
    mapping_ = nullptr;
#endif
    count_      = 0;
    file_len_   = 0;
}

bool MmapTickReader::find_at_or_before(int64_t ts, BinaryTick& out) const {
    if (!base_ || count_ == 0) return false;
    if (ts < base_[0].ts_ms) return false;
    // Binary search over the sorted-by-ts array.
    size_t lo = 0, hi = count_ - 1, ans = 0;
    while (lo <= hi) {
        const size_t mid = lo + (hi - lo) / 2;
        if (base_[mid].ts_ms <= ts) { ans = mid; lo = mid + 1; }
        else hi = mid - 1;
    }
    out = base_[ans];
    return true;
}

}  // namespace llm