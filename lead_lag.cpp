// ============================================================================
// lead_lag.cpp
// ============================================================================
// Ultra-low-latency Lead-Lag arbitrage engine.
//
//   Leader  : Binance WebSocket  (wss://stream.binance.com:9443/ws/<sym>@trade)
//   Follower: FXCM market orders (executed off the hot path on worker threads)
//
// Design goals / guarantees:
//   * The Binance read loop is 100% asynchronous (Boost.Beast + Boost.Asio).
//     It is NEVER blocked by FXCM I/O.
//   * Signal logic is strictly percentage/directional so the absolute-price
//     mismatch between the two venues is irrelevant.
//   * All FXCM network calls are pushed to a dedicated worker thread pool.
//   * Auto-reconnect with exponential backoff if the WS link drops.
//   * SL/TP are attached to each order in pips.
//
// Threading model (what is shared, and how it is protected):
//   * WS read thread      -> SignalGenerator (OWNED by this thread: no locks)
//   * WS read thread  --> FXCMExecutor::submit()   (mutex + condvar queue)
//   * Worker thread(s) --> blocking FXCM HTTP call (never touches the reactor)
//   * BinanceClient::stopped_  -> std::atomic<bool>
//   * AsyncLogger             -> single mutex-protected queue; producers only
//                                enqueue, ONE writer thread does all file I/O.
//                                Text log rotates daily; trade telemetry and
//                                (optional) ticks go to CSV. Never on the
//                                Binance read-loop hot path.
//
// Dependencies:  Boost.Beast, Boost.Asio, OpenSSL, nlohmann/json
//
// ----------------------------------------------------------------------------
// COMPILATION (Linux / macOS, g++ / clang++)
// ----------------------------------------------------------------------------
//   g++ -std=c++20 -O3 -march=native -DNDEBUG -Wall -Wextra -pedantic \
//       lead_lag.cpp -o lead_lag \
//       -I/usr/include/nlohmann \
//       -lboost_system -lssl -lcrypto -lpthread
//
// ----------------------------------------------------------------------------
// COMPILATION (Windows, MSYS2/MinGW g++)
// ----------------------------------------------------------------------------
//   g++ -std=c++20 -O3 -DNDEBUG -Wall -Wextra -D_WIN32_WINNT=0x0A00 \
//       lead_lag.cpp -o lead_lag.exe \
//       -I<path-to-nlohmann> \
//       -lboost_system -lssl -lcrypto -lws2_32 -lcrypt32 -lwsock32
//
// NOTE (FXCM): fxcmpy / the legacy FXCM REST API is retired. FXCM's current
// "Trading API" uses OAuth2 + HTTPS REST. The endpoint/host/auth constants in
// `Config` and the body of `FXCMExecutor::fxcm_post()` are intentionally
// isolated so you only have to adjust those two places. Run with
// FXCM_TOKEN set and DRY_RUN=0 for live trading; dry-run is the default.
// ============================================================================

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/core/bind_handler.hpp>
#include <boost/beast/core/tcp_stream.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/ssl/ssl_stream.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>
#include <nlohmann/json.hpp>

#include <atomic>
#include <cctype>
#include <chrono>
#include <condition_variable>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <deque>
#include <fstream>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace beast = boost::beast;
namespace http  = beast::http;
namespace websocket = beast::websocket;
namespace net   = boost::asio;
namespace ssl   = boost::asio::ssl;
using tcp = net::ip::tcp;

// ---------------------------------------------------------------------------
// Logging & telemetry (asynchronous, non-blocking, thread-safe)
// ---------------------------------------------------------------------------
// Every producer (Binance WS read loop, signal engine, FXCM worker threads)
// calls the free log()/log_warn()/log_error()/log_critical() helpers or
// AsyncLogger::log_*(); these only format the message and push it onto a
// mutex-protected queue, then return immediately. A single dedicated writer
// thread drains the queue and performs the file I/O, so no producer is ever
// blocked by disk writes — the Binance read loop stays untouched.
//
// Outputs:
//   log_YYYYMMDD.txt      -> human-readable text log (rotates daily)
//   trades_telemetry.csv  -> one row per executed FXCM order
//   ticks.csv             -> every Binance tick, only when LOG_TICKS=1

// One telemetry row -> trades_telemetry.csv
struct TradeTelemetry {
    int64_t     ts_ms;            // epoch ms (decision timestamp)
    std::string direction;        // "buy" | "sell"
    double      ref_price;
    double      change_pct;
    double      fxcm_latency_ms;  // end-to-end execution latency (incl. retry)
    int         fxcm_status;      // HTTP status (0 = network/timeout error)
    std::string order_id;         // FXCM orderId when placed, else ""

    std::string csv_row() const {
        char buf[256];
        std::snprintf(buf, sizeof(buf),
                      "%lld,%s,%.6f,%.4f,%.3f,%d,%s",
                      static_cast<long long>(ts_ms), direction.c_str(),
                      ref_price, change_pct, fxcm_latency_ms, fxcm_status,
                      order_id.c_str());
        return buf;
    }
};

static int64_t now_epoch_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

class AsyncLogger {
public:
    enum Level { INFO, WARN, ERROR, CRITICAL };

    static AsyncLogger& instance() {
        static AsyncLogger logger;
        return logger;
    }

    // Spawn the writer thread. Call once at startup, before anything logs.
    void start() {
        std::lock_guard<std::mutex> lk(mtx_);
        if (running_) return;
        running_ = true;
        writer_ = std::thread(&AsyncLogger::writer_loop, this);
    }

    // Flush all queued entries and join the writer thread. Call at shutdown.
    void stop() {
        {
            std::lock_guard<std::mutex> lk(mtx_);
            if (!running_) return;
            running_ = false;
        }
        cv_.notify_all();
        if (writer_.joinable()) writer_.join();
    }

    // Output directory for all log/csv files (default: ".").
    void set_log_dir(const std::string& dir) {
        std::lock_guard<std::mutex> lk(mtx_);
        log_dir_ = dir.empty() ? "." : dir;
    }

    // ---- non-blocking producers ---------------------------------------- //
    void log_raw(Level lvl, std::string line) {
        line = "[" + now_hms() + "] [" + level_str(lvl) + "] " + line;
        push(Entry{ Entry::LOG, std::move(line) });
    }

    void log_trade(TradeTelemetry t) {
        push(Entry{ Entry::TRADE, std::move(t.csv_row()) });
    }

    void log_tick(double price) {               // only used when LOG_TICKS=1
        push(Entry{ Entry::TICK, tick_row(now_epoch_ms(), price) });
    }

private:
    struct Entry {
        enum Kind { LOG, TRADE, TICK };
        Kind        kind;
        std::string text;
    };

    AsyncLogger() = default;
    ~AsyncLogger() { stop(); }          // safety net if main() forgets

    void push(Entry e) {
        {
            std::lock_guard<std::mutex> lk(mtx_);
            queue_.push_back(std::move(e));
        }
        cv_.notify_one();
    }

    void writer_loop() {
        for (;;) {
            Entry e;
            {
                std::unique_lock<std::mutex> lk(mtx_);
                cv_.wait(lk, [&] { return !running_ || !queue_.empty(); });
                if (!running_ && queue_.empty()) break;
                e = std::move(queue_.front());
                queue_.pop_front();
            }
            write_entry(e);
            if (e.kind == Entry::TRADE) {
                flush_all();                 // durability on every trade
            } else {
                std::lock_guard<std::mutex> lk(mtx_);
                if (queue_.empty()) flush_all();   // batched flush at idle
            }
        }
        // Drain anything still queued after the stop signal.
        std::lock_guard<std::mutex> lk(mtx_);
        while (!queue_.empty()) {
            write_entry(std::move(queue_.front()));
            queue_.pop_front();
        }
        flush_all();
    }

    void write_entry(Entry e) {
        switch (e.kind) {
            case Entry::LOG:   write_log_line(e.text);                  break;
            case Entry::TRADE: write_csv(trade_ofs_, "trades_telemetry.csv",
                                         trade_header_, e.text);        break;
            case Entry::TICK:  write_csv(tick_ofs_, "ticks.csv",
                                         tick_header_, e.text);         break;
        }
    }

    // Daily rotation: log_YYYYMMDD.txt
    void write_log_line(const std::string& line) {
        auto tt = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        std::tm tmv{};
#ifdef _WIN32
        localtime_s(&tmv, &tt);
#else
        localtime_r(&tt, &tmv);
#endif
        char name[64];
        std::strftime(name, sizeof(name), "log_%Y%m%d.txt", &tmv);
        if (name != cur_log_name_) {
            if (log_ofs_.is_open()) log_ofs_.close();
            log_ofs_.open(log_dir_ + "/" + name, std::ios::app);
            cur_log_name_ = name;
        }
        if (log_ofs_.is_open()) log_ofs_ << line << '\n';
    }

    // Appends a row; writes the CSV header only on a fresh (empty) file.
    void write_csv(std::ofstream& ofs, const std::string& path,
                   const std::string& header, const std::string& row) {
        if (!ofs.is_open()) {
            const std::string full = log_dir_ + "/" + path;
            ofs.open(full, std::ios::app);
            if (!ofs.is_open()) {
                std::fprintf(stderr, "[AsyncLogger] cannot open %s\n", full.c_str());
                return;
            }
            ofs.seekp(0, std::ios::end);
            if (ofs.tellp() == std::streampos(0)) ofs << header << '\n';
        }
        ofs << row << '\n';
    }

    void flush_all() {
        if (log_ofs_.is_open())  log_ofs_.flush();
        if (trade_ofs_.is_open()) trade_ofs_.flush();
        if (tick_ofs_.is_open())  tick_ofs_.flush();
    }

    static const char* level_str(Level l) {
        switch (l) {
            case INFO:     return "INFO ";
            case WARN:     return "WARN ";
            case ERROR:    return "ERROR";
            case CRITICAL: return "CRIT ";
        }
        return "?????";
    }

    static std::string now_hms() {
        auto tt = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        std::tm tmv{};
#ifdef _WIN32
        localtime_s(&tmv, &tt);
#else
        localtime_r(&tt, &tmv);
#endif
        char buf[32];
        std::strftime(buf, sizeof(buf), "%H:%M:%S", &tmv);
        return buf;
    }

    static std::string tick_row(int64_t ts_ms, double price) {
        char buf[64];
        std::snprintf(buf, sizeof(buf), "%lld,%.6f",
                      static_cast<long long>(ts_ms), price);
        return buf;
    }

    static constexpr const char* trade_header_ =
        "timestamp,signal_direction,ref_price,change_pct,fxcm_latency_ms,fxcm_status,order_id";
    static constexpr const char* tick_header_ = "timestamp,price";

    std::mutex              mtx_;
    std::condition_variable cv_;
    std::deque<Entry>       queue_;
    std::thread             writer_;
    bool                    running_ = false;
    std::string             log_dir_ = ".";
    std::ofstream           log_ofs_;
    std::ofstream           trade_ofs_;
    std::ofstream           tick_ofs_;
    std::string             cur_log_name_;
};

// Free printf-style helpers (INFO / WARN / ERROR / CRITICAL). Non-blocking:
// each formats once and hands the string to the AsyncLogger queue.
template <typename... Args>
void log(const char* fmt, Args... args) {
    char buf[512];
    std::snprintf(buf, sizeof(buf), fmt, args...);
    AsyncLogger::instance().log_raw(AsyncLogger::INFO, buf);
}
template <typename... Args>
void log_warn(const char* fmt, Args... args) {
    char buf[512];
    std::snprintf(buf, sizeof(buf), fmt, args...);
    AsyncLogger::instance().log_raw(AsyncLogger::WARN, buf);
}
template <typename... Args>
void log_error(const char* fmt, Args... args) {
    char buf[512];
    std::snprintf(buf, sizeof(buf), fmt, args...);
    AsyncLogger::instance().log_raw(AsyncLogger::ERROR, buf);
}
template <typename... Args>
void log_critical(const char* fmt, Args... args) {
    char buf[512];
    std::snprintf(buf, sizeof(buf), fmt, args...);
    AsyncLogger::instance().log_raw(AsyncLogger::CRITICAL, buf);
}

// ---------------------------------------------------------------------------
// 0) Configuration
// ---------------------------------------------------------------------------
struct Config {
    // Binance feed
    std::string binance_symbol = "btcusdt";
    std::string binance_host   = "stream.binance.com";
    std::string binance_port   = "9443";
    std::string binance_path;                 // "/ws/<symbol>@trade"

    // Signal logic
    double window_seconds   = 2.0;            // momentum lookback window (s)
    double threshold_pct    = 0.05;           // |% change| required to trigger
    double cooldown_seconds = 30.0;           // min seconds between signals

    // FXCM execution  (REST/HTTPS, OAuth2 Bearer token — FXCM Trading API)
    std::string fxcm_symbol   = "BTC/USD";
    std::string fxcm_host     = "api.fxcm.com";        // demo: api-demo.fxcm.com
    std::string fxcm_port     = "443";
    std::string fxcm_path     = "/trading/open_trade"; // FXCM market-order endpoint
    std::string fxcm_token;                            // env FXCM_TOKEN (OAuth2 access token)
    std::string fxcm_account_id;                       // env FXCM_ACCOUNT_ID (required)
    std::string fxcm_time_in_force = "FOK";            // AtMarket: DAY/GTC/IOC/FOK
    bool        fxcm_use_json      = false;            // FXCM documents form-urlencoded
    int         fxcm_request_timeout_ms = 3000;
    int         fxcm_retry_delay_ms    = 100;
    double      trade_amount      = 0.001;
    int         stop_loss_pips    = 50;
    int         take_profit_pips  = 100;
    int         max_open_positions = 1;
    size_t      executor_threads  = 1;

    // Connectivity / safety
    double reconnect_base_s = 2.0;
    double reconnect_max_s  = 60.0;
    bool   dry_run          = true;           // default: paper trade only

    // Logging / telemetry
    bool        log_ticks = false;            // LOG_TICKS=1 -> ticks.csv
    std::string log_dir   = ".";              // LOG_DIR -> output directory

    static Config from_env() {
        Config c;
        const char* v = nullptr;
        if ((v = std::getenv("BINANCE_SYMBOL"))) c.binance_symbol = v;
        if ((v = std::getenv("FXCM_TOKEN")))     c.fxcm_token = v;
        if ((v = std::getenv("FXCM_SYMBOL")))    c.fxcm_symbol = v;
        if ((v = std::getenv("FXCM_AMOUNT")))    c.trade_amount = std::atof(v);
        if ((v = std::getenv("FXCM_SL_PIPS")))   c.stop_loss_pips = std::atoi(v);
        if ((v = std::getenv("FXCM_TP_PIPS")))   c.take_profit_pips = std::atoi(v);
        if ((v = std::getenv("FXCM_ACCOUNT_ID"))) c.fxcm_account_id = v;
        if ((v = std::getenv("FXCM_TIME_IN_FORCE"))) c.fxcm_time_in_force = v;
        if ((v = std::getenv("FXCM_USE_JSON")))  c.fxcm_use_json = std::string(v) == "1";
        if ((v = std::getenv("FXCM_TIMEOUT_MS"))) c.fxcm_request_timeout_ms = std::atoi(v);
        if ((v = std::getenv("FXCM_RETRY_DELAY_MS"))) c.fxcm_retry_delay_ms = std::atoi(v);
        if ((v = std::getenv("WINDOW_SECONDS"))) c.window_seconds = std::atof(v);
        if ((v = std::getenv("THRESHOLD_PCT")))  c.threshold_pct = std::atof(v);
        if ((v = std::getenv("COOLDOWN_SECONDS"))) c.cooldown_seconds = std::atof(v);
        if ((v = std::getenv("DRY_RUN")) && std::string(v) == "0")
            c.dry_run = false;
        if ((v = std::getenv("LOG_TICKS")) && std::string(v) == "1")
            c.log_ticks = true;
        if ((v = std::getenv("LOG_DIR")))
            c.log_dir = v;
        c.binance_path = "/ws/" + c.binance_symbol + "@trade";
        return c;
    }
};

// ---------------------------------------------------------------------------
// 1) Signal type
// ---------------------------------------------------------------------------
struct Signal {
    std::string direction;   // "buy" | "sell"
    std::string reason;      // "momentum"
    double      ref_price;   // price at start of the window
    double      change_pct;  // % move over the window
};

// ---------------------------------------------------------------------------
// 2) Signal Generator  —  pure CPU, owned by the WS read thread (no locks)
// ---------------------------------------------------------------------------
class SignalGenerator {
public:
    explicit SignalGenerator(const Config& cfg) : cfg_(cfg) {}

    // Called once per Binance tick. Returns a tradeable signal or nullopt.
    std::optional<Signal> update(double price) {
        const auto now = std::chrono::steady_clock::now();
        buf_.emplace_back(now, price);

        // Prune anything older than we could ever need (window + margin).
        const auto keep_for = std::chrono::duration<double>(cfg_.window_seconds + 1.0);
        while (!buf_.empty() && now - buf_.front().first > keep_for)
            buf_.pop_front();

        if (buf_.size() < 2) return std::nullopt;

        // Reference price: closest tick at-or-after the window boundary.
        const auto cutoff = now - std::chrono::nanoseconds(
            static_cast<long long>(cfg_.window_seconds * 1'000'000'000.0));
        const double ref = ref_price_at(cutoff);
        const double change_pct = (price / ref - 1.0) * 100.0;

        // Cooldown gate first: a single persistent move must not spam orders.
        if (!in_cooldown(now) && std::abs(change_pct) >= cfg_.threshold_pct) {
            last_signal_ = now;
            return Signal{ change_pct > 0 ? "buy" : "sell",
                           "momentum", ref, change_pct };
        }
        return std::nullopt;
    }

private:
    double ref_price_at(std::chrono::steady_clock::time_point cutoff) const {
        for (const auto& e : buf_)
            if (e.first >= cutoff) return e.second;
        return buf_.front().second;   // fallback (should not happen after prune)
    }

    bool in_cooldown(std::chrono::steady_clock::time_point now) const {
        if (last_signal_ == std::chrono::steady_clock::time_point::min())
            return false;
        return now - last_signal_ < std::chrono::duration<double>(cfg_.cooldown_seconds);
    }

    const Config& cfg_;
    std::deque<std::pair<std::chrono::steady_clock::time_point, double>> buf_;
    std::chrono::steady_clock::time_point last_signal_ =
        std::chrono::steady_clock::time_point::min();
};

// ---------------------------------------------------------------------------
// 3) FXCM Executor  —  blocking network work lives on worker threads
// ---------------------------------------------------------------------------
class FXCMExecutor {
public:
    explicit FXCMExecutor(const Config& cfg, size_t num_threads)
        : cfg_(cfg), threads_(num_threads) {}

    void start() {
        for (auto& t : threads_)
            t = std::thread([this] { worker_loop(); });
    }

    // Non-blocking: enqueues a signal; a worker thread executes it.
    void submit(const Signal& s) {
        {
            std::lock_guard<std::mutex> lk(mtx_);
            queue_.push_back(s);
        }
        cv_.notify_one();
    }

    // Notifies workers to drain the queue and then joins them.
    void stop() {
        {
            std::lock_guard<std::mutex> lk(mtx_);
            stopping_ = true;
        }
        cv_.notify_all();
        for (auto& t : threads_)
            if (t.joinable()) t.join();
    }

private:
    void worker_loop() {
        for (;;) {
            Signal s;
            {
                std::unique_lock<std::mutex> lk(mtx_);
                cv_.wait(lk, [this] { return stopping_ || !queue_.empty(); });
                if (stopping_ && queue_.empty()) break;
                s = std::move(queue_.front());
                queue_.pop_front();
            }
            execute(s);   // long network call — safe here, off the hot path
        }
    }

    // Risk gate + order placement (retry-once + latency tracking).
    // NOTE: this whole path runs on a worker thread (never the WS reactor),
    // so the 100ms retry sleep and the timing logic can never block the
    // Binance read loop.
    void execute(const Signal& s) {
        if (cfg_.dry_run) {
            log("[DRY-RUN] would place %s %.4f %s  (reason=%s ref=%.2f chg=%+.3f%%)",
                s.direction.c_str(), cfg_.trade_amount, cfg_.fxcm_symbol.c_str(),
                s.reason.c_str(), s.ref_price, s.change_pct);
            return;
        }
        if (open_positions_.load() >= cfg_.max_open_positions) {
            log_warn("MAX POSITIONS (%d) reached — skipping %s",
                cfg_.max_open_positions, s.direction.c_str());
            return;
        }
        open_positions_.fetch_add(1);

        log("FXCM order: %s %.4f %s  SL=%d TP=%d pips",
            s.direction.c_str(), cfg_.trade_amount, cfg_.fxcm_symbol.c_str(),
            cfg_.stop_loss_pips, cfg_.take_profit_pips);

        const auto t0 = std::chrono::high_resolution_clock::now();
        const std::string body = build_order_body(s);

        PostResult last;
        bool placed = false;
        constexpr int kMaxAttempts = 2;   // initial + 1 retry (per policy)
        for (int attempt = 1; attempt <= kMaxAttempts; ++attempt) {
            last = fxcm_post(cfg_, body);              // blocking network call
            const double attempt_ms =
                std::chrono::duration<double, std::milli>(last.latency).count();
            if (last.ok) {
                placed = true;
                log("FXCM HTTP attempt %d/%d OK  latency: %.2f ms  orderId=%s",
                    attempt, kMaxAttempts, attempt_ms, last.order_id.c_str());
                break;
            }
            log("FXCM HTTP attempt %d/%d FAILED (HTTP %d: %s)  latency: %.2f ms",
                attempt, kMaxAttempts, last.status, last.detail.c_str(), attempt_ms);
            if (attempt < kMaxAttempts && last.retryable) {
                log_warn("order failed — retrying in %d ms ...", cfg_.fxcm_retry_delay_ms);
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(cfg_.fxcm_retry_delay_ms));
            }
        }

        const double total_ms = std::chrono::duration<double, std::milli>(
            std::chrono::high_resolution_clock::now() - t0).count();
        if (placed) {
            log("FXCM order PLACED  execution latency: %.2f ms", total_ms);
        } else {
            log_critical("FXCM order placement FAILED after %d attempts "
                "(last: HTTP %d, %s)  execution latency: %.2f ms",
                kMaxAttempts, last.status, last.detail.c_str(), total_ms);
        }

        // Trade telemetry row -> trades_telemetry.csv (async, non-blocking).
        AsyncLogger::instance().log_trade(TradeTelemetry{
            now_epoch_ms(), s.direction, s.ref_price, s.change_pct,
            total_ms, last.status, placed ? last.order_id : "" });

        open_positions_.fetch_sub(1);
    }

    // Builds the FXCM /trading/open_trade payload using FXCM's exact field
    // names. Default body is application/x-www-form-urlencoded (what the FXCM
    // REST spec documents); set fxcm_use_json=true if your account/API
    // revision accepts a JSON body instead. SL/TP are attached in pips
    // (is_in_pips=true): limit=+TP pips, stop=-SL pips — FXCM normalizes the
    // sign internally per direction.
    std::string build_order_body(const Signal& s) const {
        const bool is_buy = s.direction == "buy";
        const std::string amount_str = std::to_string(cfg_.trade_amount);

        if (cfg_.fxcm_use_json) {
            nlohmann::json body = {
                {"account_id",    cfg_.fxcm_account_id},
                {"symbol",        cfg_.fxcm_symbol},
                {"is_buy",        is_buy},
                {"amount",        amount_str},
                {"rate",          0},
                {"at_market",     0},
                {"order_type",    "AtMarket"},
                {"time_in_force", cfg_.fxcm_time_in_force},
                {"stop",          -cfg_.stop_loss_pips},
                {"limit",         cfg_.take_profit_pips},
                {"is_in_pips",    true},
            };
            return body.dump();
        }

        std::string body;
        body += "account_id="    + urlencode(cfg_.fxcm_account_id);
        body += "&symbol="       + urlencode(cfg_.fxcm_symbol);
        body += "&is_buy="       + std::string(is_buy ? "true" : "false");
        body += "&amount="       + urlencode(amount_str);
        body += "&rate=0";
        body += "&at_market=0";
        body += "&order_type=AtMarket";
        body += "&time_in_force=" + urlencode(cfg_.fxcm_time_in_force);
        body += "&stop="         + std::to_string(-cfg_.stop_loss_pips);
        body += "&limit="        + std::to_string(cfg_.take_profit_pips);
        body += "&is_in_pips=true";
        return body;
    }

    // Result of one HTTP round-trip.
    struct PostResult {
        bool        ok        = false;   // 2xx AND response.executed == true
        bool        retryable = false;   // timeout/network/5xx/429 -> may retry
        int         status    = 0;
        std::string order_id;
        std::string detail;
        std::chrono::high_resolution_clock::duration latency{};
    };

    static std::string urlencode(const std::string& s) {
        static const char* kHex = "0123456789ABCDEF";
        std::string out;
        out.reserve(s.size() * 3);
        for (unsigned char c : s) {
            if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
                out.push_back(static_cast<char>(c));
            } else {
                out += '%';
                out += kHex[c >> 4];
                out += kHex[c & 0x0F];
            }
        }
        return out;
    }

    // One synchronous HTTPS POST to FXCM, timed end-to-end with a hard
    // deadline. Runs on a worker thread only — never on the WS reactor.
    static PostResult fxcm_post(const Config& cfg, const std::string& body) {
        const auto t0 = std::chrono::high_resolution_clock::now();
        try {
            net::io_context ioc;
            ssl::context ssl_ctx(ssl::context::tlsv12_client);
            // NOTE: verify_none is used for latency. In production set
            // ssl::verify_peer + set_default_verify_paths() for a trusted CA.
            ssl_ctx.set_verify_mode(ssl::verify_none);

            tcp::resolver resolver(ioc);
            const auto results = resolver.resolve(cfg.fxcm_host, cfg.fxcm_port);

            beast::ssl_stream<beast::tcp_stream> stream(ioc, ssl_ctx);
            const auto timeout = std::chrono::milliseconds(cfg.fxcm_request_timeout_ms);
            stream.next_layer().expires_after(timeout);   // DNS+TCP+handshake guard
            stream.next_layer().connect(results);
            stream.next_layer().expires_after(timeout);
            stream.handshake(ssl::stream_base::client);

            http::request<http::string_body> req(http::verb::post, cfg.fxcm_path, 11);
            req.set(http::field::host, cfg.fxcm_host);
            req.set(http::field::user_agent, "lead-lag-hft/1.0");
            req.set(http::field::accept, "application/json");
            req.set(http::field::content_type,
                    cfg.fxcm_use_json ? "application/json"
                                      : "application/x-www-form-urlencoded");
            req.set(http::field::authorization, "Bearer " + cfg.fxcm_token);
            req.body() = body;
            req.prepare_payload();

            stream.next_layer().expires_after(timeout);
            http::write(stream, req);

            stream.next_layer().expires_after(timeout);
            beast::flat_buffer buffer;
            http::response<http::string_body> res;
            http::read(stream, buffer, res);

            beast::error_code ec;
            stream.socket().shutdown(tcp::socket::shutdown_both, ec);

            PostResult r;
            r.status = static_cast<int>(res.result_int());
            r.ok     = (r.status >= 200 && r.status < 300);
            // Retry policy: timeouts/network errors (status==0) and 5xx/429.
            r.retryable = (r.status >= 500 || r.status == 429);
            try {
                const auto j    = nlohmann::json::parse(res.body());
                const auto resp = j.value("response", nlohmann::json());
                if (r.ok && !resp.value("executed", false)) {
                    r.ok = false;      // 2xx but broker rejected the order
                    r.retryable = false; // do NOT re-fire a possibly-live order
                    r.detail = res.body().substr(0, 200);
                }
                const auto data = j.value("data", nlohmann::json());
                if (data.is_object() && data.contains("orderId")) {
                    const auto& oid = data.at("orderId");
                    r.order_id = oid.is_string() ? oid.get<std::string>()
                                                 : oid.dump();
                }
            } catch (...) {
                /* body not JSON — keep status-based result */
            }
            r.latency = std::chrono::high_resolution_clock::now() - t0;
            return r;
        } catch (const beast::system_error& e) {
            PostResult r;                 // incl. tcp_stream hard deadlines
            r.retryable = true;
            r.detail = e.code().message();
            r.latency = std::chrono::high_resolution_clock::now() - t0;
            return r;
        } catch (const std::exception& e) {
            PostResult r;
            r.retryable = true;
            r.detail = e.what();
            r.latency = std::chrono::high_resolution_clock::now() - t0;
            return r;
        }
    }

    const Config& cfg_;
    std::vector<std::thread> threads_;
    std::mutex mtx_;
    std::condition_variable cv_;
    std::deque<Signal> queue_;
    bool stopping_ = false;
    std::atomic<int> open_positions_{0};
};

// ---------------------------------------------------------------------------
// 4) Binance WebSocket client  —  fully async, auto-reconnecting
// ---------------------------------------------------------------------------
class BinanceClient {
public:
    BinanceClient(net::io_context& ioc, ssl::context& ssl_ctx,
                  const Config& cfg, SignalGenerator& sig, FXCMExecutor& ex)
        : ioc_(ioc), ssl_ctx_(ssl_ctx), cfg_(cfg), sig_(sig), ex_(ex) {}

    void start() {
        reconnect_delay_s_ = cfg_.reconnect_base_s;
        start_session();
    }

    void stop() { stopped_.store(true); }

    // One logical WS connection. Recreated on every reconnect.
    class Session : public std::enable_shared_from_this<Session> {
    public:
        explicit Session(BinanceClient& c)
            : c_(c),
              ws_(net::make_strand(c.ioc_), c.ssl_ctx_),
              resolver_(net::make_strand(c.ioc_)) {}

        void run() {
            resolver_.async_resolve(
                c_.cfg_.binance_host, c_.cfg_.binance_port,
                beast::bind_front_handler(&Session::on_resolve, shared_from_this()));
        }

    private:
        void on_resolve(beast::error_code ec, tcp::resolver::results_type results) {
            if (ec) return fail("resolve", ec);
            beast::get_lowest_layer(ws_).expires_after(std::chrono::seconds(30));
            beast::get_lowest_layer(ws_).async_connect(
                results,
                beast::bind_front_handler(&Session::on_connect, shared_from_this()));
        }

        void on_connect(beast::error_code ec,
                        tcp::resolver::results_type::endpoint_type ep) {
            beast::ignore_unused(ep);
            if (ec) return fail("connect", ec);
            // SNI so the TLS handshake matches Binance's certificate.
            beast::error_code sni_ec;
            ws_.next_layer().set_tls_host_name(c_.cfg_.binance_host, sni_ec);
            if (sni_ec) return fail("sni", sni_ec);
            beast::get_lowest_layer(ws_).expires_after(std::chrono::seconds(30));
            ws_.next_layer().async_handshake(
                ssl::stream_base::client,
                beast::bind_front_handler(&Session::on_ssl_handshake, shared_from_this()));
        }

        void on_ssl_handshake(beast::error_code ec) {
            if (ec) return fail("tls_handshake", ec);
            beast::get_lowest_layer(ws_).expires_never();
            ws_.set_option(websocket::stream_base::decorator(
                [&](websocket::request_type& req) {
                    req.set(http::field::host, c_.cfg_.binance_host);
                    req.set(http::field::user_agent, "lead-lag-hft/1.0");
                }));
            ws_.set_option(websocket::stream_base::timeout::suggested(beast::role_type::client));
            ws_.async_handshake(
                c_.cfg_.binance_host, c_.cfg_.binance_path,
                beast::bind_front_handler(&Session::on_ws_handshake, shared_from_this()));
        }

        void on_ws_handshake(beast::error_code ec) {
            if (ec) return fail("ws_handshake", ec);
            log("Binance connected: %s:%s%s",
                c_.cfg_.binance_host.c_str(), c_.cfg_.binance_port.c_str(),
                c_.cfg_.binance_path.c_str());
            do_read();
        }

        void do_read() {
            buffer_.clear();
            ws_.async_read(buffer_,
                beast::bind_front_handler(&Session::on_read, shared_from_this()));
        }

        void on_read(beast::error_code ec, std::size_t bytes) {
            beast::ignore_unused(bytes);
            if (ec) return fail("read", ec);          // drop/reconnect
            c_.handle_message(beast::buffers_to_string(buffer_.data()));
            do_read();                                 // immediate next read
        }

        void fail(beast::error_code ec, const char* what) {
            if (c_.stopped()) return;
            log_warn("%s: %s", what, ec.message().c_str());
            c_.on_session_failed();
        }

        BinanceClient& c_;
        websocket::stream<beast::ssl_stream<beast::tcp_stream>> ws_;
        tcp::resolver resolver_;
        beast::flat_buffer buffer_;
    };

private:
    void start_session() {
        session_ = std::make_shared<Session>(*this);
        session_->run();
    }

    // Called by a session on any fatal error/close. Schedules a new session
    // after an exponential backoff. Cancelled on shutdown.
    void on_session_failed() {
        session_.reset();
        if (stopped_.load()) return;

        const double delay = reconnect_delay_s_;
        reconnect_delay_s_ = std::min(reconnect_delay_s_ * 2.0, cfg_.reconnect_max_s);

        log_warn("reconnecting in %.1fs ...", delay);
        auto timer = std::make_shared<net::steady_timer>(
            ioc_, std::chrono::milliseconds(static_cast<long long>(delay * 1000)));
        timer->async_wait([this, timer](beast::error_code ec) {
            if (ec || stopped_.load()) return;        // aborted or shutting down
            reconnect_delay_s_ = cfg_.reconnect_base_s;
            start_session();
        });
    }

    // Runs on the WS strand thread: parse JSON, feed the signal engine,
    // and (if triggered) hand the order to the executor without blocking.
    void handle_message(const std::string& msg) {
        try {
            const auto j = nlohmann::json::parse(msg);
            const std::string ps = j.value("p", "");
            if (ps.empty()) return;                   // non-trade message
            const double price = std::stod(ps);
            // Optional per-tick telemetry (LOG_TICKS=1). Push is non-blocking,
            // so the read loop is never stalled by ticks.csv disk I/O.
            if (cfg_.log_ticks) AsyncLogger::instance().log_tick(price);
            if (auto sig = sig_.update(price)) {
                log("SIGNAL %s (reason=%s ref=%.2f chg=%+.3f%%)",
                    sig->direction.c_str(), sig->reason.c_str(),
                    sig->ref_price, sig->change_pct);
                ex_.submit(*sig);                     // never blocks
            }
        } catch (...) {
            // Malformed frame: ignore, keep reading.
        }
    }

    net::io_context&   ioc_;
    ssl::context&      ssl_ctx_;
    const Config&      cfg_;
    SignalGenerator&   sig_;
    FXCMExecutor&      ex_;
    std::shared_ptr<Session> session_;
    double reconnect_delay_s_ = 2.0;
    std::atomic<bool> stopped_{false};
};

// ---------------------------------------------------------------------------
// 5) main — reactor + graceful shutdown
// ---------------------------------------------------------------------------
int main() {
    Config cfg = Config::from_env();

    // Bring up the async logger BEFORE any component can log.
    AsyncLogger::instance().set_log_dir(cfg.log_dir);
    AsyncLogger::instance().start();

    net::io_context ioc;
    ssl::context ssl_ctx(ssl::context::tlsv12_client);
    ssl_ctx.set_verify_mode(ssl::verify_none);   // see note in fxcm_post()

    SignalGenerator sig(cfg);
    FXCMExecutor   ex(cfg, cfg.executor_threads);
    BinanceClient  client(ioc, ssl_ctx, cfg, sig, ex);

    // Graceful shutdown on Ctrl-C / SIGTERM.
    net::signal_set signals(ioc, SIGINT, SIGTERM);
    signals.async_wait([&](beast::error_code ec, int /*signum*/) {
        if (ec) return;
        log("shutdown signal received — stopping ...");
        client.stop();
        ioc.stop();
    });

    log("lead-lag trader | %s | window=%.1fs | threshold=%.3f%% | cooldown=%.1fs | %s | tick-log=%s | logdir=%s",
        cfg.binance_path.c_str(), cfg.window_seconds, cfg.threshold_pct,
        cfg.cooldown_seconds, cfg.dry_run ? "DRY-RUN" : "LIVE",
        cfg.log_ticks ? "ON" : "OFF", cfg.log_dir.c_str());

    ex.start();
    client.start();

    ioc.run();        // blocks until stop() is triggered
    ex.stop();        // drain + join worker threads
    log("bye");
    AsyncLogger::instance().stop();   // flush remaining queue + join writer
    return 0;
}
