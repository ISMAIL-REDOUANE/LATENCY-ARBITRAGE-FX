#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace llm {

// Compile-time-fixed sanity/safety constants (the "conf constants" layer).
// Runtime overrides come through env vars that mirror the systemd unit's
// [Service] Environment= lines.
struct CompileTime {
    // Staleness: any tick older than this is dropped from the window.
    static constexpr int64_t     kMaxTickAgeMs        = 50;
    // Execution caps.
    static constexpr double      kMaxSlippageBroker   = 2.0;  // pips (broker)
    static constexpr double      kMaxSlippageBtc      = 10.0; // USD on BTC index
    // IPC / ZMQ.
    static constexpr int         kZMQHwm              = 1'000'000;
    static constexpr int         kZMQSendHwm          = 1'000'000;
    static constexpr int         kZMQRecvHwm          = 1'000'000;
    static constexpr int         kSocketTimeoutMs     = 100;
    static constexpr int         kReconnectBaseS      = 2;
    static constexpr int         kReconnectMaxS       = 60;
    // FIX.
    static constexpr long        kFIXHeartbeatS       = 30;
    static constexpr long        kFIXLogonTimeoutS    = 10;
    static constexpr const char* kVersion             = "0.1.0";
};

// Runtime configuration. Every field has a compile-time default; env vars
// (mirroring the systemd unit Environment= lines) override it at startup.
struct Config {
    // ---- feeds --------------------------------------------------------- //
    std::string binance_symbol = "btcusdt";
    std::string binance_host   = "stream.binance.com";
    std::string binance_port   = "9443";
    std::string binance_path;                    // "/ws/<sym>@bookTicker"
    std::string deribit_symbol = "BTC";
    std::string deribit_ws     = "wss://www.deribit.com/ws/api/v2";

    // ---- aggregation / weighting ---------------------------------------- //
    // US hours: Deribit 60 / Binance 40. Asian hours: Deribit 40 / Binance 60.
    // US trading hours approximated as 13:30..20:00 UTC (NY cash hours).
    bool     use_daylight_savings  = true;
    int      us_open_hour_utc      = 13;
    int      us_close_hour_utc     = 20;
    double   deribit_weight_asia   = 0.40;
    double   binance_weight_asia   = 0.60;
    double   deribit_weight_us     = 0.60;
    double   binance_weight_us     = 0.40;

    // ---- strategy ------------------------------------------------------- //
    double   threshold_pips         = 0.5;  // base Dynamic_Threshold floor
    double   latency_buffer_pips    = 0.2;  // Latency_Penalty_Buffer
    double   min_profit_margin_pips = 0.3;  // Minimum_Profit_Margin
    double   min_lot_pips           = 0.0;  // MIN_LOT_PIPS
    int      max_open_lots          = 1;    // MAX_OPEN_LOTS
    double   max_daily_loss         = 500.0;
    int      max_orders_per_interval= 5;
    int      interval_seconds       = 60;
    bool     round_four_decimals    = true; // FOUR_DECIMAL_ROUNDING
    double   max_trade_per_interval = 1.0;  // BTC

    // ---- ws / connection ------------------------------------------------ //
    int      ws_ping_interval_s     = 20;
    int      ws_reconnect_base_s    = CompileTime::kReconnectBaseS;
    int      ws_reconnect_max_s     = CompileTime::kReconnectMaxS;

    // ---- broker ZMQ sub ------------------------------------------------- //
    std::string broker_zmq_bind     = "tcp://127.0.0.1:5556";  // OmsBroker pub
    std::string broker_topic        = "quote";

    // ---- execution ------------------------------------------------------ //
    std::string zmq_pub_bind        = "ipc:///tmp/latency_arb.ipc";
    std::string fix_host            = "127.0.0.1";
    std::string fix_port            = "5200";
    std::string fix_sender_comp_id  = "LEADLAG";
    std::string fix_target_comp_id  = "BROKER";
    bool        fix_enabled         = false;
    bool        dry_run             = true;
    std::string dry_run_symbol      = "BTC/USD";
    double      trade_amount        = 0.001;

    // ---- logging / mmap ------------------------------------------------- //
    bool        mmap_log_enabled    = false;  // MMAP_LOG_DIR=1 triggers
    int         mmap_prealloc_mb    = 1;      // 1MB at start
    int         mmap_prealloc_big_mb= 10;     // 10MB before threads start
    std::string log_dir             = "/var/log/lead_lag";

    // Derive feed path from symbol.
    static std::string binance_bookticker_path(const std::string& symbol);
    static Config from_env();

    Config() = default;
};

}  // namespace llm