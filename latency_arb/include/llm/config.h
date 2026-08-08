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
    std::string fast_feed       = "BINANCE";  // FAST_FEED=BINANCE|BYBIT
    std::string binance_symbol = "btcusdt";
    std::string binance_host   = "stream.binance.com";
    std::string binance_port   = "9443";
    std::string binance_path;                    // "/ws/<sym>@bookTicker"
    std::string deribit_symbol = "BTC";
    std::string deribit_ws     = "wss://www.deribit.com/ws/api/v2";

    // Bybit v5 public spot: orderbook.1.<sym> -> best bid/ask ticker.
    std::string bybit_symbol   = "BTCUSDT";      // uppercase, spot notation
    std::string bybit_host     = "stream.bybit.com";
    std::string bybit_port     = "443";
    std::string bybit_path     = "/v5/public/spot";
    std::string bybit_channel  = "orderbook.1";  // depth-1 orderbook stream

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

    // ---- FIX 4.4 execution (IC Markets cTrader gateway) ---------------- //
    // cTrader FIX runs over plain TCP + SSL — no MT5 / REST needed. The demo
    // endpoint is demo-uk-eqx-01.p.c-trader.com:5211 (TargetCompID=cServer).
    std::string fix_host            = "demo-uk-eqx-01.p.c-trader.com";
    std::string fix_port            = "5211";
    std::string fix_sender_comp_id  = "demo.icmarkets.10092442"; // SenderCompID
    std::string fix_target_comp_id  = "cServer";                 // TargetCompID
    std::string fix_password        = "";                        // Tag 554 (FIX_PASSWORD)
    std::string fix_account_id      = "";                        // Tag 1 / env FIX_ACCOUNT_ID
    long        fix_heartbeat_s     = CompileTime::kFIXHeartbeatS; // 30 s
    bool        fix_enabled         = false;

    // ---- OANDA v2 execution -------------------------------------------- //
    std::string oanda_token         = "";   // OANDA API bearer token
    std::string oanda_account_id    = "";   // "/v3/accounts/{id}" segment
    std::string oanda_host          = "api-fxpractice.oanda.com"; // or -fxtrade
    std::string oanda_instrument    = "XAU_USD";  // default mapped instrument
    bool        oanda_enabled       = false;

    // ---- cTrader Open API execution (IC Markets) ------------------------ //
    std::string ctrader_client_id     = "";   // CTRADER_CLIENT_ID
    std::string ctrader_client_secret = "";   // CTRADER_CLIENT_SECRET
    std::string ctrader_account_id    = "";   // CTRADER_ACCOUNT_ID (numeric)
    std::string ctrader_access_token  = "";   // CTRADER_ACCESS_TOKEN
    std::string ctrader_host          = "openapi.ctrader.com"; // demo host
    std::string ctrader_port          = "5030";                // demo port
    std::string ctrader_symbol        = "XAU/USD";            // CTRADER_SYMBOL
    bool        ctrader_enabled       = false;                // CTRADER_ENABLED

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