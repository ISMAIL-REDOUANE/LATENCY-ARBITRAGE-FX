#include "llm/config.h"

#include <cstdlib>
#include <string>

namespace llm {

std::string Config::binance_bookticker_path(const std::string& symbol) {
    return "/ws/" + symbol + "@bookTicker";
}

namespace {
const char* env(const char* name) {
    const char* v = std::getenv(name);
    return (v && *v) ? v : nullptr;
}
double denv(const char* name, double def) {
    const char* v = env(name);
    return v ? std::atof(v) : def;
}
int    ienv(const char* name, int def) {
    const char* v = env(name);
    return v ? std::atoi(v) : def;
}
bool   benv(const char* name, bool def) {
    const char* v = env(name);
    return v ? (std::string(v) == "1" || std::string(v) == "true" ||
                std::string(v) == "TRUE")
             : def;
}
}  // namespace

Config Config::from_env() {
    Config c;
    const char* v = nullptr;

    // ---- feeds -------------------------------------------------------- //
    if ((v = env("BINANCE_SYMBOL")))      c.binance_symbol = v;
    if ((v = env("BINANCE_HOST")))        c.binance_host   = v;
    if ((v = env("BINANCE_PORT")))        c.binance_port   = v;
    if ((v = env("DERIBIT_SYMBOL")))      c.deribit_symbol = v;
    if ((v = env("DERIBIT_WS")))          c.deribit_ws     = v;
    c.binance_path = binance_bookticker_path(c.binance_symbol);

    // ---- weighting ------------------------------------------------------ //
    c.use_daylight_savings  = benv("USE_DST", c.use_daylight_savings);
    c.us_open_hour_utc      = ienv("US_OPEN_HOUR_UTC", c.us_open_hour_utc);
    c.us_close_hour_utc     = ienv("US_CLOSE_HOUR_UTC", c.us_close_hour_utc);
    c.deribit_weight_asia   = denv("DERIBIT_WEIGHT_ASIA", c.deribit_weight_asia);
    c.binance_weight_asia   = denv("BINANCE_WEIGHT_ASIA", c.binance_weight_asia);
    c.deribit_weight_us     = denv("DERIBIT_WEIGHT_US", c.deribit_weight_us);
    c.binance_weight_us     = denv("BINANCE_WEIGHT_US", c.binance_weight_us);

    // ---- strategy ------------------------------------------------------- //
    c.threshold_pips         = denv("THRESHOLD_PIPS", c.threshold_pips);
    c.latency_buffer_pips    = denv("LATENCY_BUFFER_PIPS", c.latency_buffer_pips);
    c.min_profit_margin_pips = denv("MIN_PROFIT_MARGIN_PIPS", c.min_profit_margin_pips);
    c.min_lot_pips           = denv("MIN_LOT_PIPS", c.min_lot_pips);
    c.max_open_lots          = ienv("MAX_OPEN_LOTS", c.max_open_lots);
    c.max_daily_loss         = denv("MAX_DAILY_LOSS", c.max_daily_loss);
    c.max_orders_per_interval= ienv("MAX_ORDERS_PER_INTERVAL", c.max_orders_per_interval);
    c.interval_seconds       = ienv("INTERVAL_SECONDS", c.interval_seconds);
    c.round_four_decimals    = benv("FOUR_DECIMAL_ROUNDING", c.round_four_decimals);
    c.max_trade_per_interval = denv("MAX_TRADE_PER_INTERVAL", c.max_trade_per_interval);

    // ---- ws ------------------------------------------------------------- //
    c.ws_ping_interval_s     = ienv("WS_PING_INTERVAL_S", c.ws_ping_interval_s);
    c.ws_reconnect_base_s    = ienv("WS_RECONNECT_BASE_S", c.ws_reconnect_base_s);
    c.ws_reconnect_max_s     = ienv("WS_RECONNECT_MAX_S", c.ws_reconnect_max_s);

    // ---- broker sub ----------------------------------------------------- //
    if ((v = env("BROKER_ZMQ_BIND")))   c.broker_zmq_bind = v;
    if ((v = env("BROKER_TOPIC")))      c.broker_topic   = v;

    // ---- execution ------------------------------------------------------ //
    if ((v = env("ZMQ_PUB_BIND")))       c.zmq_pub_bind   = v;
    if ((v = env("FIX_HOST")))           c.fix_host       = v;
    if ((v = env("FIX_PORT")))           c.fix_port       = v;
    if ((v = env("FIX_SENDER_COMP_ID"))) c.fix_sender_comp_id = v;
    if ((v = env("FIX_TARGET_COMP_ID"))) c.fix_target_comp_id = v;
    c.fix_enabled  = benv("FIX_ENABLED", c.fix_enabled);
    c.dry_run      = benv("DRY_RUN", c.dry_run);
    if ((v = env("DRY_RUN_SYMBOL")))     c.dry_run_symbol = v;
    c.trade_amount = denv("TRADE_AMOUNT", c.trade_amount);

    // ---- logging / mmap -------------------------------------------------- //
    c.mmap_log_enabled    = benv("MMAP_LOG_DIR", c.mmap_log_enabled);
    c.mmap_prealloc_mb    = ienv("MMAP_PREALLOC_MB", c.mmap_prealloc_mb);
    c.mmap_prealloc_big_mb= ienv("MMAP_PREALLOC_BIG_MB", c.mmap_prealloc_big_mb);
    if ((v = env("LOG_DIR")))            c.log_dir = v;

    return c;
}

}  // namespace llm