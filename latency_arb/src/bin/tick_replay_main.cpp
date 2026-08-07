// ===========================================================================
// tick_replay — backtest replay CLI for the Lead-Lag engine.
//
// Usage:
//   tick_replay --lead <lead.bin> --lag <lag.bin> [--delay-ms 5]
//               [--report build/backtest_report.json] [--equity equity.csv]
//   tick_replay --convert-csv <input.csv> <output.bin>       (one-shot helper)
//
// Behavior:
//   * Loads both binary tick streams (zero-copy mmap), merges them
//     chronologically, replays through the production Strategy + execution
//     simulator, then exports:
//       - a JSON report (PnL, Sharpe, profit factor, max drawdown,
//         latency-decay matrix) to build/backtest_report.json
//       - an equity curve CSV (time, equity-USD, trade-pnl) when requested.
//   * The primary run uses --delay-ms; the latency-decay matrix sweeps a fixed
//     set of delays so you can see how edge decays as execution speed falls.
// ===========================================================================

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

#include "backtest/backtest_engine.hpp"
#include "backtest/execution_simulator.hpp"
#include "backtest/tick_binary.hpp"
#include "llm/config.h"
#include "llm/strategy.h"

namespace llm {

static void usage(const char* prog) {
    std::printf(
        "Usage:\n"
        "  %s --lead <lead.bin> --lag <lag.bin> [options]\n"
        "  %s --convert-csv <input.csv> <output.bin>\n"
        "\n"
        "Options (replay):\n"
        "  --delay-ms <ms>        execution latency (default 5; sweep 0/5/15/30/50/100)\n"
        "  --report <path>        JSON report output (default build/backtest_report.json)\n"
        "  --equity <path>        equity curve CSV output (optional)\n"
        "  --threshold <pts>      strategy threshold_pips override (default 0.5)\n"
        "  --latency-buffer <pts> latency_buffer_pips override (default 0.2)\n"
        "  --margin <pts>         min_profit_margin_pips override (default 0.3)\n"
        "  --max-loss <usd>       max_daily_loss override (default 500)\n"
        "  --volume <lots>        quantity override (default 1.0)\n"
        "  --help                 show this help\n"
        "\n"
        "Binary format: 24-byte llm::BinaryTick from CSV (ts_ms,bid,ask,volume),\n"
        "created with --convert-csv.\n",
        prog, prog);
}

struct CliArgs {
    std::string lead_path, lag_path;
    std::string report = "build/backtest_report.json";
    std::string equity_path;
    std::string csv_in, csv_out;
    bool        convert = false;
    int         delay_ms   = 5;
    double      threshold  = 0.5;
    double      latency_buf= 0.2;
    double      margin     = 0.3;
    double      max_loss   = 500.0;
    double      volume     = 1.0;
    bool        ok         = true;
};

// Returns the value of the flag `key` at argv[i] (key = argv[i]), consuming
// argv[i+1]. Prints an error and sets args->ok = false when missing.
static std::string flag_value(int argc, char** argv, int& i, const char* key,
                              CliArgs& args) {
    if (i + 1 >= argc) {
        std::fprintf(stderr, "error: %s requires a value\n", key);
        args.ok = false;
        return std::string();
    }
    return argv[++i];
}

}  // namespace

int main(int argc, char** argv) {
    llm::CliArgs a;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--convert-csv") {
            a.convert = true;
            a.csv_in  = llm::flag_value(argc, argv, i, "--convert-csv", a);
            a.csv_out = llm::flag_value(argc, argv, i, "--convert-csv", a);
        } else if (arg == "--lead") {
            a.lead_path = llm::flag_value(argc, argv, i, "--lead", a);
        } else if (arg == "--lag") {
            a.lag_path = llm::flag_value(argc, argv, i, "--lag", a);
        } else if (arg == "--delay-ms") {
            a.delay_ms = std::atoi(
                llm::flag_value(argc, argv, i, "--delay-ms", a).c_str());
        } else if (arg == "--report") {
            a.report = llm::flag_value(argc, argv, i, "--report", a);
        } else if (arg == "--equity") {
            a.equity_path = llm::flag_value(argc, argv, i, "--equity", a);
        } else if (arg == "--threshold") {
            a.threshold = std::atof(
                llm::flag_value(argc, argv, i, "--threshold", a).c_str());
        } else if (arg == "--latency-buffer") {
            a.latency_buf = std::atof(
                llm::flag_value(argc, argv, i, "--latency-buffer", a).c_str());
        } else if (arg == "--margin") {
            a.margin = std::atof(
                llm::flag_value(argc, argv, i, "--margin", a).c_str());
        } else if (arg == "--max-loss") {
            a.max_loss = std::atof(
                llm::flag_value(argc, argv, i, "--max-loss", a).c_str());
        } else if (arg == "--volume") {
            a.volume = std::atof(
                llm::flag_value(argc, argv, i, "--volume", a).c_str());
        } else if (arg == "--help" || arg == "-h") {
            llm::usage(argv[0]);
            return 0;
        } else {
            std::fprintf(stderr, "error: unknown argument '%s'\n", arg.c_str());
            a.ok = false;
        }
    }
    if (!a.ok) {
        llm::usage(argv[0]);
        return 2;
    }

    // ---- CSV -> binary conversion helper --------------------------------- //
    if (a.convert) {
        if (a.csv_in.empty() || a.csv_out.empty()) {
            std::fprintf(stderr, "error: --convert-csv needs <in.csv> <out.bin>\n");
            llm::usage(argv[0]);
            return 2;
        }
        llm::CsvToBinaryConverter::Options opt;
        opt.has_header = true;
        std::string err;
        const long n = llm::CsvToBinaryConverter::convert(
            a.csv_in, a.csv_out, opt, &err);
        if (n < 0) {
            std::fprintf(stderr, "convert failed: %s\n", err.c_str());
            return 1;
        }
        std::printf("converted %ld ticks -> %s\n", n, a.csv_out.c_str());
        return 0;
    }

    if (a.lead_path.empty() || a.lag_path.empty()) {
        std::fprintf(stderr, "error: --lead and --lag are required for replay\n");
        llm::usage(argv[0]);
        return 2;
    }

    // ---- load streams (zero-copy mmap) ------------------------------------ //
    llm::MmapTickReader lead(a.lead_path);
    llm::MmapTickReader lag(a.lag_path);
    std::string err;
    if (!lead.open(&err)) {
        std::fprintf(stderr, "lead open failed: %s\n", err.c_str());
        return 1;
    }
    if (!lag.open(&err)) {
        std::fprintf(stderr, "lag open failed: %s\n", err.c_str());
        return 1;
    }

    // ---- config + execution params ------------------------------------------ //
    llm::Config cfg = llm::Config::from_env();
    cfg.threshold_pips         = a.threshold;
    cfg.latency_buffer_pips    = a.latency_buf;
    cfg.min_profit_margin_pips = a.margin;
    cfg.max_daily_loss         = a.max_loss;
    cfg.interval_seconds       = 0;   // replay: no cooldown cap between signals
    cfg.max_orders_per_interval= 1'000'000;  // replay: no per-interval throttle

    llm::ExecutionParams ep;
    ep.delay_ms           = a.delay_ms;
    ep.slippage_pts       = 0.0;
    ep.spread_cap_pts     = 50.0;
    ep.slippage_cap_pts   = 50.0;
    ep.quantity           = a.volume;

    llm::BacktestEngine engine(lead, lag, cfg);

    // Primary run at the requested delay.
    llm::BacktestResult r = engine.run(ep);

    // Latency-decay matrix (edge decay as execution speed drops).
    std::vector<int> decays = {0, 5, 15, 30, 50, 100};
    const auto matrix = engine.latency_decay_matrix(decays, ep);

    // ---- emit report JSON ------------------------------------------------- //
    if (FILE* f = std::fopen(a.report.c_str(), "w")) {
        std::fprintf(f, "{\n");
        std::fprintf(f, "  \"engine\": \"lead_lag_tick_replay\",\n");
        std::fprintf(f, "  \"lead_file\": \"%s\",\n", a.lead_path.c_str());
        std::fprintf(f, "  \"lag_file\": \"%s\",\n",  a.lag_path.c_str());
        std::fprintf(f, "  \"run_delay_ms\": %d,\n",  a.delay_ms);
        std::fprintf(f, "  \"ticks\": { \"lead\": %zu, \"lag\": %zu },\n",
                     r.lead_ticks_consumed, r.lag_ticks_consumed);
        std::fprintf(f, "  \"signals\": %zu,\n",   r.signals_emitted);
        std::fprintf(f, "  \"fills\": %zu,\n",     r.fills);
        std::fprintf(f, "  \"rejections\": %zu,\n", r.rejections);
        std::fprintf(f, "  \"pnl\": { \"total\": %.6f, \"gross_profit\": %.6f,"
                        " \"gross_loss\": %.6f, \"profit_factor\": %.4f },\n",
                     r.total_pnl, r.gross_profit, r.gross_loss, r.profit_factor);
        std::fprintf(f, "  \"sharpe\": { \"per_trade\": %.4f, \"annualized\": %.4f },\n",
                     r.sharpe_per_trade, r.sharpe_annual);
        std::fprintf(f, "  \"drawdown\": { \"max\": %.4f, \"max_pct\": %.2f },\n",
                     r.max_drawdown, r.max_drawdown_pct);
        std::fprintf(f, "  \"equity\": { \"initial\": %.2f, \"final\": %.2f },\n",
                     r.initial_capital, r.final_equity);
        std::fprintf(f, "  \"latency_decay\": [");
        for (size_t i = 0; i < matrix.size(); ++i) {
            const auto& m = matrix[i];
            std::fprintf(f, "%s{\"delay_ms\":%d,\"pnl\":%.6f,\"sharpe\":%.4f,"
                            "\"fills\":%zu,\"rejections\":%zu}",
                         (i ? "," : ""), m.delay_ms, m.net_pnl, m.sharpe,
                         m.fills, m.rejections);
        }
        std::fprintf(f, "]\n}\n");
        std::fclose(f);
    } else {
        std::fprintf(stderr, "cannot write report: %s\n", a.report.c_str());
        return 1;
    }

    // ---- equity curve CSV (optional) -------------------------------------- //
    if (!a.equity_path.empty()) {
        if (FILE* f = std::fopen(a.equity_path.c_str(), "w")) {
            std::fprintf(f, "fill_ts_ms,side,fill_price,pnl,equity\n");
            for (const auto& t : r.trades) {
                std::fprintf(f, "%lld,%s,%.6f,%.6f,%.6f\n",
                             static_cast<long long>(t.fill_ts_ms),
                             t.side == llm::Side::Buy ? "BUY" :
                             (t.side == llm::Side::Sell ? "SELL" : "HOLD"),
                             t.fill_price, t.pnl, t.equity_after);
            }
            std::fclose(f);
        }
    }

    // ---- console summary --------------------------------------------------- //
    std::printf("replay complete:\n");
    std::printf("  lead ticks %zu | lag ticks %zu\n",
                r.lead_ticks_consumed, r.lag_ticks_consumed);
    std::printf("  signals %zu | fills %zu | rejects %zu\n",
                r.signals_emitted, r.fills, r.rejections);
    std::printf("  PnL %.6f | PF %.4f | Sharpe(per-trade) %.4f | MDD %.4f (%.2f%%)\n",
                r.total_pnl, r.profit_factor, r.sharpe_per_trade,
                r.max_drawdown, r.max_drawdown_pct);
    std::printf("  report -> %s\n", a.report.c_str());
    if (!a.equity_path.empty())
        std::printf("  equity -> %s\n", a.equity_path.c_str());
    return 0;
}