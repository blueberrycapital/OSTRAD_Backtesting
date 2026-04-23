"""
Run OSTRAD backtest for today using InfluxDB data.
"""

from datetime import datetime
from ostrad_engine import (
    setup_logging, load_params, create_symbol_params,
    InfluxDBDataLoader, OSTRADBacktester
)

# ── InfluxDB connection ────────────────────────────────────────────────────────
INFLUX_URL   = "http://165.22.215.65:8086"
INFLUX_TOKEN = "1cRqCHl8Zvn-l8Hc1uznO-YTZ5I8PMIOCVVTcSrnuExaQHR0ZfsfKav_ubajrM2R3gEmH7o5v93OqWG-_ANpQw=="
INFLUX_ORG   = "BBC India"
SPOT_BUCKET  = "test_nifty_options"   # fut_spot_merged measurement, data_type=SPOT
OPT_BUCKET   = "test_nifty_options"   # options_1min measurement

# ── Run date ──────────────────────────────────────────────────────────────────
TODAY = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)

# ── Symbol to backtest ────────────────────────────────────────────────────────
SYMBOL = "NIFTY"

if __name__ == "__main__":
    logger = setup_logging("ostrad_influx_today.log")
    logger.info(f"Running backtest for {SYMBOL} on {TODAY.date()}")

    # Load params
    global_params = load_params("params.json")
    params = create_symbol_params(global_params["global"], global_params["symbols"][SYMBOL])

    # Build InfluxDB loader — preload just today's data
    loader = InfluxDBDataLoader(
        url=INFLUX_URL,
        token=INFLUX_TOKEN,
        org=INFLUX_ORG,
        spot_bucket=SPOT_BUCKET,
        option_bucket=OPT_BUCKET,
        symbol=SYMBOL,
        preload_start=TODAY,
        preload_end=TODAY,
        logger=logger,
    )

    # Sanity-check data availability
    if loader.spot_df is None or loader.spot_df.empty:
        logger.error("No spot data returned — check bucket/measurement names and date range.")
        loader.close()
        raise SystemExit(1)

    if loader.options_df is None or loader.options_df.empty:
        logger.error("No options data returned — check bucket/measurement names and date range.")
        loader.close()
        raise SystemExit(1)

    logger.info(f"Spot data shape   : {loader.spot_df.shape}")
    logger.info(f"Options data shape: {loader.options_df.shape}")
    logger.info(f"Spot index range  : {loader.spot_df.index.min()} → {loader.spot_df.index.max()}")
    logger.info(f"Options index range: {loader.options_df.index.min()} → {loader.options_df.index.max()}")

    # Run backtest
    backtester = OSTRADBacktester(SYMBOL, params, loader, logger)
    results = backtester.run_backtest(TODAY, TODAY)

    # Print summary
    print("\n" + "=" * 50)
    print(f"  RESULTS: {SYMBOL}  {TODAY.date()}")
    print("=" * 50)
    for k, v in results["summary"].items():
        print(f"  {k:<25} {v}")
    print("=" * 50)

    # Save results
    saved = backtester.save_results(results, output_dir="results/influx_today")
    logger.info(f"Results saved: {saved}")

    loader.close()
