import asyncio as aio
import datetime as dt
import dttb
import logging
import logging.config
import os
import pandas as pd
import pathlib
import tomllib
import yaml
from loguru import logger
from typing import Any
from binance_common.constants import (
    # TimeUnit,
    WebsocketMode,
    DERIVATIVES_TRADING_USDS_FUTURES_REST_API_PROD_URL as REST_API_PROD_URL,
    DERIVATIVES_TRADING_USDS_FUTURES_REST_API_TESTNET_URL as REST_API_TESTNET_URL,
    DERIVATIVES_TRADING_USDS_FUTURES_WS_API_PROD_URL as WS_API_PROD_URL,
    DERIVATIVES_TRADING_USDS_FUTURES_WS_API_TESTNET_URL as WS_API_TESTNET_URL,
    DERIVATIVES_TRADING_USDS_FUTURES_WS_STREAMS_PROD_URL as WS_STREAMS_PROD_URL,
    DERIVATIVES_TRADING_USDS_FUTURES_WS_STREAMS_TESTNET_URL as WS_STREAMS_TESTNET_URL,
)
from binance_common.configuration import (
    ConfigurationRestAPI as ConfigRestAPI,
    ConfigurationWebSocketAPI as ConfigWebSocketAPI,
    ConfigurationWebSocketStreams as ConfigWebSocketStreams,
)

from bnufsa import (
    AsyncCoroutineGroup,
    BNUFSARecorder,
    BNUFSATrader,
    BNUFSAController,
    BNUFSAMonitor,
    BNUFSA,
)


def milliseconds(
    s: str,
) -> int:
    return int(pd.Timedelta(s).total_seconds()) * 1000


async def launch(
    config: dict[str, Any],
) -> None:
    aio.get_event_loop().slow_callback_duration = config["asyncio"][
        "slow_callback_duration"
    ]

    name = dt.datetime.now().strftime("%Y%m%d_%H%M%S")

    log_dir = os.path.join(".", "logs", name)
    pathlib.Path(log_dir).mkdir(parents=True, exist_ok=True)

    with open(r"./logging.yaml", mode="rb") as f:
        logging_config = yaml.safe_load(f)
        for handler in logging_config["handlers"].values():
            if "filename" not in handler:
                continue
            handler["filename"] = os.path.join(log_dir, handler["filename"])
        logging.config.dictConfig(logging_config)

    if config["loguru"]["logger"]["remove"]:
        logger.remove()
    for params in config["loguru"]["logger"]["add"]:
        params["sink"] = os.path.join(log_dir, params["sink"])
        logger.add(**params)

    prod = config["binance_url"]["prod"]

    rest_api_url = REST_API_PROD_URL if prod else REST_API_TESTNET_URL
    ws_api_url = WS_API_PROD_URL if prod else WS_API_TESTNET_URL
    ws_streams_url = WS_STREAMS_PROD_URL if prod else WS_STREAMS_TESTNET_URL

    binance_url = "prod" if prod else "testnet"
    key = config["binance_account"][binance_url]["key"]
    secret = config["binance_account"][binance_url]["secret"]

    logger.critical(config["binance_url"])
    logger.critical(config["bnufsa"])

    logger.critical(">>> ENTER >>>")
    async with AsyncCoroutineGroup(
        (
            BNUFSA(
                symbol=params["symbol"],
                config_rest_api=ConfigRestAPI(
                    api_key=key,
                    api_secret=secret,
                    base_path=rest_api_url,
                ),
                config_ws_api=ConfigWebSocketAPI(
                    api_key=key,
                    api_secret=secret,
                    stream_url=ws_api_url,
                    reconnect_delay=params["config_ws_api"]["reconnect_delay"],
                    mode=WebsocketMode(params["config_ws_api"]["mode"]),
                    pool_size=params["config_ws_api"]["pool_size"],
                    # time_unit=TimeUnit(params["config_ws_api"]["time_unit"]),
                ),
                config_ws_streams=ConfigWebSocketStreams(
                    stream_url=ws_streams_url,
                    reconnect_delay=params["config_ws_streams"]["reconnect_delay"],
                    mode=WebsocketMode(params["config_ws_streams"]["mode"]),
                    pool_size=params["config_ws_streams"]["pool_size"],
                    # time_unit=TimeUnit(params["config_ws_streams"]["time_unit"]),
                ),
                kwargs_recorder={
                    "file": os.path.join(log_dir, f"{params['name']}.csv"),
                    "mode": "w",
                    "name": f"{params['name']}:{BNUFSARecorder.__name__}",
                },
                kwargs_trader={
                    "name": f"{params['name']}:{BNUFSATrader.__name__}",
                },
                kwargs_controller={
                    "quantity": params["quantity"],
                    "risk": params["risk"],
                    "volatility": params["volatility"],
                    "sl_volatility": params["sl_volatility"],
                    "close_volatility": params["close_volatility"],
                    "open_volatility": params["open_volatility"],
                    "open_slippage": params["open_slippage"],
                    "open_sleep_interval": milliseconds(params["open_sleep_interval"]),
                    "tw_interval": milliseconds(params["tw_interval"]),
                    "tw_capacity": params["tw_capacity"],
                    "capacity": params.get("capacity"),
                    "name": f"{params['name']}:{BNUFSAController.__name__}",
                },
                kwargs_monitor={
                    "name": f"{params['name']}:{BNUFSAMonitor.__name__}",
                },
                name=f"{params['name']}:{BNUFSA.__name__}",
            )
            for params in config["bnufsa"]
        ),
        name=f"{name}:{AsyncCoroutineGroup.__name__}",
    ) as spread_arbitrage_group:
        try:
            await spread_arbitrage_group
        except aio.CancelledError as e:
            print(repr(e))
    logger.critical("<<< EXIT <<<")


def main() -> None:
    dttb.apply()

    config = {}
    with (
        open(r"./config.toml", mode="rb") as f0,
        open(r"./config-private.toml", mode="rb") as f1,
        open(r"./loguru.toml", mode="rb") as f2,
    ):
        config.update(tomllib.load(f0))
        config.update(tomllib.load(f1))
        config.update(tomllib.load(f2))

    aio.run(launch(config), debug=config["asyncio"]["debug"])


if __name__ == "__main__":
    main()
