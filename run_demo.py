import time

from django.conf import settings

import settings as hqlib_settings
from hyperquant.api import Interval
from hyperquant.clients import (
    utils, Endpoint, Platform
)
from hyperquant.clients.tests.utils import set_up_logging

settings.configure(DEBUG=True, default_settings=hqlib_settings)

# Enable logging if needed
# set_up_logging()


# Change to Platform.BINANCE to see example
TEST_PLATFORM = Platform.OKEX

TEST_SYMBOLS = {
    Platform.BINANCE: ['ETHBTC', 'BTCUSDT'],
    Platform.OKEX: ['eth_btc', 'btc_usdt'],
}

client = utils.create_rest_client(platform_id=TEST_PLATFORM)
print('\n\nTrade history\n\n')
print(client.fetch_trades_history(TEST_SYMBOLS[TEST_PLATFORM][0], limit=10))
print('\n\n---------------------')
print('\n\nCandles\n\n')
print(client.fetch_candles(TEST_SYMBOLS[TEST_PLATFORM][0], Interval.MIN_1, limit=10))
print('\n\n---------------------')

client = utils.create_ws_client(platform_id=TEST_PLATFORM)
client.on_data_item = lambda item: print(item)  # print received parsed objects
client.subscribe(endpoints=[Endpoint.TRADE, Endpoint.CANDLE], symbols=TEST_SYMBOLS[TEST_PLATFORM],
                 interval=Interval.MIN_1)

print('\n\nWebsocket data\n\n')
# Sleep to display incoming websocket items from separate thread
time.sleep(1155)
