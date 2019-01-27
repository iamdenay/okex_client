import unittest

from hyperquant.api import Platform
from hyperquant.clients.binance import BinanceRESTClient, BinanceWSClient
from hyperquant.clients.bitfinex import BitfinexRESTClient, BitfinexWSClient
from hyperquant.clients.bitmex import BitMEXRESTClient, BitMEXWSClient
from hyperquant.clients.utils import create_rest_client, create_ws_client


class TestCreateClient(unittest.TestCase):

    def test_create_rest_client(self):
        self._test_create_client()

    def test_create_ws_client(self):
        self._test_create_client(False)

    def test_create_rest_client_private(self):
        self._test_create_client(is_private=True)

    def test_create_ws_client_private(self):
        self._test_create_client(False, is_private=True)

    def _test_create_client(self, is_rest=True, is_private=False):
        create_client = create_rest_client if is_rest else create_ws_client

        # Binance
        client = create_client(Platform.BINANCE, is_private)

        self.assertIsInstance(client, BinanceRESTClient if is_rest else BinanceWSClient)
        self.assertEqual(client.version, BinanceRESTClient.version)
        if not is_private:
            self.assertIsNotNone(client._api_key,
                                 "For Binance, api_key must be set even for public API (for historyTrades endponit)")
            self.assertIsNone(client._api_secret)
        else:
            self.assertIsNotNone(client._api_key)
            self.assertIsNotNone(client._api_secret)

        # Bitfinex
        client = create_client(Platform.BITFINEX, is_private)

        self.assertIsInstance(client, BitfinexRESTClient if is_rest else BitfinexWSClient)
        self.assertEqual(client.version, BitfinexRESTClient.version)
        if not is_private:
            self.assertIsNone(client._api_key)
            self.assertIsNone(client._api_secret)
        else:
            self.assertIsNotNone(client._api_key)
            self.assertIsNotNone(client._api_secret)

        # Testing version
        client = create_client(Platform.BITFINEX, is_private, version="1")

        self.assertIsInstance(client, BitfinexRESTClient if is_rest else BitfinexWSClient)
        self.assertEqual(client.version, "1")
        self.assertNotEqual(client.version, BitfinexRESTClient.version)
        if not is_private:
            self.assertIsNone(client._api_key)
            self.assertIsNone(client._api_secret)
        else:
            self.assertIsNotNone(client._api_key)
            self.assertIsNotNone(client._api_secret)

        # BitMEX
        client = create_client(Platform.BITMEX, is_private)

        self.assertIsInstance(client, BitMEXRESTClient if is_rest else BitMEXWSClient)
        self.assertEqual(client.version, BitMEXRESTClient.version)
        if not is_private:
            self.assertIsNone(client._api_key)
            self.assertIsNone(client._api_secret)
        else:
            self.assertIsNotNone(client._api_key)
            self.assertIsNotNone(client._api_secret)

