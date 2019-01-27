from hyperquant.api import Platform
from hyperquant.clients import Error, ErrorCode
from hyperquant.clients.binance import BinanceRESTClient, BinanceRESTConverterV1, BinanceWSClient, BinanceWSConverterV1
from hyperquant.clients.tests.test_init import TestRESTClient, TestWSClient, TestConverter, TestRESTClientHistory


# REST

class TestBinanceRESTConverterV1(TestConverter):
    converter_class = BinanceRESTConverterV1


class TestBinanceRESTClientV1(TestRESTClient):
    platform_id = Platform.BINANCE
    # version = "1"


class TestBinanceRESTClientHistoryV1(TestRESTClientHistory):
    platform_id = Platform.BINANCE
    # version = "1"

    is_to_item_by_id = True

    def test_just_logging_for_paging(self, method_name="fetch_trades_history", is_auth=False, sorting=None):
        super().test_just_logging_for_paging(method_name, True, sorting)

    def test_fetch_trades_history_errors(self):
        super().test_fetch_trades_history_errors()

        # Testing create_rest_client() which must set api_key for Binance
        result = self.client.fetch_trades_history(self.testing_symbol)

        self.assertIsNotNone(result)
        self.assertGoodResult(result)

        # Note: for Binance to get trades history you must send api_key
        self.client.set_credentials(None, None)
        result = self.client.fetch_trades_history(self.testing_symbol)

        self.assertIsNotNone(result)
        self.assertIsInstance(result, Error)
        self.assertEqual(result.code, ErrorCode.UNAUTHORIZED)


# WebSocket

class TestBinanceWSConverterV1(TestConverter):
    converter_class = BinanceWSConverterV1


class TestBinanceWSClientV1(TestWSClient):
    platform_id = Platform.BINANCE
    # version = "1"
