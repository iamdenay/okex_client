from hyperquant.api import Platform
from hyperquant.clients.bitmex import BitMEXRESTConverterV1, BitMEXRESTClient, BitMEXWSClient, BitMEXWSConverterV1
from hyperquant.clients.tests.test_init import TestRESTClient, TestWSClient, TestConverter, TestRESTClientHistory


# TODO check https://www.bitmex.com/app/restAPI "Обратите внимание: все суммы в биткойнах при
# возврате запроса указываются в Satoshi: 1 XBt (Satoshi) = 0,00000001 XBT (биткойн)."

# REST

class TestBitMEXRESTConverterV1(TestConverter):
    converter_class = BitMEXRESTConverterV1


class TestBitMEXRESTClientV1(TestRESTClient):
    platform_id = Platform.BITMEX
    version = "1"
    # testing_symbol = "XBTUSD"
    testing_symbol = None  # BitMEX returns all symbols if symbol param is not specified
    testing_symbol2 = "XBTUSD"

    is_sorting_supported = True

    has_limit_error = True
    is_symbol_case_sensitive = True


class TestBitMEXRESTClientHistoryV1(TestRESTClientHistory):
    platform_id = Platform.BITMEX
    version = "1"
    # testing_symbol = "XBTUSD"
    testing_symbol = None  # BitMEX returns all symbols if symbol param is not specified
    testing_symbol2 = "XBTUSD"

    is_sorting_supported = True

    has_limit_error = True
    is_symbol_case_sensitive = True

    def test_fetch_trades_errors(self, method_name="fetch_trades", is_auth=False):
        client = self.client_authed if is_auth else self.client

        # Wrong symbol
        result = getattr(client, method_name)(self.wrong_symbol)

        # Empty list instead of error (todo check, may be we should create error for each empty list returned)
        self.assertEqual(result, [])

        if self.is_symbol_case_sensitive:
            # Symbol in lower case as wrong symbol
            result = getattr(client, method_name)(self.testing_symbol2.lower())

            self.assertIsNotNone(result)
            self.assertEqual(result, [])


# WebSocket

class TestBitMEXWSConverterV1(TestConverter):
    converter_class = BitMEXWSConverterV1


class TestBitMEXWSClientV1(TestWSClient):
    platform_id = Platform.BITMEX
    version = "1"

    testing_symbol = "XBTUSD"
    testing_symbols = ["ETHUSD", "XBTUSD"]
