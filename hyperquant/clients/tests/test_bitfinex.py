from hyperquant.api import Platform
from hyperquant.clients.bitfinex import BitfinexRESTClient, BitfinexWSClient, \
    BitfinexRESTConverterV2, BitfinexRESTConverterV1, BitfinexWSConverterV1, BitfinexWSConverterV2
from hyperquant.clients.tests.test_init import TestRESTClient, TestWSClient, TestConverter, TestRESTClientHistory


# REST

class TestBitfinexRESTConverterV1(TestConverter):
    converter_class = BitfinexRESTConverterV1


class TestBitfinexRESTConverterV2(TestConverter):
    converter_class = BitfinexRESTConverterV2


class TestBitfinexRESTClientV1(TestRESTClient):
    platform_id = Platform.BITFINEX
    version = "1"

    has_limit_error = False
    is_symbol_case_sensitive = False


class TestBitfinexRESTClientHistoryV1(TestRESTClientHistory):
    platform_id = Platform.BITFINEX
    version = "1"

    has_limit_error = False
    is_symbol_case_sensitive = False

    is_pagination_supported = False
    is_to_item_supported = False


class TestBitfinexRESTClientV2(TestRESTClient):
    client_class = BitfinexRESTClient
    version = "2"

    testing_symbol = "ETHUSD"
    is_sorting_supported = True

    has_limit_error = True
    is_symbol_case_sensitive = True

    def test_fetch_trades_errors(self, method_name="fetch_trades", is_auth=False):
        client = self.client_authed if is_auth else self.client

        # Wrong symbol
        result = getattr(client, method_name)(self.wrong_symbol)

        # Empty list instead of error
        # (todo check, may be we should create error for each empty list returned +++ yes, we should!)
        self.assertEqual(result, [])


class TestBitfinexRESTClientHistoryV2(TestRESTClientHistory):
    client_class = BitfinexRESTClient
    version = "2"

    testing_symbol = "ETHUSD"
    is_sorting_supported = True

    has_limit_error = True
    is_symbol_case_sensitive = True

    def test_fetch_trades_errors(self, method_name="fetch_trades", is_auth=False):
        client = self.client_authed if is_auth else self.client

        # Wrong symbol
        result = getattr(client, method_name)(self.wrong_symbol)

        # Empty list instead of error
        # (todo check, may be we should create error for each empty list returned +++ yes, we should!)
        self.assertEqual(result, [])


# WebSocket

class TestBitfinexWSConverterV1(TestConverter):
    converter_class = BitfinexWSConverterV1


class TestBitfinexWSConverterV2(TestConverter):
    converter_class = BitfinexWSConverterV2


class TestBitfinexWSClientV1(TestWSClient):
    platform_id = Platform.BITFINEX
    version = "1"


class TestBitfinexWSClientV2(TestBitfinexWSClientV1):
    version = "2"
