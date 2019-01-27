import logging
import time
from datetime import datetime
from unittest import TestCase

from hyperquant.api import Sorting, Interval, OrderType, Direction
from hyperquant.clients import Error, ErrorCode, ParamName, ProtocolConverter, \
    Endpoint, DataObject, Order, OrderBook
from hyperquant.clients.tests.utils import wait_for, AssertUtil, set_up_logging
from hyperquant.clients.utils import create_ws_client, create_rest_client

set_up_logging()


# Converter

class TestConverter(TestCase):
    converter_class = ProtocolConverter

    def setUp(self):
        super().setUp()

    # def test_(self):
    #     pass


# Common client

class TestClient(TestCase):
    is_rest = None
    platform_id = None
    version = None

    is_sorting_supported = False
    testing_symbol = "EOSETH"
    testing_symbols = ["EOSETH", "BNBBTC"]
    wrong_symbol = "XXXYYY"

    client = None
    client_authed = None

    def setUp(self):
        self.skipIfBase()
        super().setUp()

        if self.is_rest:
            self.client = create_rest_client(self.platform_id, version=self.version)
            self.client_authed = create_rest_client(self.platform_id, True, self.version)
        else:
            self.client = create_ws_client(self.platform_id, version=self.version)
            self.client_authed = create_ws_client(self.platform_id, True, self.version)

    def tearDown(self):
        self.client.close()
        super().tearDown()

    def skipIfBase(self):
        if self.platform_id is None:
            self.skipTest("Skip base class")

    # Utility

    def _result_info(self, result, sorting):
        is_asc_sorting = sorting == Sorting.ASCENDING
        items_info = "%s first: %s last: %s sort-ok: %s " % (
            "ASC" if is_asc_sorting else "DESC",
            self._str_item(result[0]) if result else "-",
            self._str_item(result[-1]) if result else "-",
            (result[0].timestamp < result[-1].timestamp if is_asc_sorting
             else result[0].timestamp > result[-1].timestamp) if result else "-")
        return items_info + "count: %s" % (len(result) if result else "-")

    def _str_item(self, item):
        # return str(item.item_id) + " " + str(item.timestamp / 100000)
        # return str(item.timestamp / 100000)
        dt = datetime.utcfromtimestamp(item.timestamp)
        return dt.isoformat()

    def assertRightSymbols(self, items):
        if self.testing_symbol:
            for item in items:
                # was: item.symbol = self.testing_symbol
                self.assertEqual(item.symbol, item.symbol.upper())
                self.assertEqual(item.symbol, self.testing_symbol)
        else:
            # For Trades in BitMEX
            symbols = set([item.symbol for item in items])
            self.assertGreater(len(symbols), 1)
            # self.assertGreater(len(symbols), 10)

    # (Assert items)

    def assertItemIsValid(self, trade, testing_symbol_or_symbols=None):
        if not testing_symbol_or_symbols:
            testing_symbol_or_symbols = self.testing_symbol

        AssertUtil.assertItemIsValid(self, trade, testing_symbol_or_symbols, self.platform_id)

    def assertTradeIsValid(self, trade, testing_symbol_or_symbols=None):
        if not testing_symbol_or_symbols:
            testing_symbol_or_symbols = self.testing_symbol

        AssertUtil.assertTradeIsValid(self, trade, testing_symbol_or_symbols, self.platform_id)

    def assertMyTradeIsValid(self, my_trade, testing_symbol_or_symbols=None):
        if not testing_symbol_or_symbols:
            testing_symbol_or_symbols = self.testing_symbol

        AssertUtil.assertMyTradeIsValid(self, my_trade, testing_symbol_or_symbols, self.platform_id)

    def assertCandleIsValid(self, candle, testing_symbol_or_symbols=None):
        if not testing_symbol_or_symbols:
            testing_symbol_or_symbols = self.testing_symbol

        AssertUtil.assertCandleIsValid(self, candle, testing_symbol_or_symbols, self.platform_id)

    def assertTickerIsValid(self, ticker, testing_symbol_or_symbols=None):
        # if not testing_symbol_or_symbols:
        #     testing_symbol_or_symbols = self.testing_symbol

        AssertUtil.assertTickerIsValid(self, ticker, testing_symbol_or_symbols, self.platform_id)

    def assertOrderBookIsValid(self, order_book, testing_symbol_or_symbols=None):
        if not testing_symbol_or_symbols:
            testing_symbol_or_symbols = self.testing_symbol

        AssertUtil.assertOrderBookIsValid(self, order_book, testing_symbol_or_symbols, self.platform_id)

    def assertOrderBookDiffIsValid(self, order_book, testing_symbol_or_symbols=None):
        if not testing_symbol_or_symbols:
            testing_symbol_or_symbols = self.testing_symbol

        AssertUtil.assertOrderBookDiffIsValid(self, order_book, testing_symbol_or_symbols, self.platform_id)

    # def assertOrderBookItemIsValid(self, order_book_item, testing_symbol_or_symbols=None):
    #     if not testing_symbol_or_symbols:
    #         testing_symbol_or_symbols = self.testing_symbol
    #
    #     AssertUtil.assertOrderBookItemIsValid(self, order_book_item, testing_symbol_or_symbols, self.platform_id)

    def assertAccountIsValid(self, account):
        AssertUtil.assertAccountIsValid(self, account, self.platform_id)

    def assertOrderIsValid(self, order, testing_symbol_or_symbols=None):
        if not testing_symbol_or_symbols:
            testing_symbol_or_symbols = self.testing_symbol

        AssertUtil.assertOrderIsValid(self, order, testing_symbol_or_symbols, self.platform_id)


# REST

class BaseTestRESTClient(TestClient):
    is_rest = True

    # (If False then platform supposed to use its max_limit instead
    # of returning error when we send too big limit)
    has_limit_error = False
    is_symbol_case_sensitive = True

    is_rate_limit_error = False

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.is_rate_limit_error = False

    def setUp(self):
        self.skipIfRateLimit()
        super().setUp()

    def assertGoodResult(self, result, is_iterable=True, message=None):
        if isinstance(result, Error) and result.code == ErrorCode.RATE_LIMIT:
            self.__class__.is_rate_limit_error = True
            self.skipIfRateLimit()

        self.assertIsNotNone(result, message)
        self.assertNotIsInstance(result, Error, message or Error)
        if is_iterable:
            self.assertGreater(len(result), 0, message)

    def assertErrorResult(self, result, error_code_expected=None):
        if isinstance(result, Error) and result.code == ErrorCode.RATE_LIMIT:
            self.__class__.is_rate_limit_error = True
            self.skipIfRateLimit()

        self.assertIsNotNone(result)
        self.assertIsInstance(result, Error)
        if error_code_expected is not None:
            self.assertEqual(result.code, error_code_expected)

    def skipIfRateLimit(self):
        if self.__class__.is_rate_limit_error:
            self.skipTest("Rate limit reached for this platform. Try again later.")


class TestRESTClient(BaseTestRESTClient):
    # Test all methods except history methods

    # (All numbers taken from https://api.binance.com/api/v1/exchangeInfo for EOSETH.
    # Define your dicts for other platforms in subclasses.)
    order_sell_limit_params = {
        ParamName.ORDER_TYPE: OrderType.LIMIT,
        ParamName.DIRECTION: Direction.SELL,
        # todo check to avoid problems
        ParamName.PRICE: "0.22",
        ParamName.AMOUNT: "0.1",
    }

    order_buy_market_params = {
        ParamName.ORDER_TYPE: OrderType.MARKET,
        ParamName.DIRECTION: Direction.BUY,
        # todo check to avoid problems
        # ParamName.PRICE: "0.000001",  # no price for MARKET order
        ParamName.AMOUNT: "0.01",
    }

    order_sell_market_params = {
        ParamName.ORDER_TYPE: OrderType.MARKET,
        ParamName.DIRECTION: Direction.SELL,
        # todo check to avoid problems
        # ParamName.PRICE: "0.000001",  # no price for MARKET order
        ParamName.AMOUNT: "0.01",
    }

    created_orders = None

    def tearDown(self):
        # Cancel all created orders
        if self.created_orders:
            for item in self.created_orders:
                self.client_authed.cancel_order(item)

        super().tearDown()

    # Simple methods

    def test_ping(self, is_auth=False):
        client = self.client_authed if is_auth else self.client

        result = client.ping()

        self.assertGoodResult(result, False)

    def test_get_server_timestamp(self, is_auth=False):
        client = self.client_authed if is_auth else self.client

        # With request
        client.use_milliseconds = True

        result0_ms = result = client.get_server_timestamp(is_refresh=True)

        self.assertGoodResult(result, False)
        self.assertGreater(result, 1500000000000)
        self.assertIsInstance(result, int)

        client.use_milliseconds = False

        result0_s = result = client.get_server_timestamp(is_refresh=True)

        self.assertGoodResult(result, False)
        self.assertGreater(result, 1500000000)
        self.assertLess(result, 15000000000)
        self.assertIsInstance(result, (int, float))

        # Cached
        client.use_milliseconds = True

        result = client.get_server_timestamp(is_refresh=False)

        self.assertGoodResult(result, False)
        self.assertGreater(result, 1500000000000)
        self.assertIsInstance(result, int)
        self.assertGreater(result, result0_ms)

        client.use_milliseconds = False

        result = client.get_server_timestamp(is_refresh=False)

        self.assertGoodResult(result, False)
        self.assertGreater(result, 1500000000)
        self.assertLess(result, 15000000000)
        self.assertIsInstance(result, (int, float))
        self.assertGreater(result, result0_s)

    def test_get_symbols(self, is_auth=False):
        client = self.client_authed if is_auth else self.client

        result = client.get_symbols()

        self.assertGoodResult(result)
        self.assertGreater(len(result), 1)
        self.assertGreater(len(result), 50)
        self.assertIsInstance(result[0], str)
        if self.testing_symbol:
            self.assertIn(self.testing_symbol, result)

    # fetch_trades

    def test_fetch_trades(self, method_name="fetch_trades", is_auth=False):
        client = self.client_authed if is_auth else self.client

        result = getattr(client, method_name)(self.testing_symbol)

        self.assertGoodResult(result)
        self.assertGreater(len(result), 1)
        self.assertGreater(len(result), 50)
        self.assertTradeIsValid(result[0])
        for item in result:
            self.assertTradeIsValid(item)
        self.assertRightSymbols(result)

    def test_fetch_trades_errors(self, method_name="fetch_trades", is_auth=False):
        client = self.client_authed if is_auth else self.client

        # Wrong symbol
        result = getattr(client, method_name)(self.wrong_symbol)

        self.assertIsNotNone(result)
        self.assertIsInstance(result, Error)
        self.assertEqual(result.code, ErrorCode.WRONG_SYMBOL)

        if self.is_symbol_case_sensitive:
            # Symbol in lower case as wrong symbol
            result = getattr(client, method_name)(self.testing_symbol.lower())

            self.assertIsNotNone(result)
            self.assertIsInstance(result, Error)
            self.assertTrue(result.code == ErrorCode.WRONG_SYMBOL or
                            result.code == ErrorCode.WRONG_PARAM)

    def test_fetch_trades_limit(self, method_name="fetch_trades", is_auth=False):
        client = self.client_authed if is_auth else self.client

        self.assertFalse(client.converter.is_use_max_limit)

        # Test limit
        self.assertFalse(client.use_milliseconds)
        # client.use_milliseconds = False
        result = getattr(client, method_name)(self.testing_symbol, 2)

        self.assertGoodResult(result)
        self.assertEqual(len(result), 2)
        # (Test use_milliseconds)
        self.assertLess(result[0].timestamp, time.time())

        # Test is_use_max_limit (with limit param)
        client.use_milliseconds = True
        client.converter.is_use_max_limit = True
        result = getattr(client, method_name)(self.testing_symbol, 2)

        self.assertGoodResult(result)
        self.assertEqual(len(result), 2)
        # (Test use_milliseconds)
        self.assertGreater(result[0].timestamp, time.time())

        # (Get default item count)
        result = getattr(client, method_name)(self.testing_symbol)
        self.assertGoodResult(result)
        default_item_count = len(result)

        # Test is_use_max_limit (without limit param)
        client.converter.is_use_max_limit = True
        result = getattr(client, method_name)(self.testing_symbol)

        self.assertGoodResult(result)
        self.assertGreaterEqual(len(result), default_item_count, "Sometimes needs retry (for BitMEX, for example)")
        for item in result:
            self.assertTradeIsValid(item)
        self.assertRightSymbols(result)

    def test_fetch_trades_limit_is_too_big(self, method_name="fetch_trades", is_auth=False):
        client = self.client_authed if is_auth else self.client

        # Test limit is too big
        too_big_limit = 1000000
        result = getattr(client, method_name)(self.testing_symbol, too_big_limit)

        self.assertIsNotNone(result)
        if self.has_limit_error:
            self.assertIsInstance(result, Error)
            self.assertErrorResult(result, ErrorCode.WRONG_LIMIT)
        else:
            self.assertGoodResult(result)
            self.assertGreater(len(result), 10)
            self.assertLess(len(result), too_big_limit)
            for item in result:
                self.assertTradeIsValid(item)
            self.assertRightSymbols(result)
            max_limit_count = len(result)

            # Test is_use_max_limit uses the maximum possible limit
            client.converter.is_use_max_limit = True
            result = getattr(client, method_name)(self.testing_symbol)

            self.assertEqual(len(result), max_limit_count, "is_use_max_limit doesn't work")

    def test_fetch_trades_sorting(self, method_name="fetch_trades", is_auth=False):
        if not self.is_sorting_supported:
            self.skipTest("Sorting is not supported by platform.")

        client = self.client_authed if is_auth else self.client

        self.assertEqual(client.converter.sorting, Sorting.DESCENDING)

        # Test descending (default) sorting
        result = getattr(client, method_name)(self.testing_symbol)

        self.assertGoodResult(result)
        self.assertGreater(len(result), 2)
        self.assertGreater(result[0].timestamp, result[-1].timestamp)

        # Test ascending sorting
        client.converter.sorting = Sorting.ASCENDING
        result2 = getattr(client, method_name)(self.testing_symbol)

        self.assertGoodResult(result2)
        self.assertGreater(len(result2), 2)
        self.assertLess(result2[0].timestamp, result2[-1].timestamp)

        # (not necessary)
        # print("TEMP timestamps:", result[0].timestamp, result[-1].timestamp)
        # print("TEMP timestamps:", result2[0].timestamp, result2[-1].timestamp)
        # # Test that it is the same items for both sorting types
        # self.assertGreaterEqual(result2[0].timestamp, result[-1].timestamp)
        # self.assertGreaterEqual(result[0].timestamp, result2[-1].timestamp)
        # Test that interval of items sorted ascending is far before the interval of descending
        self.assertLess(result2[0].timestamp, result[-1].timestamp)
        self.assertLess(result2[0].timestamp, result[0].timestamp)

    # Other public methods

    def test_fetch_candles(self):
        client = self.client
        testing_interval = Interval.DAY_3

        # Error
        result = client.fetch_candles(None, None)

        self.assertErrorResult(result)

        result = client.fetch_candles(self.testing_symbol, None)

        self.assertErrorResult(result)

        # Good
        result = client.fetch_candles(self.testing_symbol, testing_interval)

        self.assertGoodResult(result)
        for item in result:
            self.assertCandleIsValid(item, self.testing_symbol)
            self.assertEqual(item.interval, testing_interval)

        # todo test from_, to_, and limit

    def test_fetch_ticker(self):
        client = self.client

        # Error

        # Good

        # Empty params
        result = client.fetch_ticker(None)

        self.assertGoodResult(result)
        self.assertGreater(len(result), 2)
        for item in result:
            self.assertTickerIsValid(item)

        # Full params
        result = client.fetch_ticker(self.testing_symbol)

        self.assertGoodResult(result, False)
        self.assertTickerIsValid(result, self.testing_symbol)

    def test_fetch_tickers(self):
        client = self.client

        # Error

        # Good

        # Empty params
        result = client.fetch_tickers()

        self.assertGoodResult(result)
        self.assertGreater(len(result), 2)
        for item in result:
            self.assertTickerIsValid(item)

        # Full params
        result = client.fetch_tickers(self.testing_symbols)

        self.assertGoodResult(result)
        self.assertEqual(len(result), len(self.testing_symbols))
        for item in result:
            self.assertTickerIsValid(item, self.testing_symbols)

    def test_fetch_order_book(self):
        client = self.client

        # Error

        # Empty params
        result = client.fetch_order_book()

        self.assertErrorResult(result)

        # Good

        # Full params
        result = client.fetch_order_book(self.testing_symbol)

        self.assertGoodResult(result, False)
        self.assertOrderBookIsValid(result)

        # todo test limit and is_use_max_limit

    # Private API methods

    def test_fetch_account_info(self):
        client = self.client_authed

        # Error

        # Good

        # Empty params  # Full params
        result = client.fetch_account_info()

        self.assertGoodResult(result, is_iterable=False)
        self.assertAccountIsValid(result)

    def test_fetch_my_trades(self):
        client = self.client_authed

        # Error

        # Empty params
        result = client.fetch_my_trades(None)

        self.assertErrorResult(result)

        # Good

        # Full params
        result = client.fetch_my_trades(self.testing_symbol)

        NO_ITEMS_FOR_ACCOUNT = True
        self.assertGoodResult(result, not NO_ITEMS_FOR_ACCOUNT)
        for item in result:
            self.assertMyTradeIsValid(item, self.testing_symbols)

        # Limit
        result = client.fetch_my_trades(self.testing_symbol, 1)

        self.assertGoodResult(result, not NO_ITEMS_FOR_ACCOUNT)
        self.assertLessEqual(len(result), 1)

        result = client.fetch_my_trades(self.testing_symbol, 7)

        self.assertGoodResult(result, not NO_ITEMS_FOR_ACCOUNT)
        self.assertLessEqual(len(result), 7)
        if len(result) < 7:
            logging.warning("You have not enough my trades to test limit for sure.")
        for item in result:
            self.assertMyTradeIsValid(item, self.testing_symbols)

    def test_create_order(self):
        client = self.client_authed

        # Error

        # Empty params
        result = client.create_order(None, None, None, None, None)

        self.assertErrorResult(result)

        # Good

        # Sell, limit
        result = client.create_order(self.testing_symbol, **self.order_sell_limit_params, is_test=True)

        self.assertGoodResult(result)
        cancel_result = client.cancel_order(result)

        self.assertOrderIsValid(result, self.testing_symbol)
        self.assertEqual(result.order_type, self.order_sell_limit_params.get(ParamName.ORDER_TYPE))
        self.assertEqual(result.direction, self.order_sell_limit_params.get(ParamName.DIRECTION))
        self.assertEqual(result.price, self.order_sell_limit_params.get(ParamName.PRICE))
        self.assertEqual(result.amount, self.order_sell_limit_params.get(ParamName.AMOUNT))
        self._check_canceled(cancel_result)

        IS_REAL_MONEY = True
        if IS_REAL_MONEY:
            return

        # Full params
        # Buy, market
        result = client.create_order(self.testing_symbol, **self.order_buy_market_params, is_test=True)

        self.assertGoodResult(result, is_iterable=False)
        cancel_result = client.cancel_order(result)  # May be not already filled

        self.assertOrderIsValid(result, self.testing_symbol)
        self.assertEqual(result.order_type, self.order_buy_market_params.get(ParamName.ORDER_TYPE))
        self.assertEqual(result.direction, self.order_buy_market_params.get(ParamName.DIRECTION))
        self.assertEqual(result.price, self.order_buy_market_params.get(ParamName.PRICE))
        self.assertEqual(result.amount, self.order_buy_market_params.get(ParamName.AMOUNT))
        self._check_canceled(cancel_result)

        # Sell, market - to revert buy-market order
        result = client.create_order(self.testing_symbol, **self.order_sell_market_params, is_test=True)

        self.assertGoodResult(result, is_iterable=False)
        cancel_result = client.cancel_order(result)

        self.assertOrderIsValid(result, self.testing_symbol)
        self.assertEqual(result.order_type, self.order_sell_market_params.get(ParamName.ORDER_TYPE))
        self.assertEqual(result.direction, self.order_sell_market_params.get(ParamName.DIRECTION))
        self.assertEqual(result.price, self.order_sell_market_params.get(ParamName.PRICE))
        self.assertEqual(result.amount, self.order_sell_market_params.get(ParamName.AMOUNT))
        self._check_canceled(cancel_result)

    def _create_order(self):
        client = self.client_authed

        order = client.create_order(self.testing_symbol, **self.order_sell_limit_params, is_test=False)

        self.assertOrderIsValid(order)
        # Add for canceling in tearDown
        if not self.created_orders:
            self.created_orders = []
        self.created_orders.append(order)

        return order

    def _check_canceled(self, cancel_result):
        self.assertGoodResult(cancel_result, False, "IMPORTANT! Order was created during tests, but not canceled!")

    def assertCanceledOrder(self, order, symbol, item_id):
        self.assertItemIsValid(order, symbol)
        self.assertIsInstance(order, Order)
        self.assertEqual(order.item_id, item_id)

    def test_cancel_order(self):
        client = self.client_authed

        # Error

        # Empty params
        result = client.cancel_order(None)

        self.assertErrorResult(result)

        # Good

        # Full params
        order = self._create_order()
        result = client.cancel_order(order, "some")

        self._check_canceled(result)
        # self.assertGoodResult(result)
        self.assertNotEqual(result, order)
        self.assertCanceledOrder(result, order.symbol, order.item_id)

        # Same by item_id and symbol
        order = self._create_order()
        result = client.cancel_order(order.item_id, order.symbol)

        self._check_canceled(result)
        # self.assertGoodResult(result)
        self.assertIsNot(result, order)
        self.assertEqual(result, order)
        # self.assertNotEqual(result, order)
        self.assertOrderIsValid(result)
        self.assertCanceledOrder(result, order.symbol, order.item_id)

    def test_check_order(self):
        client = self.client_authed

        # Error

        # Empty params
        result = client.check_order(None)

        self.assertErrorResult(result)

        # temp
        result = client.check_order("someid", "somesymb")
        # Good

        # Full params
        order = self._create_order()
        result = client.check_order(order, "some")

        self.assertGoodResult(result)
        self.assertEqual(order, result)
        self.assertOrderIsValid(result)

        # Same by item_id and symbol
        result = client.check_order(order.item_id, order.symbol)

        self.assertGoodResult(result)
        self.assertEqual(order, result)
        self.assertOrderIsValid(result)

        cancel_result = client.cancel_order(order)
        self._check_canceled(cancel_result)

    def test_fetch_orders(self):
        client = self.client_authed

        # Error

        # Good
        order = None
        order = self._create_order()

        # Empty params
        # Commented because for Binance it has weight 40
        # result = client.fetch_orders()
        #
        # self.assertGoodResult(result)
        # self.assertGreater(len(result), 0)
        # for item in result:
        #     self.assertOrderIsValid(item)

        # All
        result = client.fetch_orders(self.testing_symbol, is_open=False)

        self.assertGoodResult(result)
        # self.assertGreater(len(result), 0)
        for item in result:
            self.assertOrderIsValid(item)

        # Full params
        result = client.fetch_orders(self.testing_symbol, is_open=True)

        self.assertGoodResult(result)
        # self.assertGreater(len(result), 0)
        for item in result:
            self.assertOrderIsValid(item)

        cancel_result = client.cancel_order(order)
        self._check_canceled(cancel_result)

        # All (all open are closed)
        result = client.fetch_orders(self.testing_symbol, is_open=False)

        self.assertGoodResult(result)
        self.assertGreater(len(result), 0)
        for item in result:
            self.assertOrderIsValid(item)

        # todo test also limit and from_item (and to_item? - for binance) for is_open=false


class TestRESTClientHistory(BaseTestRESTClient):
    # Test only history methods

    is_pagination_supported = True
    is_to_item_supported = True
    is_to_item_by_id = False

    # fetch_history

    def test_fetch_history_from_and_to_item(self, endpoint=Endpoint.TRADE, is_auth=True,
                                            timestamp_param=ParamName.TIMESTAMP):
        client = self.client_authed if is_auth else self.client

        # Limit must be greater than max items with same timestamp (greater than 10 at least)
        limit = 50

        # (Get items to be used to set from_item, to_item params)
        result0 = result = client.fetch_history(endpoint, self.testing_symbol,
                                                sorting=Sorting.DESCENDING, limit=limit)

        # print("\n#0", len(result), result)
        self.assertGoodResult(result)
        self.assertGreater(len(result), 2)
        if client.converter.IS_SORTING_ENABLED:
            self.assertGreater(result[0].timestamp, result[-1].timestamp)

        # Test FROM_ITEM and TO_ITEM
        result = client.fetch_history(endpoint, self.testing_symbol,
                                      sorting=Sorting.DESCENDING,  # limit=limit,
                                      from_item=result0[0], to_item=result0[-1])

        # print("\n#1", len(result), result)
        self.assertGoodResult(result)
        self.assertGreater(len(result), 2)
        self.assertIn(result[0], result0, "Try restart tests.")
        # self.assertIn(result[-10], result0, "Try restart tests.")
        if self.is_to_item_supported:
            self.assertIn(result[-1], result0, "Try restart tests.")
        # self.assertEqual(len(result), len(result0))
        # self.assertEqual(result, result0)

        # Test FROM_ITEM and TO_ITEM in wrong order
        result = client.fetch_history(endpoint, self.testing_symbol,
                                      sorting=Sorting.DESCENDING,  # limit=limit,
                                      from_item=result0[-1], to_item=result0[0])

        # print("\n#2", len(result), result)
        self.assertGoodResult(result)
        self.assertGreater(len(result), 2)
        self.assertIn(result[0], result0, "Try restart tests.")
        # self.assertIn(result[-10], result0, "Try restart tests.")
        if self.is_to_item_supported:
            self.assertIn(result[-1], result0, "Try restart tests.")
        # self.assertEqual(len(result), len(result0))
        # self.assertEqual(result, result0)

        # Test FROM_ITEM and TO_ITEM in wrong order and sorted differently
        result = client.fetch_history(endpoint, self.testing_symbol,
                                      sorting=Sorting.ASCENDING,  # limit=limit,
                                      from_item=result0[-1], to_item=result0[0])

        # print("\n#3", len(result), result)
        self.assertGoodResult(result)
        self.assertGreater(len(result), 2)
        self.assertIn(result[0], result0, "Try restart tests.")
        # self.assertIn(result[-10], result0, "Try restart tests.")
        if self.is_to_item_supported:
            self.assertIn(result[-1], result0, "Try restart tests.")
        # self.assertEqual(len(result), len(result0))
        # self.assertEqual(result, result0)

    def test_fetch_history_with_all_params(self, endpoint=Endpoint.TRADE, is_auth=True,
                                           timestamp_param=ParamName.TIMESTAMP):
        client = self.client_authed if is_auth else self.client

        # (Get items to be used to set from_item, to_item params)
        # Test SYMBOL and LIMIT
        self.assertEqual(client.converter.sorting, Sorting.DESCENDING)
        limit = 10
        result = client.fetch_history(endpoint, self.testing_symbol, limit)

        self.assertGoodResult(result)
        self.assertEqual(len(result), limit)
        if client.converter.IS_SORTING_ENABLED:
            self.assertGreater(result[0].timestamp, result[-1].timestamp)
        # print("TEMP result", result)

        # Test FROM_ITEM and TO_ITEM
        from_item = result[1]
        to_item = result[-2]
        print("Get history from_item:", from_item, "to_item:", to_item)
        result = client.fetch_history(endpoint, self.testing_symbol,
                                      from_item=from_item, to_item=to_item)

        # print("TEMP result:", result)
        self.assertGoodResult(result)
        if self.is_to_item_supported:
            if self.is_to_item_by_id:
                self.assertEqual(len(result), limit - 2)
            self.assertEqual(result[-1].timestamp, to_item.timestamp)

        # Test SORTING, get default_result_len
        result = client.fetch_history(endpoint, self.testing_symbol,
                                      sorting=Sorting.ASCENDING)

        self.assertGoodResult(result)
        self.assertGreater(len(result), limit)
        if client.converter.IS_SORTING_ENABLED:
            self.assertLess(result[0].timestamp, result[-1].timestamp)
        default_result_len = len(result)

        # Test IS_USE_MAX_LIMIT
        result = client.fetch_history(endpoint, self.testing_symbol,
                                      is_use_max_limit=True)

        self.assertGoodResult(result)
        self.assertGreaterEqual(len(result), default_result_len)

        # Test SYMBOL param as a list
        if self.testing_symbol:
            # (Note: for Binance fetch_history(endpoint, ["some", "some"])
            # sends request without 2 SYMBOL get params which cases error.)
            # (Note: for BitMEX fetch_history(endpoint, [None, None])
            # sends request without SYMBOL get param which is usual request - so skip here.)
            result = client.fetch_history(endpoint, [self.testing_symbol, self.testing_symbol])

            self.assertIsNotNone(result)
            # (Bitfinex returns [] on such error)
            if result:
                self.assertErrorResult(result)

    # fetch_trades_history

    test_fetch_trades = TestRESTClient.test_fetch_trades
    test_fetch_trades_errors = TestRESTClient.test_fetch_trades_errors
    test_fetch_trades_limit = TestRESTClient.test_fetch_trades_limit
    test_fetch_trades_limit_is_too_big = TestRESTClient.test_fetch_trades_limit_is_too_big
    test_fetch_trades_sorting = TestRESTClient.test_fetch_trades_sorting

    def test_fetch_trades_history(self):
        self.test_fetch_trades("fetch_trades_history")

    def test_fetch_trades_history_errors(self):
        self.test_fetch_trades_errors("fetch_trades_history")

    def test_fetch_trades_history_limit(self):
        self.test_fetch_trades_limit("fetch_trades_history")

    def test_fetch_trades_history_limit_is_too_big(self):
        self.test_fetch_trades_limit_is_too_big("fetch_trades_history")

    def test_fetch_trades_history_sorting(self):
        self.test_fetch_trades_sorting("fetch_trades_history")

    def test_fetch_trades_is_same_as_first_history(self):
        result = self.client_authed.fetch_trades(self.testing_symbol)
        result_history = self.client_authed.fetch_trades_history(self.testing_symbol)

        self.assertNotIsInstance(result, Error)
        self.assertGreater(len(result), 10)
        # self.assertIn(result_history[0], result, "Try restart")
        self.assertIn(result_history[10], result, "Try restart")
        self.assertIn(result[-1], result_history)
        self.assertEqual(result, result_history,
                         "Can fail sometimes due to item added between requests")

    def test_fetch_trades_history_over_and_over(self, sorting=None):
        if not self.is_pagination_supported:
            self.skipTest("Pagination is not supported by current platform version.")

        if self.is_sorting_supported and not sorting:
            self.test_fetch_trades_history_over_and_over(Sorting.DESCENDING)
            self.test_fetch_trades_history_over_and_over(Sorting.ASCENDING)
            return

        client = self.client_authed
        client.converter.is_use_max_limit = True

        print("Test trade paging with",
              "sorting: " + sorting if sorting else "default_sorting: " + client.default_sorting)
        if not sorting:
            sorting = client.default_sorting

        result = client.fetch_trades(self.testing_symbol, sorting=sorting)
        self.assertGoodResult(result)
        page_count = 1
        print("Page:", page_count, self._result_info(result, sorting))

        while result and not isinstance(result, Error):
            prev_result = result
            result = client.fetch_trades_history(self.testing_symbol, sorting=sorting, from_item=result[-1])
            page_count += 1
            self.assertGoodResult(result)
            if isinstance(result, Error):
                # Rate limit error!
                print("Page:", page_count, "error:", result)
            else:
                # Check next page
                print("Page:", page_count, self._result_info(result, sorting))
                self.assertGreater(len(result), 2)
                for item in result:
                    self.assertTradeIsValid(item)
                self.assertRightSymbols(result)
                if sorting == Sorting.ASCENDING:
                    # Oldest first
                    self.assertLess(prev_result[0].timestamp, prev_result[-1].timestamp,
                                    "Error in sorting")  # Check sorting is ok
                    self.assertLess(result[0].timestamp, result[-1].timestamp,
                                    "Error in sorting")  # Check sorting is ok
                    self.assertLessEqual(prev_result[-1].timestamp, result[0].timestamp,
                                         "Error in paging")  # Check next page
                else:
                    # Newest first
                    self.assertGreater(prev_result[0].timestamp, prev_result[-1].timestamp,
                                       "Error in sorting")  # Check sorting is ok
                    self.assertGreater(result[0].timestamp, result[-1].timestamp,
                                       "Error in sorting")  # Check sorting is ok
                    self.assertGreaterEqual(prev_result[-1].timestamp, result[0].timestamp,
                                            "Error in paging")  # Check next page

            if page_count > 2:
                print("Break to prevent RATE_LIMIT error.")
                break

        print("Pages count:", page_count)

    # For debugging only
    def test_just_logging_for_paging(self, method_name="fetch_trades_history", is_auth=False, sorting=None):
        if self.is_sorting_supported and not sorting:
            self.test_just_logging_for_paging(method_name, is_auth, Sorting.DESCENDING)
            self.test_just_logging_for_paging(method_name, is_auth, Sorting.ASCENDING)
            return

        client = self.client_authed if is_auth else self.client
        print("Logging paging with",
              "sorting: " + sorting if sorting else "default_sorting: " + client.converter.default_sorting)
        if not sorting:
            sorting = client.converter.default_sorting

        print("\n==First page==")
        result0 = result = getattr(client, method_name)(self.testing_symbol, sorting=sorting)

        self.assertGoodResult(result)
        print("_result_info:", self._result_info(result, sorting))

        print("\n==Next page==")
        # print("\nXXX", result0[-1].timestamp)
        # result0[-1].timestamp -= 100
        # print("\nXXX", result0[-1].timestamp)
        result = getattr(client, method_name)(self.testing_symbol, sorting=sorting, from_item=result0[-1])
        # print("\nXXX", result0[0].timestamp, result0[-1].timestamp)
        # print("\nYYY", result[0].timestamp, result[-1].timestamp)

        if result:
            # To check rate limit error
            self.assertGoodResult(result)
        print("_result_info:", self._result_info(result, sorting))

        print("\n==Failed page==")
        result = getattr(client, method_name)(self.testing_symbol, sorting=sorting, from_item=result0[0])

        self.assertGoodResult(result)
        print("_result_info:", self._result_info(result, sorting))


# WebSocket

class TestWSClient(TestClient):
    is_rest = False

    testing_symbols = ["ETHBTC", "BTXUSD"]
    received_items = None

    def setUp(self):
        self.skipIfBase()

        super().setUp()
        self.received_items = []

        def on_item_received(item):
            if isinstance(item, DataObject):
                self.received_items.append(item)

        self.client.on_item_received = on_item_received
        self.client_authed.on_item_received = on_item_received

    def test_trade_1_channel(self):
        self._test_endpoint_channels([Endpoint.TRADE], [self.testing_symbol], self.assertTradeIsValid)

    def test_trade_2_channel(self):
        self._test_endpoint_channels([Endpoint.TRADE], self.testing_symbols, self.assertTradeIsValid)

    def test_candle_1_channel(self):
        params = {ParamName.INTERVAL: Interval.MIN_1}
        self._test_endpoint_channels([Endpoint.CANDLE], [self.testing_symbol], self.assertCandleIsValid, params)

    def test_candle_2_channel(self):
        params = {ParamName.INTERVAL: Interval.MIN_1}
        self._test_endpoint_channels([Endpoint.CANDLE], self.testing_symbols, self.assertCandleIsValid, params)

    def test_ticker1_channel(self):
        self._test_endpoint_channels([Endpoint.TICKER], [self.testing_symbol], self.assertTickerIsValid)

    def test_ticker2_channel(self):
        self._test_endpoint_channels([Endpoint.TICKER], self.testing_symbols, self.assertTickerIsValid)

    def test_ticker_all_channel(self):
        self._test_endpoint_channels([Endpoint.TICKER_ALL], None, self.assertTickerIsValid)

    def test_order_book_1_channel(self):
        params = {ParamName.LEVEL: 5}
        self._test_endpoint_channels([Endpoint.ORDER_BOOK], [self.testing_symbol], self.assertOrderBookIsValid, params)

    def test_order_book_2_channel(self):
        params = {ParamName.LEVEL: 5}
        self._test_endpoint_channels([Endpoint.ORDER_BOOK], self.testing_symbols, self.assertOrderBookIsValid, params)

    def test_order_book_diff_1_channel(self):
        self._test_endpoint_channels([Endpoint.ORDER_BOOK_DIFF], [self.testing_symbol], self.assertOrderBookDiffIsValid)

    def test_order_book_diff_2_channel(self):
        self._test_endpoint_channels([Endpoint.ORDER_BOOK_DIFF], self.testing_symbols, self.assertOrderBookDiffIsValid)

    def _test_endpoint_channels(self, endpoints, symbols, assertIsValidFun, params=None, is_auth=False):
        client = self.client_authed if is_auth else self.client

        if not isinstance(endpoints, (list, tuple)):
            endpoints = [endpoints]
        if symbols and not isinstance(symbols, (list, tuple)):
            symbols = [symbols]

        client.subscribe(endpoints, symbols, **params or {})

        # todo wait for all endpoints and all symbols?
        wait_for(self.received_items, timeout_sec=10000000)

        self.assertGreaterEqual(len(self.received_items), 1)
        for item in self.received_items:
            assertIsValidFun(item, symbols)
