import logging
import sys
import time
from unittest import TestCase

from hyperquant.api import OrderStatus, Direction, OrderType, OrderBookDirection, Interval
from hyperquant.clients import Trade, ItemObject, Candle, MyTrade, Ticker, Order, OrderBookItem, OrderBook, Account, \
    Balance


# Utility

def set_up_logging(is_debug=True):
    logging_format = "%(asctime)s %(levelname)s:%(name)s: %(message)s"
    logging.basicConfig(level=logging.DEBUG if is_debug else logging.INFO,
                        stream=sys.stdout, format=logging_format)


def wait_for(value_or_callable, count=2, timeout_sec=10):
    # Wait for value is of "count" length or "timeout_sec" elapsed.
    start_time = time.time()
    value, fun = (None, value_or_callable) if callable(value_or_callable) \
        else (value_or_callable, None)
    print("\n### Waiting a list for count: %s or timeout_sec: %s" % (count, timeout_sec))
    while not timeout_sec or time.time() - start_time < timeout_sec:
        if fun:
            value = fun()
        if isinstance(value, bool):
            if value:
                print("\n### Result is true: %s in %s seconds" % (value, time.time() - start_time))
                return
        else:
            value_count = value if isinstance(value, int) else len(value)
            if value_count >= count:
                print("\n### Count reached: %s of %s in %s seconds" % (value_count, count, time.time() - start_time))
                return
            print("\n### Sleep... current count: %s of %s, %s seconds passed" % (value_count, count, time.time() - start_time))
        time.sleep(min(1, timeout_sec / 10) if timeout_sec else 1)
    print("\n### Time is out! (value)")
    raise Exception("Time is out!")


def wait_for_history(history_connector, timeout_sec=10):
    # Wait for item_list is of "count" length or "timeout_sec" elapsed.
    start_time = time.time()
    print("\n### Waiting a history_connector or timeout_sec: %s" % (timeout_sec))
    while not timeout_sec or time.time() - start_time < timeout_sec:
        if not history_connector.is_in_progress:
            if history_connector.is_complete:
                print("\n### All (or no) history retrieved in: %s seconds" % (time.time() - start_time))
            else:
                print("\n### All history closed complete. Worked: %s seconds" % (time.time() - start_time))
            return True
        time.sleep(min(3, timeout_sec / 10) if timeout_sec else 1)
    print("\n### Time is out! (history_connector)")
    raise Exception("Time is out!")
    # return False


class AssertUtil(TestCase):
    # Don't extend with this class, but use functions in your test classes

    def assertItemIsValid(self, item, testing_symbol_or_symbols=None, platform_id=None,
                          is_with_item_id=True, is_with_timestamp=True):
        self.assertIsNotNone(item)
        self.assertIsInstance(item, ItemObject)

        # Not empty
        self.assertIsNotNone(item.platform_id)
        self.assertIsNotNone(item.symbol)
        if is_with_timestamp:
            self.assertIsNotNone(item.timestamp)
        if is_with_item_id:
            self.assertIsNotNone(item.item_id)  # trade_id: binance, bitfinex - int converted to str; bitmex - str

        # Type
        self.assertIsInstance(item.platform_id, int)
        self.assertIsInstance(item.symbol, str)
        if is_with_timestamp:
            self.assertTrue(isinstance(item.timestamp, (float, int)))
        if is_with_item_id:
            self.assertIsInstance(item.item_id, str)

        # Value
        self.assertEqual(item.platform_id, platform_id)
        if is_with_timestamp:
            self.assertGreater(item.timestamp, 1000000000)
            if item.is_milliseconds:
                self.assertGreater(item.timestamp, 10000000000)
        if testing_symbol_or_symbols:
            self.assertEqual(item.symbol, item.symbol.upper())
            if isinstance(testing_symbol_or_symbols, str):
                self.assertEqual(item.symbol, testing_symbol_or_symbols)
            else:
                self.assertIn(item.symbol, testing_symbol_or_symbols)
        if is_with_item_id:
            self.assertGreater(len(str(item.item_id)), 0)

    def assertTradeIsValid(self, trade, testing_symbol_or_symbols=None, platform_id=None, is_dict=False):
        if is_dict and trade:
            trade = Trade(**trade)

        AssertUtil.assertItemIsValid(self, trade, testing_symbol_or_symbols, platform_id, True)

        self.assertIsInstance(trade, Trade)

        # Not empty
        self.assertIsNotNone(trade.price)
        self.assertIsNotNone(trade.amount)
        # self.assertIsNotNone(trade.direction)

        # Type
        self.assertIsInstance(trade.price, str)
        self.assertIsInstance(trade.amount, str)
        if trade.direction is not None:
            self.assertIsInstance(trade.direction, int)

        # Value
        self.assertGreater(float(trade.price), 0)
        self.assertGreater(float(trade.amount), 0)
        if trade.direction is not None:
            self.assertIn(float(trade.direction), Direction.name_by_value)

    def assertMyTradeIsValid(self, my_trade, testing_symbol_or_symbols=None, platform_id=None, is_dict=False):
        if is_dict and my_trade:
            my_trade = MyTrade(**my_trade)

        AssertUtil.assertTradeIsValid(self, my_trade, testing_symbol_or_symbols, platform_id, True)

        self.assertIsInstance(my_trade, MyTrade)

        # Not empty
        self.assertIsNotNone(my_trade.order_id)
        # self.assertIsNotNone(my_trade.fee)
        # self.assertIsNotNone(my_trade.rebate)

        # Type
        self.assertIsInstance(my_trade.order_id, str)
        if my_trade.fee is not None:
            self.assertIsInstance(my_trade.fee, str)
        if my_trade.rebate is not None:
            self.assertIsInstance(my_trade.rebate, str)

        # Value
        if my_trade.fee is not None:
            self.assertGreater(float(my_trade.fee), 0)
        if my_trade.rebate is not None:
            self.assertGreater(float(my_trade.rebate), 0)

    def assertCandleIsValid(self, candle, testing_symbol_or_symbols=None, platform_id=None, is_dict=False):
        if is_dict and candle:
            candle = Candle(**candle)

        AssertUtil.assertItemIsValid(self, candle, testing_symbol_or_symbols, platform_id, False)

        self.assertIsInstance(candle, Candle)

        # Not empty
        self.assertIsNotNone(candle.interval)
        self.assertIsNotNone(candle.price_open)
        self.assertIsNotNone(candle.price_close)
        self.assertIsNotNone(candle.price_high)
        self.assertIsNotNone(candle.price_low)
        # Optional
        # self.assertIsNotNone(candle.amount)
        # self.assertIsNotNone(candle.trades_count)

        # Type
        self.assertIsInstance(candle.interval, str)
        self.assertIsInstance(candle.price_open, str)
        self.assertIsInstance(candle.price_close, str)
        self.assertIsInstance(candle.price_high, str)
        self.assertIsInstance(candle.price_low, str)
        if candle.amount is not None:
            self.assertIsInstance(candle.amount, str)
        if candle.trades_count is not None:
            self.assertIsInstance(candle.trades_count, int)

        # Value
        self.assertIn(candle.interval, Interval.ALL)
        self.assertGreater(float(candle.price_open), 0)
        self.assertGreater(float(candle.price_close), 0)
        self.assertGreater(float(candle.price_high), 0)
        self.assertGreater(float(candle.price_low), 0)
        if candle.amount is not None:
            self.assertGreater(float(candle.amount), 0)
        if candle.trades_count is not None:
            self.assertGreater(candle.trades_count, 0)

    def assertTickerIsValid(self, ticker, testing_symbol_or_symbols=None, platform_id=None, is_dict=False):
        if is_dict and ticker:
            ticker = Ticker(**ticker)

        AssertUtil.assertItemIsValid(self, ticker, testing_symbol_or_symbols, platform_id, False, False)

        self.assertIsInstance(ticker, Ticker)

        # Not empty
        self.assertIsNotNone(ticker.price)

        # Type
        self.assertIsInstance(ticker.price, str)

        # Value
        self.assertGreater(float(ticker.price), 0)

    def assertOrderBookIsValid(self, order_book, testing_symbol_or_symbols=None, platform_id=None, is_dict=False,
                               is_diff=False):
        if is_dict and order_book:
            order_book = OrderBook(**order_book)

        # Assert order book
        AssertUtil.assertItemIsValid(self, order_book, testing_symbol_or_symbols, platform_id, False, False)

        self.assertIsInstance(order_book, OrderBook)
        self.assertIsNotNone(order_book.asks)
        self.assertIsNotNone(order_book.bids)
        # if is_diff:
        self.assertGreaterEqual(len(order_book.asks), 0)
        self.assertGreaterEqual(len(order_book.bids), 0)
        # For order book diff
        self.assertGreater(len(order_book.asks + order_book.bids), 0)
        # else:
        #     self.assertGreater(len(order_book.asks), 0)
        #     self.assertGreater(len(order_book.bids), 0)

        # Assert order book items
        for item in order_book.asks:
            AssertUtil.assertOrderBookItemIsValid(self, item)
        for item in order_book.bids:
            AssertUtil.assertOrderBookItemIsValid(self, item)

    def assertOrderBookDiffIsValid(self, order_book, testing_symbol_or_symbols=None, platform_id=None, is_dict=False):
        AssertUtil.assertOrderBookIsValid(self, order_book, testing_symbol_or_symbols, platform_id, is_dict, is_diff=True)

    def assertOrderBookItemIsValid(self, order_book_item, testing_symbol_or_symbols=None, platform_id=None, is_dict=False):
        if is_dict and order_book_item:
            order_book_item = OrderBookItem(**order_book_item)

        # AssertUtil.assertItemIsValid(self, order_book_item, testing_symbol_or_symbols, platform_id, False)

        self.assertIsInstance(order_book_item, OrderBookItem)

        # Not empty
        self.assertIsNotNone(order_book_item.price)
        self.assertIsNotNone(order_book_item.amount)
        # self.assertIsNotNone(order_book_item.direction)
        # self.assertIsNotNone(order_book_item.order_count)

        # Type
        self.assertIsInstance(order_book_item.price, str)
        self.assertIsInstance(order_book_item.amount, str)
        if order_book_item.direction is not None:
            self.assertIsInstance(order_book_item.direction, int)
        if order_book_item.order_count is not None:
            self.assertIsInstance(order_book_item.order_count, int)

        # Value
        self.assertGreater(float(order_book_item.price), 0)
        self.assertGreaterEqual(float(order_book_item.amount), 0)
        if order_book_item.direction is not None:
            self.assertIn(order_book_item.direction, OrderBookDirection.name_by_value)
        if order_book_item.order_count is not None:
            self.assertGreater(order_book_item.order_count, 0)

    def assertAccountIsValid(self, account, platform_id=None, is_dict=False):
        if is_dict and account:
            account = Account(**account)

        self.assertIsInstance(account, Account)

        # Not empty
        self.assertIsNotNone(account.platform_id)
        self.assertIsNotNone(account.timestamp)
        self.assertIsNotNone(account.balances)

        # Type
        self.assertIsInstance(account.platform_id, int)
        self.assertIsInstance(account.timestamp, (int, float))
        self.assertIsInstance(account.balances, list)

        # Value
        self.assertEqual(account.platform_id, platform_id)
        self.assertGreater(account.timestamp, 1000000000)
        if account.is_milliseconds:
            self.assertGreater(account.timestamp, 10000000000)
        self.assertGreaterEqual(len(account.balances), 0)
        for balance in account.balances:
            AssertUtil.assertBalanceIsValid(self, balance, platform_id)
        # for debug
        balances_with_money = [balance for balance in account.balances if float(balance.amount_available) or float(balance.amount_reserved)]
        pass

    def assertBalanceIsValid(self, balance, platform_id=None, is_dict=False):
        if is_dict and balance:
            order = Balance(**balance)

        self.assertIsInstance(balance, Balance)

        # Not empty
        self.assertIsNotNone(balance.platform_id)
        self.assertIsNotNone(balance.symbol)
        self.assertIsNotNone(balance.amount_available)
        self.assertIsNotNone(balance.amount_reserved)

        # Type
        self.assertIsInstance(balance.platform_id, int)
        self.assertIsInstance(balance.symbol, str)
        self.assertIsInstance(balance.amount_available, str)
        self.assertIsInstance(balance.amount_reserved, str)

        # Value
        self.assertEqual(balance.platform_id, platform_id)
        self.assertEqual(balance.symbol, balance.symbol.upper())
        self.assertGreaterEqual(float(balance.amount_available), 0)
        self.assertGreaterEqual(float(balance.amount_reserved), 0)

    def assertOrderIsValid(self, order, testing_symbol_or_symbols=None, platform_id=None, is_dict=False):
        if is_dict and order:
            order = Order(**order)

        AssertUtil.assertItemIsValid(self, order, testing_symbol_or_symbols, platform_id, True)

        self.assertIsInstance(order, Order)

        # Not empty
        self.assertIsNotNone(order.user_order_id)
        self.assertIsNotNone(order.order_type)
        self.assertIsNotNone(order.price)
        self.assertIsNotNone(order.amount_original)
        self.assertIsNotNone(order.amount_executed)
        self.assertIsNotNone(order.direction)
        self.assertIsNotNone(order.order_status)

        # Type
        self.assertIsInstance(order.user_order_id, str)
        self.assertIsInstance(order.order_type, int)
        self.assertIsInstance(order.price, str)
        self.assertIsInstance(order.amount_original, str)
        self.assertIsInstance(order.amount_executed, str)
        self.assertIsInstance(order.direction, int)
        self.assertIsInstance(order.order_status, int)

        # Value
        self.assertIn(float(order.order_type), OrderType.name_by_value)
        self.assertGreater(float(order.price), 0)
        self.assertGreater(float(order.amount_original), 0)
        self.assertGreater(float(order.amount_executed), 0)
        self.assertIn(order.direction, Direction.name_by_value)
        self.assertIn(order.order_status, OrderStatus.name_by_value)
