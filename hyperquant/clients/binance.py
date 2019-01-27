import hashlib
import hmac
from operator import itemgetter

from hyperquant.api import Platform, Sorting, Interval, Direction, OrderType
from hyperquant.clients import WSClient, Endpoint, Trade, Error, ErrorCode, \
    ParamName, WSConverter, RESTConverter, PrivatePlatformRESTClient, MyTrade, Candle, Ticker, OrderBookItem, Order, \
    OrderBook, Account, Balance


# REST

# TODO check getting trades history from_id=1
class BinanceRESTConverterV1(RESTConverter):
    # Main params:
    base_url = "https://api.binance.com/api/v{version}/"

    # Settings:

    # Converting info:
    # For converting to platform
    endpoint_lookup = {
        Endpoint.PING: "ping",
        Endpoint.SERVER_TIME: "time",
        Endpoint.SYMBOLS: "exchangeInfo",
        Endpoint.TRADE: "trades",
        Endpoint.TRADE_HISTORY: "historicalTrades",
        Endpoint.TRADE_MY: "myTrades",  # Private
        Endpoint.CANDLE: "klines",
        Endpoint.TICKER: "ticker/price",
        Endpoint.ORDER_BOOK: "depth",
        # Private
        Endpoint.ACCOUNT: "account",
        Endpoint.ORDER: "order",
        Endpoint.ORDER_CURRENT: "openOrders",
        Endpoint.ORDER_MY: "allOrders",
    }
    param_name_lookup = {
        ParamName.SYMBOL: "symbol",
        ParamName.LIMIT: "limit",
        ParamName.IS_USE_MAX_LIMIT: None,
        # ParamName.SORTING: None,
        ParamName.INTERVAL: "interval",
        ParamName.DIRECTION: "side",
        ParamName.ORDER_TYPE: "type",

        ParamName.TIMESTAMP: "timestamp",
        ParamName.FROM_ITEM: "fromId",
        ParamName.TO_ITEM: None,
        ParamName.FROM_TIME: "startTime",
        ParamName.TO_TIME: "endTime",

        ParamName.PRICE: "price",
        ParamName.AMOUNT: "quantity",
        # -ParamName.ASKS: "asks",
        # ParamName.BIDS: "bids",
    }
    param_value_lookup = {
        # Sorting.ASCENDING: None,
        # Sorting.DESCENDING: None,
        Sorting.DEFAULT_SORTING: Sorting.ASCENDING,

        Interval.MIN_1: "1m",
        Interval.MIN_3: "3m",
        Interval.MIN_5: "5m",
        Interval.MIN_15: "15m",
        Interval.MIN_30: "30m",
        Interval.HRS_1: "1h",
        Interval.HRS_2: "2h",
        Interval.HRS_4: "4h",
        Interval.HRS_6: "6h",
        Interval.HRS_8: "8h",
        Interval.HRS_12: "12h",
        Interval.DAY_1: "1d",
        Interval.DAY_3: "3d",
        Interval.WEEK_1: "1w",
        Interval.MONTH_1: "1M",

        # By properties:
        ParamName.DIRECTION: {
            Direction.SELL: "SELL",
            Direction.BUY: "BUY",
        },
        ParamName.ORDER_TYPE: {
            OrderType.LIMIT: "LIMIT",
            OrderType.MARKET: "MARKET",
        },
        # ParamName.ORDER_STATUS: {
        #     OrderStatus.: "",
        # },
    }
    max_limit_by_endpoint = {
        Endpoint.TRADE: 1000,
        Endpoint.TRADE_HISTORY: 1000,
        Endpoint.ORDER_BOOK: 1000,
        Endpoint.CANDLE: 1000,
    }

    # For parsing

    param_lookup_by_class = {
        # Error
        Error: {
            "code": "code",
            "msg": "message",
        },
        # Data
        Trade: {
            "time": ParamName.TIMESTAMP,
            "id": ParamName.ITEM_ID,
            "price": ParamName.PRICE,
            "qty": ParamName.AMOUNT,
            # "isBuyerMaker": "",
            # "isBestMatch": "",
        },
        MyTrade: {
            "symbol": ParamName.SYMBOL,
            "time": ParamName.TIMESTAMP,
            "id": ParamName.ITEM_ID,
            "price": ParamName.PRICE,
            "qty": ParamName.AMOUNT,

            "orderId": ParamName.ORDER_ID,
            "commission": ParamName.FEE,
            # "commissionAsset": ParamName.FEE_SYMBOL,
            # "": ParamName.REBATE,
        },
        Candle: [
            ParamName.TIMESTAMP,
            ParamName.PRICE_OPEN,
            ParamName.PRICE_HIGH,
            ParamName.PRICE_LOW,
            ParamName.PRICE_CLOSE,
            None,  # ParamName.AMOUNT,  # only volume present
            None,
            None,
            ParamName.TRADES_COUNT,
            # ParamName.INTERVAL,
        ],
        Ticker: {
            "symbol": ParamName.SYMBOL,
            "price": ParamName.PRICE,
        },
        Account: {
            "updateTime": ParamName.TIMESTAMP,
            "balances": ParamName.BALANCES,
        },
        Balance: {
            "asset": ParamName.SYMBOL,
            "free": ParamName.AMOUNT_AVAILABLE,
            "locked": ParamName.AMOUNT_RESERVED,
        },
        Order: {
            "symbol": ParamName.SYMBOL,
            "transactTime": ParamName.TIMESTAMP,
            "time": ParamName.TIMESTAMP,  # check "time" or "updateTime"
            "updateTime": ParamName.TIMESTAMP,
            "orderId": ParamName.ITEM_ID,
            "clientOrderId": ParamName.USER_ORDER_ID,

            "type": ParamName.ORDER_TYPE,
            "price": ParamName.PRICE,
            "origQty": ParamName.AMOUNT_ORIGINAL,
            "executedQty": ParamName.AMOUNT_EXECUTED,
            "side": ParamName.DIRECTION,
            "status": ParamName.ORDER_STATUS,
        },
        OrderBook: {
            "lastUpdateId": ParamName.ITEM_ID,
            "bids": ParamName.BIDS,
            "asks": ParamName.ASKS,
        },
        OrderBookItem: [ParamName.PRICE, ParamName.AMOUNT],
    }

    error_code_by_platform_error_code = {
        -2014: ErrorCode.UNAUTHORIZED,
        -1121: ErrorCode.WRONG_SYMBOL,
        -1100: ErrorCode.WRONG_PARAM,
    }
    error_code_by_http_status = {
        429: ErrorCode.RATE_LIMIT,
        418: ErrorCode.IP_BAN,
    }

    # For converting time
    is_source_in_milliseconds = True

    # timestamp_platform_names = [ParamName.TIMESTAMP]

    def _process_param_value(self, name, value):
        if name == ParamName.FROM_ITEM or name == ParamName.TO_ITEM:
            if isinstance(value, Trade):  # ItemObject):
                return value.item_id
        return super()._process_param_value(name, value)

    def parse(self, endpoint, data):
        if endpoint == Endpoint.SERVER_TIME and data:
            timestamp_ms = data.get("serverTime")
            return timestamp_ms / 1000 if not self.use_milliseconds and timestamp_ms else timestamp_ms
        if endpoint == Endpoint.SYMBOLS and data and ParamName.SYMBOLS in data:
            exchange_info = data[ParamName.SYMBOLS]
            # (There are only 2 statuses: "TRADING" and "BREAK")
            # symbols = [item[ParamName.SYMBOL] for item in exchange_info if item["status"] == "TRADING"]
            symbols = [item[ParamName.SYMBOL] for item in exchange_info]
            return symbols

        result = super().parse(endpoint, data)
        return result

    # def preprocess_params(self, endpoint, params):
    #     if endpoint in self.secured_endpoints:
    #         params[ParamName.TIMESTAMP] = int(time.time() * 1000)
    #
    #     return super().preprocess_params(endpoint, params)

    def _generate_and_add_signature(self, platform_params, api_key, api_secret):
        if not api_key or not api_secret:
            self.logger.error("Empty api_key or api_secret. Cannot generate signature.")
            return None
        ordered_params_list = self._order_params(platform_params)
        # print("ordered_platform_params:", ordered_params_list)
        query_string = "&".join(["{}={}".format(d[0], d[1]) for d in ordered_params_list])
        # print("query_string:", query_string)
        m = hmac.new(api_secret.encode("utf-8"), query_string.encode("utf-8"), hashlib.sha256)
        signature = m.hexdigest()
        # Add
        # platform_params["signature"] = signature  # no need
        # if ordered_params_list and ordered_params_list[-1][0] != "signature":
        ordered_params_list.append(("signature", signature))
        return ordered_params_list

    def _order_params(self, platform_params):
        # Convert params to sorted list with signature as last element.

        params_list = [(key, value) for key, value in platform_params.items() if key != "signature"]
        # Sort parameters by key
        params_list.sort(key=itemgetter(0))
        # Append signature to the end if present
        if "signature" in platform_params:
            params_list.append(("signature", platform_params["signature"]))
        return params_list


class BinanceRESTClient(PrivatePlatformRESTClient):
    # Settings:
    platform_id = Platform.BINANCE
    version = "1"  # Default version

    _converter_class_by_version = {
        "1": BinanceRESTConverterV1,
        "3": BinanceRESTConverterV1,  # Only for some methods (same converter used)
    }

    # State:
    ratelimit_error_in_row_count = 0

    @property
    def headers(self):
        result = super().headers
        result["X-MBX-APIKEY"] = self._api_key
        result["Content-Type"] = "application/x-www-form-urlencoded"
        return result

    def _on_response(self, response, result):
        # super()._on_response(response, result)

        self.delay_before_next_request_sec = 0
        if isinstance(result, Error):
            if result.code == ErrorCode.RATE_LIMIT:
                self.ratelimit_error_in_row_count += 1
                self.delay_before_next_request_sec = 60 * 2 * self.ratelimit_error_in_row_count  # some number - change
            elif result.code == ErrorCode.IP_BAN:
                self.ratelimit_error_in_row_count += 1
                self.delay_before_next_request_sec = 60 * 5 * self.ratelimit_error_in_row_count  # some number - change
            else:
                self.ratelimit_error_in_row_count = 0
        else:
            self.ratelimit_error_in_row_count = 0

    def fetch_history(self, endpoint, symbol, limit=None, from_item=None, to_item=None, sorting=None,
                      is_use_max_limit=False, from_time=None, to_time=None,
                      version=None, **kwargs):
        if from_item is None:
            from_item = 0
        return super().fetch_history(endpoint, symbol, limit, from_item, to_item, sorting, is_use_max_limit, from_time,
                                     to_time, **kwargs)

    def fetch_order_book(self, symbol=None, limit=None, is_use_max_limit=False, version=None, **kwargs):
        LIMIT_VALUES = [5, 10, 20, 50, 100, 500, 1000]
        if limit not in LIMIT_VALUES:
            self.logger.error("Limit value %s not in %s", limit, LIMIT_VALUES)
        return super().fetch_order_book(symbol, limit, is_use_max_limit, **kwargs)

    def fetch_tickers(self, symbols=None, version=None, **kwargs):
        items = super().fetch_tickers(symbols, version or "3", **kwargs)

        # (Binance returns timestamp only for /api/v1/ticker/24hr which has weight of 40.
        # /api/v3/ticker/price - has weight 2.)
        timestamp = self.get_server_timestamp(version)
        for item in items:
            item.timestamp = timestamp
            item.use_milliseconds = self.use_milliseconds

        return items

    def fetch_account_info(self, version=None, **kwargs):
        return super().fetch_account_info(version or "3", **kwargs)

    def create_order(self, symbol, order_type, direction, price=None, amount=None, is_test=False, version=None,
                     **kwargs):
        if order_type == OrderType.LIMIT:
            # (About values:
            # https://www.reddit.com/r/BinanceExchange/comments/8odvs4/question_about_time_in_force_binance_api/)
            kwargs["timeInForce"] = "GTC"
        return super().create_order(symbol, order_type, direction, price, amount, is_test, version, **kwargs)

    def cancel_order(self, order, symbol=None, version=None, **kwargs):
        if hasattr(order, ParamName.SYMBOL) and order.symbol:
            symbol = order.symbol
        return super().cancel_order(order, symbol, version, **kwargs)

    def check_order(self, order, symbol=None, version=None, **kwargs):
        if hasattr(order, ParamName.SYMBOL) and order.symbol:
            symbol = order.symbol
        return super().check_order(order, symbol, version, **kwargs)

    # def fetch_orders(self, symbol=None, limit=None, from_item=None, is_open=False, version=None, **kwargs):
    #     return super().fetch_orders(symbol, limit, from_item, is_open, version, **kwargs)

    def _send(self, method, endpoint, params=None, version=None, **kwargs):
        if endpoint in self.converter.secured_endpoints:
            server_timestamp = self.get_server_timestamp()
            params[ParamName.TIMESTAMP] = server_timestamp if self.use_milliseconds else int(server_timestamp * 1000)
        return super()._send(method, endpoint, params, version, **kwargs)


# WebSocket

class BinanceWSConverterV1(WSConverter):
    # Main params:
    base_url = "wss://stream.binance.com:9443/"

    IS_SUBSCRIPTION_COMMAND_SUPPORTED = False

    # supported_endpoints = [Endpoint.TRADE]
    # symbol_endpoints = [Endpoint.TRADE]
    # supported_symbols = None

    # Settings:

    # Converting info:
    # For converting to platform

    endpoint_lookup = {
        Endpoint.TRADE: "{symbol}@trade",
        Endpoint.CANDLE: "{symbol}@kline_{interval}",
        Endpoint.TICKER: "{symbol}@miniTicker",
        Endpoint.TICKER_ALL: "!miniTicker@arr",
        Endpoint.ORDER_BOOK: "{symbol}@depth{level}",
        Endpoint.ORDER_BOOK_DIFF: "{symbol}@depth",
    }

    # For parsing
    param_lookup_by_class = {
        # Error
        Error: {
            # "code": "code",
            # "msg": "message",
        },
        # Data
        Trade: {
            "s": ParamName.SYMBOL,
            "T": ParamName.TIMESTAMP,
            "t": ParamName.ITEM_ID,
            "p": ParamName.PRICE,
            "q": ParamName.AMOUNT,
            # "m": "",
        },
        Candle: {
            "s": ParamName.SYMBOL,
            "t": ParamName.TIMESTAMP,
            "i": ParamName.INTERVAL,

            "o": ParamName.PRICE_OPEN,
            "c": ParamName.PRICE_CLOSE,
            "h": ParamName.PRICE_HIGH,
            "l": ParamName.PRICE_LOW,
            "": ParamName.AMOUNT,  # only volume present
            "n": ParamName.TRADES_COUNT,
        },
        Ticker: {
            "s": ParamName.SYMBOL,
            "E": ParamName.TIMESTAMP,
            "c": ParamName.PRICE,  # todo check to know for sure
        },
        OrderBook: {
            # Partial Book Depth Streams
            "lastUpdateId": ParamName.ITEM_ID,
            "asks": ParamName.ASKS,
            "bids": ParamName.BIDS,
            # Diff. Depth Stream
            "s": ParamName.SYMBOL,
            "E": ParamName.TIMESTAMP,
            "u": ParamName.ITEM_ID,
            "b": ParamName.BIDS,
            "a": ParamName.ASKS,
        },
        OrderBookItem: [ParamName.PRICE, ParamName.AMOUNT],
    }
    event_type_param = "e"
    endpoint_by_event_type = {
        "trade": Endpoint.TRADE,
        "kline": Endpoint.CANDLE,
        "24hrMiniTicker": Endpoint.TICKER,
        "24hrTicker": Endpoint.TICKER,
        "depthUpdate": Endpoint.ORDER_BOOK,
        # "depthUpdate": Endpoint.ORDER_BOOK_DIFF,
    }

    # https://github.com/binance-exchange/binance-official-api-docs/blob/master/errors.md
    error_code_by_platform_error_code = {
        # -2014: ErrorCode.UNAUTHORIZED,
        # -1121: ErrorCode.WRONG_SYMBOL,
        # -1100: ErrorCode.WRONG_PARAM,
    }
    error_code_by_http_status = {}

    # For converting time
    is_source_in_milliseconds = True

    def _generate_subscription(self, endpoint, symbol=None, **params):
        return super()._generate_subscription(endpoint, symbol.lower() if symbol else symbol, **params)

    def parse(self, endpoint, data):
        if "data" in data:
            # stream = data["stream"]  # no need
            data = data["data"]
        return super().parse(endpoint, data)

    def _parse_item(self, endpoint, item_data):
        if endpoint == Endpoint.CANDLE and "k" in item_data:
            item_data = item_data["k"]
        return super()._parse_item(endpoint, item_data)


class BinanceWSClient(WSClient):
    platform_id = Platform.BINANCE
    version = "1"  # Default version

    _converter_class_by_version = {
        "1": BinanceWSConverterV1,
    }

    @property
    def url(self):
        # Generate subscriptions
        if not self.current_subscriptions:
            self.logger.warning("Making URL while current_subscriptions are empty. "
                                "There is no sense to connect without subscriptions.")
            subscriptions = ""
            # # There is no sense to connect without subscriptions
            # return None
        elif len(self.current_subscriptions) > 1:
            subscriptions = "stream?streams=" + "/".join(self.current_subscriptions)
        else:
            subscriptions = "ws/" + "".join(self.current_subscriptions)

        self.is_subscribed_with_url = True
        return super().url + subscriptions

    def subscribe(self, endpoints=None, symbols=None, **params):
        self._check_params(endpoints, symbols, **params)

        super().subscribe(endpoints, symbols, **params)

    def unsubscribe(self, endpoints=None, symbols=None, **params):
        self._check_params(endpoints, symbols, **params)

        super().unsubscribe(endpoints, symbols, **params)

    def _check_params(self, endpoints=None, symbols=None, **params):
        LEVELS_AVAILABLE = [5, 10, 20]
        if endpoints and Endpoint.ORDER_BOOK in endpoints and ParamName.LEVEL in params and \
                params.get(ParamName.LEVEL) not in LEVELS_AVAILABLE:
            self.logger.error("For %s endpoint %s param must be of values: %s, but set: %s",
                              Endpoint.ORDER_BOOK, ParamName.LEVEL, LEVELS_AVAILABLE,
                              params.get(ParamName.LEVEL))
