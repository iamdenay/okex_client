from collections import Iterable
from decimal import Decimal

from clickhouse_driver.errors import ServerException
from dateutil import parser
from django.http import JsonResponse

"""
Common out API format is defined here.

When we calling any other platform API like Binance or Bitfinex we convert 
all response data to this format. 
When anyone calling our REST API this format is used too.
"""


# Trading platforms, REST API, and DB:

# Constants

class Platform:
    BINANCE = 1
    BITFINEX = 2
    BITMEX = 3
    OKEX = 4

    name_by_id = {
        1: "BINANCE",
        2: "BITFINEX",
        3: "BITMEX",
        4: "OKEX"
    }
    id_by_name = {v: k for k, v in name_by_id.items()}

    @classmethod
    def get_platform_name_by_id(cls, platform_id):
        return cls.name_by_id.get(platform_id)

    @classmethod
    def get_platform_id_by_name(cls, platform, is_check_valid_id=False):
        # platform - name or id, all other values will be converted to None
        if isinstance(platform, str) and platform.isnumeric():
            platform = int(platform)
        return cls.id_by_name.get(str(platform).upper(),
                                  platform if not is_check_valid_id or platform in cls.name_by_id else None)


class Endpoint:
    # Note: you can use any value, but remember they will be used in all our APIs,
    # and they must differ from each other

    # ALL = "*"  # Used by WS Client

    # For all platforms and our REST API (except *_HISTORY)
    PING = "ping"
    SERVER_TIME = "time"
    SYMBOLS = "symbols"
    TRADE = "trade"
    TRADE_HISTORY = "trade/history"
    TRADE_MY = "trade/my"  # Private
    CANDLE = "candle"
    # CANDLE_HISTORY = "candle/history"
    TICKER = "ticker"
    TICKER_ALL = "ticker_all"
    # TICKER_HISTORY = "ticker/history"
    ORDER_BOOK = "orderbook"
    # ORDER_BOOK_HISTORY = "orderbook/history"
    ORDER_BOOK_DIFF = "orderbook"  # WS

    # Private
    ACCOUNT = "account"
    ORDER = "order"
    ORDER_TEST = "order/test"
    ORDER_CURRENT = "order/current"
    ORDER_MY = "order/my"
    # ORDER_HISTORY = "order/history"

    # For our REST API only
    ITEM = ""
    HISTORY = "/history"
    FORMAT = "/format"

    ALL = [SERVER_TIME, SYMBOLS, TRADE, TRADE_HISTORY, TRADE_MY, CANDLE, TICKER, TICKER_ALL,
           ORDER_BOOK, ORDER_BOOK_DIFF, ACCOUNT, ORDER, ORDER_TEST, ORDER_CURRENT, ORDER_MY,
           ITEM, HISTORY, FORMAT]


class ParamName:
    # Stores names which are used:
    # 1. in params of client.send() method;
    # 2. in value object classes!;
    # 3. field names in DB;
    # 4. in our REST APIs.

    ID = "id"
    ITEM_ID = "item_id"
    TRADE_ID = "trade_id"
    ORDER_ID = "order_id"
    USER_ORDER_ID = "user_order_id"

    SYMBOL = "symbol"
    SYMBOLS = "symbols"  # For our REST API only
    LIMIT = "limit"
    IS_USE_MAX_LIMIT = "is_use_max_limit"  # used in clients only
    LIMIT_SKIP = "limit_skip"
    PAGE = "page"  # instead of LIMIT_SKIP
    SORTING = "sorting"
    INTERVAL = "interval"
    DIRECTION = "direction"  # Sell/buy or ask/bid
    ORDER_TYPE = "order_type"
    ORDER_STATUS = "order_status"
    LEVEL = "level"  # For order book (WS)
    TRADES_COUNT = "trades_count"

    TIMESTAMP = "timestamp"
    FROM_ITEM = "from_item"
    TO_ITEM = "to_item"
    FROM_TIME = "from_time"
    TO_TIME = "to_time"
    FROM_PRICE = "from_price"
    TO_PRICE = "to_price"
    FROM_AMOUNT = "from_amount"
    TO_AMOUNT = "to_amount"

    PRICE_OPEN = "price_open"
    PRICE_CLOSE = "price_close"
    PRICE_HIGH = "price_high"
    PRICE_LOW = "price_low"
    PRICE = "price"
    AMOUNT_ORIGINAL = "amount_original"
    AMOUNT_EXECUTED = "amount_executed"
    AMOUNT_AVAILABLE = "amount_available"
    AMOUNT_RESERVED = "amount_reserved"
    AMOUNT = "amount"
    FEE = "fee"
    REBATE = "rebate"
    BALANCES = "balances"
    ASKS = "asks"
    BIDS = "bids"

    # For our REST API only
    PLATFORM_ID = "platform_id"
    PLATFORM = "platform"  # (alternative)
    PLATFORMS = "platforms"  # (alternative)
    # ENDPOINT = "endpoint"

    IS_SHORT = "is_short"

    ALL = [ID, ITEM_ID, TRADE_ID, ORDER_ID, USER_ORDER_ID,
           LIMIT, IS_USE_MAX_LIMIT, LIMIT_SKIP, PAGE, SORTING,
           SYMBOL, SYMBOLS, DIRECTION, INTERVAL, ORDER_TYPE, LEVEL,
           TIMESTAMP, FROM_ITEM, TO_ITEM, FROM_TIME, TO_TIME,
           FROM_PRICE, TO_PRICE, FROM_AMOUNT, TO_AMOUNT,
           PRICE_OPEN, PRICE_CLOSE, PRICE_HIGH, PRICE_LOW, PRICE,
           AMOUNT_ORIGINAL, AMOUNT_EXECUTED, AMOUNT, FEE, REBATE, BIDS, ASKS,
           PLATFORM_ID, PLATFORM, PLATFORMS, IS_SHORT]

    _timestamp_names = (TIMESTAMP, FROM_TIME, TO_TIME)
    _decimal_names = (PRICE, FROM_PRICE, TO_PRICE, AMOUNT, FROM_AMOUNT, TO_AMOUNT)

    @classmethod
    def is_timestamp(cls, name):
        return name in cls._timestamp_names

    @classmethod
    def is_decimal(cls, name):
        return name in cls._decimal_names


class ParamValue:
    # todo remove sometimes
    # param_names = [ParamName.SORTING]

    # For limit
    MIN = "min"
    MAX = "max"

    ALL = "all"
    UNDEFINED = None


class Sorting:
    ASCENDING = "asc"  # Oldest first
    DESCENDING = "desc"  # Newest first, usually default
    DEFAULT_SORTING = "default_sorting"  # (For internal uses only)


class Interval:
    # For candles

    MIN_1 = "1m"
    MIN_3 = "3m"
    MIN_5 = "5m"
    MIN_15 = "15m"
    MIN_30 = "30m"
    HRS_1 = "1h"
    HRS_2 = "2h"
    HRS_4 = "4h"
    HRS_6 = "6h"
    HRS_8 = "8h"
    HRS_12 = "12h"
    DAY_1 = "1d"
    DAY_3 = "3d"
    WEEK_1 = "1w"
    MONTH_1 = "1M"

    ALL = [MIN_1, MIN_3, MIN_5, MIN_15, MIN_30,
           HRS_1, HRS_2, HRS_4, HRS_6, HRS_8, HRS_12,
           DAY_1, DAY_3, WEEK_1, MONTH_1]


class Direction:
    # (trade, order)

    SELL = 1
    BUY = 2
    # (for our REST API as alternative values)
    SELL_NAME = "sell"
    BUY_NAME = "buy"

    name_by_value = {
        SELL: SELL_NAME,
        BUY: BUY_NAME,
    }
    value_by_name = {v: k for k, v in name_by_value.items()}

    @classmethod
    def get_direction_value(cls, direction, is_check_valid_id=True):
        return cls.value_by_name.get(str(direction).upper(),
                                     direction if not is_check_valid_id or direction in cls.name_by_value else None)


class OrderBookDirection:
    # Direction for order book (same as sell/buy but with different names)
    ASK = 1  # Same as sell
    BID = 2  # Same as buy
    # (for our REST API as alternative values)
    ASK_NAME = "ask"
    BID_NAME = "bid"

    name_by_value = {
        ASK: ASK_NAME,
        BID: BID_NAME,
    }
    value_by_name = {v: k for k, v in name_by_value.items()}


class OrderType:
    LIMIT = 1
    MARKET = 2
    # (for our REST API)
    LIMIT_NAME = "limit"
    MARKET_NAME = "market"

    name_by_value = {
        LIMIT: LIMIT_NAME,
        MARKET: MARKET_NAME,
    }
    value_by_name = {v: k for k, v in name_by_value.items()}


class OrderStatus:
    OPEN = 1
    CLOSED = 0

    NEW = 2
    PARTIALLY_FILLED = 3
    FILLED = 4
    # PENDING_CANCEL = 5
    CANCELED = 6
    REJECTED = 7
    EXPIRED = 8

    # (for our REST API)
    OPEN_NAME = "open"
    CLOSED_NAME = "closed"

    NEW_NAME = "new"
    PARTIALLY_FILLED_NAME = "partially_filled"
    FILLED_NAME = "filled"
    # PENDING_CANCEL_NAME = "pending_cancel"
    CANCELED_NAME = "canceled"
    REJECTED_NAME = "rejected"
    EXPIRED_NAME = "expired"

    name_by_value = {
        OPEN: OPEN_NAME,
        CLOSED: CLOSED_NAME,

        NEW: NEW_NAME,
        PARTIALLY_FILLED: PARTIALLY_FILLED_NAME,
        FILLED: FILLED_NAME,
        # PENDING_CANCEL: PENDING_CANCEL_NAME,
        CANCELED: CANCELED_NAME,
        REJECTED: REJECTED_NAME,
        EXPIRED: EXPIRED_NAME,
    }
    value_by_name = {v: k for k, v in name_by_value.items()}


class ErrorCode:
    # Provides same error codes and messages for all trading platforms

    # Надо накопить достаточно типов ошибок, систематизировать их и дать им числовые коды,
    # которые будет легко мнемонически запомнить, чтобы поотм легко можно было определить ошибку по ее коду
    UNAUTHORIZED = "any1"
    RATE_LIMIT = "any:ratelim"
    IP_BAN = "any:ipban"
    WRONG_SYMBOL = "any:wrsymbol"
    WRONG_LIMIT = "any:wrlimit"
    WRONG_PARAM = "any:wrparval"
    APP_ERROR = "any:apperr"
    APP_DB_ERROR = "any:appdberr"

    message_by_code = {
        UNAUTHORIZED: "Unauthorized. May be wrong api_key or api_secret or not defined at all.",
        RATE_LIMIT: "Rate limit reached. We must make a delay for a while.",
        WRONG_SYMBOL: "Wrong symbol. May be this symbol is not supported by platform or its name is wrong.",
        WRONG_LIMIT: "Wrong limit. May be too big.",
        WRONG_PARAM: "Wrong param value.",
        APP_ERROR: "App error!",
        APP_DB_ERROR: "App error! It's likely that app made wrong request to DB.",
    }

    @classmethod
    def get_message_by_code(cls, code, default=None, **kwargs):
        return cls.message_by_code[code].format_map(kwargs) if code in cls.message_by_code else default or "(no message: todo)"


# For DB, REST API
item_format_by_endpoint = {
    Endpoint.TRADE: [
        ParamName.PLATFORM_ID, ParamName.SYMBOL, ParamName.TIMESTAMP, ParamName.ITEM_ID,
        ParamName.PRICE, ParamName.AMOUNT, ParamName.DIRECTION
    ],
}


# REST API:

# Parse request

def parse_platform_id(params):
    param_names = [ParamName.PLATFORM, ParamName.PLATFORMS, ParamName.PLATFORM_ID]
    for name in param_names:
        value = params.get(name)
        if value:
            return _convert_platform_id(value)
    return None


def parse_platform_ids(params):
    platforms = params.get(ParamName.PLATFORMS, None) or params.get(ParamName.PLATFORM)
    platforms = platforms.split(",") if isinstance(platforms, str) else platforms
    return [_convert_platform_id(p) for p in platforms] if platforms else None


def _convert_platform_id(platform):
    if platform is None:
        return None
    return int(platform) if platform.isnumeric() else Platform.id_by_name.get(platform.upper())


def parse_symbols(params):
    # None -> None
    # "xxxzzz,yyyZZZ" -> ["XXXZZZ", "YYYZZZ"]
    symbols = params.get(ParamName.SYMBOLS) or params.get(ParamName.SYMBOL)
    if symbols is None:
        return None
    return symbols.upper().split(",") if isinstance(symbols, str) else symbols


def parse_direction(params):
    # None -> None
    # "Sell" -> 1
    # "BUY" -> 2
    direction = params.get(ParamName.DIRECTION)
    if direction is None:
        return None
    direction = int(direction) if direction.isnumeric() else \
        Direction.value_by_name.get(direction.lower())
    return direction if direction in (Direction.SELL, Direction.BUY) else None


def parse_timestamp(params, name):
    # Any time value to Unix timestamp in seconds
    time = params.get(name)
    if time is None:
        return None
    if time.isnumeric():
        return int(time)
    try:
        return float(time)
    except ValueError:
        return parser.parse(time).timestamp()


def parse_decimal(params, name):
    value = params.get(name)
    return Decimal(str(value)) if value is not None else None


def parse_limit(params, DEFAULT_LIMIT, MIN_LIMIT, MAX_LIMIT):
    limit = int(params.get(ParamName.LIMIT, DEFAULT_LIMIT))
    return min(MAX_LIMIT, max(MIN_LIMIT, limit))


def parse_sorting(params, DEFAULT_SORTING):
    sorting = params.get(ParamName.SORTING, DEFAULT_SORTING)
    return sorting
    # sorting = params.get(ParamName.SORTING)
    # # (Any wrong value treated as default)
    # is_descending = sorting == (Sorting.ASCENDING if DEFAULT_SORTING == Sorting.DESCENDING else Sorting.DESCENDING)
    # return Sorting.DESCENDING if is_descending else Sorting.ASCENDING


def sort_from_to_params(from_value, to_value):
    # Swap if from_value > to_value
    return (to_value, from_value) if from_value is not None and to_value is not None \
                                   and from_value > to_value else (from_value, to_value)


# Prepare response

def make_data_response(data, item_format, is_convert_to_list=True):
    result = None
    if data:
        if isinstance(data, Exception):
            return make_error_response(exception=data)

        if not isinstance(data, list) or not isinstance(data[0], list):
            # {"param1": "prop1", "param2": "prop2"} -> [{"param1": "prop1", "param2": "prop2"}]
            # ["prop1", "prop2"] -> [["prop1", "prop2"]]
            data = [data]

        if isinstance(data[0], list):
            # [["prop1", "prop2"], ["prop1", "prop2"]] -> same
            result = data if is_convert_to_list else convert_items_list_to_dict(data, item_format)
        elif isinstance(data[0], dict):
            # [{"param1": "prop1", "param2": "prop2"}] -> [["prop1", "prop2"]]
            result = convert_items_dict_to_list(data, item_format) if is_convert_to_list else data
        # elif isinstance(data[0], DataObject):
        else:
            result = convert_items_obj_to_list(data, item_format) if is_convert_to_list else \
                convert_items_obj_to_dict(data, item_format)

    return JsonResponse({
        "data": result if result else [],
    })


def make_error_response(error_code=None, exception=None, **kwargs):
    if not error_code and exception:
        if isinstance(exception, ServerException):
            error_code = ErrorCode.APP_DB_ERROR
        else:
            error_code = ErrorCode.APP_ERROR

    return JsonResponse({"error": {
        "code": error_code,
        "message": ErrorCode.get_message_by_code(error_code, **kwargs)
    }})


def make_format_response(item_format):
    values = {
        ParamName.PLATFORM_ID: Platform.name_by_id,
        # ParamName.PLATFORM: Platform.name_by_id,
        ParamName.DIRECTION: Direction.name_by_value,
    }
    return JsonResponse({
        "item_format": item_format,
        "values": {k: v for k, v in values.items() if k in item_format},

        "example_item": {"data": [[name + "X" for name in item_format]]},
        # "example_item": {"data": [name + "X" for name in item_format]},  # ?
        "example_history": {"data": [[name + str(i) for name in item_format] for i in range(3)]},
        "example_error": {"error": {"code": 1, "message": "Error description."}},
    })


# Utility:

# Convert items

def convert_items_obj_to_list(item_or_items, item_format):
    if not item_or_items:
        return item_or_items
    return _convert_item_or_items_with_fun(item_or_items, item_format, _convert_items_obj_to_list)


def convert_items_dict_to_list(item_or_items, item_format):
    if not item_or_items:
        return item_or_items
    return _convert_item_or_items_with_fun(item_or_items, item_format, _convert_items_dict_to_list)


def convert_items_list_to_dict(item_or_items, item_format):
    if not item_or_items:
        return item_or_items
    return _convert_item_or_items_with_fun(item_or_items, item_format, _convert_items_list_to_dict)


def convert_items_obj_to_dict(item_or_items, item_format):
    if not item_or_items:
        return item_or_items
    return _convert_item_or_items_with_fun(item_or_items, item_format, _convert_items_obj_to_dict)


def _convert_item_or_items_with_fun(item_or_items, item_format, fun):
    # Input item - output item,
    # input items - output items
    if not item_format:
        raise Exception("item_format cannot be None!")

    is_list = isinstance(item_or_items, (list, tuple))
    if is_list:
        for element in item_or_items:
            if element:
                # Check the first not None element is not an item
                # (list, dict (iterable but not a str) or object (has __dict__))
                if isinstance(element, str) or not isinstance(element, Iterable) and \
                        not hasattr(element, "__dict__"):
                    is_list = False
                break
    items = item_or_items if is_list else [item_or_items]
    # Convert
    result = fun(items, item_format) if items else []
    return result if is_list else result[0]


def _convert_items_obj_to_list(items, item_format):
    return [[getattr(item, p) for p in item_format if hasattr(item, p)] if item is not None else None
            for item in items] if items else []


def _convert_items_dict_to_list(items, item_format):
    return [[item[p] for p in item_format if p in item] if item is not None else None
            for item in items] if items else []


def _convert_items_list_to_dict(items, item_format):
    index_property_list = list(enumerate(item_format))
    return [{p: item[i] for i, p in index_property_list if i < len(item)} if item is not None else None
            for item in items] if items else []


def _convert_items_obj_to_dict(items, item_format):
    return [{p: getattr(item, p) for p in item_format if hasattr(item, p)} if item is not None else None
            for item in items] if items else []
