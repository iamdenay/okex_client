import json
import logging
import time
from datetime import datetime
from operator import itemgetter
from threading import Thread
from urllib.parse import urljoin, urlencode

import requests
from dateutil import parser
from websocket import WebSocketApp

from hyperquant.api import ParamName, ParamValue, ErrorCode, Endpoint, Platform, Sorting, OrderType

"""
API clients for various trading platforms: REST and WebSocket.

Some documentation:
https://docs.google.com/document/d/1U3kuokpeNSzxSbXhXJ3XnNYbfZaK5nY3_tAL-Uk0wKQ
"""


# Value objects

class ValueObject:
    pass


# WS
class Info(ValueObject):
    code = None
    message = None


# WS
class Channel(ValueObject):
    channel_id = None
    channel = None
    symbol = None


class Error(ValueObject):
    code = None
    message = None

    def __str__(self) -> str:
        return "[Trading-Error code: %s msg: %s]" % (self.code, self.message)


class DataObject(ValueObject):
    pass

    is_milliseconds = False


class ItemObject(DataObject):
    # (Note: Order is from abstract to concrete)
    platform_id = None
    symbol = None
    timestamp = None  # Unix timestamp in milliseconds
    item_id = None  # There is no item_id for candle, ticker, bookticker, only for trade, mytrade and order

    def __init__(self, platform_id=None, symbol=None, timestamp=None, item_id=None, is_milliseconds=False) -> None:
        super().__init__()
        self.platform_id = platform_id
        self.symbol = symbol
        self.timestamp = timestamp
        self.item_id = item_id

        self.is_milliseconds = is_milliseconds

    def __eq__(self, o: object) -> bool:
        # Identifying params:
        return o and \
               self.platform_id == o.platform_id and \
               self.item_id == o.item_id and \
               self.timestamp == o.timestamp and \
               self.symbol == o.symbol

    def __hash__(self) -> int:
        return hash((self.platform_id, self.item_id, self.timestamp))

    def __repr__(self) -> str:
        platform_name = Platform.get_platform_name_by_id(self.platform_id)
        timestamp_s = self.timestamp / 1000 if self.is_milliseconds else self.timestamp
        timestamp_iso = datetime.utcfromtimestamp(timestamp_s).isoformat() if timestamp_s else timestamp_s
        return "[Item-%s id:%s time:%s symbol:%s]" % (platform_name, self.item_id, timestamp_iso, self.symbol)


class Trade(ItemObject):
    # Trade data:
    price = None
    amount = None

    # Not for all platforms or versions:
    direction = None

    def __init__(self, platform_id=None, symbol=None, timestamp=None, item_id=None,
                 price=None, amount=None, direction=None, is_milliseconds=False) -> None:
        super().__init__(platform_id, symbol, timestamp, item_id, is_milliseconds)
        self.price = price
        self.amount = amount
        self.direction = direction


class MyTrade(Trade):
    order_id = None

    # Optional (not for all platforms):
    fee = None  # Комиссия биржи  # must be always positive; 0 if not supported
    rebate = None  # Возврат денег, скидка после покупки  # must be always positive; 0 if not supported
    # fee_symbol = None  # Currency symbol, by default, it's the same as for price
    # Note: volume = price * amount, total = volume - fee + rebate

    def __init__(self, platform_id=None, symbol=None, timestamp=None, item_id=None, price=None, amount=None,
                 direction=None, order_id=None, fee=None, rebate=None, is_milliseconds=False) -> None:
        super().__init__(platform_id, symbol, timestamp, item_id, price, amount, direction, is_milliseconds)
        self.order_id = order_id
        self.fee = fee
        self.rebate = rebate


class Candle(ItemObject):
    # platform_id = None
    # symbol = None
    # timestamp = None  # open_timestamp
    interval = None

    price_open = None
    price_close = None
    price_high = None
    price_low = None
    amount = None

    # Optional
    trades_count = None

    def __init__(self, platform_id=None, symbol=None, timestamp=None, interval=None,
                 price_open=None, price_close=None, price_high=None, price_low=None,
                 amount=None, trades_count=None, is_milliseconds=False) -> None:
        super().__init__(platform_id, symbol, timestamp, None, is_milliseconds)
        self.interval = interval
        self.price_open = price_open
        self.price_close = price_close
        self.price_high = price_high
        self.price_low = price_low
        self.amount = amount
        self.trades_count = trades_count


class Ticker(ItemObject):
    # platform_id = None
    # symbol = None
    # timestamp = None

    price = None

    def __init__(self, platform_id=None, symbol=None, timestamp=None, price=None, is_milliseconds=False) -> None:
        super().__init__(platform_id, symbol, timestamp, None, is_milliseconds)
        self.price = price


# class BookTicker(DataObject):
#     symbol = None
#     price_bid = None
#     bid_amount = None
#     price_ask = None
#     ask_amount = None


class OrderBook(ItemObject):
    asks = None
    bids = None

    def __init__(self, platform_id=None, symbol=None, timestamp=None, item_id=None, is_milliseconds=False,
                 asks=None, bids=None) -> None:
        super().__init__(platform_id, symbol, timestamp, item_id, is_milliseconds)
        self.asks = asks
        self.bids = bids


class OrderBookItem(ItemObject):
    # platform_id = None
    # order_book_item_id = None  # item_id = None
    # symbol = None

    price = None
    amount = None
    direction = None

    # Optional
    order_count = None

    def __init__(self, platform_id=None, symbol=None, timestamp=None, item_id=None, is_milliseconds=False,
                 price=None, amount=None, direction=None, order_count=None) -> None:
        super().__init__(platform_id, symbol, timestamp, item_id, is_milliseconds)

        self.price = price
        self.amount = amount
        self.direction = direction
        self.order_count = order_count


class Account(DataObject):
    platform_id = None
    timestamp = None

    balances = None

    # Binance other params:
    # "makerCommission": 15,
    # "takerCommission": 15,
    # "buyerCommission": 0,
    # "sellerCommission": 0,
    # "canTrade": true,
    # "canWithdraw": true,
    # "canDeposit": true,

    def __init__(self, platform_id=None, timestamp=None, balances=None) -> None:
        super().__init__()
        self.platform_id = platform_id
        self.timestamp = timestamp
        self.balances = balances


class Balance(ValueObject):
    # Asset, currency
    platform_id = None
    symbol = None
    amount_available = None
    amount_reserved = None

    def __init__(self, platform_id=None, symbol=None, amount_available=None, amount_reserved=None) -> None:
        super().__init__()
        self.platform_id = platform_id
        self.symbol = symbol
        self.amount_available = amount_available
        self.amount_reserved = amount_reserved


class Order(ItemObject):
    # platform_id = None
    # item_id = None
    # symbol = None
    # timestamp = None  # (transact timestamp)
    user_order_id = None

    order_type = None  # limit and market
    price = None
    amount_original = None
    amount_executed = None
    direction = None

    order_status = None  # open and close

    def __init__(self, platform_id=None, symbol=None, timestamp=None, item_id=None, is_milliseconds=False,
                 user_order_id=None, order_type=None, price=None,
                 amount_original=None, amount_executed=None, direction=None, order_status=None) -> None:
        super().__init__(platform_id, symbol, timestamp, item_id, is_milliseconds)
        self.user_order_id = user_order_id
        self.order_type = order_type
        self.price = price
        self.amount_original = amount_original
        self.amount_executed = amount_executed
        self.direction = direction
        self.order_status = order_status


# Base

class ProtocolConverter:
    """
    Contains all the info and logic to convert data between
    our library API and remote platform API.
    """

    # Main params:
    # (Set by client or set it by yourself in subclass)
    platform_id = None
    version = None
    # (Define in subclass)
    base_url = None

    # Settings:
    is_use_max_limit = False

    # Converting info:
    # Our endpoint to platform_endpoint
    endpoint_lookup = None  # {"endpoint": "platform_endpoint", ...}
    # Our param_name to platform_param_name
    param_name_lookup = None  # {ParamName.FROM_TIME: "start", "not_supported": None, ...}
    # Our param_value to platform_param_value
    param_value_lookup = None  # {Sorting.ASCENDING: 0}
    max_limit_by_endpoint = None

    # For parsing
    item_class_by_endpoint = {
        Endpoint.TRADE: Trade,
        Endpoint.TRADE_HISTORY: Trade,
        Endpoint.TRADE_MY: MyTrade,
        Endpoint.CANDLE: Candle,
        Endpoint.TICKER: Ticker,
        Endpoint.ORDER_BOOK: OrderBook,
        Endpoint.ORDER_BOOK_DIFF: OrderBook,
        # Private
        Endpoint.ACCOUNT: Account,
        Endpoint.ORDER: Order,
        Endpoint.ORDER_CURRENT: Order,
        Endpoint.ORDER_MY: Order,
    }
    # {Trade: {ParamName.ITEM_ID: "tid", ...}} - omitted properties won't be set
    param_lookup_by_class = None

    error_code_by_platform_error_code = None
    error_code_by_http_status = None

    # For converting time
    use_milliseconds = False  # todo always use milliseconds
    is_source_in_milliseconds = False
    is_source_in_timestring = False
    timestamp_platform_names = None  # ["startTime", "endTime"]
    # (If platform api is not consistent)
    timestamp_platform_names_by_endpoint = None  # {Endpoint.TRADE: ["start", "end"]}
    ITEM_TIMESTAMP_ATTR = ParamName.TIMESTAMP

    def __init__(self, platform_id=None, version=None):
        if platform_id is not None:
            self.platform_id = platform_id
        if version is not None:
            self.version = version

        # Create logger
        platform_name = Platform.get_platform_name_by_id(self.platform_id)
        self.logger = logging.getLogger("%s.%s.v%s" % ("Converter", platform_name, self.version))

    # Convert to platform format

    def make_url_and_platform_params(self, endpoint=None, params=None, is_join_get_params=False, version=None):
        # Apply version on base_url
        version = version or self.version
        url = self.base_url.format(version=version) if self.base_url and version else self.base_url
        # Prepare path and params
        url_resources, platform_params = self.prepare_params(endpoint, params)

        # Make resulting URL
        # url=ba://se_url/resou/rces?p=ar&am=s
        if url_resources and url:
            url = urljoin(url + "/", "/".join(url_resources))
        if platform_params and is_join_get_params:
            url = url + "?" + urlencode(platform_params)
        return url, platform_params

    def prepare_params(self, endpoint=None, params=None):
        # Override in subclasses if it is the only way to adopt client to platform

        # Convert our code's names to custom platform's names
        platform_params = {self._get_platform_param_name(key): self._process_param_value(key, value)
                           for key, value in params.items() if value is not None} if params else {}
        # (Del not supported by platform params which defined in lookups as empty)
        platform_params.pop("", "")
        platform_params.pop(None, None)
        self._convert_timestamp_values_to_platform(endpoint, platform_params)

        # Endpoint.TRADE -> "trades/ETHBTC" or "trades"
        platform_endpoint = self._get_platform_endpoint(endpoint, params)

        # Make path part of URL (as a list) using endpoint and params
        resources = [platform_endpoint] if platform_endpoint else []

        return resources, platform_params

    def _process_param_value(self, name, value):
        # Convert values to platform values
        # if name in ParamValue.param_names:
        value = self._get_platform_param_value(value, name)
        return value

    def _get_platform_endpoint(self, endpoint, params):
        # Convert our code's endpoint to custom platform's endpoint
        # Endpoint.TRADE -> "trades/{symbol}" or "trades" or lambda params: "trades"
        platform_endpoint = self.endpoint_lookup.get(endpoint, endpoint) if self.endpoint_lookup else endpoint
        if callable(platform_endpoint):
            platform_endpoint = platform_endpoint(params)
        if platform_endpoint:
            # "trades", {"symbol": "ETHBTC"} => "trades" (no error)
            # "trades/{symbol}/hist", {"symbol": "ETHBTC"} => "trades/ETHBTC/hist"
            # "trades/{symbol}/hist", {} => Error!
            platform_endpoint = platform_endpoint.format(**params)

        return platform_endpoint

    def _get_platform_param_name(self, name):
        # Convert our code's param name to custom platform's param name
        return self.param_name_lookup.get(name, name) if self.param_name_lookup else name

    def _get_platform_param_value(self, value, name=None):
        # Convert our code's param value to custom platform's param value
        lookup = self.param_value_lookup
        lookup = lookup.get(name, lookup) if lookup else None
        return lookup.get(value, value) if lookup else value

    # Convert from platform format

    def parse(self, endpoint, data):
        # if not endpoint or not data:
        #     self.logger.warning("Some argument is empty in parse(). endpoint: %s, data: %s", endpoint, data)
        #     return data
        if not data:
            self.logger.warning("Data argument is empty in parse(). endpoint: %s, data: %s", endpoint, data)
            return data

        # (If list of items data, but not an item data as a list)
        if isinstance(data, list):  # and not isinstance(data[0], list):
            result = [self._parse_item(endpoint, item_data) for item_data in data]
            # (Skip empty)
            result = [item for item in result if item]
            return result
        else:
            return self._parse_item(endpoint, data)

    def _parse_item(self, endpoint, item_data):
        # Check item_class by endpoint
        if not endpoint or not self.item_class_by_endpoint or endpoint not in self.item_class_by_endpoint:
            self.logger.warning("Wrong endpoint: %s in parse_item().", endpoint)
            return item_data
        item_class = self.item_class_by_endpoint[endpoint]

        # Create and set up item by item_data (using lookup to convert property names)
        item = self._create_and_set_up_object(item_class, item_data)
        item = self._post_process_item(item)
        
        
        return item

    def _post_process_item(self, item):
        # Process parsed values (convert from platform)
        # Set platform_id
        if hasattr(item, ParamName.PLATFORM_ID):
            item.platform_id = self.platform_id
        # Stringify item_id
        if hasattr(item, ParamName.ITEM_ID) and item.item_id is not None:
            item.item_id = str(item.item_id)
        # Convert timestamp
        # (If API returns milliseconds or string date we must convert them to Unix timestamp (in seconds or ms))
        # (Note: add here more timestamp attributes if you use another name in your VOs)
        if hasattr(item, self.ITEM_TIMESTAMP_ATTR) and item.timestamp:
            item.timestamp = self._convert_timestamp_from_platform(item.timestamp)
            item.is_milliseconds = self.use_milliseconds
        # Convert asks and bids to OrderBookItem type
        if hasattr(item, ParamName.ASKS) and item.asks:
            item.asks = [self._create_and_set_up_object(OrderBookItem, item_data)
                         for item_data in item.asks]
        if hasattr(item, ParamName.BIDS) and item.bids:
            item.bids = [self._create_and_set_up_object(OrderBookItem, item_data)
                         for item_data in item.bids]
        # Convert items to Balance type
        if hasattr(item, ParamName.BALANCES) and item.balances:
            item.balances = [self._create_and_set_up_object(Balance, item_data)
                             for item_data in item.balances]
            # Set platform_id
            for balance in item.balances:
                self._post_process_item(balance)

        return item

    def parse_error(self, error_data=None, response=None):
        # (error_data=None and response!=None when REST API returns 404 and html response)
        if response and response.ok:
            return None

        result = self._create_and_set_up_object(Error, error_data) or Error()
        response_message = " (status: %s %s code: %s msg: %s)" % (
            response.status_code, response.reason, result.code, result.message) if response \
            else " (code: %s msg: %s)" % (result.code, result.message)
        if not result.code:
            result.code = response.status_code
        result.code = self.error_code_by_platform_error_code.get(result.code, result.code) \
            if self.error_code_by_platform_error_code else result.code
        result.message = ErrorCode.get_message_by_code(result.code) + response_message
        return result

    def _create_and_set_up_object(self, object_class, data):
        if not object_class or not data:
            return None

        obj = object_class()
        lookup = self.param_lookup_by_class.get(object_class) if self.param_lookup_by_class else None
        if not lookup:
            # self.logger.error("There is no lookup for %s in %s", object_class, self.__class__)
            raise Exception("There is no lookup for %s in %s" % (object_class, self.__class__))
        # (Lookup is usually a dict, but can be a list when item_data is a list)
        key_pair = lookup.items() if isinstance(lookup, dict) else enumerate(lookup)
        for platform_key, key in key_pair:
            if key and (not isinstance(data, dict) or platform_key in data):
                setattr(obj, key, data[platform_key])
        return obj

    # Convert from and to platform

    def _convert_timestamp_values_to_platform(self, endpoint, platform_params):
        if not platform_params:
            return
        timestamp_platform_names = self.timestamp_platform_names_by_endpoint.get(
            endpoint, self.timestamp_platform_names) \
            if self.timestamp_platform_names_by_endpoint else self.timestamp_platform_names
        if not timestamp_platform_names:
            return

        for name in timestamp_platform_names:
            if name in platform_params:
                value = platform_params[name]
                if isinstance(value, ValueObject):
                    value = getattr(value, self.ITEM_TIMESTAMP_ATTR, value)
                platform_params[name] = self._convert_timestamp_to_platform(value)

    def _convert_timestamp_to_platform(self, timestamp):
        if not timestamp:
            return timestamp

        if self.use_milliseconds:
            timestamp /= 1000

        if self.is_source_in_milliseconds:
            timestamp *= 1000
        elif self.is_source_in_timestring:
            dt = datetime.utcfromtimestamp(timestamp)
            timestamp = dt.isoformat()
        return timestamp

    def _convert_timestamp_from_platform(self, timestamp):
        if not timestamp:
            return timestamp
        if self.is_source_in_milliseconds:
            timestamp /= 1000
            # if int(timestamp) == timestamp:
            #     timestamp = int(timestamp)
        elif self.is_source_in_timestring:
            timestamp = parser.parse(timestamp).timestamp()

        if self.use_milliseconds:
            timestamp = int(timestamp * 1000)
        return timestamp


class BaseClient:
    """
    All time params are unix timestamps in seconds (float or int).
    """

    # Main params
    _log_prefix = "Client"
    platform_id = None
    version = None
    _api_key = None
    _api_secret = None
    default_converter_class = ProtocolConverter
    _converter_class_by_version = None
    _converter_by_version = None

    # If True then if "symbol" param set to None that will return data for "all symbols"
    IS_NONE_SYMBOL_FOR_ALL_SYMBOLS = False

    @property
    def headers(self):
        # Usually returns auth and other headers (Don't return None)
        # (as a dict for requests (REST) and a list for WebSockets (WS))
        return []

    @property
    def use_milliseconds(self):
        return self.converter.use_milliseconds

    @use_milliseconds.setter
    def use_milliseconds(self, value):
        self.converter.use_milliseconds = value

    def __init__(self, version=None, **kwargs) -> None:
        super().__init__()

        if version is not None:
            self.version = str(version)

        # Set up settings
        for key, value in kwargs.items():
            setattr(self, key, value)

        # Create logger
        platform_name = Platform.get_platform_name_by_id(self.platform_id)
        self.logger = logging.getLogger("%s.%s.v%s" % (self._log_prefix, platform_name, self.version))
        # self.logger.debug("Create %s client for %s platform. url+params: %s",
        #                   self._log_prefix, platform_name, self.make_url_and_platform_params())

        # Create converter
        self.converter = self.get_or_create_converter()
        if not self.converter:
            raise Exception("There is no converter_class in %s for version: %s" % (self.__class__, self.version))

    def set_credentials(self, api_key, api_secret):
        self._api_key = api_key
        self._api_secret = api_secret

    def get_or_create_converter(self, version=None):
        # Converter stores all the info about a platform
        # Note: Using version to get converter at any time allows us to easily
        # switch version for just one request or for all further requests
        # (used for bitfinex, for example, to get symbols which enabled only for v1)

        if not version:
            version = self.version
        version = str(version)

        if not self._converter_by_version:
            self._converter_by_version = {}
        if version in self._converter_by_version:
            return self._converter_by_version[version]

        # Get class
        converter_class = self._converter_class_by_version.get(version) \
            if self._converter_class_by_version else self.default_converter_class
        # Note: platform_id could be set in converter or in client
        if not self.platform_id:
            self.platform_id = converter_class.platform_id
        # Create and store
        converter = converter_class(self.platform_id, version) if converter_class else None
        self._converter_by_version[version] = converter

        return converter

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


# REST

class RESTConverter(ProtocolConverter):
    # sorting values: ASCENDING, DESCENDING (newest first), None
    # DEFAULT_SORTING = Param.ASCENDING  # Const for current platform. See in param_name_lookup
    IS_SORTING_ENABLED = False  # False - SORTING param is not supported for current platform
    sorting = Sorting.DESCENDING  # Choose default sorting for all requests

    secured_endpoints = [Endpoint.ACCOUNT, Endpoint.TRADE_MY,
                         Endpoint.ORDER, Endpoint.ORDER_TEST, Endpoint.ORDER_MY, Endpoint.ORDER_CURRENT]

    # endpoint -> endpoint (if has different endpoint for history)
    history_endpoint_lookup = {
        Endpoint.TRADE: Endpoint.TRADE_HISTORY,
    }

    # endpoint -> platform_endpoint
    endpoint_lookup = None
    max_limit_by_endpoint = None

    @property
    def default_sorting(self):
        # Default sorting for current platform if no sorting param is specified
        return self._get_platform_param_value(Sorting.DEFAULT_SORTING)

    def preprocess_params(self, endpoint, params):
        self._process_limit_param(endpoint, params)
        self._process_sorting_param(endpoint, params)
        # Must be after sorting added
        self._process_from_item_param(endpoint, params)
        return params

    def _process_limit_param(self, endpoint, params):
        # (If LIMIT param is set to None (expected, but not defined))
        is_use_max_limit = self.is_use_max_limit or (params.get(ParamName.IS_USE_MAX_LIMIT, False) if params else False)
        is_limit_supported_here = params and ParamName.LIMIT in params
        if is_use_max_limit and is_limit_supported_here and params[ParamName.LIMIT] is None:
            value = self.max_limit_by_endpoint.get(endpoint, 1000000) if self.max_limit_by_endpoint else None
            if value is not None:
                # Set limit to maximum supported by a platform
                params[ParamName.LIMIT] = value

    def _process_sorting_param(self, endpoint, params):
        # (Add only if a platform supports it, and it is not already added)

        if not self.IS_SORTING_ENABLED and ParamName.SORTING in params:
            del params[ParamName.SORTING]
        elif self.IS_SORTING_ENABLED and not params.get(ParamName.SORTING):
            params[ParamName.SORTING] = self.sorting

    def _get_real_sorting(self, params):
        sorting = params.get(ParamName.SORTING) if params else None
        return sorting or self.default_sorting

    def _process_from_item_param(self, endpoint, params):
        from_item = params.get(ParamName.FROM_ITEM)
        if not from_item or not params:  # or not self.IS_SORTING_ENABLED:
            return

        to_item = params.get(ParamName.TO_ITEM)
        is_descending = self._get_real_sorting(params) == Sorting.DESCENDING

        # (from_item <-> to_item)
        # is_from_newer_than_to = getattr(from_item, self.ITEM_TIMESTAMP_ATTR, 0) > \
        #                         getattr(to_item, self.ITEM_TIMESTAMP_ATTR, 0)
        is_from_newer_than_to = (from_item.timestamp or 0) > (to_item.timestamp or 0)
        if from_item and to_item and is_from_newer_than_to:
            params[ParamName.FROM_ITEM] = to_item
            params[ParamName.TO_ITEM] = from_item

        # (from_item -> to_item)
        if is_descending and not to_item:
            params[ParamName.TO_ITEM] = from_item
            del params[ParamName.FROM_ITEM]

    def process_secured(self, endpoint, platform_params, api_key, api_secret):
        if endpoint in self.secured_endpoints:
            platform_params = self._generate_and_add_signature(platform_params, api_key, api_secret)
        return platform_params

    def _generate_and_add_signature(self, platform_params, api_key, api_secret):
        # Generate and add signature here
        return platform_params

    def post_process_result(self, method, endpoint, params, result):
        # Process result using request data

        if isinstance(result, Error):
            return result

        # (Symbol and interval are often not returned in response, so we have to set it here)
        # symbol = params.get(ParamName.SYMBOL) if params else None
        # if symbol:
        #     if isinstance(result, list):
        #         for item in result:
        #             if hasattr(item, ParamName.SYMBOL):
        #                 item.symbol = symbol
        #     else:
        #         if hasattr(result, ParamName.SYMBOL):
        #             result.symbol = symbol
        self._propagate_param_to_result(ParamName.SYMBOL, params, result)
        self._propagate_param_to_result(ParamName.INTERVAL, params, result)

        return result

    def _propagate_param_to_result(self, param_name, params, result):
        value = params.get(param_name) if params else None
        if value:
            if isinstance(result, list):
                for item in result:
                    if hasattr(item, param_name):
                        setattr(item, param_name, value)
            else:
                if hasattr(result, param_name):
                    setattr(result, param_name, value)


class BaseRESTClient(BaseClient):
    # Settings:
    _log_prefix = "RESTClient"

    default_converter_class = RESTConverter

    # State:
    delay_before_next_request_sec = 0

    session = None
    _last_response_for_debugging = None

    @property
    def headers(self):
        return {
            "Accept": "application/json",
            "User-Agent": "client/python",
        }

    def __init__(self, version=None, **kwargs) -> None:
        super().__init__(version, **kwargs)

        self.session = requests.session()

    def close(self):
        if self.session:
            self.session.close()

    def _send(self, method, endpoint, params=None, version=None, **kwargs):
        converter = self.get_or_create_converter(version)

        # Prepare
        params = dict(**kwargs, **(params or {}))
        params = converter.preprocess_params(endpoint, params)
        url, platform_params = converter.make_url_and_platform_params(endpoint, params, version=version)
        platform_params = converter.process_secured(endpoint, platform_params, self._api_key, self._api_secret)
        if not url:
            return None

        # Send
        kwargs = {"headers": self.headers}
        params_name = "params" if method.lower() == "get" else "data"
        kwargs[params_name] = platform_params
        self.logger.info("Send: %s %s %s", method, url, platform_params)
        response = self.session.request(method, url, **kwargs)

        # Parse
        self._last_response_for_debugging = response
        if response.ok:
            result = converter.parse(endpoint, response.json())
            result = converter.post_process_result(method, endpoint, params, result)
        else:
            is_json = "json" in response.headers.get("content-type", "")
            result = converter.parse_error(response.json() if is_json else None, response)
        self.logger.info("Response: %s Parsed result: %s %s", response,
                         len(result) if isinstance(result, list) else "",
                         str(result)[:100] + " ... " + str(result)[-100:])
        self._on_response(response, result)

        # Return parsed value objects or Error instance
        return result

    def _on_response(self, response, result):
        pass


class PlatformRESTClient(BaseRESTClient):
    """
    Important! Behavior when some param is None or for any other case should be same for all platforms.
    Important! from and to params must be including: [from, to], not [from, to) or (from, to).

    Закомментированные методы скорее всего не понадобятся, но на всякий случай они добавлены,
    чтобы потом не возвращаться и не думать заново.
    """
    _server_time_diff_s = None

    def ping(self, version=None, **kwargs):
        endpoint = Endpoint.PING
        return self._send("GET", endpoint, version=version, **kwargs)

    def get_server_timestamp(self, force_from_server=False, version=None, **kwargs):
        endpoint = Endpoint.SERVER_TIME

        if not force_from_server and self._server_time_diff_s is not None:
            # (Calculate using time difference with server taken from previous call)
            result = self._server_time_diff_s + time.time()
            return int(result * 1000) if self.use_milliseconds else result

        time_before = time.time()

        result = self._send("GET", endpoint, version=version, **kwargs)
        if isinstance(result, Error):
            return result

        # (Update time diff)
        self._server_time_diff_s = (result / 1000 if self.use_milliseconds else result) - time_before
        return result

    def get_symbols(self, version=None, **kwargs):
        endpoint = Endpoint.SYMBOLS
        return self._send("GET", endpoint, version=version, **kwargs)

    def fetch_history(self, endpoint, symbol, limit=None, from_item=None, to_item=None,
                      sorting=None, is_use_max_limit=False, from_time=None, to_time=None,
                      version=None, **kwargs):
        # Common method for fetching history for any endpoint. Used in REST connector.

        # (Convert endpoint to history endpoint if they differ)
        history_endpoint_lookup = self.converter.history_endpoint_lookup
        endpoint = history_endpoint_lookup.get(endpoint, endpoint) if history_endpoint_lookup else endpoint
        params = {
            ParamName.SYMBOL: symbol,
            ParamName.LIMIT: limit,
            ParamName.FROM_ITEM: from_item,
            ParamName.TO_ITEM: to_item,
            ParamName.SORTING: sorting,
            ParamName.IS_USE_MAX_LIMIT: is_use_max_limit,
            ParamName.FROM_TIME: from_time,
            ParamName.TO_TIME: to_time,
        }

        self.logger.debug("fetch_history from: %s to: %s", from_item or from_time, to_item or to_time)
        result = self._send("GET", endpoint, params, version, **kwargs)
        return result

    # Trade

    def fetch_trades(self, symbol, limit=None, version=None, **kwargs):
        # Fetch current (last) trades to display at once.

        endpoint = Endpoint.TRADE
        params = {
            ParamName.SYMBOL: symbol,
            ParamName.LIMIT: limit,
        }

        result = self._send("GET", endpoint, params, version, **kwargs)
        return result

    def fetch_trades_history(self, symbol, limit=None, from_item=None, to_item=None,
                             sorting=None, is_use_max_limit=False, from_time=None, to_time=None,
                             version=None, **kwargs):
        # Fetching whole trades history as much as possible.
        # from_time and to_time used along with from_item and to_item as we often need to fetch
        # history by time and only Binance (as far as I know) doesn't support that (only by id)

        return self.fetch_history(Endpoint.TRADE, symbol, limit, from_item, to_item,
                                  sorting, is_use_max_limit, from_time, to_time,
                                  version, **kwargs)

    # Candle

    def fetch_candles(self, symbol, interval, limit=None, from_time=None, to_time=None,
                      is_use_max_limit=False, version=None, **kwargs):
        endpoint = Endpoint.CANDLE
        params = {
            ParamName.SYMBOL: symbol,
            ParamName.INTERVAL: interval,
            ParamName.LIMIT: limit,
            ParamName.FROM_TIME: from_time,
            ParamName.TO_TIME: to_time,
            ParamName.IS_USE_MAX_LIMIT: is_use_max_limit,
        }

        result = self._send("GET", endpoint, params, version, **kwargs)
        return result

    # Ticker

    def fetch_ticker(self, symbol=None, version=None, **kwargs):
        endpoint = Endpoint.TICKER
        params = {
            ParamName.SYMBOL: symbol,
        }

        result = self._send("GET", endpoint, params, version, **kwargs)
        return result

    def fetch_tickers(self, symbols=None, version=None, **kwargs):
        endpoint = Endpoint.TICKER
        # (Send None for all symbols)
        # params = {
        #     ParamName.SYMBOLS: None,
        # }

        result = self._send("GET", endpoint, None, version, **kwargs)

        if symbols:
            # Filter result for symbols defined
            symbols = [symbol.upper() if symbol else symbol for symbol in symbols]
            return [item for item in result if item.symbol in symbols]

        return result

    # Order Book

    def fetch_order_book(self, symbol=None, limit=None, is_use_max_limit=False,
                         version=None, **kwargs):
        # Level 2 (price-aggregated) order book for a particular symbol.

        endpoint = Endpoint.ORDER_BOOK
        params = {
            ParamName.SYMBOL: symbol,
            ParamName.LIMIT: limit,
        }

        result = self._send("GET", endpoint, params, version, **kwargs)
        return result

    # def fetch_order_book_L2_L3(self, symbol=None, limit=None, version=None, **kwargs):
    #     # Fetch L2/L3 order book (with all orders enlisted) for a particular market trading symbol.
    #     pass


class PrivatePlatformRESTClient(PlatformRESTClient):

    def __init__(self, api_key=None, api_secret=None, version=None, **kwargs) -> None:
        super().__init__(version=version, **kwargs)

        self._api_key = api_key
        self._api_secret = api_secret

    # Trades

    def fetch_account_info(self, version=None, **kwargs):
        # Balance included to account
        endpoint = Endpoint.ACCOUNT
        params = {}

        result = self._send("GET", endpoint, params, version or "3", **kwargs)
        return result

    def fetch_my_trades(self, symbol, limit=None, version=None, **kwargs):
        endpoint = Endpoint.TRADE_MY
        params = {
            ParamName.SYMBOL: symbol,
            ParamName.LIMIT: limit,
        }

        result = self._send("GET", endpoint, params, version or "3", **kwargs)
        return result

    # def fetch_my_trades_history(self, symbol, limit=None, from_item=None, to_item=None,
    #                             sorting=None, is_use_max_limit=False, version=None, **kwargs):
    #     pass

    # Order (private)

    def create_order(self, symbol, order_type, direction, price=None, amount=None, is_test=False,
                     version=None, **kwargs):
        endpoint = Endpoint.ORDER_TEST if is_test else Endpoint.ORDER

        # if order_type != OrderType.MARKET:
        #     price = None
        params = {
            ParamName.SYMBOL: symbol,
            ParamName.ORDER_TYPE: order_type,
            ParamName.DIRECTION: direction,
            ParamName.PRICE: price if order_type == OrderType.LIMIT else None,
            ParamName.AMOUNT: amount,
        }

        result = self._send("POST", endpoint, params, version=version or "3", **kwargs)
        return result

    def cancel_order(self, order, symbol=None, version=None, **kwargs):
        endpoint = Endpoint.ORDER
        params = {
            # ParamName.ORDER_ID: order.item_id if isinstance(order, Order) else order,
            ParamName.ORDER_ID: order,
            ParamName.SYMBOL: symbol,  # move to converter(?):  or (order.symbol if hasattr(order, ParamName.SYMBOL) else None),
        }

        result = self._send("DELETE", endpoint, params, version or "3", **kwargs)
        return result

    # was fetch_order
    def check_order(self, order, symbol=None, version=None, **kwargs):  # , direction=None
        # item_id should be enough, but some platforms also need symbol and direction
        endpoint = Endpoint.ORDER
        params = {
            ParamName.SYMBOL: symbol,
            ParamName.ORDER_ID: order,
            # ParamName.: ,
        }

        result = self._send("GET", endpoint, params, version or "3", **kwargs)
        return result

    def fetch_orders(self, symbol=None, limit=None, from_item=None, is_open=False,
                     version=None, **kwargs):  # , order_status=None
        endpoint = Endpoint.ORDER_CURRENT if is_open else Endpoint.ORDER_MY
        params = {
            ParamName.SYMBOL: symbol,
            # ParamName.: ,
            ParamName.LIMIT: limit,
            ParamName.FROM_ITEM: from_item,
            # ParamName.: ,
        }

        result = self._send("GET", endpoint, params, version or "3", **kwargs)
        return result


# WebSocket

class WSConverter(ProtocolConverter):
    # Main params:
    # False - Subscribing by connecting URL: BitMEX, Binance.
    # True - Subscribing by command: Bitfinex (v1, v2).
    IS_SUBSCRIPTION_COMMAND_SUPPORTED = True

    # supported_endpoints = None
    # symbol_endpoints = None  # In subclass you can call REST API to get symbols
    supported_endpoints = [Endpoint.TRADE, Endpoint.CANDLE, Endpoint.TICKER, Endpoint.TICKER_ALL,
                           Endpoint.ORDER_BOOK, Endpoint.ORDER_BOOK_DIFF]
    symbol_endpoints = [Endpoint.TRADE, Endpoint.CANDLE,
                        Endpoint.TICKER,  # can be used as symbol and as generic endpoint
                        Endpoint.ORDER_BOOK, Endpoint.ORDER_BOOK_DIFF]
    # generic_endpoints = None  # = supported_endpoints.difference(symbol_endpoints)
    supported_symbols = None

    # Converting info:
    # For converting to platform

    # For parsing from platform
    event_type_param = None
    endpoint_by_event_type = None
    item_class_by_endpoint = dict(**ProtocolConverter.item_class_by_endpoint, **{
        # # Item class by event type
        # "error": Error,
        # "info": Info,
        # "subscribed": Channel,
    })

    # For converting time

    @property
    def generic_endpoints(self):
        # Non-symbol endpoints
        return self.supported_endpoints.difference(self.symbol_endpoints or set()) \
            if self.supported_endpoints else set()

    def generate_subscriptions(self, endpoints, symbols, **params):
        result = set()
        for endpoint in endpoints:
            if endpoint in self.symbol_endpoints:
                if symbols:
                    for symbol in symbols:
                        result.add(self._generate_subscription(endpoint, symbol, **params))
                else:
                    result.add(self._generate_subscription(endpoint, None, **params))
            else:
                result.add(self._generate_subscription(endpoint, **params))
        return result

    def _generate_subscription(self, endpoint, symbol=None, **params):
        channel = self._get_platform_endpoint(endpoint, {ParamName.SYMBOL: symbol, **params})
        return channel

    def parse(self, endpoint, data):
        # (Get endpoint from event type)
        if not endpoint and data and isinstance(data, dict) and self.event_type_param:
            event_type = data.get(self.event_type_param, endpoint)
            endpoint = self.endpoint_by_event_type.get(event_type, event_type) \
                if self.endpoint_by_event_type else event_type
            # if not endpoint:
            #     self.logger.error("Cannot find event type by name: %s in data: %s", self.event_type_param, data)
            # self.logger.debug("Endpoint: %s by name: %s in data: %s", endpoint, self.event_type_param, data)

        return super().parse(endpoint, data)


class WSClient(BaseClient):
    """
    Using:
        client = WSClient(api_key, api_secret)
        client.subscribe([Endpoint.TRADE], ["ETHUSD", "ETHBTC"])
        # (Will reconnect for platforms which needed that)
        client.subscribe([Endpoint.TRADE], ["ETHBTC", "ETHUSD"])
        # Resulting subscriptions: [Endpoint.TRADE] channel for symbols:
        # ["ETHUSD", "ETHBTC", "ETHBTC", "ETHUSD"]
    """
    # Settings:
    _log_prefix = "WSClient"

    default_converter_class = WSConverter

    is_auto_reconnect = True
    reconnect_delay_sec = 3
    reconnect_count = 3

    on_connect = None
    on_data = None
    on_data_item = None
    on_disconnect = None

    # State:
    # Subscription sets
    endpoints = None
    symbols = None
    # endpoints + symbols = subscriptions
    current_subscriptions = None
    pending_subscriptions = None
    successful_subscriptions = None
    failed_subscriptions = None
    is_subscribed_with_url = False

    # Connection
    is_started = False
    _is_reconnecting = True
    _reconnect_tries = 0
    ws = None
    thread = None
    _data_buffer = None

    @property
    def url(self):
        # Override if you need to introduce some get params
        # (Set self.is_subscribed_with_url=True if subscribed in here in URL)
        url, platform_params = self.converter.make_url_and_platform_params()
        return url if self.converter else ""

    @property
    def is_connected(self):
        return self.ws.sock.connected if self.ws and self.ws.sock else False

    def __init__(self, api_key=None, api_secret=None, version=None, **kwargs) -> None:
        super().__init__(version, **kwargs)
        self._api_key = api_key
        self._api_secret = api_secret

        # (For convenience)
        self.IS_SUBSCRIPTION_COMMAND_SUPPORTED = self.converter.IS_SUBSCRIPTION_COMMAND_SUPPORTED

    # Subscription

    def subscribe(self, endpoints=None, symbols=None, **params):
        """
        Subscribe and connect.

        None means all: all previously subscribed or (if none) all supported.

            subscribe()  # subscribe to all supported endpoints (currently only generic ones)
            unsubscribe()  # unsubscribe all
            subscribe(symbols=["BTCUSD"])  # subscribe to all supported endpoints for "BTCUSD"
            unsubscribe(endpoints=["TRADE"])  # unsubscribe all "TRADE" channels - for all symbols
            unsubscribe()  # unsubscribe all (except "TRADE" which has been already unsubscribed before)

            subscribe(endpoints=["TRADE"], symbols=["BTCUSD"])  # subscribe to all supported endpoints for "BTCUSD"
            unsubscribe()  # unsubscribe all "TRADE" channels
            subscribe()  # subscribe to all "TRADE" channels back because it was directly
            unsubscribe(endpoints=["TRADE"])  # unsubscribe all "TRADE" channels directly (currently only for "BTCUSD")
            subscribe()  # subscribe all supported channels for symbol "BTCUSD" (as this symbol wasn't unsubscribed directly)
            unsubscribe(symbols=["BTCUSD"])  # unsubscribe all channels for "BTCUSD"

        :param endpoints:
        :param symbols:
        :return:
        """

        self.logger.debug("Subscribe on endpoints: %s and symbols: %s prev: %s %s",
                          endpoints, symbols, self.endpoints, self.symbols)
        # if not endpoints and not symbols:
        #     subscriptions = self.prev_subscriptions
        # else:
        if not endpoints:
            endpoints = self.endpoints or self.converter.supported_endpoints
        else:
            endpoints = set(endpoints).intersection(self.converter.supported_endpoints)
            self.endpoints = self.endpoints.union(endpoints) if self.endpoints else endpoints
        if not symbols:
            symbols = self.symbols or self.converter.supported_symbols
        else:
            self.symbols = self.symbols.union(symbols) if self.symbols else set(symbols)
        if not endpoints:
            return

        subscriptions = self.converter.generate_subscriptions(endpoints, symbols, **params)

        self.current_subscriptions = self.current_subscriptions.union(subscriptions) \
            if self.current_subscriptions else subscriptions

        self._subscribe(subscriptions)

    def unsubscribe(self, endpoints=None, symbols=None, **params):
        # None means "all"

        self.logger.debug("Subscribe from endpoints: %s and symbols: %s", endpoints, symbols)
        subscribed = self.pending_subscriptions.union(self.successful_subscriptions or set()) \
            if self.pending_subscriptions else set()
        if not endpoints and not symbols:
            subscriptions = self.current_subscriptions.copy()

            # if self.current_subscriptions:
            #     self.prev_subscriptions = self.current_subscriptions
            self.current_subscriptions.clear()
            self.failed_subscriptions.clear()
            self.pending_subscriptions.clear()
            self.successful_subscriptions.clear()
        else:
            if not endpoints:
                endpoints = self.endpoints
            else:
                self.endpoints = self.endpoints.difference(endpoints) if self.endpoints else set()
            if not symbols:
                symbols = self.symbols
            else:
                self.symbols = self.symbols.difference(symbols) if self.symbols else set()
            if not endpoints:
                return

            subscriptions = self.converter.generate_subscriptions(endpoints, symbols, **params)

            self.current_subscriptions = self.current_subscriptions.difference(subscriptions)
            self.failed_subscriptions = self.failed_subscriptions.difference(subscriptions)
            self.pending_subscriptions = self.pending_subscriptions.difference(subscriptions)
            self.successful_subscriptions = self.successful_subscriptions.difference(subscriptions)

        self._unsubscribe(subscriptions.intersection(subscribed))

    def resubscribe(self):
        self.logger.debug("Resubscribe all current subscriptions")
        # Unsubscribe & subscribe all
        if self.IS_SUBSCRIPTION_COMMAND_SUPPORTED:
            # Send unsubscribe all and subscribe all back again not interrupting a connection
            self.unsubscribe()
            self.subscribe()
        else:
            # Platforms which subscribe in WS URL need reconnection
            self.reconnect()

    def _subscribe(self, subscriptions):
        # Call subscribe command with "subscriptions" param or reconnect with
        # "self.current_subscriptions" in URL - depending on platform
        self.logger.debug(" Subscribe to subscriptions: %s", subscriptions)
        if not self.is_started or not self.IS_SUBSCRIPTION_COMMAND_SUPPORTED:
            # Connect on first subscribe() or reconnect on the further ones
            self.reconnect()
        else:
            self._send_subscribe(subscriptions)

    def _unsubscribe(self, subscriptions):
        # Call unsubscribe command with "subscriptions" param or reconnect with
        # "self.current_subscriptions" in URL - depending on platform
        self.logger.debug(" Subscribe from subscriptions: %s", subscriptions)
        if not self.is_started or not self.IS_SUBSCRIPTION_COMMAND_SUPPORTED:
            self.reconnect()
        else:
            self._send_unsubscribe(subscriptions)

    def _send_subscribe(self, subscriptions):
        # Implement in subclass
        pass

    def _send_unsubscribe(self, subscriptions):
        # Implement in subclass
        pass

    # Connection

    def connect(self, version=None):
        # Check ready
        if not self.current_subscriptions:
            self.logger.warning("Please subscribe before connect.")
            return

        # Do nothing if was called before
        if self.ws and self.is_started:
            self.logger.warning("WebSocket is already started.")
            return

        # Connect
        if not self.ws:
            self.ws = WebSocketApp(self.url, header=self.headers,
                                   on_open=self._on_open,
                                   on_message=self._on_message,
                                   on_error=self._on_error,
                                   on_close=self._on_close)
        else:
            self.ws.url = self.url
            self.ws.header = self.headers

        # (run_forever() will raise an exception if previous socket is still not closed)
        self.logger.debug("Start WebSocket with url: %s" % self.ws.url)
        self.is_started = True
        self.thread = Thread(target=self.ws.run_forever)
        self.thread.daemon = True
        self.thread.start()

    def reconnect(self):
        self.logger.debug("Reconnect WebSocket")
        self.close()
        self.connect()

    def close(self):
        if not self.is_started:
            # Nothing to close
            return

        self.logger.debug("Close WebSocket")
        # (If called directly or from _on_close())
        self.is_started = False
        if self.is_connected:
            # (If called directly)
            self.ws.close()

        super().close()

    def _on_open(self):
        self.logger.debug("On open. %s", "Connected." if self.is_connected else "NOT CONNECTED. It's impossible!")

        # (Stop reconnecting)
        self._is_reconnecting = False
        self._reconnect_tries = 0

        if self.on_connect:
            self.on_connect()

        # Subscribe by command on connect
        if self.IS_SUBSCRIPTION_COMMAND_SUPPORTED and not self.is_subscribed_with_url:
            self.subscribe()

    def _on_message(self, message):
        self.logger.debug("On message: %s", message[:200])
        # str -> json
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            self.logger.error("Wrong JSON is received! Skipped. message: %s", message)
            return

        # json -> items
        result = self._parse(None, data)

        # Process items
        self._data_buffer = []

        if result and isinstance(result, list):
            for item in result:
                self.on_item_received(item)
        else:
            self.on_item_received(result)

        if self.on_data and self._data_buffer:
            self.on_data(self._data_buffer)

    def _parse(self, endpoint, data):
        if data and isinstance(data, list):
            return [self.converter.parse(endpoint, data_item) for data_item in data]
        return self.converter.parse(endpoint, data)

    def on_item_received(self, item):
        # To skip empty and unparsed data
        if self.on_data_item and isinstance(item, DataObject):
            self.on_data_item(item)
            self._data_buffer.append(item)

    def _on_error(self, error_exc):
        self.logger.exception("On error exception from websockets: %s", error_exc)
        pass

    def _on_close(self):
        self.logger.info("On WebSocket close")

        if self.on_disconnect:
            self.on_disconnect()

        if self.is_started or (self._is_reconnecting and self._reconnect_tries < self.reconnect_count):
            self._is_reconnecting = True
            if self._reconnect_tries == 0:
                # Don't wait before the first reconnection try
                time.sleep(self.reconnect_delay_sec)
            self._reconnect_tries += 1
            self.reconnect()
            return
        self._is_reconnecting = False

        self.close()

    def _send(self, data):
        if not data:
            return

        message = json.dumps(data)
        self.logger.debug("Send message: %s", message)
        self.ws.send(message)

    # Processing
