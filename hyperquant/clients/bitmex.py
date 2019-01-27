import hashlib
import hmac
import json
import time
import urllib

from hyperquant.api import Platform, Sorting, Direction
from hyperquant.clients import WSClient, Trade, Error, ErrorCode, Endpoint, \
    ParamName, WSConverter, RESTConverter, PlatformRESTClient, PrivatePlatformRESTClient, ItemObject


# REST

class BitMEXRESTConverterV1(RESTConverter):
    """
    Go https://www.bitmex.com/api/v1/schema for whole API schema with param types keys
    which help to distinguish items from each other (for updates and removing).
    """

    # Main params:
    base_url = "https://www.bitmex.com/api/v{version}"

    IS_SORTING_ENABLED = True

    # Settings:

    # Converting info:
    # For converting to platform
    endpoint_lookup = {
        Endpoint.TRADE: "trade",
        Endpoint.TRADE_HISTORY: "trade",
    }
    param_name_lookup = {
        ParamName.LIMIT: "count",
        ParamName.SORTING: "reverse",
        ParamName.FROM_ITEM: "startTime",
        ParamName.TO_ITEM: "endTime",
        ParamName.FROM_TIME: "startTime",
        ParamName.TO_TIME: "endTime",
    }
    param_value_lookup = {
        Sorting.ASCENDING: "false",
        Sorting.DESCENDING: "true",
        Sorting.DEFAULT_SORTING: Sorting.ASCENDING,
    }
    max_limit_by_endpoint = {
        Endpoint.TRADE: 500,
        Endpoint.TRADE_HISTORY: 500,
    }

    # For parsing
    param_lookup_by_class = {
        Error: {
            "name": "code",
            "message": "message",
        },
        Trade: {
            "trdMatchID": ParamName.ITEM_ID,
            "timestamp": ParamName.TIMESTAMP,
            "symbol": ParamName.SYMBOL,
            "price": ParamName.PRICE,
            "size": ParamName.AMOUNT,
            "side": ParamName.DIRECTION,
        },
    }

    error_code_by_platform_error_code = {
        # "": ErrorCode.UNAUTHORIZED,
        "Unknown symbol": ErrorCode.WRONG_SYMBOL,
        # "ERR_RATE_LIMIT": ErrorCode.RATE_LIMIT,
    }
    error_code_by_http_status = {
        400: ErrorCode.WRONG_PARAM,
        401: ErrorCode.UNAUTHORIZED,
        429: ErrorCode.RATE_LIMIT,  #?
    }

    # For converting time
    is_source_in_timestring = True
    timestamp_platform_names = ["startTime", "endTime"]

    def _process_param_value(self, name, value):
        if name == ParamName.FROM_ITEM or name == ParamName.TO_ITEM:
            if isinstance(value, ItemObject):
                timestamp = value.timestamp
                if name == ParamName.TO_ITEM:
                    # Make to_item an including param (for BitMEX it's excluding)
                    timestamp += (1000 if value.is_milliseconds else 1)
                return timestamp
        return super()._process_param_value(name, value)

    def _parse_item(self, endpoint, item_data):
        result = super()._parse_item(endpoint, item_data)

        # (For Trade)
        if hasattr(result, ParamName.SYMBOL) and result.symbol[0] == ".":
            # # ".ETHUSD" -> "ETHUSD"
            # result.symbol = result.symbol[1:]
            # https://www.bitmex.com/api/explorer/#!/Trade/Trade_get Please note
            # that indices (symbols starting with .) post trades at intervals to
            # the trade feed. These have a size of 0 and are used only to indicate
            # a changing price.
            return None

        # Convert direction
        if result and isinstance(result, Trade):
            result.direction = Direction.BUY if result.direction == "Buy" else (
                Direction.SELL if result.direction == "Sell" else None)
            result.price = str(result.price)
            result.amount = str(result.amount)
        return result

    def parse_error(self, error_data=None, response=None):
        if error_data and "error" in error_data:
            error_data = error_data["error"]
            if "Maximum result count is 500" in error_data["message"]:
                error_data["name"] = ErrorCode.WRONG_LIMIT
        result = super().parse_error(error_data, response)
        return result


class BitMEXRESTClient(PrivatePlatformRESTClient):
    platform_id = Platform.BITMEX
    version = "1"  # Default version

    IS_NONE_SYMBOL_FOR_ALL_SYMBOLS = True

    _converter_class_by_version = {
        "1": BitMEXRESTConverterV1,
    }

    def _on_response(self, response, result):
        # super()._on_response(response)

        if not response.ok and "Retry-After" in response.headers:
            self.delay_before_next_request_sec = int(response.headers["Retry-After"])
        else:
            # "x-ratelimit-limit": 300
            # "x-ratelimit-remaining": 297
            # "x-ratelimit-reset": 1489791662
            try:
                ratelimit = int(response.headers["x-ratelimit-limit"])
                remaining_requests = float(response.headers["x-ratelimit-remaining"])
                reset_ratelimit_timestamp = int(response.headers["x-ratelimit-reset"])
                if remaining_requests < ratelimit * 0.1:
                    precision_sec = 1  # Current machine time may not precise which can cause ratelimit error
                    self.delay_before_next_request_sec = reset_ratelimit_timestamp - time.time() + precision_sec
                else:
                    self.delay_before_next_request_sec = 0
                self.logger.debug("Ratelimit info. remaining_requests: %s/%s delay: %s",
                                  remaining_requests, ratelimit, self.delay_before_next_request_sec)
            except Exception as error:
                self.logger.exception("Error while defining delay_before_next_request_sec.", error)

    def get_symbols(self, version=None):
        # BitMEX has no get_symbols method in API,
        # and None means "all symbols" if defined as symbol param.
        return None

    # If symbol not specified all symbols will be returned
    # todo fetch_latest_trades()
    def fetch_trades(self, symbol=None, limit=None, **kwargs):
        # symbol = None
        return super().fetch_trades(symbol, limit, **kwargs)

    # If symbol not specified all symbols will be returned
    def fetch_trades_history(self, symbol=None, limit=None, from_item=None,
                           sorting=None, from_time=None, to_time=None, **kwargs):
        # Note: from_item used automatically for paging; from_time and to_time - used for custom purposes
        return super().fetch_trades_history(symbol, limit, from_item, sorting=sorting,
                                          from_time=from_time, to_time=to_time, **kwargs)

    # tickers are in instruments


# WebSockets

class BitMEXWSConverterV1(WSConverter):
    # Main params:
    base_url = "wss://www.bitmex.com/realtime"

    IS_SUBSCRIPTION_COMMAND_SUPPORTED = True

    # # symbol_endpoints = ["execution", "instrument", "order", "orderBookL2", "position", "quote", "trade"]
    # # supported_endpoints = symbolSubs + ["margin"]
    # supported_endpoints = [Endpoint.TRADE]
    # symbol_endpoints = [Endpoint.TRADE]

    # Settings:

    # Converting info:
    # For converting to platform
    endpoint_lookup = {
        Endpoint.TRADE: "trade:{symbol}",
        # Endpoint.TRADE: lambda params: "trade:" + params[Param.SYMBOL] if Param.SYMBOL in params else "trade",
    }

    # For parsing
    param_lookup_by_class = {
        Error: {
            "status": "code",
            "error": "message",
        },
        Trade: {
            "trdMatchID": ParamName.ITEM_ID,
            "timestamp": ParamName.TIMESTAMP,
            "symbol": ParamName.SYMBOL,
            "price": ParamName.PRICE,
            "size": ParamName.AMOUNT,
            "side": ParamName.DIRECTION,
        },
    }
    event_type_param = "table"

    # error_code_by_platform_error_code = {
    #     # # "": ErrorCode.UNAUTHORIZED,
    #     # "Unknown symbol": ErrorCode.WRONG_SYMBOL,
    #     # # "ERR_RATE_LIMIT": ErrorCode.RATE_LIMIT,
    # }

    # For converting time
    is_source_in_timestring = True
    # timestamp_platform_names = []

    def parse(self, endpoint, data):
        if data:
            endpoint = data.get(self.event_type_param)
            if "error" in data:
                result = self.parse_error(data)
                if "request" in data:
                    result.message += "request: " + json.dumps(data["request"])
                return result
            if "data" in data:
                data = data["data"]
        return super().parse(endpoint, data)

    def _parse_item(self, endpoint, item_data):
        result = super()._parse_item(endpoint, item_data)

        # (For Trade)
        if hasattr(result, ParamName.SYMBOL) and result.symbol[0] == ".":
            # # ".ETHUSD" -> "ETHUSD"
            # result.symbol = result.symbol[1:]
            # https://www.bitmex.com/api/explorer/#!/Trade/Trade_get Please note
            # that indices (symbols starting with .) post trades at intervals to
            # the trade feed. These have a size of 0 and are used only to indicate
            # a changing price.
            return None

        # Convert direction
        if result and isinstance(result, Trade):
            result.direction = Direction.BUY if result.direction == "Buy" else (
                Direction.SELL if result.direction == "Sell" else None)
            result.price = str(result.price)
            result.amount = str(result.amount)
        return result


class BitMEXWSClient(WSClient):
    platform_id = Platform.BITMEX
    version = "1"  # Default version

    _converter_class_by_version = {
        "1": BitMEXWSConverterV1,
    }

    @property
    def url(self):
        self.is_subscribed_with_url = True
        params = {"subscribe": ",".join(self.current_subscriptions)}
        url, platform_params = self.converter.make_url_and_platform_params(params=params, is_join_get_params=True)
        return url

    @property
    def headers(self):
        result = super().headers or []
        # Return auth headers
        if self._api_key:
            self.logger.info("Authenticating with API Key.")
            # To auth to the WS using an API key, we generate
            # a signature of a nonce and the WS API endpoint.
            expire = generate_nonce()
            result += [
                "api-expires: " + str(expire),
            ]
            if self._api_key and self._api_secret:
                signature = generate_signature(self._api_secret, "GET", "/realtime", expire, "")
                result += [
                    "api-signature: " + signature,
                    "api-key: " + self._api_key,
                ]
        else:
            self.logger.info("Not authenticating by headers because api_key is not set.")

        return result

    # def _on_message(self, message):
    #     """Handler for parsing WS messages."""
    #     self.logger.debug(message)
    #     message = json.loads(message)

    # def on_item_received(self, item):
    #     super().on_item_received(item)
    #
    #     # table = message["table"] if "table" in message else None
    #     # action = message["action"] if "action" in message else None
    #     # try:
    #     #     if "subscribe" in message:
    #     #         self.logger.debug("Subscribed to %s." % message["subscribe"])
    #     #     elif action:
    #     #
    #     #         if table not in self.data:
    #     #             self.data[table] = []
    #     #
    #     #         # There are four possible actions from the WS:
    #     #         # "partial" - full table image
    #     #         # "insert"  - new row
    #     #         # "update"  - update row
    #     #         # "delete"  - delete row
    #     #         if action == "partial":
    #     #             self.logger.debug("%s: partial" % table)
    #     #             self.data[table] += message["data"]
    #     #             # Keys are communicated on partials to let you know how to uniquely identify
    #     #             # an item. We use it for updates.
    #     #             self.keys[table] = message["keys"]
    #     #         elif action == "insert":
    #     #             self.logger.debug("%s: inserting %s" % (table, message["data"]))
    #     #             self.data[table] += message["data"]
    #     #
    #     #             # Limit the max length of the table to avoid excessive memory usage.
    #     #             # Don't trim orders because we'll lose valuable state if we do.
    #     #             if table not in ["order", "orderBookL2"] and len(self.data[table]) > BitMEXWebsocket.MAX_TABLE_LEN:
    #     #                 self.data[table] = self.data[table][int(BitMEXWebsocket.MAX_TABLE_LEN / 2):]
    #     #
    #     #         elif action == "update":
    #     #             self.logger.debug("%s: updating %s" % (table, message["data"]))
    #     #             # Locate the item in the collection and update it.
    #     #             for updateData in message["data"]:
    #     #                 item = findItemByKeys(self.keys[table], self.data[table], updateData)
    #     #                 if not item:
    #     #                     return  # No item found to update. Could happen before push
    #     #                 item.update(updateData)
    #     #                 # Remove cancelled / filled orders
    #     #                 if table == "order" and item["leavesQty"] <= 0:
    #     #                     self.data[table].remove(item)
    #     #         elif action == "delete":
    #     #             self.logger.debug("%s: deleting %s" % (table, message["data"]))
    #     #             # Locate the item in the collection and remove it.
    #     #             for deleteData in message["data"]:
    #     #                 item = findItemByKeys(self.keys[table], self.data[table], deleteData)
    #     #                 self.data[table].remove(item)
    #     #         else:
    #     #             raise Exception("Unknown action: %s" % action)
    #     # except:
    #     #     self.logger.error(traceback.format_exc())

    # def get_instrument(self):
    #     """Get the raw instrument data for this symbol."""
    #     # Turn the "tickSize" into "tickLog" for use in rounding
    #     instrument = self.data["instrument"][0]
    #     instrument["tickLog"] = int(math.fabs(math.log10(instrument["tickSize"])))
    #     return instrument
    #
    # def get_ticker(self):
    #     """Return a ticker object. Generated from quote and trade."""
    #     lastQuote = self.data["quote"][-1]
    #     lastTrade = self.data["trade"][-1]
    #     ticker = {
    #         "last": lastTrade["price"],
    #         "buy": lastQuote["bidPrice"],
    #         "sell": lastQuote["askPrice"],
    #         "mid": (float(lastQuote["bidPrice"] or 0) + float(lastQuote["askPrice"] or 0)) / 2
    #     }
    #
    #     # The instrument has a tickSize. Use it to round values.
    #     instrument = self.data["instrument"][0]
    #     return {k: round(float(v or 0), instrument["tickLog"]) for k, v in ticker.items()}
    #
    # def funds(self):
    #     """Get your margin details."""
    #     return self.data["margin"][0]
    #
    # def market_depth(self):
    #     """Get market depth (orderbook). Returns all levels."""
    #     return self.data["orderBookL2"]
    #
    # def open_orders(self, clOrdIDPrefix):
    #     """Get all your open orders."""
    #     orders = self.data["order"]
    #     # Filter to only open orders (leavesQty > 0) and those that we actually placed
    #     return [o for o in orders if str(o["clOrdID"]).startswith(clOrdIDPrefix) and o["leavesQty"] > 0]
    #
    # def recent_trades(self):
    #     """Get recent trades."""
    #     return self.data["trade"]

    def _send_subscribe(self, subscriptions):
        self._send_command("subscribe", subscriptions)

    def _send_unsubscribe(self, subscriptions):
        self._send_command("unsubscribe", subscriptions)

    def _send_command(self, command, params=None):
        if params is None:
            params = []
        self._send({"op": command, "args": list(params)})


# Utility

def generate_nonce():
    return int(round(time.time() + 3600))


def generate_signature(secret, method, url, nonce, data):
    """
    Generates an API signature compatible with BitMEX..
    A signature is HMAC_SHA256(secret, method + path + nonce + data), hex encoded.
    Verb must be uppercased, url is relative, nonce must be an increasing 64-bit integer
    and the data, if present, must be JSON without whitespace between keys.

    For example, in pseudocode (and in real code below):
        method=POST
        url=/api/v1/order
        nonce=1416993995705
        data={"symbol":"XBTZ14","quantity":1,"price":395.01}
        signature = HEX(HMAC_SHA256(secret, 'POST/api/v1/order1416993995705{"symbol":"XBTZ14","amount":1,"price":395.01}'))
    """
    # Parse the url so we can remove the base and extract just the path.
    parsed_url = urllib.parse.urlparse(url)
    path = parsed_url.path
    if parsed_url.query:
        path = path + '?' + parsed_url.query

    # print "Computing HMAC: %s" % verb + path + str(nonce) + data
    message = (method + path + str(nonce) + data).encode('utf-8')

    signature = hmac.new(secret.encode('utf-8'), message, digestmod=hashlib.sha256).hexdigest()
    return signature
