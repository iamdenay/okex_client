import hashlib
import hmac
import time

from hyperquant.api import Platform, Sorting, Direction
from hyperquant.clients import Endpoint, WSClient, Trade, ParamName, Error, \
    ErrorCode, Channel, \
    Info, WSConverter, RESTConverter, PlatformRESTClient, PrivatePlatformRESTClient


# https://docs.bitfinex.com/v1/docs
# https://docs.bitfinex.com/v2/docs

# REST

class BitfinexRESTConverterV1(RESTConverter):
    # Main params:
    base_url = "https://api.bitfinex.com/v{version}/"

    IS_SORTING_ENABLED = False

    # Settings:

    # Converting info:
    # For converting to platform
    endpoint_lookup = {
        Endpoint.TRADE: "trades/{symbol}",
        Endpoint.TRADE_HISTORY: "trades/{symbol}",  # same, not implemented for this version
    }
    param_name_lookup = {
        ParamName.LIMIT: "limit_trades",
        ParamName.IS_USE_MAX_LIMIT: None,
        ParamName.SORTING: None,  # not supported
        ParamName.FROM_ITEM: "timestamp",
        ParamName.TO_ITEM: "timestamp",  # ?
        ParamName.FROM_TIME: "timestamp",
        ParamName.TO_TIME: None,  # ?
    }
    param_value_lookup = {
        # Sorting.ASCENDING: None,
        # Sorting.DESCENDING: None,
        Sorting.DEFAULT_SORTING: Sorting.DESCENDING,
    }
    max_limit_by_endpoint = {
        Endpoint.TRADE: 1000,
        Endpoint.TRADE_HISTORY: 1000,  # same, not implemented for this version
    }

    # For parsing

    param_lookup_by_class = {
        Error: {
            "message": "code",
            # "error": "code",
            # "message": "message",
        },
        Trade: {
            "tid": ParamName.ITEM_ID,
            "timestamp": ParamName.TIMESTAMP,
            "price": ParamName.PRICE,
            "amount": ParamName.AMOUNT,
            "type": ParamName.DIRECTION,
        },
    }

    error_code_by_platform_error_code = {
        # "": ErrorCode.UNAUTHORIZED,
        "Unknown symbol": ErrorCode.WRONG_SYMBOL,
        # "ERR_RATE_LIMIT": ErrorCode.RATE_LIMIT,
    }
    error_code_by_http_status = {
        429: ErrorCode.RATE_LIMIT,
    }

    # For converting time
    # is_source_in_milliseconds = True
    timestamp_platform_names = [ParamName.TIMESTAMP]

    def prepare_params(self, endpoint=None, params=None):
        resources, platform_params = super().prepare_params(endpoint, params)

        # (SYMBOL was used in URL path) (not necessary)
        if platform_params and ParamName.SYMBOL in platform_params:
            del platform_params[ParamName.SYMBOL]
        return resources, platform_params

    def parse(self, endpoint, data):
        if data and endpoint == Endpoint.SYMBOLS:
            return [item.upper() for item in data]
        return super().parse(endpoint, data)

    def _parse_item(self, endpoint, item_data):
        result = super()._parse_item(endpoint, item_data)

        # Convert Trade.direction
        if result and isinstance(result, Trade) and result.direction:
            # (Can be of "sell"|"buy|"")
            result.direction = Direction.SELL if result.direction == "sell" else \
                (Direction.BUY if result.direction == "buy" else None)

        return result


class BitfinexRESTConverterV2(RESTConverter):
    # Main params:
    base_url = "https://api.bitfinex.com/v{version}/"
    IS_SORTING_ENABLED = True

    # Settings:

    # Converting info:
    # For converting to platform
    endpoint_lookup = {
        Endpoint.TRADE: "trades/t{symbol}/hist",  # same, not implemented for this version
        Endpoint.TRADE_HISTORY: "trades/t{symbol}/hist",
    }
    param_name_lookup = {
        ParamName.LIMIT: "limit",
        ParamName.IS_USE_MAX_LIMIT: None,
        ParamName.SORTING: "sort",
        ParamName.FROM_ITEM: "start",
        ParamName.TO_ITEM: "end",
        ParamName.FROM_TIME: "start",
        ParamName.TO_TIME: "end",
    }
    param_value_lookup = {
        Sorting.ASCENDING: 1,
        Sorting.DESCENDING: 0,
        Sorting.DEFAULT_SORTING: Sorting.DESCENDING,
    }
    max_limit_by_endpoint = {
        Endpoint.TRADE: 1000,  # same, not implemented for this version
        Endpoint.TRADE_HISTORY: 1000,
    }

    # For parsing
    param_lookup_by_class = {
        # ["error",10020,"limit: invalid"]
        Error: ["", "code", "message"],
        # on trading pairs (ex. tBTCUSD) [ID, MTS, AMOUNT, PRICE]
        # [305430435,1539757383787,-0.086154,6760.7]
        # (on funding currencies (ex. fUSD) [ID, MTS, AMOUNT, RATE, PERIOD]) - not used now
        Trade: [ParamName.ITEM_ID, ParamName.TIMESTAMP, ParamName.AMOUNT, ParamName.PRICE],
    }

    error_code_by_platform_error_code = {
        # "": ErrorCode.UNAUTHORIZED,
        10020: ErrorCode.WRONG_LIMIT,
        11010: ErrorCode.RATE_LIMIT,
    }
    error_code_by_http_status = {}

    # For converting time
    is_source_in_milliseconds = True
    timestamp_platform_names = ["start", "end"]

    def prepare_params(self, endpoint=None, params=None):
        # # Symbol needs "t" prefix for trading pair
        # if ParamName.SYMBOL in params:
        #     params[ParamName.SYMBOL] = "t" + str(params[ParamName.SYMBOL])

        resources, platform_params = super().prepare_params(endpoint, params)

        # (SYMBOL was used in URL path) (not necessary)
        if platform_params and ParamName.SYMBOL in platform_params:
            del platform_params[ParamName.SYMBOL]
        return resources, platform_params

    def _process_param_value(self, name, value):
        # # Symbol needs "t" prefix for trading pair
        # if name == ParamName.SYMBOL and value:
        #     return "t" + value
        # elif
        if name == ParamName.FROM_ITEM or name == ParamName.TO_ITEM:
            if isinstance(value, Trade):
                return value.timestamp

        return super()._process_param_value(name, value)

    def _parse_item(self, endpoint, item_data):
        result = super()._parse_item(endpoint, item_data)

        if result and isinstance(result, Trade):
            # Determine direction
            result.direction = Direction.BUY if result.amount > 0 else Direction.SELL
            # Stringify and check sign
            result.price = str(result.price)
            result.amount = str(result.amount) if result.amount > 0 else str(-result.amount)
        return result

    def parse_error(self, error_data=None, response=None):
        result = super().parse_error(error_data, response)

        if error_data and isinstance(error_data, dict) and "error" in error_data:
            if error_data["error"] == "ERR_RATE_LIMIT":
                result.error_code = ErrorCode.RATE_LIMIT
                result.message = ErrorCode.get_message_by_code(result.code) + result.message
        return result


class BitfinexRESTClient(PrivatePlatformRESTClient):
    platform_id = Platform.BITFINEX
    version = "2"  # Default version
    _converter_class_by_version = {
        "1": BitfinexRESTConverterV1,
        "2": BitfinexRESTConverterV2,
    }

    def get_symbols(self, version=None):
        self.logger.info("Note: Bitfinex supports get_symbols only in v1.")
        return super().get_symbols(version="1")

    # # after_timestamp param can be added for v1, and after_timestamp, before_timestamp for v2
    # def fetch_trades(self, symbol, limit=None, **kwargs):
    #     return super().fetch_trades(symbol, limit, **kwargs)

    # v1: Same as fetch_trades(), but result can be only reduced, but not extended
    def fetch_trades_history(self, symbol, limit=None, from_item=None,
                           sorting=None, from_time=None, to_time=None, **kwargs):
        if from_item and self.version == "1":
            # todo check
            self.logger.warning("Bitfinex v1 API has no trades-history functionality.")
            return None
        # return self.fetch_trades(symbol, limit, **kwargs)
        return super().fetch_trades_history(symbol, limit, from_item, sorting=sorting,
                                          from_time=from_time, to_time=to_time, **kwargs)

    def _on_response(self, response, result):
        # super()._on_response(response)

        if not response.ok and "Retry-After" in response.headers:
            self.delay_before_next_request_sec = int(response.headers["Retry-After"])
        elif isinstance(result, Error):
            if result.code == ErrorCode.RATE_LIMIT:
                # Bitfinex API access is rate limited. The rate limit applies if an
                # IP address exceeds a certain number of requests per minute. The current
                # limit is between 10 and 45 to a specific REST API endpoint (ie. /ticker).
                # In case a client reaches the limit, we block the requesting IP address
                # for 10-60 seconds on that endpoint. The API will return the JSON response
                # {"error": "ERR_RATE_LIMIT"}. These DDoS defenses may change over time to
                # further improve reliability.
                self.delay_before_next_request_sec = 60
            else:
                self.delay_before_next_request_sec = 10


# WebSocket


class BitfinexWSConverterV2(WSConverter):
    # Main params:
    base_url = "wss://api.bitfinex.com/ws/{version}/"

    IS_SUBSCRIPTION_COMMAND_SUPPORTED = True

    # supported_endpoints = [Endpoint.TRADE]
    # symbol_endpoints = [Endpoint.TRADE]
    # supported_symbols = None

    # Settings:

    # Converting info:
    # For converting to platform
    endpoint_lookup = {
        Endpoint.TRADE: "trades",
    }

    # For parsing
    item_class_by_endpoint = dict(**WSConverter.item_class_by_endpoint, **{
        # Item class by event type
        "error": Error,
        "info": Info,
        "subscribed": Channel,
    })
    param_lookup_by_class = {
        Error: {
            "code": "code",
            "msg": "message",
        },
        Info: {
            "code": "code",
            "msg": "message",
        },
        Channel: {
            "chanId": "channel_id",
            "channel": "channel",
            "pair": ParamName.SYMBOL,
        },
        #
        Trade: [ParamName.ITEM_ID, ParamName.TIMESTAMP, ParamName.AMOUNT, ParamName.PRICE],
    }

    # https://docs.bitfinex.com/v2/docs/abbreviations-glossary
    # 10300 : Subscription failed (generic)
    # 10301 : Already subscribed
    # 10302 : Unknown channel
    # 10400 : Unsubscription failed (generic)
    # 10401 : Not subscribed
    # errors = {10000: 'Unknown event',
    #           10001: 'Generic error',
    #           10008: 'Concurrency error',
    #           10020: 'Request parameters error',
    #           10050: 'Configuration setup failed',
    #           10100: 'Failed authentication',
    #           10111: 'Error in authentication request payload',
    #           10112: 'Error in authentication request signature',
    #           10113: 'Error in authentication request encryption',
    #           10114: 'Error in authentication request nonce',
    #           10200: 'Error in un-authentication request',
    #           10300: 'Subscription Failed (generic)',
    #           10301: 'Already Subscribed',
    #           10302: 'Unknown channel',
    #           10400: 'Subscription Failed (generic)',
    #           10401: 'Not subscribed',
    #           11000: 'Not ready, try again later',
    #           20000: 'User is invalid!',
    #           20051: 'Websocket server stopping',
    #           20060: 'Websocket server resyncing',
    #           20061: 'Websocket server resync complete'
    #           }
    error_code_by_platform_error_code = {
        # 10000: ErrorCode.WRONG_EVENT,
        10001: ErrorCode.WRONG_SYMBOL,
        # 10305: ErrorCode.CHANNEL_LIMIT,
    }
    event_type_param = "event"

    # For converting time
    is_source_in_milliseconds = True

    def __init__(self, platform_id=None, version=None):
        self.channel_by_id = {}
        super().__init__(platform_id, version)

    def _generate_subscription(self, endpoint, symbol=None, **params):
        channel = super()._generate_subscription(endpoint, symbol, **params)
        return (channel, symbol)

    def parse(self, endpoint, data):
        # if data:
        #     endpoint = data.get(self.event_type_param)
        #     if "data" in data:
        #         data = data["data"]
        if isinstance(data, list):
            # [284792,[[306971149,1540470353199,-0.76744631,0.031213],...] (1)
            # todo add tests
            # or [102165,"te",[306995378,1540485961266,-0.216139,0.031165]]
            # or [102165,"tu",[306995378,1540485961266,-0.216139,0.031165]] (2)
            channel_id = data[0]
            channel = self.channel_by_id.get(channel_id)
            if channel:
                # Get endpoint by channel
                endpoint = None
                for k, v in self.endpoint_lookup.items():
                    if v == channel.channel:
                        endpoint = k

                # Parse
                if data[1] == "tu":
                    # Skip "tu" as an item have been already added as "te"
                    return None
                # if data[1] == "te":
                #     # Skip "te" as an item has no id yet, waiting for "tu" (actually there is an id already)
                #     return None

                # (data[1] - for v1, data[1] or [data[2]] - for v2, see above (1) and (2) examples)
                real_data = data[1] if isinstance(data[1], list) else [data[2]]

                result = super().parse(endpoint, real_data)

                # Set symbol
                for item in result:
                    if hasattr(item, ParamName.SYMBOL):
                        item.symbol = channel.symbol
                return result

        return super().parse(endpoint, data)

    def _parse_item(self, endpoint, item_data):
        result = super()._parse_item(endpoint, item_data)

        if isinstance(result, Channel):
            self.channel_by_id[result.channel_id] = result
        elif result and isinstance(result, Trade):
            if result.symbol and result.symbol.begins_with("."):
                return None

            if not result.item_id:
                result.item_id = "%s_%s_%s" % (result.timestamp, result.price, result.amount)
            # Determine direction
            result.direction = Direction.BUY if result.amount > 0 else Direction.SELL
            # Stringify and check sign
            result.price = str(result.price)
            result.amount = str(result.amount) if result.amount > 0 else str(-result.amount)
        return result


# (not necessary)
class BitfinexWSConverterV1(BitfinexWSConverterV2):
    # Main params:
    base_url = "wss://api.bitfinex.com/ws/{version}/"

    # # Settings:
    #
    # # Converting info:
    # # For converting to platform
    # endpoint_lookup = {
    #     Endpoint.TRADE: "trades",
    # }

    # For parsing
    param_lookup_by_class = {
        Error: {
            "code": "code",
            "msg": "message",
        },
        Info: {
            "code": "code",
            "msg": "message",
        },
        Channel: {
            "channel": "channel",
            "chanId": "channel_id",
            "pair": ParamName.SYMBOL,
        },
        # [ 5, "te", "1234-BTCUSD", 1443659698, 236.42, 0.49064538 ]
        # Trade: ["", "", ParamName.ITEM_ID, ParamName.TIMESTAMP, ParamName.PRICE, ParamName.AMOUNT],
        Trade: [ParamName.ITEM_ID, ParamName.TIMESTAMP, ParamName.PRICE, ParamName.AMOUNT],
    }

    # # 10300 : Subscription failed (generic)
    # # 10301 : Already subscribed
    # # 10302 : Unknown channel
    # # 10400 : Unsubscription failed (generic)
    # # 10401 : Not subscribed
    # error_code_by_platform_error_code = {
    #     # 10000: ErrorCode.WRONG_EVENT,
    #     10001: ErrorCode.WRONG_SYMBOL,
    # }
    #
    # # For converting time
    # # is_source_in_milliseconds = True

    # def parse_item(self, endpoint, item_data):
    #     result = super().parse_item(endpoint, item_data)
    #
    #     # Convert Channel.symbol "tXXXYYY" -> "XXXYYY"
    #     if result and isinstance(result, Channel) and result.symbol:
    #         if result.symbol[0] == "t":
    #             result.symbol = result.symbol[1:]
    #
    #     return result


class BitfinexWSClient(WSClient):
    # TODO consider reconnection and resubscription
    # TODO consider reconnect on connection, pong and other timeouts

    # Settings:
    platform_id = Platform.BITFINEX
    version = "2"  # Default version

    _converter_class_by_version = {
        "1": BitfinexWSConverterV1,
        "2": BitfinexWSConverterV2,
    }

    # State:

    def _send_subscribe(self, subscriptions):
        for channel, symbol in subscriptions:
            trading_pair_symbol = "t" + symbol
            event_data = {
                "event": "subscribe",
                "channel": channel,
                "symbol": trading_pair_symbol}
            self._send(event_data)

    def _parse(self, endpoint, data):
        if isinstance(data, list) and len(data) > 1 and data[1] == "hb":
            # Heartbeat. skip for now...
            return None
        return super()._parse(endpoint, data)

    # Закомментированные методы можно свободно удалять, если проще переносить код из другой библиотеки заново

    # def on_item_received(self, item):
    #     # if isinstance(item, Channel):
    #     #     self.channel_by_id[item.channel_id] = item
    #     #     return
    #     #
    #     super().on_item_received(item)
    #
    #     # # Handle data
    #     # if isinstance(data, dict):
    #     #     # This is a system message
    #     #     self._system_handler(data, received_at)
    #     # else:
    #     #     # This is a list of data
    #     #     if data[1] == 'hb':
    #     #         self._heartbeat_handler()
    #     #     else:
    #     #         self._data_handler(data, received_at)

    # def _system_handler(self, data, ts):
    #     """Distributes system messages to the appropriate handler.
    #     System messages include everything that arrives as a dict,
    #     or a list containing a heartbeat.
    #     :param data:
    #     :param ts:
    #     :return:
    #     """
    #     self.log.debug("_system_handler(): Received a system message: %s", data)
    #     # Unpack the data
    #     event = data.pop('event')
    #     if event == 'pong':
    #         self.log.debug("_system_handler(): Distributing %s to _pong_handler..",
    #                        data)
    #         self._pong_handler()
    #     elif event == 'info':
    #         self.log.debug("_system_handler(): Distributing %s to _info_handler..",
    #                        data)
    #         self._info_handler(data)
    #     elif event == 'error':
    #         self.log.debug("_system_handler(): Distributing %s to _error_handler..",
    #                        data)
    #         self._error_handler(data)
    #     elif event in ('subscribed', 'unsubscribed', 'conf', 'auth', 'unauth'):
    #         self.log.debug("_system_handler(): Distributing %s to "
    #                        "_response_handler..", data)
    #         self._response_handler(event, data, ts)
    #     else:
    #         self.log.error("Unhandled event: %s, data: %s", event, data)

    #     if event_name in ('subscribed', 'unsubscribed', 'conf', 'auth', 'unauth'):
    #         try:
    #             self._response_handlers[event_name](event_name, data, ts)
    #         except KeyError:
    #             self.log.error("Dtype '%s' does not have a response "
    #                            "handler! (%s)", event_name, message)
    #     elif event_name == 'data':
    #         try:
    #             channel_id = data[0]
    #             if channel_id != 0:
    #                 # Get channel type associated with this data to the
    #                 # associated data type (from 'data' to
    #                 # 'book', 'ticker' or similar
    #                 channel_type, *_ = self.channel_directory[channel_id]
    #
    #                 # Run the associated data handler for this channel type.
    #                 self._data_handlers[channel_type](channel_type, data, ts)
    #                 # Update time stamps.
    #                 self.update_timestamps(channel_id, ts)
    #             else:
    #                 # This is data from auth channel, call handler
    #                 self._handle_account(data=data, ts=ts)
    #         except KeyError:
    #             self.log.error("Channel ID does not have a data handler! %s",
    #                            message)
    #     else:
    #         self.log.error("Unknown event_name on queue! %s", message)
    #         continue

    #     self._response_handlers = {'unsubscribed': self._handle_unsubscribed,
    #                                'subscribed': self._handle_subscribed,
    #                                'conf': self._handle_conf,
    #                                'auth': self._handle_auth,
    #                                'unauth': self._handle_auth}
    #     self._data_handlers = {'ticker': self._handle_ticker,
    #                            'book': self._handle_book,
    #                            'raw_book': self._handle_raw_book,
    #                            'candles': self._handle_candles,
    #                            'trades': self._handle_trades}

    # https://github.com/Crypto-toolbox/btfxwss/blob/master/btfxwss/queue_processor.py

    # def _handle_subscribed(self, dtype, data, ts,):
    #     """Handles responses to subscribe() commands.
    #     Registers a channel id with the client and assigns a data handler to it.
    #     :param dtype:
    #     :param data:
    #     :param ts:
    #     :return:
    #     """
    #     self.log.debug("_handle_subscribed: %s - %s - %s", dtype, data, ts)
    #     channel_name = data.pop('channel')
    #     channel_id = data.pop('chanId')
    #     config = data
    #
    #     if 'pair' in config:
    #         symbol = config['pair']
    #         if symbol.startswith('t'):
    #             symbol = symbol[1:]
    #     elif 'symbol' in config:
    #         symbol = config['symbol']
    #         if symbol.startswith('t'):
    #             symbol = symbol[1:]
    #     elif 'key' in config:
    #         symbol = config['key'].split(':')[2][1:]  #layout type:interval:tPair
    #     else:
    #         symbol = None
    #
    #     if 'prec' in config and config['prec'].startswith('R'):
    #         channel_name = 'raw_' + channel_name
    #
    #     self.channel_handlers[channel_id] = self._data_handlers[channel_name]
    #
    #     # Create a channel_name, symbol tuple to identify channels of same type
    #     if 'key' in config:
    #         identifier = (channel_name, symbol, config['key'].split(':')[1])
    #     else:
    #         identifier = (channel_name, symbol)
    #     self.channel_handlers[channel_id] = identifier
    #     self.channel_directory[identifier] = channel_id
    #     self.channel_directory[channel_id] = identifier
    #     self.log.info("Subscription succesful for channel %s", identifier)
    #
    # def _handle_unsubscribed(self, dtype, data, ts):
    #     """Handles responses to unsubscribe() commands.
    #     Removes a channel id from the client.
    #     :param dtype:
    #     :param data:
    #     :param ts:
    #     :return:
    #     """
    #     self.log.debug("_handle_unsubscribed: %s - %s - %s", dtype, data, ts)
    #     channel_id = data.pop('chanId')
    #
    #     # Unregister the channel from all internal attributes
    #     chan_identifier = self.channel_directory.pop(channel_id)
    #     self.channel_directory.pop(chan_identifier)
    #     self.channel_handlers.pop(channel_id)
    #     self.last_update.pop(channel_id)
    #     self.log.info("Successfully unsubscribed from %s", chan_identifier)
    #
    # def _handle_auth(self, dtype, data, ts):
    #     """Handles authentication responses.
    #     :param dtype:
    #     :param data:
    #     :param ts:
    #     :return:
    #     """
    #     # Contains keys status, chanId, userId, caps
    #     if dtype == 'unauth':
    #         raise NotImplementedError
    #     channel_id = data.pop('chanId')
    #     user_id = data.pop('userId')
    #
    #     identifier = ('auth', user_id)
    #     self.channel_handlers[identifier] = channel_id
    #     self.channel_directory[identifier] = channel_id
    #     self.channel_directory[channel_id] = identifier

    # def _handle_trades(self, dtype, data, ts):
    #     """Files trades in self._trades[chan_id].
    #     :param dtype:
    #     :param data:
    #     :param ts:
    #     :return:
    #     """
    #     self.log.debug("_handle_trades: %s - %s - %s", dtype, data, ts)
    #     channel_id, *data = data
    #     channel_identifier = self.channel_directory[channel_id]
    #     entry = (data, ts)
    #     self.trades[channel_identifier].put(entry)

    def _send_auth(self):
        # Generate nonce
        auth_nonce = str(int(time.time() * 10000000))
        # Generate signature
        auth_payload = "AUTH" + auth_nonce
        auth_sig = hmac.new(self._api_secret.encode(), auth_payload.encode(),
                            hashlib.sha384).hexdigest()

        payload = {"event": "auth", "apiKey": self._api_key, "authSig": auth_sig,
                   "authPayload": auth_payload, "authNonce": auth_nonce}

        self._send(payload)


# # Auth v1:
# import hmac
# import hashlib
# import time
#
# nonce = int(time.time() * 1000000)
# auth_payload = "AUTH" + str(nonce)
# signature = hmac.new(
#     API_SECRET.encode(),
#     msg = auth_payload.encode(),
#     digestmod = hashlib.sha384
# ).hexdigest()
#
# payload = {
#     "apiKey": API_KEY,
#     "event": "auth",
#     "authPayload": auth_payload,
#     "authNonce": nonce,
#     "authSig": signature
# }
#
# ws.send(json.dumps(payload))

# https://github.com/bitfinexcom/bitfinex-api-node
# How do te and tu messages differ?
# A te packet is sent first to the client immediately after a trade has been
# matched & executed, followed by a tu message once it has completed processing.
# During times of high load, the tu message may be noticably delayed, and as
# such only the te message should be used for a realtime feed.
