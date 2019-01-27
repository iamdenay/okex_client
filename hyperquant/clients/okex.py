import hashlib
import hmac
import json
from operator import itemgetter
import zlib
import datetime

from hyperquant.api import Platform, Sorting, Interval, Direction, OrderType
from hyperquant.clients import WSClient, Endpoint, Trade, Error, ErrorCode, \
    ParamName, WSConverter, RESTConverter, PrivatePlatformRESTClient, MyTrade, Candle, Ticker, OrderBookItem, Order, \
    OrderBook, Account, Balance

#REST

class OkexRESTConverterV1(RESTConverter):

    base_url = "https://www.okex.com/api/v1"
    is_source_in_milliseconds = True
    endpoint_lookup = {
        Endpoint.TRADE: "trades.do",
        Endpoint.TRADE_HISTORY: "trades.do", # not implemented in OKex API v1 at this moment
        Endpoint.CANDLE: "kline.do",
        Endpoint.TICKER: "ticker.do",

        # #Private 
        # Endpoint.ACCOUNT: "userinfo.do",
        # Endpoint.ORDER: "trade.do",
    }

    param_name_lookup = {
        ParamName.SYMBOL: "symbol",
        ParamName.LIMIT: "size",
        ParamName.FROM_TIME: "since",
        ParamName.INTERVAL: "type",

    }

    param_value_lookup = {
        Sorting.DEFAULT_SORTING: Sorting.ASCENDING,

        Interval.MIN_1: "1min",
        Interval.MIN_3: "3min",
        Interval.MIN_5: "5min",
        Interval.MIN_15: "15min",
        Interval.MIN_30: "30min",
        Interval.HRS_1: "1hour",
        Interval.HRS_2: "2hhour",
        Interval.HRS_4: "4hour",
        Interval.HRS_6: "6hour",
        Interval.HRS_12: "12hour",
        Interval.DAY_1: "1day",
        Interval.WEEK_1: "1week",

        ParamName.DIRECTION: {
            Direction.SELL: "sell",
            Direction.BUY: "buy",
        },
    }

    max_limit_by_endpoint = {
        Endpoint.CANDLE: 200,
    }

    param_lookup_by_class = {
        Error: {
            "error_code": "code",
        },
        Trade: {
            "tid": ParamName.ITEM_ID,
            "date": ParamName.TIMESTAMP,
            "price": ParamName.PRICE,
            "amount": ParamName.AMOUNT,
            "type": ParamName.DIRECTION,
        },
        Candle: [
            ParamName.TIMESTAMP,
            ParamName.PRICE_OPEN,
            ParamName.PRICE_HIGH,
            ParamName.PRICE_LOW,
            ParamName.PRICE_CLOSE,
            ParamName.AMOUNT,
        ],
        
    }

    

class OkexRESTClient(PrivatePlatformRESTClient):

    platform_id = Platform.OKEX
    version = "1" 

    _converter_class_by_version = {
        "1": OkexRESTConverterV1,
    }

    @property
    def headers(self):
        result = super().headers
        result["Content-Type"] = "application/x-www-form-urlencoded"
        return result

    def fetch_history(self, endpoint, symbol, limit=None, from_item=None, to_item=None, sorting=None,
                      is_use_max_limit=False, from_time=None, to_time=None,
                      version=None, **kwargs):
        if from_item is None:
            from_item = 0
        result = super().fetch_history(endpoint, symbol, limit, from_item, to_item, sorting, is_use_max_limit, from_time,
                                     to_time, **kwargs)
        return result[:limit] # there is no limit functionality in OKEX API

    def fetch_candles(self, symbol, interval, limit=None, from_time=None, **kwargs):
        result = super().fetch_candles(symbol, interval, limit, from_time, **kwargs)

        return result


#WS

class OkexWSConverterV1(WSConverter):

    base_url = "wss://real.okex.com:10440/ws/v1"

    IS_SUBSCRIPTION_COMMAND_SUPPORTED = True

    endpoint_lookup = {
        Endpoint.TRADE: "ok_sub_spot_{symbol}_deals",
        Endpoint.CANDLE: "ok_sub_spot_{symbol}_kline_{interval}",
    }

    Interval.MIN_1 = "1min"
    Interval.MIN_3 = "3min"
    Interval.MIN_5 = "5min"
    Interval.MIN_15 = "15min"
    Interval.MIN_30 = "30min"
    Interval.HRS_1 = "1hour"
    Interval.HRS_2 = "2hhour"
    Interval.HRS_4 = "4hour"
    Interval.HRS_6 = "6hour"
    Interval.HRS_12 = "12hour"
    Interval.DAY_1 = "1day"
    Interval.WEEK_1 = "1week"

    param_lookup_by_class = {
        Trade: [
            ParamName.ITEM_ID,
            ParamName.PRICE,
            ParamName.AMOUNT,
            ParamName.TIMESTAMP,
            ParamName.DIRECTION,
            ParamName.SYMBOL
        ],       
        Candle: [
            ParamName.TIMESTAMP, # No ID in OKEX kline
            ParamName.PRICE_OPEN,
            ParamName.PRICE_HIGH, 
            ParamName.PRICE_LOW, 
            ParamName.PRICE_CLOSE, 
            ParamName.AMOUNT,
            ParamName.SYMBOL
        ]
    }
    event_type_param = "channel"

    endpoint_by_event_type = {
        "ok_sub_spot_{symbol}_deals": Endpoint.TRADE,
        "ok_sub_spot_{symbol}_kline_{interval}": Endpoint.CANDLE,
    }

    is_source_in_milliseconds = True

    def parse(self, endpoint, data):
        if data["channel"] == "addChannel":
            # If message is not real data, just response to subscription
            # Then we don't need to parse it
            return 

        if "deals" in data["channel"]:
            endpoint = Endpoint.TRADE
        elif "kline" in data["channel"]: 
            endpoint = Endpoint.CANDLE
        else:
            # For future
            return

        if "data" in data:
            symbol = ""
            symbol = data["channel"].split("_")
            symbol = symbol[3] + "_" + symbol[4] 
            data = data["data"]
            if endpoint == Endpoint.CANDLE:
                for item in data:
                    item[0] = int(item[0])
                    item.append(symbol)
            elif endpoint == Endpoint.TRADE: 
                for item in data:
                    # OKEX doesn't provide TIMESTAMP, so we get it from TIME 
                    now = datetime.datetime.now()
                    hour = int(item[3].split(":")[0])
                    minute = int(item[3].split(":")[1])
                    second = int(item[3].split(":")[2])
                    
                    now = now.replace(hour=hour, minute=minute, second=second)
                    item[3] = now.timestamp()*1000
                    item.append(symbol)
            else:
                # For future
                return
   
        return super().parse(endpoint, data)


class OkexWSClient(WSClient):
    platform_id = Platform.OKEX
    version = "1" 

    _converter_class_by_version = {
        "1": OkexWSConverterV1,
    }

    # hqlip API does not provide previous value for INTERVAL, which we need in run_demo.py
    # We need to repeat hqlib's solution like in subscribe()

    interval = None 
    def subscribe(self, endpoints=None, symbols=None, interval=None, **params):
        self.logger.debug("Previous interval: %s",self.interval)
        if not interval:
            interval = self.interval
        else:
            self.interval = interval
        
        params["interval"] = interval

        super().subscribe(endpoints, symbols, **params)

    # OKEX responds compressed GZip data

    def inflate(self, data): 
        decompress = zlib.decompressobj(
                -zlib.MAX_WBITS  # see above
        )
        inflated = decompress.decompress(data)
        inflated += decompress.flush()
        return inflated

    def _on_message(self, message):
        message = self.inflate(message)
        super()._on_message(message)

    def _parse(self, endpoint, data):
        return super()._parse(None,data)[0]
    
    def _send_subscribe(self, subscriptions):
        for channel in subscriptions:
            event_data = {
                "event": "addChannel",
                "channel": channel
            }
            self._send(event_data)

    
        