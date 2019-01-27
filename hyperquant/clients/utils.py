from django.conf import settings

from hyperquant.api import Platform
from hyperquant.clients.binance import BinanceRESTClient, BinanceWSClient
from hyperquant.clients.bitfinex import BitfinexRESTClient, BitfinexWSClient
from hyperquant.clients.bitmex import BitMEXRESTClient, BitMEXWSClient
from hyperquant.clients.okex import OkexRESTClient, OkexWSClient

# temp
# if not settings.configured:
#     # todo add default credentials
#     print("settings.configure() for clients")
#     settings.configure(base)

_rest_client_class_by_platform_id = {
    Platform.BINANCE: BinanceRESTClient,
    Platform.BITFINEX: BitfinexRESTClient,
    Platform.BITMEX: BitMEXRESTClient,
    Platform.OKEX: OkexRESTClient,
}

_ws_client_class_by_platform_id = {
    Platform.BINANCE: BinanceWSClient,
    Platform.BITFINEX: BitfinexWSClient,
    Platform.BITMEX: BitMEXWSClient,
    Platform.OKEX: OkexWSClient,
}

_rest_client_by_platform_id = {}
_private_rest_client_by_platform_id = {}
_ws_client_by_platform_id = {}
_private_ws_client_by_platform_id = {}


def create_rest_client(platform_id, is_private=False, version=None):
    return _create_client(platform_id, True, is_private, version)


def get_or_create_rest_client(platform_id, is_private=False):
    return _get_or_create_client(platform_id, True, is_private)


def create_ws_client(platform_id, is_private=False, version=None):
    return _create_client(platform_id, False, is_private, version)


def get_or_create_ws_client(platform_id, is_private=False):
    return _get_or_create_client(platform_id, False, is_private)


def get_credentials_for(platform_id):
    platform_name = Platform.get_platform_name_by_id(platform_id)
    api_key, api_secret = settings.CREDENTIALS_BY_PLATFORM.get(platform_name)
    return api_key, api_secret


def _create_client(platform_id, is_rest, is_private=False, version=None):
    # Create
    class_lookup = _rest_client_class_by_platform_id if is_rest else _ws_client_class_by_platform_id
    client_class = class_lookup.get(platform_id)
    if is_private:
        api_key, api_secret = get_credentials_for(platform_id)
        client = client_class(api_key, api_secret, version)
        client.platform_id = platform_id  # If not set in class
    else:
        client = client_class(version=version)
        client.platform_id = platform_id  # If not set in class

        # For Binance's "historicalTrades" endpoint
        if platform_id == Platform.BINANCE:
            api_key, _ = get_credentials_for(platform_id)
            client.set_credentials(api_key, None)
    return client


def _get_or_create_client(platform_id, is_rest, is_private=False):
    # Get
    if is_rest:
        lookup = _private_rest_client_by_platform_id if is_private else _rest_client_by_platform_id
    else:
        lookup = _private_ws_client_by_platform_id if is_private else _ws_client_by_platform_id
    client = lookup.get(platform_id)
    if client:
        return client

    # Create
    lookup[platform_id] = client = _create_client(platform_id, is_rest, is_private)
    return client
