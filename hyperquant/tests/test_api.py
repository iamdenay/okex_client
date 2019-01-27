from unittest import TestCase

from hyperquant.api import item_format_by_endpoint, Endpoint, Direction, convert_items_obj_to_list, \
    convert_items_dict_to_list, convert_items_list_to_dict, convert_items_obj_to_dict, ParamName
from hyperquant.clients import Trade, ItemObject


class TestConverting(TestCase):
    endpoint = None
    item_format = None

    obj_items = None
    list_items = None
    dict_items = None
    
    obj_item_short = None
    list_item_short = None
    dict_item_short = None

    def setUp(self):
        super().setUp()

        if not self.endpoint:
            self.skipTest("Base test")

        self.item_format = item_format_by_endpoint[self.endpoint]

    def test_convert_items_obj_to_list(self):
        # Items to items
        self._test_convert_items(self.obj_items, self.list_items, convert_items_obj_to_list)
        # Item to item
        self._test_convert_items(self.obj_items[0], self.list_items[0], convert_items_obj_to_list)
        # Check for items which are shorter than item_format (i.e. item is ItemObject, and item_format is for Trade)
        self._test_convert_items(self.obj_item_short, self.list_item_short, convert_items_obj_to_list)

        # Empty to empty, None to None
        self._test_convert_items([], [], convert_items_obj_to_list)
        self._test_convert_items([None, None], [None, None], convert_items_obj_to_list)
        self._test_convert_items(None, None, convert_items_obj_to_list)

    def test_convert_items_dict_to_list(self):
        self._test_convert_items(self.dict_items, self.list_items, convert_items_dict_to_list)
        self._test_convert_items(self.dict_items[0], self.list_items[0], convert_items_dict_to_list)
        self._test_convert_items(self.dict_item_short, self.list_item_short, convert_items_dict_to_list)

        self._test_convert_items([], [], convert_items_dict_to_list)
        self._test_convert_items([None, None], [None, None], convert_items_dict_to_list)
        self._test_convert_items(None, None, convert_items_dict_to_list)

    def test_convert_items_list_to_dict(self):
        self._test_convert_items(self.list_items, self.dict_items, convert_items_list_to_dict)
        self._test_convert_items(self.list_items[0], self.dict_items[0], convert_items_list_to_dict)
        self._test_convert_items(self.list_item_short, self.dict_item_short, convert_items_list_to_dict)

        self._test_convert_items([], [], convert_items_list_to_dict)
        self._test_convert_items([None, None], [None, None], convert_items_list_to_dict)
        self._test_convert_items(None, None, convert_items_list_to_dict)

    def test_convert_items_obj_to_dict(self):
        self._test_convert_items(self.obj_items, self.dict_items, convert_items_obj_to_dict)
        self._test_convert_items(self.obj_items[0], self.dict_items[0], convert_items_obj_to_dict)
        self._test_convert_items(self.obj_item_short, self.dict_item_short, convert_items_obj_to_dict)

        self._test_convert_items([], [], convert_items_obj_to_dict)
        self._test_convert_items([None, None], [None, None], convert_items_obj_to_dict)
        self._test_convert_items(None, None, convert_items_obj_to_dict)

    def _test_convert_items(self, items, expected, fun):
        result = fun(items, self.item_format)

        self.assertEqual(expected, result)


class TestConvertingTrade(TestConverting):
    endpoint = Endpoint.TRADE

    obj_item1 = Trade()
    obj_item1.platform_id = None  # None needed to test convert_items_list_to_dict() with 1 item in params
    obj_item1.symbol = "ETHUSD"
    obj_item1.timestamp = 143423531
    obj_item1.item_id = "14121214"
    obj_item1.price = "23424546543.3"
    obj_item1.amount = "1110.0034"
    obj_item1.direction = Direction.SELL
    obj_item2 = Trade()
    obj_item2.platform_id = 2
    obj_item2.symbol = "BNBUSD"
    obj_item2.timestamp = 143423537
    obj_item2.item_id = 15121215
    obj_item2.price = 23.235656723
    obj_item2.amount = "0.0034345452"
    obj_item2.direction = Direction.BUY

    obj_items = [obj_item1, obj_item2]
    list_items = [[None, "ETHUSD", 143423531, "14121214", "23424546543.3", "1110.0034", Direction.SELL],
                  [2, "BNBUSD", 143423537, 15121215, 23.235656723, "0.0034345452", Direction.BUY]]
    dict_items = [{ParamName.PLATFORM_ID: None, ParamName.SYMBOL: "ETHUSD",
                   ParamName.TIMESTAMP: 143423531, ParamName.ITEM_ID: "14121214",
                   ParamName.PRICE: "23424546543.3", ParamName.AMOUNT: "1110.0034", ParamName.DIRECTION: 1},
                  {ParamName.PLATFORM_ID: 2, ParamName.SYMBOL: "BNBUSD",
                   ParamName.TIMESTAMP: 143423537, ParamName.ITEM_ID: 15121215,
                   ParamName.PRICE: 23.235656723, ParamName.AMOUNT: "0.0034345452", ParamName.DIRECTION: 2}]

    obj_item_short = ItemObject()
    obj_item_short.platform_id = None  # None needed to test convert_items_list_to_dict() with 1 item in params
    obj_item_short.symbol = "ETHUSD"
    obj_item_short.timestamp = 143423531
    obj_item_short.item_id = "14121214"
    list_item_short = [None, "ETHUSD", 143423531, "14121214"]
    dict_item_short = {ParamName.PLATFORM_ID: None, ParamName.SYMBOL: "ETHUSD",
                       ParamName.TIMESTAMP: 143423531, ParamName.ITEM_ID: "14121214"}
