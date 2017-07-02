# PyAlgoTrade
#
# Copyright 2011-2015 Gabriel Martin Becedillas Ruiz
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
.. moduleauthor:: Gabriel Martin Becedillas Ruiz <gabriel.becedillas@gmail.com>
"""

# private query signing
import base64
import datetime
import hashlib
import hmac
import logging
import threading
import time
import urllib
from time import sleep

import requests
from requests.exceptions import HTTPError

from pyalgotrade.kraken import common
from pyalgotrade.utils import dt

logger = logging.getLogger("httpclient")
logger.setLevel(logging.INFO)




def parse_datetime(timestamp):
    try:
        ret = datetime.datetime.fromtimestamp(timestamp)
    except ValueError:
        ret = None
    return dt.as_utc(ret)


class AccountBalance(object):
    def __init__(self, jsonDict):
        self.__jsonDict = jsonDict

    def getDict(self):
        return self.__jsonDict

    def getEURAvailable(self):
        result = self.__jsonDict["result"]
        if result:
            try:
                return float(result["ZEUR"])
            except KeyError:
                return None

    def getBTCAvailable(self):
        result = self.__jsonDict["result"]
        if result:
            try:
                return float(result[common.btc_symbol])
            except KeyError:
                return None


class Order(object):
    def __init__(self, jsonDict):
        self.__jsonDict = jsonDict

    def getDict(self):
        return self.__jsonDict

    def getId(self):
        return self.__jsonDict["id"]

    def isBuy(self):
        return self.__jsonDict["type"] == "buy"

    def isSell(self):
        return self.__jsonDict["type"] == "sell"

    def getPrice(self):
        return float(self.__jsonDict["price"])

    def getAmount(self):
        return float(self.__jsonDict["amount"])

    def getDateTime(self):
        return parse_datetime(self.__jsonDict["datetime"])


class UserTransaction(object):
    def __init__(self, jsonDict):
        self.__jsonDict = jsonDict

    def getDict(self):
        return self.__jsonDict

    def getBTC(self):
        return float(self.__jsonDict["vol"])

    def getFillPrice(self):
        return float(self.__jsonDict["price"])

    def getDateTime(self):
        return parse_datetime(self.__jsonDict["time"])

    def getFee(self):
        return float(self.__jsonDict["fee"])

    def getId(self):
        return int(self.__jsonDict["id"])

    def getOrderId(self):
        return int(self.__jsonDict["ordertxid"])

    # TODO: it's EUR, need to be "base currency agnostic"
    def getUSD(self):
        return float(self.__jsonDict["cost"])

    def getOrderingPropertyValue(self):
        return float(self.__jsonDict["time"])


class Trade(object):
    """A trade event."""

    def __init__(self, dateTime, price, amount, type):
        self.__dateTime = dateTime
        self.__price = price
        self.__amount = amount
        self.__type = type

    def getDateTime(self):
        """Returns the :class:`datetime.datetime` when this event was received."""
        return self.__dateTime

    def getId(self):
        """Returns the trade id."""
        return self.__dateTime

    def getPrice(self):
        """Returns the trade price."""
        return self.__price

    def getAmount(self):
        """Returns the trade amount."""
        return self.__amount

    def isBuy(self):
        """Returns True if the trade was a buy."""
        return self.__type == 'b'

    def isSell(self):
        """Returns True if the trade was a sell."""
        return self.__type == 's'


class OrderBookUpdate(object):
    """An order book update event."""

    def __init__(self, jsonDict):
        self.__jsonDict = jsonDict

    def getDateTime(self):
        """Returns the :class:`datetime.datetime` when this event was received."""
        return self.__dateTime

    def getBidPrices(self):
        """Returns a list with the top 20 bid prices."""
        return [float(bid[0]) for bid in self.getData()["bids"]]

    def getBidVolumes(self):
        """Returns a list with the top 20 bid volumes."""
        return [float(bid[1]) for bid in self.getData()["bids"]]

    def getAskPrices(self):
        """Returns a list with the top 20 ask prices."""
        return [float(ask[0]) for ask in self.getData()["asks"]]

    def getAskVolumes(self):
        """Returns a list with the top 20 ask volumes."""
        return [float(ask[1]) for ask in self.getData()["asks"]]


class HTTPClient(object):
    USER_AGENT = "PyAlgoTrade"
    REQUEST_TIMEOUT = 30

    class UserTransactionType:
        MARKET_TRADE = 2

    def __init__(self):
        """Create an object with authentication information.

        Arguments:
        key    -- key required to make queries to the API (default: '')
        secret -- private key used to sign API messages (default: '')

        """
        self.load_key('kraken.key')
        self.uri = 'https://api.kraken.com'
        self.apiversion = '0'

        self.__prevNonce = None
        self.__lock = threading.Lock()

    def load_key(self, path):
        """Load key and secret from file.

        Argument:
        path -- path to file (string, no default)

        """
        # TODO handle IO error
        with open(path, "r") as f:
            self.__key = f.readline().strip()
            self.__secret = f.readline().strip()

    def _getNonce(self):
        ret = int(time.time())
        if ret == self.__prevNonce:
            ret += 1
        self.__prevNonce = ret
        return ret

    def _buildQuery(self, urlpath, params):

        # set nonce
        nonce = self._getNonce()
        params["nonce"] = nonce

        # create signature
        postdata = urllib.urlencode(params)
        message = urlpath + hashlib.sha256(str(nonce) + postdata).digest()
        signature = hmac.new(base64.b64decode(self.__secret), message, hashlib.sha512)

        # Headers
        headers = {}
        headers["User-Agent"] = HTTPClient.USER_AGENT
        headers["API-Key"] = self.__key
        headers["API-Sign"] = base64.b64encode(signature.digest())

        # POST data.
        data = {}
        data.update(params)

        return (data, headers)

    def _post(self, urlpath, params):
        common.logger.debug("POST to %s with params %s" % (urlpath, str(params)))

        # Serialize access to nonce generation and http requests to avoid
        # sending them in the wrong order.
        with self.__lock:
            data, headers = self._buildQuery(urlpath, params)

            response = requests.post(self.uri + urlpath, headers=headers, data=data, timeout=HTTPClient.REQUEST_TIMEOUT)
            try:
                response.raise_for_status()
            except HTTPError, e:
                if e.response.status_code == 504:
                    logger.error("504, skipping")
                    return

        jsonResponse = response.json()

        # Check for errors.
        if isinstance(jsonResponse, dict):
            error = jsonResponse.get("error")
            if error:
                raise Exception(error)

        return jsonResponse

    def _get_public_api_urlpath(self, method):
        return '/' + self.apiversion + '/public/' + method

    def _get_private_api_urlpath(self, method):
        return '/' + self.apiversion + '/private/' + method

    def getAccountBalance(self):
        logger.info("getAccountBalance")
        endpoint = 'Balance'
        url = self._get_private_api_urlpath(endpoint)
        jsonResponse = self._post(url, {})
        if jsonResponse:
            return AccountBalance(jsonResponse)
        else:
            raise Exception("Balance not available")

    def getOpenOrders(self):
        logger.info("getOpenOrders")
        endpoint = 'OpenOrders'
        url = self._get_private_api_urlpath(endpoint)
        jsonResponse = self._post(url, {})
        orders = []
        result = jsonResponse.get("result")
        if result and result.get("open"):
            for oid, o_details in result.get('open').iteritems():
                o_dict = {}
                o_dict["id"] = oid
                o_dict["type"] = o_details["descr"]["type"]
                o_dict["price"] = o_details["descr"]["price"]  # limit price
                o_dict["amount"] = o_details["vol"]
                o_dict["datetime"] = o_details["opentm"]
                orders.append(Order(o_dict))
        return orders
# sample output
# {u'result':
#      {u'open':
#           {u'OVRXBV-YRC5W-PKCC4N':
#                {u'status': u'open',
#                 u'fee': u'0.00000',
#                 u'expiretm': 0,
#                 u'descr':
#                     {u'leverage': u'none',
#                      u'ordertype': u'limit',
#                      u'price': u'2200.500',
#                      u'pair': u'XBTEUR',
#                      u'price2': u'0',
#                      u'type': u'buy',
#                      u'order': u'buy 0.00050000 XBTEUR @ limit 2200.500'
#                      },
#                 u'vol': u'0.00050000',
#                 u'cost': u'0.00000',
#                 u'misc': u'',
#                 u'price': u'0.00000',
#                 u'starttm': 0,
#                 u'userref': None,
#                 u'vol_exec': u'0.00000000',
#                 u'oflags': u'fciq',
#                 u'refid': None,
#                 u'opentm': 1498685688.616}
#            }
#       },
#  u'error': []}


    # def cancelOrder(self, orderId):
    #     url = "https://www.bitstamp.net/api/cancel_order/"
    #     params = {"id": orderId}
    #     jsonResponse = self._post(url, params)
    #     if jsonResponse != True:
    #         raise Exception("Failed to cancel order")
    #
    #
    # def buyLimit(self, limitPrice, quantity):
    #     url = "https://www.bitstamp.net/api/buy/"
    #
    #     # Rounding price to avoid 'Ensure that there are no more than 2 decimal places'
    #     # error.
    #     price = round(limitPrice, 2)
    #     # Rounding amount to avoid 'Ensure that there are no more than 8 decimal places'
    #     # error.
    #     amount = round(quantity, 8)
    #
    #     params = {"price": price, "amount": amount}
    #     jsonResponse = self._post(url, params)
    #     return Order(jsonResponse)
    #
    #
    # def sellLimit(self, limitPrice, quantity):
    #     url = "https://www.bitstamp.net/api/sell/"
    #
    #     # Rounding price to avoid 'Ensure that there are no more than 2 decimal places'
    #     # error.
    #     price = round(limitPrice, 2)
    #     # Rounding amount to avoid 'Ensure that there are no more than 8 decimal places'
    #     # error.
    #     amount = round(quantity, 8)
    #
    #     params = {"price": price, "amount": amount}
    #     jsonResponse = self._post(url, params)
    #     return Order(jsonResponse)

    # def getOrdersInfo(self, txidsArr):
    #     logger.info("getOrdersInfo")
    #     endpoint = "QueryOrders"
    #     url = self._get_private_api_urlpath(endpoint)
    #     jsonResponse = self._post(url, {'txid': ",".join(txidsArr)})
    #     if jsonResponse:
    #         result = jsonResponse.get("result")
    #         return result

    def getUserTransactions(self):
        logger.info("getUserTransactions")
        endpoint = "TradesHistory"
        url = self._get_private_api_urlpath(endpoint)
        user_transactions = []
        jsonResponse = self._post(url, {})
        if jsonResponse:
            result = jsonResponse.get("result")
            for txid, trade_info in result['trades'].iteritems():
                trade_info.update(id=txid)
                user_transactions.append(UserTransaction(trade_info))
        return user_transactions


    def getLastTrades(self, since=None):
        logger.info("getLastTrades")
        endpoint = "Trades"
        url = self._get_public_api_urlpath(endpoint)
        last_trades = []
        last_trade_id = since
        jsonResponse = self._post(url, {'pair': common.traded_pair, 'since': since})
        if jsonResponse:
            result = jsonResponse.get("result")
            trades_arrs = result[common.traded_pair]
            last_trade_id = result['last']
            # <price>, <volume>, <time>, <buy/sell>, <market/limit>, <miscellaneous>
            for trade_arr in trades_arrs:
                # TODO: not 100% sure if only market orders should be processed or all of them
                trade = Trade(price=float(trade_arr[0]), amount=float(trade_arr[1]), dateTime=parse_datetime(trade_arr[2]), type=trade_arr[3])
                last_trades.append(trade)

        return last_trades, last_trade_id

    def getAssets(self):
        logger.info("getAssets")
        endpoint = "AssetPairs"
        url = self._get_public_api_urlpath(endpoint)
        jsonResponse = self._post(url, {})
        if jsonResponse:
            result = jsonResponse.get("result")