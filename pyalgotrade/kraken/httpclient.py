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

import requests

from pyalgotrade.kraken import common
from pyalgotrade.utils import dt

logging.getLogger("requests").setLevel(logging.ERROR)


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

    def getUSD(self):
        return float(self.__jsonDict["cost"])

    def getOrderingPropertyValue(self):
        return float(self.__jsonDict["time"])


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
            response.raise_for_status()

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
        endpoint = 'Balance'
        url = self._get_private_api_urlpath(endpoint)
        jsonResponse = self._post(url, {})
        return AccountBalance(jsonResponse)

    def getOpenOrders(self):
        endpoint = 'OpenOrders'
        url = self._get_private_api_urlpath(endpoint)
        jsonResponse = self._post(url, {})
        result = jsonResponse.get("result")
        orders = None
        if jsonResponse and result and result.get("open"):
            orders = []
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


    def cancelOrder(self, orderId):
        url = "https://www.bitstamp.net/api/cancel_order/"
        params = {"id": orderId}
        jsonResponse = self._post(url, params)
        if jsonResponse != True:
            raise Exception("Failed to cancel order")


    def buyLimit(self, limitPrice, quantity):
        url = "https://www.bitstamp.net/api/buy/"

        # Rounding price to avoid 'Ensure that there are no more than 2 decimal places'
        # error.
        price = round(limitPrice, 2)
        # Rounding amount to avoid 'Ensure that there are no more than 8 decimal places'
        # error.
        amount = round(quantity, 8)

        params = {"price": price, "amount": amount}
        jsonResponse = self._post(url, params)
        return Order(jsonResponse)


    def sellLimit(self, limitPrice, quantity):
        url = "https://www.bitstamp.net/api/sell/"

        # Rounding price to avoid 'Ensure that there are no more than 2 decimal places'
        # error.
        price = round(limitPrice, 2)
        # Rounding amount to avoid 'Ensure that there are no more than 8 decimal places'
        # error.
        amount = round(quantity, 8)

        params = {"price": price, "amount": amount}
        jsonResponse = self._post(url, params)
        return Order(jsonResponse)

    def getOrdersInfo(self, txidsArr):
        endpoint = "QueryOrders"
        url = self._get_private_api_urlpath(endpoint)
        jsonResponse = self._post(url, {'txid': ",".join(txidsArr)})
        result = jsonResponse.get("result")
        return result

    def getUserTransactions(self, transactionType=None):
        endpoint = "TradesHistory"
        url = self._get_private_api_urlpath(endpoint)
        jsonResponse = self._post(url, {})
        result = jsonResponse.get("result")

        # order_tx_ids = []
        # for trade_info in result['trades'].itervalues():
        #     order_tx_ids.append(trade_info['ordertxid'])
        #
        # orders_info = self.getOrdersInfo(order_tx_ids)
        user_transactions = []
        for txid, trade_info in result['trades'].iteritems():
            trade_info.update(id=txid)
            user_transactions.append(UserTransaction(trade_info))
        return user_transactions
