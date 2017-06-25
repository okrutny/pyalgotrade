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


def parse_datetime(dateTime):
    try:
        ret = datetime.datetime.strptime(dateTime, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        ret = datetime.datetime.strptime(dateTime, "%Y-%m-%d %H:%M:%S.%f")
    return dt.as_utc(ret)


class AccountBalance(object):
    def __init__(self, jsonDict):
        self.__jsonDict = jsonDict

    def getDict(self):
        return self.__jsonDict

    # TODO: remove mocking
    def getUSDAvailable(self):
        return 3000.0 #float(self.__jsonDict["usd_available"])

    def getBTCAvailable(self):
        return 1.5 #float(self.__jsonDict["btc_available"])


class Order(object):
    def __init__(self, jsonDict):
        self.__jsonDict = jsonDict

    def getDict(self):
        return self.__jsonDict

    def getId(self):
        return int(self.__jsonDict["id"])

    def isBuy(self):
        return self.__jsonDict["type"] == 0

    def isSell(self):
        return self.__jsonDict["type"] == 1

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
        return float(self.__jsonDict["btc"])

    def getBTCUSD(self):
        return float(self.__jsonDict["btc_usd"])

    def getDateTime(self):
        return parse_datetime(self.__jsonDict["datetime"])

    def getFee(self):
        return float(self.__jsonDict["fee"])

    def getId(self):
        return int(self.__jsonDict["id"])

    def getOrderId(self):
        return int(self.__jsonDict["order_id"])

    def getUSD(self):
        return float(self.__jsonDict["usd"])


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

            response = requests.post(self.uri+urlpath, headers=headers, data=data, timeout=HTTPClient.REQUEST_TIMEOUT)
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
        # TODO: parse response properly
        # jsonResponse = self._post(url, {})
        #return AccountBalance(jsonResponse)
        return AccountBalance({})

    def getOpenOrders(self):
        # url = "https://www.bitstamp.net/api/open_orders/"
        # jsonResponse = self._post(url, {})
        # return [Order(json_open_order) for json_open_order in jsonResponse]
        # TODO implement this
        return []

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

        params = {
            "price": price,
            "amount": amount
        }
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

        params = {
            "price": price,
            "amount": amount
        }
        jsonResponse = self._post(url, params)
        return Order(jsonResponse)

    def getUserTransactions(self, transactionType=None):
        # url = "https://www.bitstamp.net/api/user_transactions/"
        # jsonResponse = self._post(url, {})
        # if transactionType is not None:
        #     jsonUserTransactions = filter(
        #         lambda jsonUserTransaction: jsonUserTransaction["type"] == transactionType, jsonResponse
        #     )
        # else:
        #     jsonUserTransactions = jsonResponse
        # return [UserTransaction(jsonUserTransaction) for jsonUserTransaction in jsonUserTransactions]
        # TODO: implement this
        return []
