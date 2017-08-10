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
import json
import logging
import threading
import time
import urllib
import httplib

from kraken.connection import Connection
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


def get_current_datetime():
    return datetime.datetime.now()


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
    def __init__(self, id, type=None, amount=None, price=None, timestamp=None):
        self.id = id
        self.type = type
        self.amount = amount
        self.price = price
        if timestamp:
            self.datetime = parse_datetime(timestamp)

    def parse_order_descr(self, descr):
        """
        
        :param descr: sample descr: u'buy 0.00200000 XBTEUR @ limit 2322.000'
        :return: 
        """

        descr_parts = descr.split(" ")
        if len(descr_parts) != 6:
            raise Exception("Not expected order description format: %s", descr)
        self.type = descr_parts[0]  # buy or sell
        self.amount = float(descr_parts[1])
        self.pair = descr_parts[2]
        self.price = float(descr_parts[5])

    def getId(self):
        return self.id

    def isBuy(self):
        return self.type == "buy"

    def isSell(self):
        return self.type == "sell"

    def getPrice(self):
        return self.price

    def getAmount(self):
        return self.amount

    def getDateTime(self):
        return self.datetime


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
        # TODO: fixme ids (strings can't be converted to int)
        return self.__jsonDict["id"]

    def getOrderId(self):
        return self.__jsonDict["ordertxid"]

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

    def __init__(self, dateTime, bidPrices, bidVols, askPrices, askVols):
        self.__dateTime = dateTime
        self.__bidPrices = bidPrices
        self.__bidVols = bidVols
        self.__askPrices = askPrices
        self.__askVols = askVols

    def getDateTime(self):
        """Returns the :class:`datetime.datetime` when this event was received."""
        return self.__dateTime

    def getBidPrices(self):
        """Returns a list with the top 20 bid prices."""
        return self.__bidPrices

    def getBidVolumes(self):
        """Returns a list with the top 20 bid volumes."""
        return self.__bidVols

    def getAskPrices(self):
        """Returns a list with the top 20 ask prices."""
        return self.__askPrices

    def getAskVolumes(self):
        """Returns a list with the top 20 ask volumes."""
        return self.__askVols


class HTTPClient(object):
    USER_AGENT = "PyAlgoTrade"
    REQUEST_TIMEOUT = 45

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

    def _buildQuery(self, urlpath, params, nonce=None):

        # set nonce
        if not nonce:
            nonce = self._getNonce()
        params["nonce"] = nonce

        # create signature
        postdata = urllib.urlencode(params)
        message = urlpath + hashlib.sha256(str(nonce) + postdata).digest()
        signature = hmac.new(base64.b64decode(self.__secret), message, hashlib.sha512)

        # Headers
        headers = {}
        #headers["User-Agent"] = HTTPClient.USER_AGENT
        headers["API-Key"] = self.__key
        headers["API-Sign"] = base64.b64encode(signature.digest())

        # POST data.
        # data = {}
        # data.update(params)

        return (params, headers)

    def _post(self, urlpath, params, nonce=None):
        common.logger.debug("POST to %s with params %s" % (urlpath, str(params)))

        jsonResponse = response = response_content = None

        # Serialize access to nonce generation and http requests to avoid
        # sending them in the wrong order.
        with self.__lock:
            data, headers = self._buildQuery(urlpath, params, nonce)

            conn = Connection()
            response = conn._request(self.uri + urlpath, data, headers)
            # response = requests.post(self.uri + urlpath, headers=headers, data=data, timeout=HTTPClient.REQUEST_TIMEOUT)
            response_content = response.read()

        if response:
            if response.status != httplib.OK and response.status != 504:
                raise httplib.HTTPException

            try:
                jsonResponse = json.loads(response_content)
            except ValueError:
                logger.error("No JSON object could be decoded: %s", response_content)
                return

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
        if jsonResponse:
            result = jsonResponse.get("result")
            if result and result.get("open"):
                for oid, o_details in result.get('open').iteritems():
                    order = Order(id=oid, type=o_details["descr"]["type"], price=o_details["descr"]["price"], amount=o_details["vol"],
                                  timestamp=o_details["opentm"])
                    orders.append(order)
        else:
            logger.error("getOpenOrders haven't returned valid response")
        return orders

    # def cancelOrder(self, orderId):
    #     url = "https://www.bitstamp.net/api/cancel_order/"
    #     params = {"id": orderId}
    #     jsonResponse = self._post(url, params)
    #     if jsonResponse != True:
    #         raise Exception("Failed to cancel order")
    #
    #

    def _place_limit_order(self, type, limitPrice, quantity):
        endpoint = "AddOrder"
        url = self._get_private_api_urlpath(endpoint)
        params = {"pair": common.traded_pair, "type": type, "ordertype": "limit", "price": str(limitPrice), "volume": str(quantity)}
        jsonResponse = self._post(url, params)
        if jsonResponse:
            oid = jsonResponse['result']['txid'][0]
            # get timestamp of the order
            ordersInfo = self.getOrdersInfo([oid])
            order = Order(id=oid, timestamp=ordersInfo[oid]['opentm'])
            order.parse_order_descr(jsonResponse['result']['descr']['order'])
            return order


    def buyLimit(self, limitPrice, quantity):
        logger.info("buyLimit")
        return self._place_limit_order("buy", limitPrice, quantity)


    def sellLimit(self, limitPrice, quantity):
        logger.info("buyLimit")
        return self._place_limit_order("sell", limitPrice, quantity)


    def getOrdersInfo(self, txidsArr):
        logger.info("getOrdersInfo")
        endpoint = "QueryOrders"
        url = self._get_private_api_urlpath(endpoint)
        jsonResponse = self._post(url, {'txid': ",".join(txidsArr)})
        if jsonResponse:
            result = jsonResponse.get("result")
            return result

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
        else:
            logger.error("getUserTransactions haven't returned valid response")
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
        else:
            logger.error("getLastTrades haven't returned valid response")

        return last_trades, last_trade_id

    def getAssets(self):
        logger.info("getAssets")
        endpoint = "AssetPairs"
        url = self._get_public_api_urlpath(endpoint)
        jsonResponse = self._post(url, {})
        if jsonResponse:
            result = jsonResponse.get("result")
        else:
            logger.error("getAssets haven't returned valid response")

    def getOrderBookUpdates(self):
        logger.info('getOrderBookUpdates')
        endpoint = "Depth"
        url = self._get_public_api_urlpath(endpoint)

        bidPrices = []
        bidVols = []
        askPrices = []
        askVols = []

        jsonResponse = self._post(url, {'pair': common.traded_pair, 'count': 20})
        if jsonResponse:
            result = jsonResponse.get("result")
            bids = result[common.traded_pair]['bids']
            asks = result[common.traded_pair]['asks']

            for bidArr in bids:
                bidPrices.append(float(bidArr[0]))
                bidVols.append(float(bidArr[1]))

            for askArr in asks:
                askPrices.append(float(askArr[0]))
                askVols.append(float(askArr[1]))

            return OrderBookUpdate(get_current_datetime(), bidPrices=bidPrices, bidVols=bidVols, askPrices=askPrices, askVols=askVols)

        else:
            logger.error("getOrderBookUpdates haven't returned valid response")
