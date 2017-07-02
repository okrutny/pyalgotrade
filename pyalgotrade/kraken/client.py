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

import Queue
import datetime
import threading
from time import time, sleep

from kraken.httpclient import Trade
from pyalgotrade.kraken import common

from kraken import httpclient


def get_current_datetime():
    return datetime.datetime.now()

#     if event == "trade":
#         self.onTrade(Trade(get_current_datetime(), msg))
#     elif event == "data" and msg.get("channel") == "order_book":
#         self.onOrderBookUpdate(OrderBookUpdate(get_current_datetime(), msg))
# def onTrade(self, trade):
#     self.__queue.put((WebSocketClient.ON_TRADE, trade))
#
# def onOrderBookUpdate(self, orderBookUpdate):
#     self.__queue.put((WebSocketClient.ON_ORDER_BOOK_UPDATE, orderBookUpdate))


class ExchangeMonitor(threading.Thread):
    """
    Monitors public orders books and updates OHLC data of trades made on exchange
    """
    POLL_FREQUENCY = 10

    # Events
    ON_TRADE = 1
    ON_ORDER_BOOK_UPDATE = 2

    def __init__(self, httpClient):
        super(ExchangeMonitor, self).__init__()
        self.__lastTradeId = None
        self.__httpClient = httpClient
        self.__queue = Queue.Queue()
        self.__stop = False

    def getQueue(self):
        return self.__queue

    def processNewTrades(self):
        trades, self.__lastTradeId = self.__httpClient.getLastTrades(self.__lastTradeId)

        if len(trades):
            common.logger.info("%d new trade/s found" % (len(trades)))

        for trade in trades:
            self.onTrade(trade)

    def start(self):
        self.processNewTrades()
        super(ExchangeMonitor, self).start()

    def onTrade(self, trade):
        self.__queue.put((ExchangeMonitor.ON_TRADE, trade))

    def run(self):
        while not self.__stop:
            self.processNewTrades()
            sleep(ExchangeMonitor.POLL_FREQUENCY)

    def stop(self):
        self.__stop = True

