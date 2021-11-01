class FuturePositions:
    __ID = 0
    SHORT = -1
    LONG = 1

    def __init__(self, timestamp,
                 contract,
                 size,
                 entry_price,
                 position,
                 leverage,
                 targets: callable = None,
                 stop_loss: callable = None):
        self.id = FuturePositions.__ID
        FuturePositions.__ID += 1
        self.timestamp = timestamp
        self.contract = contract
        self.entry_price = entry_price
        self.margin = size * entry_price / leverage
        self.position = position
        self.size = size
        self.leverage = leverage
        self.liquid_price = entry_price - entry_price / leverage
        self.targets = targets
        self.stop_loss = stop_loss

    def pnl(self, mark_price):
        """
        :param mark_price: current price
        :return: the profit / loss of this contract for the current price
        """
        return (mark_price - self.entry_price) * self.size * self.position

    def is_got_liquid(self, candle):
        """
        in case that the contract got close during this candle
        the size of this contract is set to 0 and should be closed.
        :param candle: the last candle to check if this candle got closed.
        :return: true if the contract got close
        """
        price = candle['Low']
        # (price / self.entry_price - 1) * self.position <= -1 / self.leverage
        res = self.pnl(price) <= -self.margin
        if res:
            self.size = 0

        return self.size == 0

    def close_position(self, size, mark_price):
        """
        Close a position by the size of values that you want to close
        for example the position on 1 BTC and you want to close only 1/3 in when the price is reached to 60K
        than size should be 1/3 and price needs to be 60k

        * notice this method is assume that the price is the current price and not set a profit limit order for it
        :param size: the size of the contract you want to close
        :param mark_price: the current price of the contract
        :return: the profit / lose of this close contract, and margin that closed
        """
        if size > self.size:
            size = self.size
        margin = self.margin * size / self.size
        self.margin -= margin
        self.size -= size
        return (mark_price - self.entry_price) * size * self.position, margin

