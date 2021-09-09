from exchange_bots.Portfolio import Portfolio


class BinancePortfolio(Portfolio):

    BINANCE_FEE = 0.1

    def __init__(self, client, timestamp, **coins):
        super().__init__(timestamp, BinancePortfolio.BINANCE_FEE, **coins)
        self.client = client
