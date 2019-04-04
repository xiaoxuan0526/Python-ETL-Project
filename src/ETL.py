import os
import csv
import sqlite3
from collections import deque


class AbstractDAO:
    def __init__(self, db_name):
        self.db_name = db_name

    def insert_records(self, records):
        raise NotImplementedError

    def select_all(self):
        raise NotImplementedError

    def connect(self):
        conn = sqlite3.connect(self.db_name)
        return conn


class BaseballStatsDAO(AbstractDAO):
    def __init__(self, db_name):
        super().__init__(db_name)

    def insert_records(self, records):
        conn = self.connect()
        c = conn.cursor()

        for recordItem in records:
            name = recordItem.name
            number_games_played = recordItem.g
            avg = recordItem.avg
            salary = recordItem.salary
            c.execute("INSERT INTO baseball_stats VALUES (?, ?, ?, ?)", (name, number_games_played, avg, salary))

        conn.commit()
        conn.close()

    def select_all(self):
        conn = self.connect()
        c = conn.cursor()

        recordsDeque = deque()

        c.execute("SELECT player_name, games_played, average, salary FROM baseball_stats;")
        rows = c.fetchall()

        for row in rows:
            tmpName = row[0]
            tmpG = row[1]
            tmpAVG = row[2]
            tmpSalary = row[3]

            tmpRecord = BaseballStatRecord(tmpName, tmpSalary, tmpG, tmpAVG)
            recordsDeque.append(tmpRecord)

        conn.close()
        return recordsDeque


class StockStatsDAO(AbstractDAO):
    def __init__(self, db_name):
        super().__init__(db_name)

    def insert_records(self, records):

        conn = self.connect()
        c = conn.cursor()

        for record in records:
            ticker = record.name
            company_name = record.company_name
            exchange_country = record.exchange_country
            price = record.price
            exchange_rate = record.exchange_rate
            shares_outstanding = record.shares_outstanding
            net_income = record.net_income
            market_value_usd = record.market_value_usd
            pe_ratio = record.pe_ratio

            c.execute("INSERT INTO stock_stats VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (
            ticker, company_name, exchange_country, price, exchange_rate, shares_outstanding, net_income,
            market_value_usd, pe_ratio))
        # END for record in records

        conn.commit()
        conn.close()

    def select_all(self):
        conn = self.connect()
        c = conn.cursor()

        recordsDeque = deque()

        c.execute("SELECT ticker, company_name, exchange_country, price, exchange_rate, shares_outstanding, net_income, market_value, pe_ratio FROM stock_stats")
        rows = c.fetchall()

        for row in rows:
            tmpName = row[0]
            tmpCompanyName = row[1]
            tmpExchangeCountry = row[2]
            tmpPrice = row[3]
            tmpExchangeRate = row[4]
            tmpSharesOutstanding = row[5]
            tmpNetIncome = row[6]
            tmpMarketValueUSD = row[7]
            tmpPERatio = row[8]

            tmpRecord = StockStatRecord(tmpName, tmpCompanyName, tmpExchangeCountry, tmpPrice, tmpExchangeRate,
                                        tmpSharesOutstanding, tmpNetIncome, tmpMarketValueUSD, tmpPERatio)
            recordsDeque.append(tmpRecord)

        conn.close()

        return recordsDeque


class AbstractRecord:
    def __init__(self, name):
        self.name = name


class StockStatRecord(AbstractRecord):
    def __init__(self, name, company_name, exchange_country, price, exchange_rate, shares_outstanding, net_income,
                 market_value_usd, pe_ratio):
        super().__init__(name)
        self.company_name = company_name
        self.exchange_country = exchange_country
        self.price = price
        self.exchange_rate = exchange_rate
        self.shares_outstanding = shares_outstanding
        self.net_income = net_income
        self.market_value_usd = market_value_usd
        self.pe_ratio = pe_ratio

    def __str__(self):
        return "StockStatRecord({0},{1},$price={2:.2f},$Cap={3:.2f},P/E={4:.2f})".format(self.name, self.company_name,
                                                                                         self.price * self.exchange_rate,
                                                                                         self.market_value_usd,
                                                                                         self.pe_ratio)


class BaseballStatRecord(AbstractRecord):
    def __init__(self, name, salary, g, avg):
        super().__init__(name)
        self.salary = salary
        self.g = g
        self.avg = avg

    def __str__(self):
        return "BaseballStatRecord({0},{1},{2},{3:.3f})".format(self.name, self.salary, self.g, self.avg)


# Define Reader Class

class AbstractCSVReader:
    def __init__(self, path):
        self.path = path

    def row_to_record(self, row):
        raise NotImplementedError

    def load(self):
        records_list = []

        with open(self.path, "r") as csvfile:
            reader = csv.DictReader(csvfile)

            for row in reader:
                try:
                    row_record = self.row_to_record(row)
                except BadData:
                    continue

                records_list.append(row_record)

        return records_list


class StocksCSVReader(AbstractCSVReader):
    # The Class for Reading Stocks CSV File

    def row_to_record(self, row):

        # Validation

        # If any piece of info is missing
        for key, value in row.items():
            if value == '':
                raise BadData

        # If any of the numbers cannot be parsed

        try:
            price_number = float(row['price'])
            exchange_rate_number = float(row['exchange_rate'])
            shares_outstanding_number = float(row['shares_outstanding'])
            net_income_number = float(row['net_income'])
        except ValueError:
            raise BadData

        # If the name is empty
        if row['ticker'] == '':
            raise BadData

        # Calculation using Extracted Records
        market_value_usd_number = price_number * exchange_rate_number * shares_outstanding_number

        # In case of division by zero error
        try:
            pe_ratio_number = price_number / net_income_number
        except ZeroDivisionError:
            raise BadData

        stock_record = StockStatRecord(row['ticker'], row['company_name'], row['exchange_country'], price_number,
                                       exchange_rate_number, shares_outstanding_number, net_income_number,
                                       market_value_usd_number, pe_ratio_number)

        return stock_record


class BaseballCSVReader(AbstractCSVReader):
    def row_to_record(self, row):

        # Validation

        # If any piece of info is missing
        for key, value in row.items():
            if value == '':
                raise BadData

        # If any of the numbers cannot be parsed
        try:
            salary_number = int(row['SALARY'])
            g_number = int(row['G'])
            avg_number = float(row['AVG'])
        except ValueError:
            raise BadData

        # If the name is empty
        if row['PLAYER'] == '':
            raise BadData

        baseball_record = BaseballStatRecord(row['PLAYER'], salary_number, g_number, avg_number)

        return baseball_record


# Define the BadData Exception
class BadData(Exception):
    pass


if __name__ == "__main__":

    currDir = os.getcwd()

    baseballRecord = BaseballCSVReader(os.path.join(currDir,'MLB2008.csv')).load()
    stocksRecord = StocksCSVReader(os.path.join(currDir,'StockValuations.csv')).load()

    baseballDAO = BaseballStatsDAO('baseball.db')
    stocksDAO = StockStatsDAO('stocks.db')

    baseballDAO.insert_records(baseballRecord)
    stocksDAO.insert_records(stocksRecord)

    baseballDeque = baseballDAO.select_all()
    stocksDeque = stocksDAO.select_all()

    # Calculate and print the number of tickers by exchange_country
    tickerNumberDict = {}

    while True:

        try:
            stocksRecordItem = stocksDeque.popleft()
        except IndexError:
            break

        itemExchangeCountry = stocksRecordItem.exchange_country

        if itemExchangeCountry not in tickerNumberDict:
            tickerNumberDict.update({itemExchangeCountry: 1})
        else:
            tickerNumber = tickerNumberDict.get(itemExchangeCountry)
            tickerNumberDict.update({itemExchangeCountry: tickerNumber + 1})
    # end of while loop

    # Compute the average salary
    averageSalaryDict = {}

    while True:

        try:
            baseballRecordItem = baseballDeque.popleft()
        except IndexError:
            break

        itemAVG = baseballRecordItem.avg
        itemSalary = baseballRecordItem.salary

        if itemAVG not in averageSalaryDict:
            salaryList = []
        else:
            salaryList = averageSalaryDict[itemAVG]

        salaryList.append(itemSalary)
        averageSalaryDict.update({itemAVG: salaryList})
        # end of while loop

    for k, v in averageSalaryDict.items():
        averageSalaryDict.update({k: round(sum(v) / len(v), 3)})

    # Print Average Salary
    for k, v in averageSalaryDict.items():
        print("{0:.3f} {1:.2f}".format(k, v))

    # Print Ticker Number
    for k, v in tickerNumberDict.items():
        print(k, v)



