import sqlite3
import os

conn = sqlite3.connect('baseball.db')
c = conn.cursor()

try:
    c.execute(" CREATE TABLE baseball_stats (player_name text, games_played int, average real, salary real)")
    print ("New SQLite table with column baseball_stats has been created")

except Exception as exc:
    print ("Exception:", str (exc))

# Commit changes and close the connection to database
conn.commit()
conn.close()

conn = sqlite3.connect('stocks.db')
c = conn.cursor()

try:
    c.execute("CREATE TABLE stock_stats (ticker text, company_name text, exchange_country text, price real, exchange_rate real, shares_outstanding real, net_income real, market_value real, pe_ratio real)")
    print("New SQLite table with column stock_stats has been created")

except Exception as exc:
    print ("Exception:", str (exc))

conn.commit()
conn.close()


