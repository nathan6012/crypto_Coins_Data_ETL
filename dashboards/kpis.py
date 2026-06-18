from sqlalchemy import text



# KPI: Total coins tracked
def total_coins(conn):

    result =  conn.execute(
        text("""
        SELECT DISTINCT COUNT(*)
        FROM crypto_markets
        """)
    )

    return result.scalar() or 0
    
# KPI: Total market cap    
def total_market_cap(conn):

    result =  conn.execute(
        text("""
        SELECT SUM(market_cap)
        FROM crypto_markets
        """)
    )

    return result.scalar()

# KPI: Average price
def avg_price(conn):

    result = conn.execute(
        text("""
        SELECT AVG(current_price)
        FROM crypto_markets
        """)
    )

    return result.scalar() or 0
    
    
# KPI: Top coin by market cap    
def top_coin(conn):

    result = conn.execute(
        text("""
SELECT 
    name,
    symbol,
    current_price
FROM crypto_markets
ORDER BY current_price DESC
LIMIT 1;

""" ))

    row = result.fetchone()

    if row:
        return {
            "name": row[0],
            "market_cap": row[1]
        }

    return None  


# KPI: Highest 24h gai    
    
def top_gainer(conn):

    result = conn.execute(
        text("""
SELECT 
    name,
    price_change_percentage_24h
FROM crypto_markets
ORDER BY price_change_percentage_24h DESC
LIMIT 1;
        """)
    )

    row = result.fetchone()

    if row:
        return {
            "name": row[0],
            "change": row[1]
        }

    return None    
    
    
# KPI: Biggest 24h loser    
def top_loser(conn):

    result = conn.execute(
        text("""
SELECT 
    name,
    price_change_percentage_24h
FROM crypto_markets
ORDER BY price_change_percentage_24h ASC
LIMIT 1;
        """)
    )

    row = result.fetchone()

    if row:
        return {
            "name": row[0],
            "change": row[1]
        }

    return None    