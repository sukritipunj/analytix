import sqlite3
import pandas as pd

# Load your CSV file into a pandas DataFrame
financial_data = pd.read_csv('Financial_Sample_Cleaned.csv')

# Create an in-memory SQLite database and load the DataFrame as a SQL table
conn = sqlite3.connect(':memory:')
financial_data.to_sql('financial_data', conn, index=False, if_exists='replace')

# Define and execute the SQL queries

# Hour 2: Basic aggregation
query_hour2 = """
SELECT 
    Segment,
    SUM(Sales) as total_revenue,
    AVG(Profit) as avg_profit,
    COUNT(*) as transactions
FROM financial_data 
GROUP BY Segment
ORDER BY total_revenue DESC;
"""
hour2_result = pd.read_sql_query(query_hour2, conn)
print("Hour 2 - Basic Aggregation:")
print(hour2_result)

# Hour 4: Window functions (SQLite supports window functions from version 3.25+)
query_hour4 = """
SELECT 
    Month_Name,
    SUM(Sales) as monthly_sales,
    LAG(SUM(Sales)) OVER (ORDER BY Month_Number) as prev_month,
    (SUM(Sales) - LAG(SUM(Sales)) OVER (ORDER BY Month_Number)) * 100.0 / 
    LAG(SUM(Sales)) OVER (ORDER BY Month_Number) as growth_rate
FROM financial_data
GROUP BY Month_Number, Month_Name
ORDER BY Month_Number;
"""
hour4_result = pd.read_sql_query(query_hour4, conn)
print("\nHour 4 - Window Functions:")
print(hour4_result)

# Hour 6: CTEs and window functions
query_hour6 = """
WITH segment_performance AS (
    SELECT 
        Segment,
        SUM(Sales) as revenue,
        SUM(COGS) as costs,
        SUM(Profit) as profit
    FROM financial_data
    GROUP BY Segment
)
SELECT 
    Segment,
    revenue,
    profit,
    ROUND(profit*100.0/revenue, 2) as profit_margin,
    ROW_NUMBER() OVER (ORDER BY profit DESC) as profit_rank
FROM segment_performance;
"""
hour6_result = pd.read_sql_query(query_hour6, conn)
print("\nHour 6 - CTEs and Ranking:")
print(hour6_result)

# Close connection when done
conn.close()
