from my_agent.utils.nosql_agent import NoSQLQueryExecutor


nosql_agent = NoSQLQueryExecutor()
result = nosql_agent.execute_query("list all the product_id")
print(result)