import time
import datetime
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import GenieAPI
from databricks.sdk.service.dashboards import GenieMessage
from databricks.sdk.service.sql import StatementExecutionAPI

import pandas as pd

from decouple import config

w = WorkspaceClient(
    host = config('DATABRICKS_HOST'),
    token = config('DATABRICKS_TOKEN')
)

# spark = SparkSession.builder \
#     .appName("RpGenieConnectorYearbook") \
#     .master("local[*]") \
#     .getOrCreate()

api_client = w.api_client
g = GenieAPI(api_client=api_client)
# g = w.genie
# space_id = '01ef63ad27dd17109ed3489f3de23a89'
space_id = '01ef6d284bf211369b3a182d7b37ed23'

# APPROACH 1: Below seems one complete function that can do everything in one shot, it created a question and we can verify it from Genie console, but sitting endlessly, never come out of it
# If timeout is set, it always come back with timeout error, with EXECUTING_QUERY state
# timeout = datetime.timedelta(seconds=30)
# genie_direct_message = g.start_conversation_and_wait(space_id=space_id, content='What is the annual reports for South Korea?', timeout=timeout)

# APPROACH 2: Broke down a few steps, it created a question and we can verify it from Genie console, but sitting endlessly, never come out of it
# genie_waiter = g.start_conversation(space_id=space_id, content='What is the annual reports for Korea?')
# conversation_id = genie_waiter.response.conversation_id
# message_id = genie_waiter.response.message_id
# genie_message = g.wait_get_message_genie_completed(conversation_id=conversation_id, message_id=message_id, space_id=space_id)

# APPROACH 3: Documentation says to check the query result
first_question = "What are the top 3 countries by pioneer growth rate over the last 3 years?"
genie_waiter = g.start_conversation(space_id=space_id, content=first_question)
conversation_id = genie_waiter.response.conversation_id
message_id = genie_waiter.response.message_id
time.sleep(10)

genie_response = g.get_message_query_result(space_id=space_id, conversation_id=conversation_id, message_id=message_id)
resp = genie_response.statement_response
status_state = genie_response.statement_response.status.state
# if status_state.value == 'SUCCEEDED':
#     data = resp['result']
#     meta = resp['manifest']
#     rows = [[c['str'] for c in r['values']] for r in data['data_typed_array']]
#     columns = [c['name'] for c in meta['schema']['columns']]
#     # df = spark.createDataFrame(rows, schema=columns)
#     df = pd.DataFrame(rows, columns=columns)
#     df.show()

# json_data = genie_response.statement_response.as_dict()
# print("STATEMENT RESPONSE: ")
# print(json_data)

# result = genie_response.statement_response.result
# result_data = result.as_dict()
# json_data = result.data_array
# print("RESULT DATA ARRAY: ")
# print(json_data)

result_column_data = genie_response.statement_response.as_dict()
meta = result_column_data['manifest']

statement_id = genie_response.statement_response.statement_id
stmtAPI = StatementExecutionAPI(api_client=api_client)
statement_response = stmtAPI.get_statement(statement_id=statement_id)

result_row_data = stmtAPI.get_statement_result_chunk_n(statement_id=statement_id, chunk_index=0).as_dict()
# rows = [[c['str'] for c in r['values']] for r in result_row_data['data_array']]

rows = [[str(c) for c in r] for r in result_row_data['data_array']]
columns = [c['name'] for c in meta['schema']['columns']]
#     # df = spark.createDataFrame(rows, schema=columns)
df = pd.DataFrame(rows, columns=columns)
with pd.option_context('display.max_columns', None):
    print(df)


# output_data = result_data.as_dict()





# result_response = g.execute_message_query(space_id=space_id, conversation_id=conversation_id, message_id=message_id)
# print(result_response)
# time.sleep(5)

# result_response = g.execute_message_query(space_id=space_id, conversation_id=conversation_id, message_id=message_id)
# print(result_response.as_dict())


# genie_message = g.create_message(space_id='space_id', conversation_id='conversation_id', content='What is the annual reports for Korea?')


# List all dashboards
# dashboards = w.dashboards.list()
# for dashboard in dashboards:
#     print(f"Dashboard Name: {dashboard.name}, ID: {dashboard.id}")
