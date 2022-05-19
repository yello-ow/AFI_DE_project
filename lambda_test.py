import Pipeline
import json
import time
import boto3

# athena
DATABASE = ''
TABLE = ''

# S3
S3_OUTPUT = 's3://'
S3_BUCKET = ''

# retries
RETRY_COUNT = 10


#query

def lambda_handler(event, context):
    
    
    #event['queryStringParameters']['query']
    #query = 'select  cast (game_id as json) as json FROM "test"."testjson2" limit 10;'
    query = event['queryStringParameters']['query']#'SELECT * FROM "test"."testjson2" limit 10' #"SELECT * FROM %s.%s;" % (DATABASE, TABLE)#keyword game_id,indate,tableandcolumn
   
    # athena client
    client = boto3.client('athena')

    # 쿼리 response
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': DATABASE
        },
        ResultConfiguration={
            'OutputLocation': S3_OUTPUT,
        }
    )
    
    # 쿼리 실행 iD
    query_execution_id = response['QueryExecutionId']
    
    
      # 쿼리? 상태
    for i in range(1, 1 + RETRY_COUNT):

        # 쿼리 실행
        query_status = client.get_query_execution(QueryExecutionId=query_execution_id)
        query_execution_status = query_status['QueryExecution']['Status']['State']
        if query_execution_status == 'SUCCEEDED':
            print("STATUS:" + query_execution_status)
            break

        if query_execution_status == 'FAILED':
            #raise Exception("STATUS:" + query_execution_status)
            print("STATUS:" + query_execution_status +query_status['QueryExecution']['Status']['StateChangeReason'])

        else:
            print("STATUS:" + query_execution_status)
            time.sleep(i)
    else:
        # 타임오버
        client.stop_query_execution(QueryExecutionId=query_execution_id)
        raise Exception('TIME OVER')
    import ast    
    # 쿼리 결과
    result = client.get_query_results(QueryExecutionId=query_execution_id)
    result_json=json.dumps(result['ResultSet']['Rows'])#query 결과(csv)를 json 형식으로 변환
    result_json = ast.literal_eval(result_json)
    
    while 'NextToken' in result:
        result = client.get_query_results(QueryExecutionId=query_execution_id,NextToken=result['NextToken'])
        result_json_next=json.dumps(result['ResultSet']['Rows'])
        result_json_next = ast.literal_eval(result_json_next)
        result_json.extend(result_json_next)
        print(len(result_json))
    
        
    # #result = client.get_query_batch_results(QueryExecutionId=query_execution_id)
    # #result_json=json.dumps(result['ResultSet']['Rows'])#query 결과(csv)를 json 형식으로 변환
    #result_json=json.dumps(result_json)
    result_data=Pipeline.csv2json(result_json) #json 형식으로 변환된걸 원본 형식으로 변환
    result_data=Pipeline.key_enc(result_data,2) # key 복원
    result_data=Pipeline.method_trans(result_data,2)
    result_data=Pipeline.url_trans(result_data,2) # url 복원
    result_data=Pipeline.table_trans(result_data,2)# db생성 및 복원
    result_data=Pipeline.gamer_dec(result_data)# gamer_id 복원
    
    return {
      'statusCode': 200,
      'body': json.dumps(result_data)
  }
   