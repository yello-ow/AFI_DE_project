import Pipeline

query = 'SELECT * FROM "test"."testjson2"'
url = f'https://4jiijehisd.execute-api.ap-northeast-2.amazonaws.com/default/test?query={query}'

data = Pipeline.api_load(url)
print(len(data))