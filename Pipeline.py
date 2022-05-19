import json
import datetime
import boto3
import os 
import gzip
import io
import time
from b64uuid import B64UUID 

import urllib3

http = urllib3.PoolManager()
r = http.request('GET', 'http://httpbin.org/robots.txt')


#s3에 연결
ACCESS_KEY=''
SECRET_KEY=''
BUCKET_NAME=''
REGION='ap-northeast-2'
URL="http://ec2-13-125-225-205.ap-northeast-2.compute.amazonaws.com/codestates/recent-data"
s3 = boto3.client( 's3', region_name = REGION, aws_access_key_id = ACCESS_KEY, aws_secret_access_key= SECRET_KEY ) 


def api_load(URL):
    """
    URL을 입력하면 Json형식으로 받는다
    

    Args:
        URL (str): API URL
    Returns:
        Json data
    """
    # url = requests.get(URL)
    # text = url.text #url에서 데이터를 text로 가져옴
    # data = json.loads(text)#가져온 text를 json으로 인식 만개를 가져오는데만 17초걸렸음..
    # return data
    #------------ lambda에서 requests를 사용못해서 바꿈
    http = urllib3.PoolManager()
    url = http.request('GET', URL)
    data=url.data    
    data = json.loads(data)#가져온 text를 json으로 인식 만개를 가져오는데만 17초걸렸음..
    return data

def Save_data(Data,save_name="data.json",do=1):
    """
    Data를 로컬에 저장한다 
    경로를 지정하지않으면 기본 경로로 저장된다1

    Args:
        Data (json): json 형식의 데이터
        save_name (str.json): 저장할 이름 경로를 설정해줘도 된다 기본 값= "data.json".
    """
    if do ==1 :
            
        with open(save_name, "w") as f: #계속 API요청하면 좀 그럴거같아서 저장했음
            json.dump(Data, f)
    if do ==2:
        with open(save_name, "w") as f: #계속 API요청하면 좀 그럴거같아서 저장했음
            json.dump(Data, f,indent=1)
        
def load_data(save_name="data.json"):
    """
    로컬에서 Data를 읽어온다
    경로를 지정하지않으면 기본경로

    Args:
        
        save_name (str.json): 기본 값= "data.json".

    Returns:
        Json data
    """
    with open(save_name, "r") as f: #저장된 json을 불러옴
        Data=json.load(f)   
    return Data

def key_enc(Data,do=1):
    """
    json의 key값을 정수로 enc한다 
    현재 가능성만 보는거라 enc,dec의 dict를 넣어놨지만 
    수정가능하고 확장가능하게 그리고 안전하게 바꿀예정

    Args:
        Data (Json)): Json 파일
        do (int): 뭘할 건지 정함 enc=1, dec=2. Defaults to 1.

    Returns:
        Json data
    """
    en={'game_id':0, 'gamer_id':1, 'inDate':2, 'url':3, 'method':4,"tableAndColumn":5}
    de={'0':'game_id', '1':'gamer_id', '2':'inDate', '3':'url', '4':'method','5':"tableAndColumn"}
    if do==1:
        for x in range(0,len(Data)):
            Data[x]=dict((en[key], value) for (key, value) in Data[x].items())
    if do==2:
        for x in range(0,len(Data)):
            Data[x]=dict((de[key], value) for (key, value) in Data[x].items())
    return Data

def gamer_enc(data):
    for d in data: 
        d['gamer_id'] = str(B64UUID(d['gamer_id']))
    return data

def gamer_dec(data):
    for r in data:
        r['gamer_id'] = str(B64UUID(r['gamer_id']).uuid)
    return data

def base62(index): 
    """
    정수를 base62로 인코딩한다 (글자 수가 줄어듬)

    Args:
        index (int)): 리스트의 인덱스
    Return : 인코딩된 값
    """
    result = "" 
# Base62 인코딩의 기본이 되는 문자들(배열은 상관없이 중복이 없으면 됩니다.) 
    words = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'] 
    while index % 62 > 0 or result == "": 
    # index가 62인 경우에도 적용되기 위해 do-while 형식이 되도록 구현했다. 
        result = result + words[index % 62] 
        index = int(index / 62) 
    return result # URL을 단축 URL로 만드는 함수 

def params(data) : 
    """
    파라미터의 불필요한 부분을 제거

    Args:
        data (json): json data

    Returns:
        json: json data
    """
    u ='url'
    for n in range(len(data)):
        url = data[n][u]
        b = url.split('/') # url을 /로 나눔
        index = len(b)

        for i in range(len(b)): # 나눠진 url을 하나씩 불러옴
            if '-' in b[i]: # uuid나 시간으로 표시되어 있는 url내용을 없애기 위해 uuid랑 시간에만 포함되어있는 '-'를 검색
                index = i
            elif b[i].isdigit():
                index = i
                break
        b = b[:index] # '-'가 검색되면 그 뒤로 다 날림
        if '?' in b[-1]: # 파라미터 값중 ?뒤로 j쿼리 같은게 있으니 날림
            index = b[-1].find('?')
            b[-1] =  b[-1][:index]
        elif '%' in b[-1]: # %가 url 인코딩? 이런게 있으니 없앰
            index = b[-1].find('%')
            b[-1] =  b[-1][:index]
        i ='/'.join(b)
        data[n][u] = i
    return data 

    
#시간 문제로 2중 인코딩을 한번으로 변경 주석처리 해두겠음
def url_enc(URL,DB): # DB에 URL Insert 
    """
    url을 인코딩한다 

    Args:
        URL (str): url

    Returns:
        [str]]: base62로 짧아진 url
    """
    
    if URL not in DB:
        DB.append(URL)
    index =  DB.index(URL)# URL이 등록 된 Index를 Base62로 인코딩 
    # shortURL = base62(index) # 인코딩 된 정보 DB에 갱신 
    # DBD[shortURL]=index
    
    return index

def url_dec(URL,DB): # DB에 URL Insert 
    """
    base62로 짧아진 url을 다시 원래 url로 복원한다

    Args:
        URL (str): 짧아진 url

    Returns:
        [str]: 복원된 url
    """
    
    return DB[int(URL)] #2중으로 바꿀시 제거후 아래 주석 풀면됨 
    #global DBD
    #return DB[DBD[URL]]   
    
def url_trans(Data,do=1):
    """
    url을 변환한다 
    enc =1
    dec =2
    reset db =9
    가능성이있는지만 테스트 
    당장 좀 개판이라 사용하게된다면 개선이 많이 필요하다

    Args:
        Data (Json): Json data
        do (int): url을 변환한다 enc=1, dec=2, reset DB = 9 default to 1

    Returns:
        json data
    """
    S3_DB=s3.get_object( Bucket = BUCKET_NAME, Key='DB/DB.json')
    #s3에서 오브젝트를 가져오면 본문은 body에있다
    DB=json.loads(S3_DB['Body'].read())['DB']   
    
    #key를 변환시켜서 key가 안맞는 경우가있어 추가해줬음
    url='url'
    #if any(url in Data[0].keys() for url in ('url', '3')):
    if url in Data[0].keys():            
        if do==1:
            for x in range(0,len(Data)):
                Data[x][url]=url_enc(Data[x][url],DB)
            JDB={'DB':DB}#리스트를 json에 넣기위한 편법
            #DB로 쓸것들은 s3에 저장    
            s3.put_object( Body=json.dumps(JDB, ensure_ascii=False), Bucket = BUCKET_NAME, Key='DB/DB.json')
            #s3.put_object( Body=json.dumps(DBD, ensure_ascii=False), Bucket = BUCKET_NAME, Key='DB/DBD.json') 1중으로 바꿔서 제거했음
        elif do==2:    
            for x in range(0,len(Data)):
                Data[x][url]=url_dec(Data[x][url],DB)
        elif do==9:
            #S3 DB 초기화 
            DB=[]
            #DBD={}
            JDB={'DB':DB}
            s3.put_object( Body=json.dumps(JDB, ensure_ascii=False), Bucket = BUCKET_NAME, Key='DB/DB.json')
            #s3.put_object( Body=json.dumps(DBD, ensure_ascii=False), Bucket = BUCKET_NAME, Key='DB/DBD.json')
            
        return  Data
    return Data
            
def method_trans(Data, do=1):
    a= {'GET':0,
        'PUT':1,
        'POST':2,
        'DELETE':3}
    b={0:'GET', # 추가수정 json으로 만들때(json.dump) key값은 전부 str로 변함
        1:'PUT',
        2:'POST',
        3:'DELETE'}
    if 'method' in Data[0].keys():
        if do ==1:
            for i in Data:
                z = i['method']
                i['method'] = a[z]
            return Data
        elif do ==2:
            for i in Data:
                z=i['method']
                i['method'] = b[z]
            return Data
    return Data



def indate_trans(Data,do=1):
    """
    indate형식을 timestamp로 변환 복원을 한다
    enc =1
    dec =2
    
    Args:
        Data (Json): Json data
        do (int): url을 변환한다 enc=1 dec=2 default to 1
    Return:
        json data
    
    """
    #key를 변환시켜서 key가 안맞는 경우가있어 추가해줬음
    #indate='inDate'
    
    #if any(indate in Data[0].keys() for indate in ('indate', '2')):    
    
    
    
    def timestamp():
        """
        indate 를 timestamp 형식으로 변환한다
        
        Returns:
            timestamp list
        """
        ## inDate(str) -> datetime -> timestamp 변환 
        timestamp = [datetime.datetime.strptime(i[indate], '%Y-%m-%dT%H:%M:%S.%fZ').timestamp() for i in Data]
        return timestamp
   
    def conv_date():
        """
        timestamp를 indate 형식으로 변환한다

        Returns:
            indate 원본 list
        """
        
        ## timestamp -> datetime -> 원래 형식으로 변환 
        conv_date = [datetime.datetime.strftime(datetime.datetime.fromtimestamp(t[indate]), '%Y-%m-%dT%H:%M:%S.%f')[:-3]+'Z' for t in Data]
        return conv_date
    indate='inDate'
    if 'inDate' in Data[0].keys():        
        if do==1:
            timestamp_data=timestamp()        
            for x in range(0,len(Data)):
                Data[x][indate]=timestamp_data[x]
        if do==2:
            conv_date_data=conv_date()        
            for x in range(0,len(Data)):
                Data[x][indate]=conv_date_data[x]
        return Data
    return Data    

def use_gz(Data,save_name="data.json.gz",do=1):
    """
    Data를 로컬에 gz로 저장,불러오기
    경로를 지정하지않으면 기본 경로로 저장된다

    Args:
        Data (json): json 형식의 데이터
        save_name (str.json): 저장할 이름 경로를 설정해줘도 된다 기본 값= "data.json.gz".
        save =1
        load =2
    Return:
        load 시 json data
    """    
    if do==1:
        with gzip.GzipFile(save_name, 'w') as f:
            f.write(json.dumps(Data,indent=1).encode('utf-8')) #줄바꿈 확인용 인덴트
    if do==2:
        with gzip.GzipFile(save_name, 'r') as f:
            data = json.loads(f.read().decode('utf-8'))
            return data

def upload_json_gz(data,save_name,BUCKET_NAME=BUCKET_NAME,s3=s3, default=None, encoding='utf-8'):
    """
    S3에 data를 (파일저장없이) 
    바로 저장하는 함수

    Args:
        data ([json]): json data
        save_name ([str]): 저장이름(경로포함),키 값
        BUCKET_NAME (str)): 버켓이름. Defaults to BUCKET_NAME.
        s3 (str): 연결된 s3 client. Defaults to s3.
        default : 모름
        encoding (str): 인코딩 딱히건들필요없음. Defaults to 'utf-8'.
    """
    #jsonl형식으로 s3에 저장
    inmem = io.BytesIO()
    with gzip.GzipFile(fileobj=inmem, mode='wb') as fh:
        with io.TextIOWrapper(fh, encoding=encoding) as wrapper:
            for x in data:
                json.dump(x, wrapper)
                wrapper.write('\n') #athena에서 인식하게 하기 위함
    inmem.seek(0)
    s3=s3.put_object(Bucket=BUCKET_NAME, Body=inmem, Key=save_name)

        
def download_json_gz(key, BUCKET_NAME=BUCKET_NAME,s3=s3):
    """
    키를 입력해서 gz 파일을 불러오기

    Args:
        key (str)): 파일 경로
        BUCKET_NAME (str)): 버켓이름. Defaults to BUCKET_NAME.
        s3 (str): s3 클라이언트. Defaults to s3.

    Returns:
        json data
    """
    
    data = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    content = data['Body'].read()
    with gzip.GzipFile(fileobj=io.BytesIO(content), mode='rb') as f:
        return json.load(f)

def make_table_enc(data, table): # tableAndColumns 에 있는 내용들을 간단하게 정리하기 위해 게임 아이디 별로 내용을 분류하기 위한 함수
  for i in data:
    if i['game_id'] not in table.keys():#게임아이디로 이루어진 key값이 있나 확인
      z = i['game_id']
      table[z] = {}#게임아이디와 짝지어질 dict만들기
      if i['tableAndColumn'] not in table[z].values():#tableAndColumn 내용이 게임아이디의 값에 있나 확인.
        ind= len(table[z])#dict에 있는 거 갯수
        table[z][ind]=i['tableAndColumn'] #위의 갯수를 키로 이용해서 dict에 넣는다.
    elif i['game_id'] in table.keys():#이미 게임 아이디로 만들어진 값이 있다면 새로운 딕셔너리를 만들지 않고 위의 반복
      z = i['game_id'] 
      if i['tableAndColumn'] not in table[z].values():
        ind= len(table[z])
        table[z][ind]=i['tableAndColumn']
    z = format(i['game_id'], '04')# 인코딩할때 숫자에서 게임아이디와 키값을 찾기 편하기 위해서 게임아이디를 4자리로 만든다.
    for key,value in table[i['game_id']].items():
        if i['tableAndColumn'] == value:
            a = format(key, '02')# 인코딩할때 숫자를 찾기 쉽기 위해 키값을 두자리로 만든다.
    b = z+a    
    i['tableAndColumn']=b
  
  return data,table
"""
위에서 만들어진 table은 
{게임아이디: {0: tableAndColumn 내용,
            1: tableAndColumn 내용2 
            ...}.
}
이런 식으로 만들어 진다.
"""


def table_enc(data,table):# 넣을때 json 파일 전체를 넣어야함. 그래야 game id로 구별 가능, 무조건 위에 make table을 먼저 거칠것!
  result = []

  for i in data:
    z = format(i['game_id'], '04')# 인코딩할때 숫자에서 게임아이디와 키값을 찾기 편하기 위해서 게임아이디를 4자리로 만든다.
    for key,value in table[i['game_id']].items():
      if i['tableAndColumn'] == value:
        a = format(key, '02')# 인코딩할때 숫자를 찾기 쉽기 위해 키값을 두자리로 만든다.
    b = z+a    
    result.append(b)
  return result

def table_dec(data, table): #여기서에 데이터는 위에 table_enc를 거친 데이터이다.
    
    for i in data:
        a= int(i['tableAndColumn'][:4]) # 6글자중 앞에 4글자는 game_id    
        b= int(i['tableAndColumn'][4:]) #6글자 중 뒤에 2글자는 원래 데이터가 저장되어 잇는 table 키
        
        i['tableAndColumn'] = table[str(a)][str(b)] #원래 데이터
        
    return data

def table_trans(data,do=1):
    if 'tableAndColumn' in data[0].keys():
        S3_TC_DB=s3.get_object( Bucket = BUCKET_NAME, Key='DB/TC_DB.json')
        TC_DB=json.loads(S3_TC_DB['Body'].read())    

        
        if do==1:
            data,TC_DB=make_table_enc(data,TC_DB)
            
            s3.put_object( Body=json.dumps(TC_DB, ensure_ascii=False), Bucket = BUCKET_NAME, Key='DB/TC_DB.json')
            
            return data
            
        elif do==2:   
            data=table_dec(data,TC_DB)
            return data
        
        elif do ==9:
            #초기화    
            TC_DB={}
            s3.put_object( Body=json.dumps(TC_DB, ensure_ascii=False), Bucket = BUCKET_NAME, Key='DB/TC_DB.json')
    return data

# athena 결과물 (csv) -> json 변환 
def csv2json(result_json):
    data = json.loads(result_json)
    make_json={}
    make_json_list=[]
    #원래 json 형식으로 바꿔줌
    for z in range(len(data)-1): 
        for x in range(len(data[0]['Data'])):
            if data[z+1]['Data'][x] =={}:
                make_json[data[0]['Data'][x]['VarCharValue']]=None
            else:
                make_json[data[0]['Data'][x]['VarCharValue']]=data[z+1]['Data'][x]['VarCharValue']
        make_json_list.append(make_json)
        make_json={}    
    
    return make_json_list