import boto3
import time
import json
import Pipeline as pi

ACCESS_KEY=''
SECRET_KEY=''
REGION='ap-northeast-2'

client = boto3.client('kinesis',region_name = REGION,aws_access_key_id = ACCESS_KEY, aws_secret_access_key= SECRET_KEY)


def get_records():

    response = client.describe_stream(StreamName='kine_test')
    
    my_shard_id = response['StreamDescription']['Shards'][0]['ShardId']
    # shard가 여러개이거나 reshard되어 일시적으로 여러개인 경우 Shards 갯수만큼 for loop 해야함.

    shard_iterator = client.get_shard_iterator(StreamName='kine_test',
                                                        ShardId=my_shard_id,
                                                        ShardIteratorType='LATEST')
                                                        # LATEST 는 get_records 호출시점부터 들어온 데이터. 즉 이 프로세스 실행시점부터 들어온 데이터만 수신한다는 뜻.
                                                        # kinesis data stream은 기본적으로 24시간 데이터 보관.
                                                    

    my_shard_iterator = shard_iterator['ShardIterator']

    record_response = client.get_records(ShardIterator=my_shard_iterator,
                                                Limit=10000)

    while 'NextShardIterator' in record_response:
        record_response = client.get_records(ShardIterator=record_response['NextShardIterator'],
                                                    Limit=10000)
        # get_records 매번 호출시마다 NextShardIterator 값이 나오므로, 그걸로 session(?)을 유지해서 놓치는 데이터가 없도록 함.

        print('record_response: {}'.format(record_response))

        records = record_response['Records']
        data = []
        if len(records) > 0:
            for x in records:
                a = x['Data'].decode('utf-8')
                a = json.loads(a)
                data.append(a)
                #2021-12-30T05:45:31.541Z.json.gz
                indate=data[0]['inDate']
                #indate_sp=indate.split('-')
                #z = f'kine/{indate_sp[0]}/{indate_sp[1]}/{indate_sp[2][:2]}/{indate_sp[2][3:15]}.json.gz'
                z = f'kine/{indate}.json.gz'
                #print('data: {}'.format(x['Data']))
            print('===============')
            #print(len(records))
        # wait for 5 seconds
        if len(data) !=0:
            data = pi.gamer_enc(data)
            data = pi.params(data)            
            data= pi.method_trans(data)
            data=pi.indate_trans(data)
            data = pi.url_trans(data)
            data=pi.table_trans(data)
            data= pi.key_enc(data)
            
            pi.upload_json_gz(data, z)
        print('im sleep now~')        
        time.sleep(10)


def main():
    get_records()


if __name__ == "__main__":
    # execute only if run as a script
    main()