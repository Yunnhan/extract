from redis import StrictRedis

redis = StrictRedis(host='192.168.1.60', port=6379, db=7, password='foobared')
# print(redis.lpush('zj_data', 'xxx'))
print(redis.lpop('zj_data'))
print(redis.llen('zj_data'))
