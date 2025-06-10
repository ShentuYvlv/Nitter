#!/usr/bin/env python3
"""
测试发送包含图片的推文到Redis
用于调试SSE服务器的图片显示功能
"""

import redis
import json
import time
from datetime import datetime

# Redis配置
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0
TWEET_STREAM_KEY = "tweets"

def send_test_tweet_with_images():
    """发送包含图片的测试推文"""
    
    # 连接Redis
    try:
        redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
        redis_client.ping()
        print("✅ Redis连接成功")
    except Exception as e:
        print(f"❌ Redis连接失败: {e}")
        return
    
    # 测试用例1：单张图片
    test_tweet_1 = {
        "id": f"test_single_image_{int(time.time())}",
        "user_id": "test_user",
        "username": "测试用户",
        "content": "这是一条包含单张图片的测试推文",
        "html": "<p>这是一条包含单张图片的测试推文</p>",
        "published_at": datetime.now().strftime("%a, %d %b %Y %H:%M:%S"),
        "url": "#",
        "timestamp": datetime.now().isoformat(),
        "images": json.dumps(["https://pbs.twimg.com/media/GtA3pwYaoAA0tJa.jpg"])
    }
    
    # 测试用例2：多张图片
    test_tweet_2 = {
        "id": f"test_multi_images_{int(time.time())}",
        "user_id": "test_user",
        "username": "测试用户",
        "content": "这是一条包含多张图片的测试推文",
        "html": "<p>这是一条包含多张图片的测试推文</p>",
        "published_at": datetime.now().strftime("%a, %d %b %Y %H:%M:%S"),
        "url": "#",
        "timestamp": datetime.now().isoformat(),
        "images": json.dumps([
            "https://pbs.twimg.com/media/GtA3pwYaoAA0tJa.jpg",
            "https://pbs.twimg.com/media/Gs-6x5zakAAFYXf.jpg"
        ])
    }
    
    # 测试用例3：无图片
    test_tweet_3 = {
        "id": f"test_no_images_{int(time.time())}",
        "user_id": "test_user",
        "username": "测试用户",
        "content": "这是一条不包含图片的测试推文",
        "html": "<p>这是一条不包含图片的测试推文</p>",
        "published_at": datetime.now().strftime("%a, %d %b %Y %H:%M:%S"),
        "url": "#",
        "timestamp": datetime.now().isoformat(),
        "images": json.dumps([])
    }
    
    # 测试用例4：错误的图片数据格式
    test_tweet_4 = {
        "id": f"test_invalid_images_{int(time.time())}",
        "user_id": "test_user",
        "username": "测试用户",
        "content": "这是一条包含错误图片数据的测试推文",
        "html": "<p>这是一条包含错误图片数据的测试推文</p>",
        "published_at": datetime.now().strftime("%a, %d %b %Y %H:%M:%S"),
        "url": "#",
        "timestamp": datetime.now().isoformat(),
        "images": "invalid_json_data"  # 故意使用无效的JSON
    }
    
    test_tweets = [test_tweet_1, test_tweet_2, test_tweet_3, test_tweet_4]
    
    print(f"准备发送 {len(test_tweets)} 条测试推文...")
    
    for i, tweet in enumerate(test_tweets, 1):
        try:
            # 发送到Redis流
            stream_id = redis_client.xadd(
                TWEET_STREAM_KEY,
                tweet,
                maxlen=1000,
                approximate=True
            )
            
            print(f"✅ 测试推文 {i} 发送成功: {stream_id}")
            print(f"   内容: {tweet['content']}")
            print(f"   图片: {tweet['images']}")
            print()
            
            # 短暂延迟
            time.sleep(1)
            
        except Exception as e:
            print(f"❌ 测试推文 {i} 发送失败: {e}")
    
    print("测试推文发送完成！")
    print("请检查SSE服务器前端页面，按 Ctrl+D 开启调试模式查看详细信息。")

def check_redis_stream():
    """检查Redis流中的数据"""
    try:
        redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
        redis_client.ping()
        
        # 获取流长度
        stream_length = redis_client.xlen(TWEET_STREAM_KEY)
        print(f"Redis流长度: {stream_length}")
        
        if stream_length > 0:
            # 读取最近的几条消息
            messages = redis_client.xrevrange(TWEET_STREAM_KEY, count=5)
            print(f"最近 {len(messages)} 条消息:")
            
            for message_id, fields in messages:
                print(f"  消息ID: {message_id}")
                print(f"  用户: {fields.get('username', 'N/A')}")
                print(f"  内容: {fields.get('content', 'N/A')[:50]}...")
                print(f"  图片: {fields.get('images', 'N/A')}")
                print()
        
    except Exception as e:
        print(f"❌ 检查Redis流失败: {e}")

if __name__ == "__main__":
    print("Redis推文测试工具")
    print("="*50)
    
    # 检查当前流状态
    print("1. 检查当前Redis流状态:")
    check_redis_stream()
    
    print("\n2. 发送测试推文:")
    send_test_tweet_with_images()
    
    print("\n3. 检查发送后的流状态:")
    check_redis_stream()
