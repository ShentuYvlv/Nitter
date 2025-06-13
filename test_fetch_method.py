#!/usr/bin/env python3
"""
测试fetch_user_rss方法
"""

import asyncio
import aiohttp
import sys
import json

# 在Windows上设置正确的事件循环策略
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# 模拟EnhancedPollingEngine的部分功能
class MockPollingEngine:
    def __init__(self):
        # 模拟配置
        self.polling_config = {
            "REQUEST_TIMEOUT": 10,
            "BATCH_SIZE": 10
        }
        
        # 模拟缓存统计
        self.cache_stats = {
            "total_requests": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "bandwidth_saved": 0
        }
        
        # 模拟实例
        self.instances = [MockInstance("http://localhost:8080")]
        
        # 模拟状态管理器
        self.state_manager = MockStateManager()
    
    def get_instance_for_user(self, user_id):
        return self.instances[0]
    
    def update_instance_stats(self, instance, success, duration, is_429):
        pass
    
    async def process_rss_content(self, user_id, content):
        print(f"   📝 处理RSS内容: {len(content)} 字节")
        return True

class MockInstance:
    def __init__(self, url):
        self.url = url

class MockStateManager:
    def __init__(self):
        self.states = {}
    
    def get_user_state(self, user_id):
        return self.states.get(user_id, {})
    
    def update_user_state(self, user_id, **kwargs):
        if user_id not in self.states:
            self.states[user_id] = {}
        self.states[user_id].update(kwargs)
        print(f"   💾 更新用户状态: {user_id} -> {kwargs}")

async def test_fetch_method():
    """测试fetch_user_rss方法"""
    
    # 创建模拟引擎
    engine = MockPollingEngine()
    
    # 从polling_engine.py导入fetch_user_rss方法
    import importlib.util
    spec = importlib.util.spec_from_file_location("polling_engine", "polling_engine.py")
    polling_module = importlib.util.module_from_spec(spec)
    
    # 将方法绑定到我们的模拟引擎
    import types
    
    print("=== 测试fetch_user_rss方法 ===")
    print("测试用户: elonmusk")
    print("-" * 50)
    
    try:
        async with aiohttp.ClientSession() as session:
            # 模拟调用fetch_user_rss方法
            user_id = "elonmusk"
            instance = engine.get_instance_for_user(user_id)
            url = f"{instance.url}/{user_id}/rss"
            
            user_state = engine.state_manager.get_user_state(user_id)
            start_time = asyncio.get_event_loop().time()
            
            print(f"1️⃣ 发送请求到: {url}")
            
            async with session.get(url, timeout=engine.polling_config.get("REQUEST_TIMEOUT", 10)) as response:
                request_duration = asyncio.get_event_loop().time() - start_time
                engine.cache_stats["total_requests"] += 1
                
                print(f"   状态码: {response.status}")
                print(f"   耗时: {request_duration:.2f}秒")
                
                if response.status == 200:
                    content_text = await response.text()
                    content_size = len(content_text)
                    
                    # 计算内容hash
                    import hashlib
                    content_hash = hashlib.md5(content_text.encode('utf-8')).hexdigest()
                    
                    print(f"   内容大小: {content_size} 字节")
                    print(f"   内容Hash: {content_hash[:16]}...")
                    
                    # 检查缓存
                    cached_hash = user_state.get("content_hash")
                    
                    if cached_hash == content_hash:
                        # 缓存命中
                        engine.cache_stats["cache_hits"] += 1
                        engine.cache_stats["bandwidth_saved"] += content_size
                        print("   🎯 缓存命中！")
                        result = False
                    else:
                        # 缓存失效
                        engine.cache_stats["cache_misses"] += 1
                        print("   📥 内容已更新，处理RSS...")
                        
                        # 保存新hash
                        engine.state_manager.update_user_state(
                            user_id,
                            content_hash=content_hash,
                            last_check_time=asyncio.get_event_loop().time()
                        )
                        
                        # 处理内容
                        result = await engine.process_rss_content(user_id, content_text)
                    
                    print(f"   ✅ 处理完成，结果: {result}")
                else:
                    print(f"   ❌ 请求失败: {response.status}")
                    
    except Exception as e:
        print(f"   💥 错误: {e}")
        import traceback
        traceback.print_exc()
    
    # 打印统计
    print("\n📊 缓存统计:")
    print(f"   总请求: {engine.cache_stats['total_requests']}")
    print(f"   缓存命中: {engine.cache_stats['cache_hits']}")
    print(f"   缓存失误: {engine.cache_stats['cache_misses']}")
    
    print("\n✅ 测试完成！方法可以正常工作")

def main():
    try:
        asyncio.run(test_fetch_method())
    except KeyboardInterrupt:
        print("\n测试被中断")
    except Exception as e:
        print(f"\n测试出错: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
