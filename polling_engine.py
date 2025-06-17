import json
import time
import random
import asyncio
import aiohttp
import redis
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import List, Dict, Set, Optional, Tuple
import os
import argparse
import sys
from pathlib import Path
import re
from dataclasses import dataclass
import hashlib

# 在Windows上设置正确的事件循环策略
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("enhanced_polling")

# Redis配置 - 仅用于推文流
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_DB = int(os.environ.get("REDIS_DB", 0))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD", None)

# 简化的Redis配置 - 仅用于推文流
TWEET_STREAM_KEY = "tweets"

# 状态文件配置
STATE_FILE = "state.json"
DEFAULT_STATE = {
    "system": {
        "last_updated": None,
        "version": "1.0"
    },
    "users": {},
    "instances": {}
}

# 轮询配置 - 从Redis动态加载
POLLING_CONFIG_KEY = "polling_config"

# 默认轮询配置
DEFAULT_POLLING_CONFIG = {
    "PRIORITY_POLL_INTERVAL": 2,    # 优先用户轮询间隔（秒）
    "NORMAL_POLL_INTERVAL": 2,      # 普通用户轮询间隔（秒）
    "BATCH_SIZE": 8,                # 每批处理的用户数
    "REQUEST_TIMEOUT": 10,           # 请求超时时间（秒）
    "MAX_RETRIES": 5,                # 最大重试次数
    "REQUEST_RATE": 10.0,            # 每秒请求数限制
    "BURST_CAPACITY": 20             # 突发容量
}

# 兼容性配置（从动态配置中获取）
POLL_INTERVAL = 15          # 将从Redis动态更新
CONCURRENT_USERS = 5        # 将从Redis动态更新
REQUEST_TIMEOUT = 5         # 将从Redis动态更新
MAX_RETRIES = 2             # 将从Redis动态更新
RETRY_DELAYS = [0.5, 1.0]   # 重试延迟（秒）
RATE_LIMIT_DELAY = 2.0      # 遇到429错误时的额外延迟
BATCH_DELAY = 0.5           # 批次间基础延迟

@dataclass
class NitterInstance:
    url: str
    assigned_users: int = 0          # 分配的用户数
    active_connections: int = 0      # 当前活跃连接数
    max_connections: int = 50        # 最大连接数
    recent_429_count: int = 0        # 最近的429错误计数
    last_reset_time: float = 0.0     # 上次重置时间
    total_requests: int = 0          # 总请求数

class StateManager:
    """状态管理器 - 使用JSON文件存储所有状态"""
    
    def __init__(self, state_file: str = STATE_FILE):
        self.state_file = Path(state_file)
        self.state = self._load_state()
        
    def _load_state(self) -> dict:
        """加载状态文件"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    state = json.load(f)
                logger.info(f"已加载状态文件，包含 {len(state.get('users', {}))} 个用户")
                return state
            except (json.JSONDecodeError, IOError) as e:
                logger.error(f"读取状态文件失败: {e}")
                
        logger.info("创建新的状态文件")
        return DEFAULT_STATE.copy()
    
    def save_state(self):
        """保存状态到文件"""
        try:
            self.state["system"]["last_updated"] = datetime.now().isoformat()

            # 添加调试信息
            user_count = len(self.state.get("users", {}))
            logger.debug(f"准备保存状态文件: {self.state_file}, 包含 {user_count} 个用户")

            # 检查最近更新的用户
            recent_updates = []
            current_time = time.time()
            for user_id, user_data in self.state.get("users", {}).items():
                last_check = user_data.get("last_check_time", 0)
                if current_time - last_check < 300:  # 5分钟内更新的
                    recent_updates.append(user_id)

            if recent_updates:
                logger.debug(f"最近5分钟内更新的用户: {recent_updates[:5]}" +
                           (f" 等{len(recent_updates)}个" if len(recent_updates) > 5 else ""))

            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(self.state, f, indent=2, ensure_ascii=False)

            logger.debug(f"状态文件保存成功: {self.state_file}")

        except IOError as e:
            logger.error(f"保存状态文件失败: {e}")
        except Exception as e:
            logger.error(f"保存状态时出现未知错误: {e}")
    
    def get_user_state(self, user_id: str) -> dict:
        """获取用户状态"""
        return self.state["users"].get(user_id, {})
    
    def update_user_state(self, user_id: str, **kwargs):
        """更新用户状态"""
        if user_id not in self.state["users"]:
            self.state["users"][user_id] = {}

        # 记录更新前的状态（用于调试）
        old_state = self.state["users"][user_id].copy()

        self.state["users"][user_id].update(kwargs)
        self.state["users"][user_id]["last_updated"] = datetime.now().isoformat()

        # 添加调试信息
        if "last_check_time" in kwargs:
            logger.debug(f"更新用户 {user_id} 状态: last_check_time={kwargs['last_check_time']}")
        if "last_tweet_id" in kwargs:
            old_tweet_id = old_state.get("last_tweet_id", "无")
            new_tweet_id = kwargs["last_tweet_id"]
            if old_tweet_id != new_tweet_id:
                logger.debug(f"用户 {user_id} 推文ID更新: {old_tweet_id} -> {new_tweet_id}")
    
    def get_all_users(self) -> List[str]:
        """获取所有用户ID"""
        return list(self.state["users"].keys())
    
    def update_instance_state(self, instance_url: str, **kwargs):
        """更新实例状态"""
        if instance_url not in self.state["instances"]:
            self.state["instances"][instance_url] = {"assigned_users": 0}
        
        self.state["instances"][instance_url].update(kwargs)

class EnhancedPollingEngine:
    """增强的轮询引擎"""
    
    def __init__(self, nitter_instances: List[str], following_file: str):
        self.instances = [NitterInstance(url) for url in nitter_instances]
        self.following_file = following_file
        self.state_manager = StateManager()
        self.use_sse = True  # 启用SSE
        self.sse_connections = {}  # 存储SSE连接
        
        # 多实例负载均衡
        self.user_instance_mapping = {}  # 用户到实例的映射
        self.instance_stats = {}         # 实例统计信息
        

        
        # 动态并发控制（现在按实例管理）
        self.current_concurrent = CONCURRENT_USERS
        self.recent_errors = []  # 记录最近的错误
        self.max_concurrent = 5  # 最大并发数
        self.min_concurrent = 1  # 最小并发数
        
        # 失败用户队列 - 用于链式批次处理
        self.pending_users = []  # 需要重新处理的用户（主要是429错误）

        # 应用层缓存统计
        self.cache_stats = {
            "total_requests": 0,
            "cache_hits": 0,  # 内容hash匹配
            "cache_misses": 0,  # 内容有变化
            "bandwidth_saved": 0,  # 估算节省的带宽
        }
        
        # 轮询统计
        self.polling_stats = {
            "total_users": 0,
            "successful_users": 0,
            "failed_users": 0,
            "last_cycle_time": None,
            "current_cycle": 0,
            "success_rate": 0.0,
            "failed_user_list": []
        }

        # 当前轮询周期的统计
        self.current_cycle_stats = {
            "processed_users": set(),  # 已处理的用户
            "successful_users": set(),  # 成功的用户
            "failed_users": set(),     # 最终失败的用户
            "user_attempts": {}        # 用户尝试次数记录
        }
        
        # 连接Redis - 仅用于推文流
        try:
            self.redis_client = redis.Redis(
                host=REDIS_HOST, 
                port=REDIS_PORT, 
                db=REDIS_DB,
                password=REDIS_PASSWORD,
                decode_responses=True
            )
            self.redis_client.ping()
            logger.info(f"成功连接到Redis: {REDIS_HOST}:{REDIS_PORT}")
                
        except redis.ConnectionError as e:
            logger.error(f"无法连接到Redis: {e}")
            raise
            
        # 初始化实例统计
        for instance in self.instances:
            self.instance_stats[instance.url] = {
                "requests_this_cycle": 0,
                "errors_this_cycle": 0,
                "response_times": [],
                "last_reset": time.time()
            }
            
        # 加载轮询配置
        self.polling_config = self.load_polling_config()
        self.apply_polling_config()

        logger.info(f"初始化完成，使用 {len(self.instances)} 个Nitter实例")
        self.print_instance_info()
        
    def print_instance_info(self):
        """打印实例信息"""
        logger.info(f"=== Nitter实例信息 ===")
        for i, instance in enumerate(self.instances):
            logger.info(f"实例 {i+1}: {instance.url}")
            logger.info(f"  当前连接数: {instance.active_connections}")
            logger.info(f"  最大连接数: {instance.max_connections}")
            logger.info(f"  分配用户数: {instance.assigned_users}")
            logger.info(f"  最近429错误: {instance.recent_429_count}")
            logger.info(f"  总请求数: {instance.total_requests}")
        logger.info(f"======================")
        
        # 检查用户分配映射
        mapping_count = len(self.user_instance_mapping)
        logger.info(f"用户实例映射数量: {mapping_count}")
        
        # 统计每个实例的分配情况
        instance_user_count = {}
        for user_id, instance in self.user_instance_mapping.items():
            url = instance.url
            instance_user_count[url] = instance_user_count.get(url, 0) + 1
        
        logger.info("实际映射分布:")
        for url, count in instance_user_count.items():
            logger.info(f"  {url}: {count} 个用户")
        logger.info(f"=======================")

    def load_polling_config(self) -> dict:
        """从Redis加载轮询配置"""
        try:
            config_json = self.redis_client.get(POLLING_CONFIG_KEY)
            if config_json:
                config = json.loads(config_json)
                logger.info(f"从Redis加载轮询配置: {config}")
                return config
            else:
                logger.info("Redis中没有轮询配置，使用默认配置")
                # 保存默认配置到Redis
                self.redis_client.set(POLLING_CONFIG_KEY, json.dumps(DEFAULT_POLLING_CONFIG))
                return DEFAULT_POLLING_CONFIG.copy()
        except Exception as e:
            logger.error(f"加载轮询配置失败: {e}，使用默认配置")
            return DEFAULT_POLLING_CONFIG.copy()

    def apply_polling_config(self):
        """应用轮询配置到全局变量"""
        global POLL_INTERVAL, CONCURRENT_USERS, REQUEST_TIMEOUT, MAX_RETRIES

        # 映射配置到兼容的全局变量
        POLL_INTERVAL = self.polling_config.get("NORMAL_POLL_INTERVAL", 60)
        CONCURRENT_USERS = min(self.polling_config.get("BATCH_SIZE", 15), 10)  # 限制最大并发
        REQUEST_TIMEOUT = self.polling_config.get("REQUEST_TIMEOUT", 10)
        MAX_RETRIES = self.polling_config.get("MAX_RETRIES", 5)

        # 更新实例级别的配置
        self.current_concurrent = CONCURRENT_USERS

        logger.info(f"应用轮询配置:")
        logger.info(f"  轮询间隔: {POLL_INTERVAL}秒")
        logger.info(f"  并发用户数: {CONCURRENT_USERS}")
        logger.info(f"  请求超时: {REQUEST_TIMEOUT}秒")
        logger.info(f"  最大重试: {MAX_RETRIES}次")

    def reload_polling_config(self):
        """重新加载轮询配置"""
        old_config = self.polling_config.copy()
        self.polling_config = self.load_polling_config()

        if old_config != self.polling_config:
            logger.info("检测到轮询配置变更，重新应用配置")
            self.apply_polling_config()
            return True
        return False

    def load_following_list(self) -> List[Dict]:
        """加载关注用户列表"""
        try:
            with open(self.following_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # 检查数据格式
            if isinstance(data, list):
                # 旧格式：直接是用户列表
                following_list = data
            elif isinstance(data, dict) and ('priority_users' in data or 'normal_users' in data):
                # 新格式：分组格式
                following_list = []

                # 添加优先用户
                priority_users = data.get('priority_users', [])
                for user in priority_users:
                    following_list.append({
                        'userId': user,
                        'username': user,
                        'priority': True
                    })

                # 添加普通用户
                normal_users = data.get('normal_users', [])
                for user in normal_users:
                    following_list.append({
                        'userId': user,
                        'username': user,
                        'priority': False
                    })

                logger.info(f"加载分组格式配置: {len(priority_users)} 个优先用户, {len(normal_users)} 个普通用户")
            else:
                logger.error(f"不支持的配置文件格式: {type(data)}")
                return []

            logger.info(f"成功加载 {len(following_list)} 个关注用户")
            return following_list

        except (json.JSONDecodeError, FileNotFoundError) as e:
            logger.error(f"无法加载关注列表: {e}")
            return []
            
    def get_instance_for_user(self, user_id: str) -> NitterInstance:
        """为用户获取实例（简化版本，轮询分配）"""
        # 检查是否已有分配
        if user_id in self.user_instance_mapping:
            assigned_instance = self.user_instance_mapping[user_id]
            logger.debug(f"用户 {user_id} 使用已分配实例 {assigned_instance.url}")
            return assigned_instance
        
        # 简单轮询分配：选择分配用户最少的实例
        selected_instance = min(self.instances, key=lambda x: x.assigned_users)
        
        # 更新映射和计数
        self.user_instance_mapping[user_id] = selected_instance
        selected_instance.assigned_users += 1
        
        logger.debug(f"为用户 {user_id} 分配实例 {selected_instance.url}")
        return selected_instance
    
    def update_instance_stats(self, instance: NitterInstance, success: bool, duration: float, is_429: bool = False):
        """更新实例统计（简化版本）"""
        instance.total_requests += 1

        if is_429:
            instance.recent_429_count += 1

        # 定期重置429计数（每小时）
        current_time = time.time()
        if current_time - instance.last_reset_time > 3600:  # 1小时
            instance.recent_429_count = 0
            instance.last_reset_time = current_time
            logger.debug(f"重置实例 {instance.url} 429计数")
    
    def rebalance_users(self):
        """重新平衡用户分配（简化版本）"""
        total_users = len(self.user_instance_mapping)
        instance_count = len(self.instances)

        if instance_count == 0:
            logger.error("没有可用实例，无法重新平衡")
            return

        users_per_instance = total_users // instance_count
        extra_users = total_users % instance_count

        logger.info(f"开始重新平衡 {total_users} 个用户到 {instance_count} 个实例")

        # 重置所有实例的用户计数
        for instance in self.instances:
            instance.assigned_users = 0

        # 获取所有用户列表
        all_users = list(self.user_instance_mapping.keys())
        user_index = 0

        for i, instance in enumerate(self.instances):
            # 计算这个实例应该分配多少用户
            target_users = users_per_instance + (1 if i < extra_users else 0)

            for _ in range(target_users):
                if user_index < len(all_users):
                    user_id = all_users[user_index]
                    self.user_instance_mapping[user_id] = instance
                    instance.assigned_users += 1
                    user_index += 1

            logger.info(f"实例 {instance.url} 重新分配了 {instance.assigned_users} 个用户")

    def reset_cycle_stats(self):
        """重置当前轮询周期统计"""
        self.current_cycle_stats = {
            "processed_users": set(),
            "successful_users": set(),
            "failed_users": set(),
            "user_attempts": {}
        }

    def update_batch_stats(self, batch_results: List[Tuple[str, bool, str]]):
        """更新批次统计信息（累计到当前轮询周期）并实时更新总体统计"""
        for user_id, success, error_msg in batch_results:
            # 记录用户已被处理
            self.current_cycle_stats["processed_users"].add(user_id)

            # 记录尝试次数
            if user_id not in self.current_cycle_stats["user_attempts"]:
                self.current_cycle_stats["user_attempts"][user_id] = []
            self.current_cycle_stats["user_attempts"][user_id].append((success, error_msg))

            # 如果成功，从失败列表中移除（如果存在）
            if success:
                self.current_cycle_stats["successful_users"].add(user_id)
                self.current_cycle_stats["failed_users"].discard(user_id)
            else:
                # 429错误不算失败，会继续重试
                # 只有真正的错误（网络、解析等）才算失败
                if "429" not in error_msg and "Rate Limited" not in error_msg:
                    self.current_cycle_stats["failed_users"].add(user_id)
                    self.current_cycle_stats["successful_users"].discard(user_id)
                # 429错误的用户保持在处理中状态，不算成功也不算失败

        # 实时更新总体统计（每批次都更新）
        self.update_realtime_stats()

    def update_realtime_stats(self):
        """实时更新总体统计（每批次调用）"""
        successful_users = self.current_cycle_stats["successful_users"]
        failed_users = self.current_cycle_stats["failed_users"]
        processed_users = self.current_cycle_stats["processed_users"]

        # 计算总用户数（应该是所有需要处理的用户）
        all_users = self.state_manager.get_all_users()
        total_users = len(all_users)

        # 计算处理中的用户（pending队列中的用户，主要是429错误）
        pending_count = len(self.pending_users)

        # 更新实时统计数据
        self.polling_stats["successful_users"] = len(successful_users)
        self.polling_stats["failed_users"] = len(failed_users)
        self.polling_stats["total_users"] = total_users
        self.polling_stats["processed_users"] = len(processed_users)
        self.polling_stats["pending_users"] = pending_count
        self.polling_stats["failed_user_list"] = list(failed_users)
        self.polling_stats["last_cycle_time"] = datetime.now().isoformat()

        if total_users > 0:
            self.polling_stats["success_rate"] = len(successful_users) / total_users
        else:
            self.polling_stats["success_rate"] = 0.0

        # 保存到Redis（实时更新）
        try:
            self.redis_client.set("polling_stats", json.dumps(self.polling_stats))

            # 更详细的日志
            processing_users = len(processed_users) - len(successful_users) - len(failed_users)
            logger.info(f"📊 实时统计: {len(successful_users)}/{total_users} 成功, "
                       f"{len(failed_users)} 失败, {pending_count} 待重试 - "
                       f"成功率: {self.polling_stats['success_rate']:.1%}")

        except Exception as e:
            logger.error(f"保存实时统计失败: {e}")

    def finalize_cycle_stats(self):
        """完成当前轮询周期，输出最终统计摘要"""
        successful_users = self.current_cycle_stats["successful_users"]
        failed_users = self.current_cycle_stats["failed_users"]
        total_users = len(self.current_cycle_stats["processed_users"])

        # 最后一次更新统计（确保数据最新）
        self.update_realtime_stats()

        # 输出周期完成摘要
        logger.info(f"🏁 轮询周期完成: {len(successful_users)}/{total_users} 成功 "
                   f"({len(failed_users)} 最终失败) - 成功率: {self.polling_stats['success_rate']:.1%}")

        if failed_users:
            logger.warning(f"最终失败用户: {', '.join(list(failed_users)[:5])}" +
                         (f" 等{len(failed_users)}个" if len(failed_users) > 5 else ""))

        # 详细重试统计
        retry_stats = {}
        for user_id, attempts in self.current_cycle_stats["user_attempts"].items():
            retry_count = len(attempts) - 1  # 减去初次尝试
            if retry_count > 0:
                retry_stats[user_id] = retry_count

        if retry_stats:
            logger.info(f"重试统计: {len(retry_stats)} 个用户需要重试，"
                       f"平均重试 {sum(retry_stats.values()) / len(retry_stats):.1f} 次")
    
    def print_load_distribution(self):
        """打印负载分布情况"""
        logger.info("=== 实例负载分布 ===")
        for instance in self.instances:
            logger.info(f"🔵 {instance.url}:")
            logger.info(f"  分配用户: {instance.assigned_users}")
            logger.info(f"  活跃连接: {instance.active_connections}")
            logger.info(f"  最近429错误: {instance.recent_429_count}")
        logger.info("==================")
    
    async def setup_sse_connection(self, user_id: str):
        """为用户建立SSE连接"""
        instance = self.get_instance_for_user(user_id)
        sse_url = f"{instance.url}/stream/user/{user_id}"
        
        try:
            session = aiohttp.ClientSession()
            response = await session.get(
                sse_url,
                headers={"Accept": "text/event-stream"},
                timeout=aiohttp.ClientTimeout(total=None)  # SSE需要无限超时
            )
            
            if response.status == 200:
                self.sse_connections[user_id] = {
                    "session": session,
                    "response": response,
                    "instance": instance
                }
                instance.active_connections += 1
                logger.info(f"为用户 {user_id} 建立SSE连接: {sse_url}")
                
                # 启动SSE数据处理
                asyncio.create_task(self.process_sse_stream(user_id))
                return True
            else:
                await session.close()
                logger.warning(f"SSE连接失败: {user_id}, 状态码: {response.status}")
                return False
                
        except Exception as e:
            logger.error(f"建立SSE连接时出错: {user_id}, {e}")
            return False
    
    async def process_sse_stream(self, user_id: str):
        """处理SSE数据流"""
        connection_info = self.sse_connections.get(user_id)
        if not connection_info:
            return
            
        response = connection_info["response"]
        session = connection_info["session"]
        
        try:
            async for line in response.content:
                line = line.decode('utf-8').strip()
                
                if line.startswith('data: '):
                    data = line[6:]  # 移除 'data: ' 前缀
                    try:
                        tweet_data = json.loads(data)
                        # 处理推文数据...
                        await self.process_tweet_from_sse(user_id, tweet_data)
                    except json.JSONDecodeError:
                        continue
                        
        except Exception as e:
            logger.error(f"处理SSE流时出错: {user_id}, {e}")
        finally:
            # 清理连接
            if user_id in self.sse_connections:
                del self.sse_connections[user_id]
                connection_info["instance"].active_connections -= 1
            await session.close()
    
    async def fetch_user_rss(self, session: aiohttp.ClientSession, user_id: str) -> bool:
        """获取用户RSS内容（带应用层缓存优化）"""
        instance = self.get_instance_for_user(user_id)
        url = f"{instance.url}/{user_id}/rss"

        user_state = self.state_manager.get_user_state(user_id)
        start_time = time.time()
        success = False
        is_429 = False
        
        try:
            async with session.get(url, timeout=self.polling_config.get("REQUEST_TIMEOUT", 10)) as response:
                request_duration = time.time() - start_time
                
                if response.status == 200:
                    content_text = await response.text()
                    content_size = len(content_text)
                    self.cache_stats["total_requests"] += 1
                    
                    # 计算内容hash
                    import hashlib
                    content_hash = hashlib.md5(content_text.encode('utf-8')).hexdigest()
                    
                    # 检查是否有缓存的hash
                    cached_hash = user_state.get("content_hash")
                    
                    if cached_hash == content_hash:
                        # 缓存命中！内容没有变化
                        self.cache_stats["cache_hits"] += 1
                        self.cache_stats["bandwidth_saved"] += content_size
                        
                        logger.info(f"🎯 用户 {user_id} 内容缓存命中！耗时: {request_duration:.2f}秒，大小: {content_size} 字节 [实例: {instance.url}]")
                        
                        # 更新检查时间但不处理内容
                        self.state_manager.update_user_state(
                            user_id,
                            last_check_time=time.time(),
                            last_success_time=time.time()
                        )
                        logger.debug(f"🔄 更新用户 {user_id} 状态：内容缓存命中")
                        
                        success = True
                        result = False  # 没有新内容
                    else:
                        # 缓存失效，内容有变化
                        self.cache_stats["cache_misses"] += 1
                        
                        logger.info(f"📥 用户 {user_id} 内容已更新，耗时: {request_duration:.2f}秒，大小: {content_size} 字节 [实例: {instance.url}]")
                        
                        # 保存新的内容hash
                        self.state_manager.update_user_state(
                            user_id,
                            content_hash=content_hash,
                            last_check_time=time.time()
                        )
                        
                        # 处理RSS内容
                        result = await self.process_rss_content(user_id, content_text)
                        success = True
                    
                elif response.status == 429:
                    is_429 = True
                    # 429错误也要更新检查时间
                    self.state_manager.update_user_state(
                        user_id,
                        last_check_time=time.time(),
                        rate_limit_count=user_state.get("rate_limit_count", 0) + 1
                    )
                    logger.debug(f"🔄 更新用户 {user_id} 状态：429限流错误")
                    
                    # 抛出特殊异常以便在批次处理中识别
                    raise aiohttp.ClientResponseError(
                        request_info=response.request_info,
                        history=response.history,
                        status=429,
                        message="Rate Limited"
                    )
                else:
                    logger.warning(f"❌ 用户 {user_id} 获取失败: HTTP {response.status} [实例: {instance.url}]")
                    # 其他HTTP错误也要更新检查时间
                    self.state_manager.update_user_state(
                        user_id,
                        last_check_time=time.time(),
                        http_error_count=user_state.get("http_error_count", 0) + 1
                    )
                    logger.debug(f"🔄 更新用户 {user_id} 状态：HTTP {response.status} 错误")
                    success = False
                    result = False
                    
        except aiohttp.ClientResponseError as e:
            request_duration = time.time() - start_time
            if e.status == 429:
                is_429 = True
                # 只打印一条429错误信息，包含完整信息
                logger.warning(f"⏰ 用户 {user_id} 遇到速率限制，耗时: {request_duration:.2f}秒 [实例: {instance.url}]")
            else:
                logger.error(f"💥 用户 {user_id} HTTP错误: {e.status}，耗时: {request_duration:.2f}秒 [实例: {instance.url}]")
            success = False
            result = False
            raise  # 重新抛出异常以便上层处理
            
        except Exception as e:
            request_duration = time.time() - start_time
            logger.error(f"💥 用户 {user_id} 网络错误: {e}，耗时: {request_duration:.2f}秒 [实例: {instance.url}]")
            # 网络错误也要更新检查时间
            self.state_manager.update_user_state(
                user_id,
                last_check_time=time.time(),
                network_error_count=user_state.get("network_error_count", 0) + 1
            )
            logger.debug(f"🔄 更新用户 {user_id} 状态：网络错误")
            success = False
            result = False
            
        finally:
            # 更新实例统计
            request_duration = time.time() - start_time
            self.update_instance_stats(instance, success, request_duration, is_429)
            
        return result if success else False
    
    def print_cache_stats(self):
        """打印缓存统计信息"""
        total_requests = self.cache_stats["total_requests"]

        if total_requests > 0:
            hit_rate = (self.cache_stats["cache_hits"] / total_requests) * 100
            saved_mb = self.cache_stats["bandwidth_saved"] / (1024 * 1024)

            logger.info(f"""
📊 应用层缓存统计:
   总请求数: {total_requests}
   缓存命中: {self.cache_stats["cache_hits"]} ({hit_rate:.1f}%)
   缓存失误: {self.cache_stats["cache_misses"]}
   节省带宽: {saved_mb:.1f} MB
            """)
        else:
            logger.info("📊 应用层缓存统计: 暂无数据")
    
    async def process_rss_content(self, user_id: str, content: str) -> bool:
        """处理RSS内容"""
        tweet_data = None  # 预定义变量避免作用域问题
        
        try:
            root = ET.fromstring(content)
            items = root.findall(".//item")
            
            if not items:
                logger.debug(f"用户 {user_id} RSS没有推文项目")
                return False
                
            # 尝试从RSS中提取用户名
            username = user_id  # 默认使用user_id
            
            # 方法1: 从channel title中提取用户名 (格式通常是 "/ Twitter")
            channel_title = root.find(".//channel/title")
            if channel_title is not None and channel_title.text:
                title_text = channel_title.text
                # 处理不同的标题格式
                if "/ Twitter" in title_text:
                    username = title_text.replace("/ Twitter", "").strip()
                elif "/ Nitter" in title_text:
                    username = title_text.replace("/ Nitter", "").strip()
                elif " / @" in title_text:
                    # 处理 'Aster / @Aster_DEX' 这种格式，只取"/"前面的部分
                    username = title_text.split(" / @")[0].strip()
                elif " /" in title_text:
                    # 处理其他包含"/"的格式，取"/"前面的部分
                    username = title_text.split(" /")[0].strip()
                else:
                    # 如果没有特殊格式，直接使用标题
                    username = title_text.strip()
                
                logger.debug(f"从RSS channel title提取用户名: {user_id} -> {username} (原标题: {title_text})")
                
            # 方法2: 从第一个推文的作者信息中提取 (备选方案)
            if username == user_id:
                latest_item = items[0]
                # 尝试从creator或author字段获取
                creator = latest_item.find(".//{http://purl.org/dc/elements/1.1/}creator")
                if creator is not None and creator.text:
                    username = creator.text.strip()
                    logger.debug(f"从RSS creator字段提取用户名: {user_id} -> {username}")
                
                # 如果还是没有，尝试从推文标题中提取 (通常格式是 "RT by username: content")
                if username == user_id:
                    title = latest_item.find("title")
                    if title is not None and title.text:
                        title_text = title.text
                        if "RT by " in title_text and ":" in title_text:
                            # 提取 "RT by username:" 中的用户名
                            parts = title_text.split("RT by ")[1].split(":")[0]
                            username = parts.strip()
                            logger.debug(f"从推文标题提取用户名: {user_id} -> {username}")
                
            # 获取最新推文
            latest_item = items[0]
            link = latest_item.find("link")
            title = latest_item.find("title")
            description = latest_item.find("description")
            pub_date = latest_item.find("pubDate")
            
            # 提取图片URL
            images = []

            # 方法1: 从enclosure元素中提取图片
            enclosures = latest_item.findall("enclosure")
            for enclosure in enclosures:
                if enclosure.get("type", "").startswith("image/"):
                    url = enclosure.get("url", "")
                    if url:
                        images.append(url)

            # 方法2: 从description的HTML内容中提取图片
            if description is not None and description.text:
                # 匹配img标签中的src属性
                img_pattern = r'<img[^>]+src=["\']([^"\']+)["\'][^>]*>'
                img_matches = re.findall(img_pattern, description.text, re.IGNORECASE)
                images.extend(img_matches)

                # 注意：不再使用通用URL模式匹配，因为它会重复提取已经从img标签提取的图片
                # 只有在没有从img标签找到图片时，才使用URL模式匹配作为备用方案
                if not img_matches:
                    url_pattern = r'https?://[^\s<>"]+\.(?:jpg|jpeg|png|gif|webp)(?:\?[^\s<>"]*)?'
                    url_matches = re.findall(url_pattern, description.text, re.IGNORECASE)
                    images.extend(url_matches)

            # 标准化和去重图片URL
            def normalize_image_url(url):
                """标准化图片URL，用于去重"""
                if not url:
                    return ""

                # 修复端口号问题：如果URL是localhost但没有端口，添加8080端口
                if url.startswith('http://localhost/') and ':8080' not in url:
                    url = url.replace('http://localhost/', 'http://localhost:8080/')

                # 移除URL中的查询参数进行去重比较（但保留原始URL）
                from urllib.parse import urlparse
                parsed = urlparse(url)
                normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
                return normalized

            # 去重并过滤有效的图片URL
            unique_images = []
            seen_normalized = set()

            for img_url in images:
                if not img_url or img_url.startswith('data:') or len(img_url) <= 10:
                    continue

                # 修复端口号问题
                if img_url.startswith('http://localhost/') and ':8080' not in img_url:
                    img_url = img_url.replace('http://localhost/', 'http://localhost:8080/')
                    logger.debug(f"修复图片URL端口: {img_url}")

                # 使用标准化URL进行去重
                normalized = normalize_image_url(img_url)
                if normalized not in seen_normalized:
                    seen_normalized.add(normalized)
                    unique_images.append(img_url)
            
            if unique_images:
                logger.debug(f"用户 {user_id} 推文包含 {len(unique_images)} 张图片: {unique_images[:2]}...")  # 只显示前2个URL
                # 添加详细的图片提取调试信息
                logger.debug(f"原始图片列表长度: {len(images)}, 去重后: {len(unique_images)}")
                if len(images) != len(unique_images):
                    logger.debug(f"检测到重复图片，原始: {images}, 去重后: {unique_images}")
            else:
                logger.debug(f"用户 {user_id} 推文不包含图片")
            
            # 修复检查逻辑 - 检查元素是否存在且有文本内容
            if link is None or not link.text:
                logger.warning(f"用户 {user_id} 推文链接为空")
                return False
            
            if title is None or not title.text:
                logger.warning(f"用户 {user_id} 推文标题为空")
                return False
                
            if description is None or not description.text:
                logger.warning(f"用户 {user_id} 推文描述为空")
                return False
                
            if pub_date is None or not pub_date.text:
                logger.warning(f"用户 {user_id} 推文发布时间为空")
                return False
            
            tweet_id = link.text.split("/")[-1]
            user_state = self.state_manager.get_user_state(user_id)
            
            # 检查是否为新推文
            current_last_tweet_id = user_state.get("last_tweet_id")
            logger.debug(f"用户 {user_id} 当前保存的tweet_id: {current_last_tweet_id}, 新tweet_id: {tweet_id}")
            
            if current_last_tweet_id == tweet_id:
                logger.debug(f"用户 {user_id} 推文未更新，跳过")
                # 即使没有新推文，也要更新最后检查时间和成功时间
                self.state_manager.update_user_state(
                    user_id,
                    last_check_time=time.time(),
                    last_success_time=time.time(),
                    username=username
                )
                logger.debug(f"🔄 更新用户 {user_id} 状态：无新推文但更新检查时间")
                return False
                        
            # 解析发布时间
            try:
                pub_time = self.parse_date(pub_date.text)
                today = datetime.now().date()
                
                # 如果是首次运行，只推送当日推文
                if not user_state.get("initialized") and pub_time.date() != today:
                    logger.info(f"用户 {user_id} 首次运行，跳过非当日推文: {pub_time.date()}")
                    self.state_manager.update_user_state(
                        user_id, 
                        last_tweet_id=tweet_id, 
                        initialized=True,
                        last_check_time=time.time(),
                        username=username
                    )
                    logger.debug(f"🔄 更新用户 {user_id} 状态：首次运行初始化")
                    return False
                    
            except Exception as e:
                logger.warning(f"解析用户 {user_id} 推文时间失败: {e}")
                pub_time = datetime.now()
            
            # 构建推文数据


            tweet_data = {
                "id": tweet_id,
                "user_id": user_id,
                "username": username,
                "content": title.text or "",
                "html": description.text or "",
                "published_at": pub_date.text,
                "url": link.text,
                "timestamp": pub_time.isoformat(),
                "images": json.dumps(unique_images)
            }

            # 添加到Redis流
            try:
                stream_id = self.redis_client.xadd(
                    TWEET_STREAM_KEY,
                    tweet_data,
                    maxlen=1000,  # 限制流长度
                    approximate=True
                )
                
                # 更新用户状态
                self.state_manager.update_user_state(
                    user_id,
                    last_tweet_id=tweet_id,
                    last_success_time=time.time(),
                    last_check_time=time.time(),
                    initialized=True,
                    username=username  # 保存用户名到状态
                )
                
                logger.info(f"✅ 用户 {username}(@{user_id}) 新推文已推送: {tweet_id}")
                logger.debug(f"🔄 更新用户 {user_id} 状态：新推文 {tweet_id}")
                return True
                
            except Exception as e:
                logger.error(f"推文添加到Redis失败: {e}, 推文数据: {tweet_data}")
                # 即使Redis失败，也要更新状态避免重复尝试
                self.state_manager.update_user_state(
                    user_id,
                    last_tweet_id=tweet_id,
                    last_check_time=time.time(),
                    username=username
                )
                logger.debug(f"🔄 更新用户 {user_id} 状态：Redis失败但更新tweet_id")
                return False
                
        except ET.ParseError as e:
            logger.error(f"解析用户 {user_id} RSS失败: {e}")
            # 解析失败也要更新检查时间
            self.state_manager.update_user_state(
                user_id,
                last_check_time=time.time(),
                parse_error_count=user_state.get("parse_error_count", 0) + 1
            )
            logger.debug(f"🔄 更新用户 {user_id} 状态：RSS解析失败")
            return False
        except Exception as e:
            logger.error(f"处理RSS内容时出错: {user_id}, {e}, 推文数据: {tweet_data}")
            # 其他错误也要更新检查时间
            self.state_manager.update_user_state(
                user_id,
                last_check_time=time.time(),
                error_count=user_state.get("error_count", 0) + 1
            )
            logger.debug(f"🔄 更新用户 {user_id} 状态：处理出错")
            return False
    
    def parse_date(self, date_str: str) -> datetime:
        """解析日期字符串"""
        if not date_str:
            return datetime.now()
            
        formats = [
            "%a, %d %b %Y %H:%M:%S %z",
            "%a, %d %b %Y %H:%M:%S",
            "%a, %d %b %Y %H:%M:%S GMT",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S"
        ]
        
        # 处理GMT
        if " GMT" in date_str:
            date_str = date_str.replace(" GMT", " +0000")
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
                
        logger.warning(f"无法解析日期: {date_str}")
        return datetime.now()
    


    async def initialize_users(self):
        """初始化用户和ETag检查"""
        # 加载关注列表
        following_list = self.load_following_list()
        
        # 初始化状态管理器中的用户
        for user_info in following_list:
            user_id = user_info['userId']  # 使用userId作为用户ID
            if not self.state_manager.get_user_state(user_id):
                # 如果用户不存在，初始化用户状态
                self.state_manager.update_user_state(
                    user_id,
                    display_name=user_info.get('name', user_info.get('username', user_id)),  # 使用name或username作为显示名
                    last_check=None,
                    last_tweet_id=None
                )
        
        logger.info(f"成功加载 {len(following_list)} 个关注用户")

        # 执行初始负载均衡
        self.perform_initial_load_balancing()
        
        # 打印详细的实例信息用于调试
        self.print_instance_info()
        
        logger.info(f"初始化完成，共 {len(following_list)} 个用户")
        
    def perform_initial_load_balancing(self):
        """执行初始负载均衡"""
        # 只对当前following_list中的用户进行负载均衡，不包括历史用户
        following_list = self.load_following_list()
        users = [user_info['userId'] for user_info in following_list]  # 只使用当前关注列表的用户
        
        total_users = len(users)
        instance_count = len(self.instances)
        
        if instance_count == 0:
            logger.error("没有可用的Nitter实例")
            return
            
        users_per_instance = total_users // instance_count
        extra_users = total_users % instance_count
        
        logger.info(f"开始分配 {total_users} 个用户到 {instance_count} 个实例")
        logger.info(f"每个实例平均 {users_per_instance} 个用户，{extra_users} 个实例各多分配1个用户")
        
        # 清空现有映射
        self.user_instance_mapping.clear()
        for instance in self.instances:
            instance.assigned_users = 0
        
        user_index = 0
        for i, instance in enumerate(self.instances):
            # 计算这个实例应该分配多少用户
            target_users = users_per_instance + (1 if i < extra_users else 0)
            
            for _ in range(target_users):
                if user_index < len(users):
                    user_id = users[user_index]
                    self.user_instance_mapping[user_id] = instance
                    instance.assigned_users += 1
                    user_index += 1
            
            logger.info(f"实例 {instance.url} 分配了 {instance.assigned_users} 个用户")
        
        logger.info("初始负载均衡完成")
    
    def adjust_concurrency(self, success_count: int, total_count: int, error_count: int):
        """根据成功率动态调整并发数"""
        if total_count == 0:
            return
            
        success_rate = success_count / total_count
        error_rate = error_count / total_count
        
        # 记录最近的错误率
        self.recent_errors.append(error_rate)
        if len(self.recent_errors) > 5:  # 只保留最近5次的记录
            self.recent_errors.pop(0)
        
        avg_error_rate = sum(self.recent_errors) / len(self.recent_errors)
        
        old_concurrent = self.current_concurrent
        
        # if avg_error_rate > 0.3:  # 错误率超过30%，减少并发
        #     self.current_concurrent = max(self.min_concurrent, self.current_concurrent - 1)
        #     logger.info(f"错误率过高 ({avg_error_rate:.1%})，降低并发数: {old_concurrent} -> {self.current_concurrent}")
        # elif avg_error_rate < 0.1 and success_rate > 0.8:  # 错误率低于10%且成功率高，增加并发
        #     self.current_concurrent = min(self.max_concurrent, self.current_concurrent + 1)
        #     logger.info(f"性能良好 (错误率: {avg_error_rate:.1%})，提高并发数: {old_concurrent} -> {self.current_concurrent}")
        
    async def poll_users_batch(self, user_batch: List[str]):
        """批量轮询用户"""
        batch_start = time.time()
        
        # 统计本批次用户的实例分布
        batch_instance_count = {}
        for user_id in user_batch:
            instance = self.get_instance_for_user(user_id)
            url = instance.url
            batch_instance_count[url] = batch_instance_count.get(url, 0) + 1
        
        logger.info(f"开始处理批次: {len(user_batch)} 个用户")
        logger.info(f"批次实例分布: {batch_instance_count}")
        logger.debug(f"用户列表: {user_batch}")
        
        async with aiohttp.ClientSession() as session:
            tasks = [self.fetch_user_rss(session, user_id) for user_id in user_batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            success_count = sum(1 for r in results if r is True)
            error_count = sum(1 for r in results if isinstance(r, Exception))
            rate_limit_count = sum(1 for r in results if isinstance(r, aiohttp.ClientResponseError) and r.status == 429)
            batch_duration = time.time() - batch_start
            
            # 收集429限流的用户，加入待处理队列
            rate_limited_users = []
            batch_results = []

            for i, result in enumerate(results):
                user_id = user_batch[i]
                if isinstance(result, aiohttp.ClientResponseError) and result.status == 429:
                    rate_limited_users.append(user_id)
                    batch_results.append((user_id, False, "429 Rate Limited"))
                elif isinstance(result, Exception):
                    batch_results.append((user_id, False, str(result)))
                else:
                    batch_results.append((user_id, result, ""))

            if rate_limited_users:
                self.pending_users.extend(rate_limited_users)
                logger.info(f"🔄 {len(rate_limited_users)} 个用户因429限流加入下一批次: {rate_limited_users}")

            # 更新批次统计
            self.update_batch_stats(batch_results)

            # 每批次完成后保存状态
            try:
                self.state_manager.save_state()
                logger.debug(f"💾 批次完成后状态已保存")
            except Exception as e:
                logger.error(f"💾 批次状态保存失败: {e}")

            logger.info(f"批次完成: {len(user_batch)} 用户, {success_count} 个有新推文, {error_count} 个异常 (其中 {rate_limit_count} 个429限流), 耗时: {batch_duration:.2f}秒")
            
            # 如果429错误太多，自动降低并发数
            if rate_limit_count > len(user_batch) * 0.5:  # 超过50%是429错误
                logger.warning(f"429错误过多 ({rate_limit_count}/{len(user_batch)})，建议降低并发数")
            
            # 动态调整并发数
            self.adjust_concurrency(success_count, len(user_batch), error_count)
            
            # 打印ETag统计（每10个批次打印一次）
            if hasattr(self, '_batch_counter'):
                self._batch_counter += 1
            else:
                self._batch_counter = 1
                
            if self._batch_counter % 10 == 0:
                self.print_cache_stats()
    
    def get_next_batch(self, users: List[str], batch_size: int, current_index: int) -> Tuple[List[str], int]:
        """获取下一个批次，优先处理失败的用户 + 负载均衡"""
        batch = []

        # 首先添加待处理的用户（主要是429限流用户）
        while len(batch) < batch_size and self.pending_users:
            batch.append(self.pending_users.pop(0))

        # 然后从正常队列补充用户
        remaining_slots = batch_size - len(batch)
        if remaining_slots > 0 and current_index < len(users):
            end_index = min(current_index + remaining_slots, len(users))
            batch.extend(users[current_index:end_index])
            current_index = end_index

        # 方案A: 打乱批次内用户顺序，避免实例集中
        if len(batch) > 1:
            import random
            random.shuffle(batch)

        return batch, current_index

    def get_balanced_batch(self, users: List[str], batch_size: int, current_index: int) -> Tuple[List[str], int]:
        """方案B: 获取负载均衡的批次，确保来自不同实例"""
        batch = []

        # 首先添加待处理的用户（主要是429限流用户）
        while len(batch) < batch_size and self.pending_users:
            batch.append(self.pending_users.pop(0))

        remaining_slots = batch_size - len(batch)
        if remaining_slots > 0 and current_index < len(users):
            # 按实例分组剩余用户
            instance_users = {}
            for i in range(current_index, len(users)):
                user_id = users[i]
                if user_id in self.user_instance_mapping:
                    instance = self.user_instance_mapping[user_id]
                    instance_url = instance.url
                    if instance_url not in instance_users:
                        instance_users[instance_url] = []
                    instance_users[instance_url].append((user_id, i))

            # 轮询从每个实例选择用户
            selected_users = []
            max_index = current_index

            while len(selected_users) < remaining_slots and instance_users:
                for instance_url in list(instance_users.keys()):
                    if len(selected_users) >= remaining_slots:
                        break

                    if instance_users[instance_url]:
                        user_id, user_index = instance_users[instance_url].pop(0)
                        selected_users.append(user_id)
                        max_index = max(max_index, user_index + 1)

                        # 如果这个实例没有更多用户，移除它
                        if not instance_users[instance_url]:
                            del instance_users[instance_url]

                # 如果所有实例都没有用户了，退出循环
                if not instance_users:
                    break

            batch.extend(selected_users)
            current_index = max_index

        return batch, current_index

    def get_batch_instance_distribution(self, batch: List[str]) -> Dict[str, int]:
        """获取批次的实例分布情况"""
        distribution = {}
        for user_id in batch:
            if user_id in self.user_instance_mapping:
                instance = self.user_instance_mapping[user_id]
                instance_url = instance.url
                distribution[instance_url] = distribution.get(instance_url, 0) + 1
            else:
                # 如果用户没有分配实例，临时分配一个
                instance = self.get_instance_for_user(user_id)
                instance_url = instance.url
                distribution[instance_url] = distribution.get(instance_url, 0) + 1
        return distribution

    async def run(self):
        """运行轮询引擎"""
        logger.info("启动增强轮询引擎...")
        
        # 初始化用户
        await self.initialize_users()

        # 不再发送测试推文，避免干扰时间排序
        # await self.send_test_tweet()
        
        cycle_count = 0
        last_rebalance_cycle = 0
        
        while True:
            try:
                cycle_count += 1
                cycle_start = time.time()
                logger.info(f"开始第 {cycle_count} 轮轮询...")

                # 更新轮询周期
                self.polling_stats["current_cycle"] = cycle_count

                # 重置当前轮询周期统计
                self.reset_cycle_stats()

                # 每轮检查配置是否有更新
                if self.reload_polling_config():
                    logger.info("轮询配置已更新")

                users = self.state_manager.get_all_users()
                if not users:
                    logger.warning("没有用户需要轮询")
                    await asyncio.sleep(POLL_INTERVAL)
                    continue

                # 方案A: 打乱用户列表顺序，避免实例集中
                import random
                random.shuffle(users)

                pending_count = len(self.pending_users)
                logger.info(f"本轮将处理 {len(users)} 个用户，并发数: {self.current_concurrent}，待处理队列: {pending_count} 个用户")

                # 使用新的批次获取逻辑
                batch_count = 0
                current_index = 0
                
                while current_index < len(users) or self.pending_users:
                    batch_count += 1
                    # 使用负载均衡的批次获取方法
                    batch, current_index = self.get_balanced_batch(users, self.current_concurrent, current_index)
                    
                    if not batch:  # 没有更多用户需要处理
                        break

                    # 显示批次实例分布
                    batch_distribution = self.get_batch_instance_distribution(batch)
                    logger.info(f"处理第 {batch_count} 批用户...")
                    logger.info(f"批次实例分布: {batch_distribution}")

                    await self.poll_users_batch(batch)
                    
                    # 批次间短暂延迟
                    if current_index < len(users) or self.pending_users:
                        await asyncio.sleep(BATCH_DELAY)
                
                # 完成轮询周期统计
                self.finalize_cycle_stats()

                # 保存状态
                save_start = time.time()
                logger.info(f"💾 开始保存状态文件...")
                self.state_manager.save_state()
                save_duration = time.time() - save_start
                logger.info(f"💾 状态文件保存完成，耗时: {save_duration:.2f}秒")

                cycle_duration = time.time() - cycle_start
                remaining_pending = len(self.pending_users)
                logger.info(f"第 {cycle_count} 轮轮询完成! 总耗时: {cycle_duration:.2f}秒, 状态保存耗时: {save_duration:.2f}秒, 剩余待处理: {remaining_pending} 个用户")
                
                # 每10轮打印负载分布
                if cycle_count % 10 == 0:
                    self.print_load_distribution()
                
                # 等待下一轮
                logger.info(f"等待 {POLL_INTERVAL} 秒后开始下一轮...")
                await asyncio.sleep(POLL_INTERVAL)
                
            except Exception as e:
                logger.error(f"轮询过程出错: {e}")
                # 即使出错也要保存状态
                try:
                    logger.info(f"💾 异常情况下保存状态...")
                    self.state_manager.save_state()
                    logger.info(f"💾 异常情况下状态保存完成")
                except Exception as save_error:
                    logger.error(f"💾 异常情况下保存状态失败: {save_error}")
                await asyncio.sleep(5)
    
    async def send_test_tweet(self):
        """发送测试推文"""
        try:
            test_tweet = {
                "id": f"test_{int(time.time())}",
                "user_id": "system",
                "username": "系统测试",
                "content": f"系统测试推文 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                "html": "<p>系统测试推文</p>",
                "published_at": datetime.now().strftime("%a, %d %b %Y %H:%M:%S"),
                "url": "#",
                "timestamp": datetime.now().isoformat(),
                "images": json.dumps([])
            }
            
            self.redis_client.xadd(TWEET_STREAM_KEY, test_tweet)
            logger.info("测试推文已发送")
            
        except Exception as e:
            logger.error(f"发送测试推文失败: {e}")

async def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="增强的推特轮询引擎")
    
    parser.add_argument(
        "--following-file",
        default="./config/following_list.json",
        help="关注列表文件路径"
    )
    
    parser.add_argument(
        "--nitter-instances",
        default="http://localhost:8080",
        help="Nitter实例URL，多个实例用逗号分隔"
    )
    
    parser.add_argument(
        "--debug",
        action="store_true",
        help="启用调试日志"
    )



    args = parser.parse_args()
    
    if args.debug:
        logger.setLevel(logging.DEBUG)
    
    # 解析实例URL
    nitter_instances = [url.strip() for url in args.nitter_instances.split(",")]
    logger.info(f"使用Nitter实例: {nitter_instances}")
    
    # 检查关注列表文件
    if not os.path.exists(args.following_file):
        logger.error(f"关注列表文件不存在: {args.following_file}")
        sys.exit(1)
    
    try:
        engine = EnhancedPollingEngine(nitter_instances, args.following_file)
        await engine.run()
    except KeyboardInterrupt:
        logger.info("接收到中断信号，正在退出...")
    except Exception as e:
        logger.error(f"引擎运行失败: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main()) 