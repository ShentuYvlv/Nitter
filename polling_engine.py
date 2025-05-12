import json
import time
import random
import asyncio
import aiohttp
import redis
import logging
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import List, Dict, Set, Optional, Tuple, DefaultDict
from collections import defaultdict
import os
import argparse
import sys

# 添加令牌桶限流器
class TokenBucket:
    """简单的令牌桶限流器实现"""
    def __init__(self, rate: float, capacity: int = 1):
        """
        初始化令牌桶
        
        参数:
            rate: 每秒生成的令牌数
            capacity: 桶的容量（最大令牌数）
        """
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.last_refill = time.time()
        self.lock = asyncio.Lock()
        
    async def acquire(self, tokens: int = 1) -> float:
        """
        尝试获取指定数量的令牌
        
        参数:
            tokens: 需要获取的令牌数
            
        返回:
            需要等待的时间（秒）
        """
        async with self.lock:
            # 计算从上次填充到现在应该生成的令牌数
            now = time.time()
            elapsed = now - self.last_refill
            new_tokens = elapsed * self.rate
            
            # 更新令牌数，不超过容量
            self.tokens = min(self.capacity, self.tokens + new_tokens)
            self.last_refill = now
            
            # 如果令牌不足，计算需要等待的时间
            if tokens > self.tokens:
                wait_time = (tokens - self.tokens) / self.rate
                return wait_time
            
            # 否则，消耗令牌并立即返回
            self.tokens -= tokens
            return 0
    
    async def wait(self, tokens: int = 1):
        """
        等待直到可以获取指定数量的令牌
        
        参数:
            tokens: 需要获取的令牌数
        """
        wait_time = await self.acquire(tokens)
        if wait_time > 0:
            await asyncio.sleep(wait_time)

# 在Windows上设置正确的事件循环策略
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# 配置日志
logging.basicConfig(
    level=logging.DEBUG,  # 将日志级别改为DEBUG
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("polling_engine")

# Redis配置
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_DB = int(os.environ.get("REDIS_DB", 0))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD", None)

# 状态版本控制 - 每次数据结构变更时更新
STATE_VERSION = "v1"

# 推文流配置
TWEET_STREAM_KEY = "tweet_stream"
LAST_STREAM_ID_KEY = "last_stream_id"
LAST_TWEET_KEY_PREFIX = f"last_{STATE_VERSION}:tweet:"  # 添加版本前缀
USER_ETAG_PREFIX = f"etag_{STATE_VERSION}:"  # 添加版本前缀
DEFAULT_FIRST_STREAM_ID = "$"  # 从最新消息开始

# 用户分组配置
PRIORITY_USERS_KEY = "priority_users"
NORMAL_USERS_KEY = "normal_users"
USER_ACTIVITY_PREFIX = "activity:"
POLLING_QUEUE_KEY = "polling_queue"  # 轮询队列
LAST_POLL_TIME_PREFIX = "last_poll:"  # 上次轮询时间前缀

# 轮询配置
POLLING_CONFIG_KEY = "polling_config"  # Redis中存储轮询配置的键名
PRIORITY_POLL_INTERVAL = 10  # 优先用户组轮询间隔（秒）
NORMAL_POLL_INTERVAL = 60    # 普通用户组轮询间隔（秒）
BATCH_SIZE = 15               # 每批处理的用户数量（从10改为5）
REQUEST_TIMEOUT = 10         # 请求超时时间（秒）
MAX_RETRIES = 5              # 最大重试次数
REQUEST_RATE = 10             # 每秒请求数（从默认改为2）
BURST_CAPACITY = 20           # 突发请求容量

# 从Redis加载轮询配置
def load_polling_config_from_redis(redis_client):
    """从Redis加载轮询配置"""
    try:
        config_json = redis_client.get(POLLING_CONFIG_KEY)
        if config_json:
            config = json.loads(config_json)
            logger.info(f"从Redis加载轮询配置: {config}")
            
            # 更新全局配置
            global PRIORITY_POLL_INTERVAL, NORMAL_POLL_INTERVAL, BATCH_SIZE
            global REQUEST_TIMEOUT, MAX_RETRIES, REQUEST_RATE, BURST_CAPACITY
            
            PRIORITY_POLL_INTERVAL = config.get("PRIORITY_POLL_INTERVAL", PRIORITY_POLL_INTERVAL)
            NORMAL_POLL_INTERVAL = config.get("NORMAL_POLL_INTERVAL", NORMAL_POLL_INTERVAL)
            BATCH_SIZE = config.get("BATCH_SIZE", BATCH_SIZE)
            REQUEST_TIMEOUT = config.get("REQUEST_TIMEOUT", REQUEST_TIMEOUT)
            MAX_RETRIES = config.get("MAX_RETRIES", MAX_RETRIES)
            REQUEST_RATE = config.get("REQUEST_RATE", REQUEST_RATE)
            BURST_CAPACITY = config.get("BURST_CAPACITY", BURST_CAPACITY)
            
            return True
    except Exception as e:
        logger.error(f"加载轮询配置出错: {str(e)}")
    
    return False

# 辅助函数：解析各种日期格式
def parse_date(date_str: str) -> datetime:
    """
    尝试解析各种格式的日期字符串
    
    参数:
        date_str: 日期字符串
        
    返回:
        解析后的datetime对象，如果解析失败则返回当前时间
    """
    if not date_str:
        return datetime.now()
        
    # 预处理日期字符串
    date_str = date_str.strip()
    
    # 尝试不同的日期格式
    formats = [
        # RFC 822/1123 格式 (Twitter API常用格式)
        "%a, %d %b %Y %H:%M:%S %z",  # 带时区
        "%a, %d %b %Y %H:%M:%S",     # 不带时区
        
        # 处理GMT特殊情况
        "%a, %d %b %Y %H:%M:%S GMT",
        
        # ISO 8601格式
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        
        # 常见格式
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M:%S",
        
        # 仅日期
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d/%m/%Y",
        "%m/%d/%Y",
    ]
    
    # 处理GMT特殊情况
    if " GMT" in date_str:
        date_str_fixed = date_str.replace(" GMT", " +0000")
        formats.insert(0, "%a, %d %b %Y %H:%M:%S +0000")  # 优先尝试这个格式
    
    # 尝试所有格式
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    # 如果所有格式都失败，记录错误并返回当前时间
    logging.warning(f"无法解析日期: {date_str}")
    return datetime.now()

class PollingEngine:
    def __init__(self, nitter_instances: List[str], following_file: str):
        """
        初始化轮询引擎
        
        参数:
            nitter_instances: Nitter实例URL列表
            following_file: 关注用户列表的JSON文件路径
        """
        self.nitter_instances = nitter_instances
        self.current_instance_index = 0
        self.following_file = following_file
        
        # 添加实例健康度跟踪
        self.instance_weights = [10.0] * len(nitter_instances)  # 初始权重为10
        self.instance_call_counter = defaultdict(int)  # 记录每个实例的调用计数
        self.consecutive_failures = 0  # 连续失败计数
        
        # 创建令牌桶限流器
        self.rate_limiter = TokenBucket(rate=REQUEST_RATE, capacity=BURST_CAPACITY)
        
        # 连接Redis
        try:
            self.redis_client = redis.Redis(
                host=REDIS_HOST, 
                port=REDIS_PORT, 
                db=REDIS_DB,
                password=REDIS_PASSWORD,
                decode_responses=True
            )
            
            # 检查Redis连接
            self.redis_client.ping()
            logger.info(f"成功连接到Redis服务器 {REDIS_HOST}:{REDIS_PORT}")
            
            # 从Redis加载轮询配置
            load_polling_config_from_redis(self.redis_client)
            
            # 重新创建令牌桶限流器，使用更新后的配置
            self.rate_limiter = TokenBucket(rate=REQUEST_RATE, capacity=BURST_CAPACITY)
            
            # 检查Redis流是否存在
            if self.redis_client.exists(TWEET_STREAM_KEY):
                stream_length = self.redis_client.xlen(TWEET_STREAM_KEY)
                logger.info(f"Redis流 {TWEET_STREAM_KEY} 存在，当前长度: {stream_length}")
            else:
                logger.warning(f"Redis流 {TWEET_STREAM_KEY} 不存在，将在首次推文时创建")
                
        except redis.ConnectionError as e:
            logger.error(f"无法连接到Redis服务器 {REDIS_HOST}:{REDIS_PORT}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Redis初始化错误: {str(e)}")
            raise
            
        self.is_first_run = True  # 标记是否是首次运行
        
    def update_instance_health(self, instance_index: int, response_time: float = None, error: bool = False):
        """
        更新Nitter实例的健康状态
        
        参数:
            instance_index: 实例索引
            response_time: 响应时间（秒）
            error: 是否发生错误
        """
        # 记录调用计数
        self.instance_call_counter[instance_index] += 1
        
        # 如果发生错误，减少权重
        if error:
            self.instance_weights[instance_index] = max(1.0, self.instance_weights[instance_index] * 0.8)
            self.consecutive_failures += 1
            logger.debug(f"实例 {self.nitter_instances[instance_index]} 发生错误，权重降为 {self.instance_weights[instance_index]:.1f}")
        else:
            # 根据响应时间调整权重
            if response_time is not None:
                if response_time < 0.5:
                    # 响应快，增加权重
                    self.instance_weights[instance_index] = min(20.0, self.instance_weights[instance_index] + 0.5)
                elif response_time > 2.0:
                    # 响应慢，减少权重
                    self.instance_weights[instance_index] = max(1.0, self.instance_weights[instance_index] - 1.0)
                
            # 重置连续失败计数
            self.consecutive_failures = 0
            
    def get_best_instance(self) -> int:
        """
        获取当前最佳的Nitter实例索引
        
        返回:
            实例索引
        """
        if len(self.nitter_instances) == 1:
            return 0
            
        # 使用加权随机选择
        total_weight = sum(self.instance_weights)
        if total_weight <= 0:
            # 如果所有权重都为0，重置权重
            self.instance_weights = [5.0] * len(self.nitter_instances)
            total_weight = sum(self.instance_weights)
            
        # 随机选择一个实例，权重越高越可能被选中
        r = random.uniform(0, total_weight)
        upto = 0
        for i, weight in enumerate(self.instance_weights):
            upto += weight
            if upto >= r:
                return i
                
        # 默认返回第一个实例
        return 0
        
    async def validate_redis_state(self):
        """智能状态校验与修复"""
        logger.info("开始状态校验与修复...")
        valid_users = set()
        
        # 获取所有存储的用户状态
        all_last_keys = self.redis_client.keys(f"{LAST_TWEET_KEY_PREFIX}*")
        all_etag_keys = self.redis_client.keys(f"{USER_ETAG_PREFIX}*")
        
        logger.info(f"找到 {len(all_last_keys)} 个最后推文ID记录和 {len(all_etag_keys)} 个ETag记录")
        
        # 获取当前的优先用户和普通用户
        priority_users = self.redis_client.smembers(PRIORITY_USERS_KEY)
        normal_users = self.redis_client.smembers(NORMAL_USERS_KEY)
        current_users = set(priority_users) | set(normal_users)
        
        # 检查最后推文ID记录
        invalid_last_keys = 0
        for key in all_last_keys:
            user_id = key.split(":")[-1]
            last_tweet_id = self.redis_client.get(key)
            
            # 校验推文ID格式
            if not self._is_valid_tweet_id(last_tweet_id):
                logger.warning(f"无效tweet_id: {user_id} -> {last_tweet_id}")
                self.redis_client.delete(key)
                invalid_last_keys += 1
                continue
                
            # 校验用户是否仍在关注列表
            if user_id not in current_users:
                logger.info(f"移除未关注用户状态: {user_id}")
                self.redis_client.delete(key)
                continue
                
            valid_users.add(user_id)
        
        # 检查ETag记录
        invalid_etag_keys = 0
        for key in all_etag_keys:
            user_id = key.split(":")[-1]
            etag = self.redis_client.get(key)
            
            # 简单检查ETag是否为空或格式异常
            if not etag or len(etag) < 5:
                logger.warning(f"无效ETag: {user_id} -> {etag}")
                self.redis_client.delete(key)
                invalid_etag_keys += 1
                continue
            
            # 校验用户是否仍在关注列表
            if user_id not in current_users:
                logger.info(f"移除未关注用户ETag: {user_id}")
                self.redis_client.delete(key)
                continue
        
        logger.info(f"状态校验完成，发现 {invalid_last_keys} 个无效推文ID和 {invalid_etag_keys} 个无效ETag")
        logger.info(f"当前有 {len(valid_users)} 个用户有有效状态")
        
        return valid_users
    
    def _is_valid_tweet_id(self, tweet_id: str) -> bool:
        """校验推文ID格式"""
        # 推特ID通常是一个19位的数字字符串
        if not tweet_id:
            return False
        
        # 处理带有后缀的推文ID (例如: 1921193426596622789#m)
        if "#" in tweet_id:
            # 提取实际ID部分
            tweet_id = tweet_id.split("#")[0]
        
        # 检查是否纯数字
        if not tweet_id.isdigit():
            return False
            
        # 检查长度
        if len(tweet_id) < 15 or len(tweet_id) > 20:
            return False
            
        return True
    
    async def incremental_recovery(self):
        """渐进式状态恢复"""
        logger.info("开始渐进式状态恢复...")
        
        # 获取优先用户和普通用户
        priority_users = self.redis_client.smembers(PRIORITY_USERS_KEY)
        normal_users = self.redis_client.smembers(NORMAL_USERS_KEY)
        
        # 计数器
        total_verified = 0
        total_fixed = 0
        
        # 1. 优先恢复高活跃用户
        logger.info(f"开始恢复优先用户状态 ({len(priority_users)}个)...")
        verified, fixed = await self._recover_user_batch(priority_users, batch_size=5)
        total_verified += verified
        total_fixed += fixed
        
        # 2. 恢复普通用户
        logger.info(f"开始恢复普通用户状态 ({len(normal_users)}个)...")
        verified, fixed = await self._recover_user_batch(normal_users, batch_size=10)
        total_verified += verified
        total_fixed += fixed
        
        logger.info(f"状态恢复完成: 验证了 {total_verified} 个用户，修复了 {total_fixed} 个用户的状态")
    
    async def _recover_user_batch(self, users: set, batch_size: int):
        """分批次恢复用户状态"""
        users_list = list(users)
        verified_count = 0
        fixed_count = 0
        
        for i in range(0, len(users_list), batch_size):
            batch = users_list[i:i+batch_size]
            logger.debug(f"处理用户批次 {i//batch_size + 1}/{(len(users_list) + batch_size - 1) // batch_size}，包含 {len(batch)} 个用户")
            
            # 并发验证状态有效性
            tasks = [self._verify_user_state(user) for user in batch]
            results = await asyncio.gather(*tasks)
            
            # 统计结果
            verified_count += len(batch)
            
            # 清理无效状态
            for user, valid in zip(batch, results):
                if not valid:
                    self.redis_client.delete(f"{LAST_TWEET_KEY_PREFIX}{user}")
                    self.redis_client.delete(f"{USER_ETAG_PREFIX}{user}")
                    fixed_count += 1
            
            # 避免请求太频繁
            await asyncio.sleep(1)
        
        return verified_count, fixed_count
    
    async def _verify_user_state(self, user_id: str) -> bool:
        """验证用户状态是否有效"""
        # 检查是否有缓存
        has_tweet_id = self.redis_client.exists(f"{LAST_TWEET_KEY_PREFIX}{user_id}")
        has_etag = self.redis_client.exists(f"{USER_ETAG_PREFIX}{user_id}")
        
        # 如果没有缓存，认为状态无效
        if not has_tweet_id and not has_etag:
            return True  # 没有状态不需要清理
        
        try:
            # 获取最佳实例
            instance_index = self.get_best_instance()
            base_url = self.nitter_instances[instance_index]
            
            # 发起HEAD请求快速验证
            async with aiohttp.ClientSession() as session:
                url = f"{base_url}/{user_id}/rss"
                async with session.head(url, timeout=5) as resp:
                    # 如果用户存在且可访问，则状态有效
                    if resp.status == 200:
                        return True
                    
                    # 如果用户不存在或无法访问，则状态无效
                    if resp.status == 404:
                        logger.warning(f"用户 {user_id} 不存在，清理状态")
                        return False
                    
                    # 其他错误（如429、500等）暂时保留状态
                    logger.warning(f"验证用户 {user_id} 状态时收到HTTP {resp.status}，保留状态")
                    return True
        except Exception as e:
            logger.warning(f"验证用户 {user_id} 状态时出错: {str(e)}，保留状态")
            return True  # 出错时保留状态，避免误删
            
    async def initialize(self):
        """初始化用户列表和分组"""
        # 加载关注用户列表
        following_list = self.load_following_list()
        
        # 将用户分为优先组和普通组
        priority_users = {
            "daidaibtc", "brc20niubi", "Nuanran01", "binancezh", 
            "0x0xFeng", "hexiecs", "CryptoDevinL", "bwenews", 
            "dotyyds1234", "0xSunNFT", "xiaomucrypto", "Bitfatty", 
            "ai_9684xtpa", "cz_binance", "EmberCN", "Vida_BWE"
        }
        
        pipe = self.redis_client.pipeline()
        
        # 清空现有用户组
        pipe.delete(PRIORITY_USERS_KEY)
        pipe.delete(NORMAL_USERS_KEY)
        pipe.delete(POLLING_QUEUE_KEY)  # 清空轮询队列
        
        # 添加用户到各自的组
        for user in following_list:
            user_id = user.get("userId", "").lower()
            if not user_id:
                continue
                
            if user_id in priority_users:
                pipe.sadd(PRIORITY_USERS_KEY, user_id)
            else:
                pipe.sadd(NORMAL_USERS_KEY, user_id)
            
            # 添加到轮询队列
            pipe.zadd(POLLING_QUEUE_KEY, {user_id: 0})
        
        pipe.execute()
        
        priority_count = self.redis_client.scard(PRIORITY_USERS_KEY)
        normal_count = self.redis_client.scard(NORMAL_USERS_KEY)
        
        logger.info(f"已加载 {priority_count} 个优先用户和 {normal_count} 个普通用户")
        
        # 执行状态校验和增量恢复
        await self.validate_redis_state()
        await self.incremental_recovery()
        
    def load_following_list(self) -> List[Dict]:
        """从JSON文件加载关注用户列表"""
        try:
            with open(self.following_file, 'r', encoding='utf-8') as f:
                following_list = json.load(f)
            logger.info(f"成功加载关注列表，共 {len(following_list)} 个用户")
            return following_list
        except (json.JSONDecodeError, FileNotFoundError) as e:
            logger.error(f"无法加载关注列表: {str(e)}")
            return []
            
    def get_nitter_instance(self) -> str:
        """获取当前使用的Nitter实例URL"""
        # 使用最佳实例
        self.current_instance_index = self.get_best_instance()
        return self.nitter_instances[self.current_instance_index]
        
    def switch_nitter_instance(self):
        """切换到下一个可用的Nitter实例"""
        if len(self.nitter_instances) > 1:
            # 将当前实例的权重降低
            self.instance_weights[self.current_instance_index] = max(1.0, self.instance_weights[self.current_instance_index] * 0.5)
            
            # 选择权重最高的实例
            best_index = self.get_best_instance()
            
            # 如果选出的最佳实例仍然是当前实例，则强制切换
            if best_index == self.current_instance_index:
                self.current_instance_index = (self.current_instance_index + 1) % len(self.nitter_instances)
            else:
                self.current_instance_index = best_index
                
            logger.warning(f"切换到Nitter实例: {self.nitter_instances[self.current_instance_index]} (权重: {self.instance_weights[self.current_instance_index]:.1f})")
        else:
            logger.warning(f"只有一个Nitter实例可用，无法切换")
        
    async def fetch_user_tweets(self, session: aiohttp.ClientSession, user_id: str, retry_count: int = 0) -> Tuple[bool, bool]:
        """
        获取用户的最新推文
        
        参数:
            session: aiohttp会话
            user_id: 用户ID
            retry_count: 当前重试次数
            
        返回:
            (是否成功获取到新推文, 是否出现错误)
        """
        # 使用令牌桶限流
        await self.rate_limiter.wait()
        
        # 选择最佳实例
        instance_index = self.get_best_instance()
        base_url = self.nitter_instances[instance_index]
        rss_url = f"{base_url}/{user_id}/rss"
        
        # 获取上一次的ETag
        etag = self.redis_client.get(f"{USER_ETAG_PREFIX}{user_id}")
        
        headers = {}
        if etag:
            headers["If-None-Match"] = etag
            
        # 添加随机延迟，避免请求集中
        jitter = random.uniform(0.1, 0.5)
        await asyncio.sleep(jitter)
        
        logger.debug(f"开始获取用户 {user_id} 的推文，URL: {rss_url}")
        
        try:
            start_time = time.time()
            async with session.get(rss_url, headers=headers, timeout=REQUEST_TIMEOUT) as response:
                response_time = time.time() - start_time
                
                # 记录请求响应情况
                logger.debug(f"用户 {user_id} 请求响应: {response.status} ({response_time:.2f}s)")
                
                # 更新实例健康度
                self.update_instance_health(instance_index, response_time, error=(response.status >= 400))
                
                # 如果内容未更新
                if response.status == 304:
                    logger.debug(f"用户 {user_id} 的内容未更新")
                    # 更新最后轮询时间
                    self.redis_client.zadd(POLLING_QUEUE_KEY, {user_id: time.time()})
                    return False, False
                    
                # 如果请求成功
                if response.status == 200:
                    # 保存ETag以便下次请求
                    if "ETag" in response.headers:
                        self.redis_client.set(
                            f"{USER_ETAG_PREFIX}{user_id}", 
                            response.headers["ETag"]
                        )
                    
                    # 解析RSS内容
                    content = await response.text()
                    
                    # 记录获取到的内容长度
                    logger.debug(f"获取到用户 {user_id} 的RSS内容，长度: {len(content)} 字节")
                    
                    # 如果内容非常短，可能有问题
                    if len(content) < 100:
                        logger.warning(f"用户 {user_id} 的RSS内容异常短: {content}")
                        if retry_count < MAX_RETRIES:
                            # 添加随机延迟
                            wait_time = (2 ** retry_count) * random.uniform(0.5, 1.5)
                            logger.info(f"等待 {wait_time:.1f} 秒后重试获取用户 {user_id} 的推文 (尝试 {retry_count + 1}/{MAX_RETRIES})")
                            await asyncio.sleep(wait_time)
                            return await self.fetch_user_tweets(session, user_id, retry_count + 1)
                        return False, True
                    
                    # 更新最后轮询时间
                    self.redis_client.zadd(POLLING_QUEUE_KEY, {user_id: time.time()})
                    
                    # 处理RSS内容
                    has_new_tweets = await self.process_rss_content(user_id, content)
                    return has_new_tweets, False
                
                # 处理错误
                if response.status == 404:
                    logger.warning(f"用户 {user_id} 不存在")
                    # 更新最后轮询时间，避免频繁请求不存在的用户
                    self.redis_client.zadd(POLLING_QUEUE_KEY, {user_id: time.time() + 3600})  # 1小时后再试
                    return False, False
                
                # 处理限流错误
                if response.status == 429:
                    logger.error(f"获取用户 {user_id} 推文失败: 请求过多 (HTTP 429)")
                    
                    # 降低当前实例权重
                    self.instance_weights[instance_index] *= 0.5
                    logger.debug(f"实例 {base_url} 被限流，权重降为 {self.instance_weights[instance_index]:.1f}")
                    
                    # 删除全局请求速率降低逻辑，仅保留实例切换
                    if len(self.nitter_instances) > 1:
                        self.switch_nitter_instance()
                    
                    # 重试
                    if retry_count < MAX_RETRIES:
                        # 指数退避策略 + 随机抖动
                        wait_time = (2 ** retry_count) * random.uniform(1.0, 2.0)
                        logger.info(f"等待 {wait_time:.1f} 秒后重试获取用户 {user_id} 的推文 (尝试 {retry_count + 1}/{MAX_RETRIES})")
                        await asyncio.sleep(wait_time)
                        return await self.fetch_user_tweets(session, user_id, retry_count + 1)
                
                # 其他错误
                logger.error(f"获取用户 {user_id} 推文失败: HTTP {response.status}")
                
                # 记录响应内容，辅助调试
                try:
                    error_content = await response.text()
                    logger.error(f"错误响应内容: {error_content[:200]}...")  # 只记录前200个字符
                except:
                    logger.error("无法读取错误响应内容")
                
                # 重试
                if retry_count < MAX_RETRIES:
                    # 指数退避策略
                    wait_time = (2 ** retry_count) * random.uniform(1.0, 2.0)
                    logger.info(f"等待 {wait_time:.1f} 秒后重试获取用户 {user_id} 的推文 (尝试 {retry_count + 1}/{MAX_RETRIES})")
                    await asyncio.sleep(wait_time)
                    return await self.fetch_user_tweets(session, user_id, retry_count + 1)
                
                return False, True
                
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.error(f"请求用户 {user_id} 推文时出错: {str(e)}")
            
            # 更新实例健康度
            self.update_instance_health(instance_index, error=True)
            
            # 重试
            if retry_count < MAX_RETRIES:
                # 指数退避策略
                wait_time = (2 ** retry_count) * random.uniform(1.0, 2.0)
                logger.info(f"等待 {wait_time:.1f} 秒后重试获取用户 {user_id} 的推文 (尝试 {retry_count + 1}/{MAX_RETRIES})")
                await asyncio.sleep(wait_time)
                return await self.fetch_user_tweets(session, user_id, retry_count + 1)
            
            return False, True
    
    async def process_rss_content(self, user_id: str, content: str) -> bool:
        """
        处理RSS内容，提取新推文
        
        参数:
            user_id: 用户ID
            content: RSS内容
            
        返回:
            是否有新推文
        """
        try:
            # 解析XML
            logger.debug(f"开始解析用户 {user_id} 的RSS内容")
            root = ET.fromstring(content)
            
            # 查找所有item元素
            items = root.findall(".//item")
            item_count = len(items)
            logger.debug(f"找到用户 {user_id} 的 {item_count} 条推文")
            
            if not items:
                return False
                
            # 获取上一次处理的最新推文ID
            last_tweet_id = self.redis_client.get(f"{LAST_TWEET_KEY_PREFIX}{user_id}")
            logger.debug(f"用户 {user_id} 上次推文ID: {last_tweet_id}")
            
            # 如果是第一次轮询，只记录最新的推文ID，不添加到流
            if not last_tweet_id and items:
                newest_item = items[0]
                link = newest_item.find("link")
                if link is not None and link.text:
                    newest_tweet_id = link.text.split("/")[-1]
                    self.redis_client.set(f"{LAST_TWEET_KEY_PREFIX}{user_id}", newest_tweet_id)
                    logger.info(f"首次轮询用户 {user_id}，记录最新推文ID: {newest_tweet_id}")
                return False
            
            has_new_tweets = False
            new_tweet_count = 0
            
            # 检查是否有新推文
            if items and last_tweet_id:
                newest_item = items[0]
                link = newest_item.find("link")
                if link is not None and link.text:
                    newest_tweet_id = link.text.split("/")[-1]
                    
                    # 处理最新推文ID和上次推文ID，移除可能的后缀
                    if "#" in newest_tweet_id:
                        newest_tweet_id_clean = newest_tweet_id.split("#")[0]
                    else:
                        newest_tweet_id_clean = newest_tweet_id
                        
                    if "#" in last_tweet_id:
                        last_tweet_id_clean = last_tweet_id.split("#")[0]
                    else:
                        last_tweet_id_clean = last_tweet_id
                    
                    # 使用清理后的ID进行比较
                    if newest_tweet_id_clean != last_tweet_id_clean:
                        # 提取推文内容
                        title = newest_item.find("title")
                        description = newest_item.find("description")
                        pub_date = newest_item.find("pubDate")
                        
                        if title is None or description is None or pub_date is None:
                            logger.warning(f"用户 {user_id} 的推文 {newest_tweet_id} 缺少必要字段")
                            return False
                        
                        # 确保日期格式正确
                        date_str = pub_date.text.strip() if pub_date.text else ""
                        timestamp = ""
                        try:
                            # 使用辅助函数解析日期
                            dt = parse_date(date_str)
                            timestamp = dt.strftime("%Y-%m-%dT%H:%M:%S")
                            logger.debug(f"成功解析日期: {date_str} -> {timestamp}")
                        except Exception as e:
                            logger.warning(f"日期解析失败 '{date_str}': {str(e)}")
                            # 使用当前时间作为备选
                            timestamp = datetime.now().isoformat()
                        
                        # 记录提取到的信息
                        logger.debug(f"提取到推文 - ID: {newest_tweet_id}, 标题: {title.text[:30] if title.text else '无标题'}..., 发布时间: {date_str}")
                        
                        # 将推文内容添加到Redis流
                        tweet_content = title.text if title.text else "无内容"
                        tweet_html = description.text if description.text else ""
                        tweet_url = link.text if link.text else "#"
                        
                        tweet_data = {
                            "id": newest_tweet_id,
                            "user_id": user_id,
                            "content": tweet_content,
                            "html": tweet_html,
                            "published_at": date_str,  # 保留原始日期字符串
                            "url": tweet_url,
                            "timestamp": timestamp  # 添加规范化的ISO格式时间戳
                        }
                        
                        # 记录将要添加到Redis的数据
                        logger.debug(f"将添加推文到Redis - 用户: {user_id}, ID: {newest_tweet_id}, 内容: {tweet_content[:50]}...")
                        
                        # 添加到Redis流
                        try:
                            stream_id = self.redis_client.xadd(
                                TWEET_STREAM_KEY,
                                tweet_data,
                                maxlen=10000,  # 限制流的最大长度
                                approximate=True
                            )
                            logger.debug(f"推文已添加到Redis流，ID: {stream_id}")
                            new_tweet_count = 1
                            has_new_tweets = True
                        except Exception as e:
                            logger.error(f"将推文添加到Redis流时出错: {str(e)}")
                        
                        # 更新用户活跃度
                        self.redis_client.hincrby(f"{USER_ACTIVITY_PREFIX}{user_id}", "tweet_count", 1)
                        self.redis_client.hset(
                            f"{USER_ACTIVITY_PREFIX}{user_id}", 
                            "last_tweet_at", 
                            datetime.now().isoformat()
                        )
                        
                        # 更新最新推文ID
                        self.redis_client.set(f"{LAST_TWEET_KEY_PREFIX}{user_id}", newest_tweet_id)
                        logger.info(f"用户 {user_id} 有 {new_tweet_count} 条新推文，最新ID: {newest_tweet_id}")
            
            # 测试Redis流是否有数据
            stream_length = self.redis_client.xlen(TWEET_STREAM_KEY)
            logger.debug(f"当前Redis推文流长度: {stream_length}")
            
            return has_new_tweets
                
        except ET.ParseError as e:
            logger.error(f"解析用户 {user_id} 的RSS内容失败: {str(e)}")
            logger.error(f"问题内容前100个字符: {content[:100]}")
            return False
            
    async def get_users_to_poll(self) -> List[str]:
        """
        获取需要轮询的用户列表
        
        返回:
            需要轮询的用户ID列表
        """
        current_time = time.time()
        
        # 获取所有用户及其上次轮询时间
        all_users = self.redis_client.zrange(POLLING_QUEUE_KEY, 0, -1, withscores=True)
        
        # 将用户分为优先组和普通组
        priority_users = set(self.redis_client.smembers(PRIORITY_USERS_KEY))
        
        # 创建优先级队列
        priority_queue = []  # (优先级, 上次轮询时间, 用户ID)
        
        for user_id, last_poll_time in all_users:
            # 计算距离上次轮询的时间
            time_since_last_poll = current_time - last_poll_time
            
            # 根据用户组确定轮询间隔和优先级
            if user_id in priority_users:
                poll_interval = PRIORITY_POLL_INTERVAL
                priority = 1  # 优先级高
            else:
                poll_interval = NORMAL_POLL_INTERVAL
                priority = 2  # 优先级低
                
                # 检查用户活跃度，降低不活跃用户的优先级
                last_active = self.redis_client.hget(f"{USER_ACTIVITY_PREFIX}{user_id}", "last_tweet_at")
                if last_active:
                    try:
                        last_active_time = datetime.fromisoformat(last_active)
                        days_inactive = (datetime.now() - last_active_time).days
                        if days_inactive > 7:  # 7天未活跃
                            priority = 3  # 更低优先级
                            poll_interval = NORMAL_POLL_INTERVAL * 2  # 延长轮询间隔
                    except (ValueError, TypeError):
                        pass
            
            # 如果已经达到轮询间隔，添加到待轮询列表
            if time_since_last_poll >= poll_interval:
                # 使用上次轮询时间作为排序依据，时间越久优先级越高
                priority_queue.append((priority, last_poll_time, user_id))
                
        # 按优先级和上次轮询时间排序
        priority_queue.sort()
        
        # 取出前BATCH_SIZE个用户
        users_to_poll = [user_id for _, _, user_id in priority_queue[:BATCH_SIZE]]
            
        # 如果没有足够的用户需要轮询，等待一会再试
        if not users_to_poll:
            logger.debug("没有用户需要轮询，等待下一个轮询周期")
            
        return users_to_poll
            
    async def poll_users(self, user_ids: List[str]):
        """
        轮询一批用户的推文
        
        参数:
            user_ids: 用户ID列表
        """
        if not user_ids:
            return
            
        logger.info(f"开始轮询 {len(user_ids)} 个用户")
        
        async with aiohttp.ClientSession() as session:
            # 分散请求，避免同时发送大量请求
            tasks = []
            for i, user_id in enumerate(user_ids):
                # 添加随机延迟，避免请求集中
                delay = i * 0.5 + random.uniform(0, 0.5)
                
                # 创建延迟任务
                task = asyncio.create_task(self._delayed_fetch(session, user_id, delay))
                tasks.append(task)
            
            # 等待所有任务完成
            results = await asyncio.gather(*tasks)
            
            # 统计结果
            success_count = sum(1 for r, _ in results if r)
            error_count = sum(1 for _, e in results if e)
            
            logger.info(f"轮询完成: {len(user_ids)} 个用户, {success_count} 个有新推文, {error_count} 个出错")
    
    async def _delayed_fetch(self, session: aiohttp.ClientSession, user_id: str, delay: float) -> Tuple[bool, bool]:
        """
        延迟一段时间后获取用户推文
        
        参数:
            session: aiohttp会话
            user_id: 用户ID
            delay: 延迟时间（秒）
            
        返回:
            (是否成功获取到新推文, 是否出现错误)
        """
        # 延迟
        await asyncio.sleep(delay)
        
        # 获取推文
        return await self.fetch_user_tweets(session, user_id)
    
    async def run(self):
        """启动轮询引擎"""
        logger.info("启动推文轮询引擎...")
        
        # 初始化
        await self.initialize()
        
        # 发送测试推文
        await self.send_test_tweet()
        
        # 配置检查计时器
        last_config_check = time.time()
        config_check_interval = 60  # 每60秒检查一次配置更新
        
        # 主轮询循环
        while True:
            try:
                # 获取需要轮询的用户
                users_to_poll = await self.get_users_to_poll()
                
                if users_to_poll:
                    # 轮询用户
                    await self.poll_users(users_to_poll)
                
                # 短暂休眠，避免CPU使用率过高
                await asyncio.sleep(1)
                
                # 在run方法中完成首次轮询后
                self.is_first_run = False
                # 调整参数
                global REQUEST_RATE, BATCH_SIZE
                if not self.is_first_run:
                    REQUEST_RATE = 20  # 增加请求速率
                    BATCH_SIZE = 30    # 增加批处理大小
                    self.rate_limiter = TokenBucket(rate=REQUEST_RATE, capacity=BURST_CAPACITY)
                
                # 定期检查配置更新
                current_time = time.time()
                if current_time - last_config_check > config_check_interval:
                    # 从Redis加载最新配置
                    if load_polling_config_from_redis(self.redis_client):
                        # 如果配置已更新，重新创建令牌桶限流器
                        self.rate_limiter = TokenBucket(rate=REQUEST_RATE, capacity=BURST_CAPACITY)
                        logger.info(f"已更新轮询配置: BATCH_SIZE={BATCH_SIZE}, REQUEST_RATE={REQUEST_RATE}")
                    
                    # 更新检查时间
                    last_config_check = current_time
                
            except Exception as e:
                logger.error(f"轮询过程中出错: {str(e)}")
                await asyncio.sleep(5)  # 出错后等待一段时间再继续
    
    async def send_test_tweet(self):
        """向Redis发送一条测试推文，用于验证系统正常工作"""
        try:
            logger.info("发送测试推文到Redis...")
            
            # 创建测试推文数据
            test_tweet = {
                "id": f"test_{int(time.time())}",
                "user_id": "test_user",
                "content": "这是一条测试推文，用于验证系统是否正常工作。时间戳: " + datetime.now().isoformat(),
                "html": "<p>这是一条测试推文，用于验证系统是否正常工作。</p>",
                "published_at": datetime.now().strftime("%a, %d %b %Y %H:%M:%S %z"),
                "url": "#",
                "timestamp": datetime.now().isoformat()
            }
            
            # 添加到Redis流
            stream_id = self.redis_client.xadd(
                TWEET_STREAM_KEY,
                test_tweet,
                maxlen=10000,
                approximate=True
            )
            
            logger.info(f"测试推文已添加到Redis流，ID: {stream_id}")
            return True
        except Exception as e:
            logger.error(f"发送测试推文失败: {str(e)}")
            return False

async def main():
    """主函数"""
    # 声明全局变量
    global BATCH_SIZE, REQUEST_RATE, STATE_VERSION
    
    parser = argparse.ArgumentParser(description="推特实时轮询引擎")
    
    parser.add_argument(
        "--following-file",
        default="./config/following_list.json",
        help="关注列表文件路径 (默认: ./config/following_list.json)"
    )
    
    parser.add_argument(
        "--nitter-instances",
        default="http://localhost:8080",
        help="Nitter实例URL，多个实例用逗号分隔 (默认: http://localhost:8080)"
    )
    
    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE,
        help=f"每批处理的用户数量 (默认: {BATCH_SIZE})"
    )
    
    parser.add_argument(
        "--request-rate",
        type=float,
        default=REQUEST_RATE,
        help=f"每秒请求数 (默认: {REQUEST_RATE})"
    )
    
    parser.add_argument(
        "--debug",
        action="store_true",
        help="启用调试日志"
    )
    
    # 替换原来的reset-cache参数
    parser.add_argument(
        "--reset-state",
        action="store_true",
        help="重置所有用户状态（包括ETag和最后推文ID）"
    )
    
    parser.add_argument(
        "--smart-fix",
        action="store_true",
        default=True,
        help="执行智能状态修复（默认行为，只修复问题状态）"
    )
    
    parser.add_argument(
        "--force-fix",
        action="store_true",
        help="强制执行状态修复，无视版本号（适用于数据格式变更）"
    )
    
    parser.add_argument(
        "--skip-state-check",
        action="store_true",
        help="跳过状态校验和修复步骤"
    )
    
    parser.add_argument(
        "--state-version",
        default=STATE_VERSION,
        help=f"指定状态版本（当前: {STATE_VERSION}）"
    )
    
    args = parser.parse_args()
    
    # 设置日志级别
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.INFO)
        logger.setLevel(logging.INFO)
    
    # 更新全局配置
    BATCH_SIZE = args.batch_size
    REQUEST_RATE = args.request_rate
    
    # 如果指定了状态版本，更新全局变量
    if args.state_version != STATE_VERSION:
        logger.warning(f"使用自定义状态版本: {args.state_version}，原版本: {STATE_VERSION}")
        STATE_VERSION = args.state_version
        # 更新键前缀
        global LAST_TWEET_KEY_PREFIX, USER_ETAG_PREFIX
        LAST_TWEET_KEY_PREFIX = f"last_{STATE_VERSION}:tweet:"
        USER_ETAG_PREFIX = f"etag_{STATE_VERSION}:"
    
    # 解析Nitter实例URL
    nitter_instances = [url.strip() for url in args.nitter_instances.split(",")]
    logger.info(f"使用以下Nitter实例: {', '.join(nitter_instances)}")
    
    # 测试Nitter实例可访问性
    available_instances = []
    try:
        async with aiohttp.ClientSession() as session:
            for url in nitter_instances:
                try:
                    logger.info(f"测试Nitter实例: {url}")
                    async with session.get(f"{url}", timeout=5) as response:
                        if response.status == 200:
                            logger.info(f"Nitter实例 {url} 可访问")
                            available_instances.append(url)
                        else:
                            logger.warning(f"Nitter实例 {url} 返回状态码: {response.status}")
                except Exception as e:
                    logger.error(f"无法连接到Nitter实例 {url}: {str(e)}")
    except Exception as e:
        logger.error(f"创建aiohttp会话失败: {str(e)}")
    
    # 如果没有可用的Nitter实例，退出
    if not available_instances:
        logger.critical("没有可用的Nitter实例，无法继续")
        sys.exit(1)
    
    # 测试Redis连接
    try:
        redis_client = redis.Redis(
            host=REDIS_HOST, 
            port=REDIS_PORT, 
            db=REDIS_DB,
            password=REDIS_PASSWORD,
            decode_responses=True
        )
        redis_client.ping()
        logger.info(f"Redis连接测试成功: {REDIS_HOST}:{REDIS_PORT}")
        
        # 如果指定了重置所有状态
        if args.reset_state:
            # 执行全面重置
            logger.warning("执行全面状态重置...")
            
            # 获取所有用户
            priority_users = redis_client.smembers(PRIORITY_USERS_KEY)
            normal_users = redis_client.smembers(NORMAL_USERS_KEY)
            all_users = list(priority_users) + list(normal_users)
            
            # 删除所有用户的状态
            pipe = redis_client.pipeline()
            for user_id in all_users:
                pipe.delete(f"{USER_ETAG_PREFIX}{user_id}")
                pipe.delete(f"{LAST_TWEET_KEY_PREFIX}{user_id}")
            
            # 执行删除操作
            results = pipe.execute()
            deleted_count = sum(1 for r in results if r)
            logger.info(f"已删除 {deleted_count} 个用户状态记录")
            
            # 重置轮询队列
            redis_client.delete(POLLING_QUEUE_KEY)
            logger.info("已重置轮询队列")
        
        # 如果指定了强制修复
        elif args.force_fix:
            logger.warning("执行强制状态修复...")
            
            # 查找所有版本的状态键
            all_etag_keys = redis_client.keys("etag_*:*")
            all_last_keys = redis_client.keys("last_*:tweet:*")
            
            # 仅保留当前版本的键
            pipe = redis_client.pipeline()
            for key in all_etag_keys:
                if not key.startswith(f"etag_{STATE_VERSION}:"):
                    pipe.delete(key)
                    logger.debug(f"删除旧版本键: {key}")
            
            for key in all_last_keys:
                if not key.startswith(f"last_{STATE_VERSION}:tweet:"):
                    pipe.delete(key)
                    logger.debug(f"删除旧版本键: {key}")
                    
            # 执行删除操作
            results = pipe.execute()
            deleted_count = sum(1 for r in results if r)
            logger.info(f"已删除 {deleted_count} 个旧版本状态记录")
        
        redis_client.close()
    except Exception as e:
        logger.error(f"Redis连接测试失败: {str(e)}")
        logger.critical("无法连接到Redis，请检查Redis服务器是否运行")
        sys.exit(1)
    
    # 检查following_file是否存在
    if not os.path.exists(args.following_file):
        logger.error(f"关注列表文件不存在: {args.following_file}")
        # 尝试在不同的位置查找
        alt_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "following_list.json")
        if os.path.exists(alt_path):
            logger.info(f"使用替代路径: {alt_path}")
            args.following_file = alt_path
        else:
            logger.critical("无法找到关注列表文件，请确保文件存在")
            sys.exit(1)
    
    try:
        # 创建轮询引擎
        engine = PollingEngine(available_instances, args.following_file)
        
        # 如果需要跳过状态校验
        if args.skip_state_check:
            logger.info("跳过状态校验和修复步骤")
            # 保存原方法
            original_initialize = engine.initialize
            # 创建新方法
            async def skip_state_check():
                # 加载关注用户列表
                following_list = engine.load_following_list()
                
                # 将用户分为优先组和普通组
                priority_users = {
                    "daidaibtc", "brc20niubi", "Nuanran01", "binancezh", 
                    "0x0xFeng", "hexiecs", "CryptoDevinL", "bwenews", 
                    "dotyyds1234", "0xSunNFT", "xiaomucrypto", "Bitfatty", 
                    "ai_9684xtpa", "cz_binance", "EmberCN", "Vida_BWE"
                }
                
                pipe = engine.redis_client.pipeline()
                
                # 清空现有用户组
                pipe.delete(PRIORITY_USERS_KEY)
                pipe.delete(NORMAL_USERS_KEY)
                pipe.delete(POLLING_QUEUE_KEY)  # 清空轮询队列
                
                # 添加用户到各自的组
                for user in following_list:
                    user_id = user.get("userId", "").lower()
                    if not user_id:
                        continue
                        
                    if user_id in priority_users:
                        pipe.sadd(PRIORITY_USERS_KEY, user_id)
                    else:
                        pipe.sadd(NORMAL_USERS_KEY, user_id)
                    
                    # 添加到轮询队列
                    pipe.zadd(POLLING_QUEUE_KEY, {user_id: 0})
                
                pipe.execute()
                
                priority_count = engine.redis_client.scard(PRIORITY_USERS_KEY)
                normal_count = engine.redis_client.scard(NORMAL_USERS_KEY)
                
                logger.info(f"已加载 {priority_count} 个优先用户和 {normal_count} 个普通用户")
                
                # 跳过状态校验和增量恢复
                logger.info("跳过状态校验和增量恢复...")
            
            # 替换原方法
            engine.initialize = skip_state_check
        
        # 运行引擎
        await engine.run()
    except KeyboardInterrupt:
        logger.info("接收到中断信号，正在退出...")
    except Exception as e:
        logger.critical(f"运行轮询引擎时出错: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main()) 