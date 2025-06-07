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

# 在Windows上设置正确的事件循环策略
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("polling_engine")

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

# 轮询配置
POLL_INTERVAL = 30          # 统一轮询间隔（秒）
CONCURRENT_USERS = 1       # 并发轮询用户数
REQUEST_TIMEOUT = 15        # 请求超时时间（秒）
MAX_RETRIES = 3             # 最大重试次数
RETRY_DELAYS = [1, 3, 5]    # 重试延迟（秒）

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
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(self.state, f, indent=2, ensure_ascii=False)
        except IOError as e:
            logger.error(f"保存状态文件失败: {e}")
    
    def get_user_state(self, user_id: str) -> dict:
        """获取用户状态"""
        return self.state["users"].get(user_id, {})
    
    def update_user_state(self, user_id: str, **kwargs):
        """更新用户状态"""
        if user_id not in self.state["users"]:
            self.state["users"][user_id] = {}
        
        self.state["users"][user_id].update(kwargs)
        self.state["users"][user_id]["last_updated"] = datetime.now().isoformat()
    
    def get_all_users(self) -> List[str]:
        """获取所有用户ID"""
        return list(self.state["users"].keys())
    
    def update_instance_state(self, instance_url: str, **kwargs):
        """更新实例状态"""
        if instance_url not in self.state["instances"]:
            self.state["instances"][instance_url] = {"weight": 10.0, "consecutive_failures": 0}
        
        self.state["instances"][instance_url].update(kwargs)

class SimplifiedPollingEngine:
    """简化的轮询引擎"""
    
    def __init__(self, nitter_instances: List[str], following_file: str):
        self.nitter_instances = nitter_instances
        self.following_file = following_file
        self.state_manager = StateManager()
        
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
        
    def load_following_list(self) -> List[Dict]:
        """加载关注用户列表"""
        try:
            with open(self.following_file, 'r', encoding='utf-8') as f:
                following_list = json.load(f)
            logger.info(f"成功加载 {len(following_list)} 个关注用户")
            return following_list
        except (json.JSONDecodeError, FileNotFoundError) as e:
            logger.error(f"无法加载关注列表: {e}")
            return []
            
    def get_best_instance(self) -> str:
        """获取最佳实例"""
        if len(self.nitter_instances) == 1:
            return self.nitter_instances[0]
        
        # 根据权重选择实例
        best_instance = self.nitter_instances[0]
        best_weight = 0
        
        for instance in self.nitter_instances:
            instance_state = self.state_manager.state["instances"].get(instance, {"weight": 10.0})
            if instance_state["weight"] > best_weight:
                best_weight = instance_state["weight"]
                best_instance = instance
                
        return best_instance
        
    async def fetch_user_tweets(self, session: aiohttp.ClientSession, user_id: str) -> bool:
        """获取用户推文"""
        for attempt in range(MAX_RETRIES):
            try:
                instance = self.get_best_instance()
                url = f"{instance}/{user_id}/rss"
                
                # 获取用户状态
                user_state = self.state_manager.get_user_state(user_id)
                headers = {}
                if user_state.get("etag"):
                    headers["If-None-Match"] = user_state["etag"]
                
                logger.debug(f"请求用户 {user_id} 的RSS: {url}")
                
                async with session.get(url, headers=headers, timeout=REQUEST_TIMEOUT) as response:
                    # 更新实例权重
                    if response.status == 200:
                        self.state_manager.update_instance_state(instance, consecutive_failures=0)
                        
                        # 保存ETag
                        if "ETag" in response.headers:
                            self.state_manager.update_user_state(user_id, etag=response.headers["ETag"])
                        
                        # 处理RSS内容
                        content = await response.text()
                        return await self.process_rss_content(user_id, content)
                        
                    elif response.status == 304:
                        # 内容未更新
                        logger.debug(f"用户 {user_id} 内容未更新")
                        return False
                        
                    elif response.status == 429:
                        # 请求过多，降低实例权重
                        logger.warning(f"实例 {instance} 请求过多 (429)")
                        current_weight = self.state_manager.state["instances"].get(instance, {}).get("weight", 10.0)
                        self.state_manager.update_instance_state(
                            instance, 
                            weight=max(1.0, current_weight * 0.5),
                            consecutive_failures=self.state_manager.state["instances"].get(instance, {}).get("consecutive_failures", 0) + 1
                        )
                        
                        if attempt < MAX_RETRIES - 1:
                            await asyncio.sleep(RETRY_DELAYS[attempt])
                            continue
                        else:
                            logger.error(f"用户 {user_id} 最终失败: HTTP 429")
                            return False
                            
                    else:
                        logger.warning(f"用户 {user_id} 请求失败: HTTP {response.status}")
                        if attempt < MAX_RETRIES - 1:
                            await asyncio.sleep(RETRY_DELAYS[attempt])
                            continue
                        else:
                            return False
                            
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.warning(f"用户 {user_id} 请求异常 (尝试 {attempt + 1}/{MAX_RETRIES}): {e}")
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(RETRY_DELAYS[attempt])
                else:
                    # 降低实例权重
                    instance = self.get_best_instance()
                    current_weight = self.state_manager.state["instances"].get(instance, {}).get("weight", 10.0)
                    self.state_manager.update_instance_state(
                        instance,
                        weight=max(1.0, current_weight * 0.8),
                        consecutive_failures=self.state_manager.state["instances"].get(instance, {}).get("consecutive_failures", 0) + 1
                    )
                    return False
        
        return False
    
    async def process_rss_content(self, user_id: str, content: str) -> bool:
        """处理RSS内容"""
        try:
            root = ET.fromstring(content)
            items = root.findall(".//item")
            
            if not items:
                return False
                
            # 获取最新推文
            latest_item = items[0]
            link = latest_item.find("link")
            title = latest_item.find("title")
            description = latest_item.find("description")
            pub_date = latest_item.find("pubDate")
            
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
            if user_state.get("last_tweet_id") == tweet_id:
                return False
                
            # 解析发布时间
            try:
                pub_time = self.parse_date(pub_date.text)
                today = datetime.now().date()
                
                # 如果是首次运行，只推送当日推文
                if not user_state.get("initialized") and pub_time.date() != today:
                    logger.info(f"用户 {user_id} 首次运行，跳过非当日推文: {pub_time.date()}")
                    self.state_manager.update_user_state(user_id, last_tweet_id=tweet_id, initialized=True)
                    return False
                    
            except Exception as e:
                logger.warning(f"解析用户 {user_id} 推文时间失败: {e}")
                pub_time = datetime.now()
            
            # 构建推文数据
            tweet_data = {
                "id": tweet_id,
                "user_id": user_id,
                "content": title.text or "",
                "html": description.text or "",
                "published_at": pub_date.text,
                "url": link.text,
                "timestamp": pub_time.isoformat()
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
                    initialized=True
                )
                
                logger.info(f"用户 {user_id} 新推文已推送: {tweet_id}")
                return True
                
            except Exception as e:
                logger.error(f"推文添加到Redis失败: {e}")
                return False
                
        except ET.ParseError as e:
            logger.error(f"解析用户 {user_id} RSS失败: {e}")
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
        """初始化用户列表"""
        following_list = self.load_following_list()
        
        for user in following_list:
            user_id = user.get("userId", "").lower()
            if user_id and user_id not in self.state_manager.get_all_users():
                self.state_manager.update_user_state(user_id, initialized=False)
        
        logger.info(f"初始化完成，共 {len(self.state_manager.get_all_users())} 个用户")
    
    async def poll_users_batch(self, user_batch: List[str]):
        """批量轮询用户"""
        async with aiohttp.ClientSession() as session:
            tasks = [self.fetch_user_tweets(session, user_id) for user_id in user_batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            success_count = sum(1 for r in results if r is True)
            if success_count > 0:
                logger.info(f"批次完成: {len(user_batch)} 用户, {success_count} 个有新推文")
    
    async def run(self):
        """运行轮询引擎"""
        logger.info("启动简化轮询引擎...")
        
        # 初始化用户
        await self.initialize_users()
        
        # 发送测试推文
        await self.send_test_tweet()
        
        while True:
            try:
                users = self.state_manager.get_all_users()
                if not users:
                    logger.warning("没有用户需要轮询")
                    await asyncio.sleep(POLL_INTERVAL)
                    continue
                
                # 分批处理用户
                for i in range(0, len(users), CONCURRENT_USERS):
                    batch = users[i:i + CONCURRENT_USERS]
                    await self.poll_users_batch(batch)
                    
                    # 批次间短暂延迟
                    if i + CONCURRENT_USERS < len(users):
                        await asyncio.sleep(1)
                
                # 保存状态
                self.state_manager.save_state()
                
                # 等待下一轮
                logger.debug(f"轮询完成，等待 {POLL_INTERVAL} 秒")
                await asyncio.sleep(POLL_INTERVAL)
                
            except Exception as e:
                logger.error(f"轮询过程出错: {e}")
                await asyncio.sleep(5)
    
    async def send_test_tweet(self):
        """发送测试推文"""
        try:
            test_tweet = {
                "id": f"test_{int(time.time())}",
                "user_id": "system",
                "content": f"系统测试推文 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                "html": "<p>系统测试推文</p>",
                "published_at": datetime.now().strftime("%a, %d %b %Y %H:%M:%S"),
                "url": "#",
                "timestamp": datetime.now().isoformat()
            }
            
            self.redis_client.xadd(TWEET_STREAM_KEY, test_tweet)
            logger.info("测试推文已发送")
            
        except Exception as e:
            logger.error(f"发送测试推文失败: {e}")

async def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="简化的推特轮询引擎")
    
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
        engine = SimplifiedPollingEngine(nitter_instances, args.following_file)
        await engine.run()
    except KeyboardInterrupt:
        logger.info("接收到中断信号，正在退出...")
    except Exception as e:
        logger.error(f"引擎运行失败: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main()) 