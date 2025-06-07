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
POLL_INTERVAL = 15          # 统一轮询间隔（秒）- 从30秒减少到15秒
CONCURRENT_USERS = 8        # 并发轮询用户数 - 平衡性能和稳定性
REQUEST_TIMEOUT = 5         # 请求超时时间（秒）- 从15秒减少到5秒
MAX_RETRIES = 2             # 最大重试次数 - 从3次减少到2次
RETRY_DELAYS = [0.5, 1.0]   # 重试延迟（秒）- 大幅减少延迟
RATE_LIMIT_DELAY = 2.0      # 遇到429错误时的额外延迟
BATCH_DELAY = 0.3           # 批次间基础延迟

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
        
        # 动态并发控制
        self.current_concurrent = CONCURRENT_USERS
        self.recent_errors = []  # 记录最近的错误
        self.max_concurrent = 5  # 最大并发数
        self.min_concurrent = 1  # 最小并发数
        
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
        start_time = time.time()
        
        for attempt in range(MAX_RETRIES):
            try:
                instance = self.get_best_instance()
                url = f"{instance}/{user_id}/rss"
                
                # 获取用户状态
                user_state = self.state_manager.get_user_state(user_id)
                headers = {}
                if user_state.get("etag"):
                    headers["If-None-Match"] = user_state["etag"]
                
                logger.info(f"开始请求用户 {user_id} 的RSS: {url}")
                request_start = time.time()
                
                async with session.get(url, headers=headers, timeout=REQUEST_TIMEOUT) as response:
                    request_duration = time.time() - request_start
                    logger.info(f"用户 {user_id} HTTP请求耗时: {request_duration:.2f}秒, 状态码: {response.status}")
                    
                    # 更新实例权重
                if response.status == 200:
                    self.state_manager.update_instance_state(instance, consecutive_failures=0)
                        
                        # 保存ETag
                    if "ETag" in response.headers:
                        self.state_manager.update_user_state(user_id, etag=response.headers["ETag"])
                    
                        # 处理RSS内容
                        content_start = time.time()
                        content = await response.text()
                        content_read_duration = time.time() - content_start
                        logger.info(f"用户 {user_id} 内容读取耗时: {content_read_duration:.2f}秒")
                        
                        process_start = time.time()
                        result = await self.process_rss_content(user_id, content)
                        process_duration = time.time() - process_start
                        total_duration = time.time() - start_time
                        
                        logger.info(f"用户 {user_id} RSS处理耗时: {process_duration:.2f}秒, 总耗时: {total_duration:.2f}秒")
                        return result
                        
                    elif response.status == 304:
                        # 内容未更新
                        total_duration = time.time() - start_time
                        logger.info(f"用户 {user_id} 内容未更新 (304), 总耗时: {total_duration:.2f}秒")
                        return False
                        
                    elif response.status == 429:
                        # 请求过多，降低实例权重
                        logger.warning(f"实例 {instance} 请求过多 (429) - 用户 {user_id}")
                        current_weight = self.state_manager.state["instances"].get(instance, {}).get("weight", 10.0)
                        self.state_manager.update_instance_state(
                            instance, 
                            weight=max(1.0, current_weight * 0.3),  # 更激进地降低权重
                            consecutive_failures=self.state_manager.state["instances"].get(instance, {}).get("consecutive_failures", 0) + 1
                        )
                        
                        # 遇到429错误时，增加额外延迟
                        rate_limit_delay = RATE_LIMIT_DELAY * (attempt + 1)
                        logger.warning(f"遇到速率限制，将延迟 {rate_limit_delay:.1f} 秒")
                        await asyncio.sleep(rate_limit_delay)
                        
                        if attempt < MAX_RETRIES - 1:
                            logger.info(f"用户 {user_id} 将在 {RETRY_DELAYS[attempt]} 秒后重试")
                            await asyncio.sleep(RETRY_DELAYS[attempt])
                            continue
                        else:
                            total_duration = time.time() - start_time
                            logger.error(f"用户 {user_id} 最终失败: HTTP 429, 总耗时: {total_duration:.2f}秒")
                            return False
                            
                    else:
                        logger.warning(f"用户 {user_id} 请求失败: HTTP {response.status}")
                        if attempt < MAX_RETRIES - 1:
                            logger.info(f"用户 {user_id} 将在 {RETRY_DELAYS[attempt]} 秒后重试")
                            await asyncio.sleep(RETRY_DELAYS[attempt])
                            continue
                        else:
                            total_duration = time.time() - start_time
                            logger.warning(f"用户 {user_id} 最终失败: HTTP {response.status}, 总耗时: {total_duration:.2f}秒")
                            return False
                            
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.warning(f"用户 {user_id} 请求异常 (尝试 {attempt + 1}/{MAX_RETRIES}): {e}")
                if attempt < MAX_RETRIES - 1:
                    logger.info(f"用户 {user_id} 将在 {RETRY_DELAYS[attempt]} 秒后重试")
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
                    total_duration = time.time() - start_time
                    logger.error(f"用户 {user_id} 网络异常失败, 总耗时: {total_duration:.2f}秒")
                    return False
        
        return False
    
    async def process_rss_content(self, user_id: str, content: str) -> bool:
        """处理RSS内容"""
        try:
            root = ET.fromstring(content)
            items = root.findall(".//item")
            
            if not items:
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
                    images.append(enclosure.get("url", ""))
            
            # 方法2: 从description的HTML内容中提取图片
            if description is not None and description.text:
                # 匹配img标签中的src属性
                img_pattern = r'<img[^>]+src=["\']([^"\']+)["\'][^>]*>'
                img_matches = re.findall(img_pattern, description.text, re.IGNORECASE)
                images.extend(img_matches)
                
                # 匹配其他可能的图片URL模式
                url_pattern = r'https?://[^\s<>"]+\.(?:jpg|jpeg|png|gif|webp)(?:\?[^\s<>"]*)?'
                url_matches = re.findall(url_pattern, description.text, re.IGNORECASE)
                images.extend(url_matches)
            
            # 去重并过滤有效的图片URL，同时修复端口号问题
            unique_images = []
            for img_url in images:
                if img_url and img_url not in unique_images:
                    # 过滤掉一些无效的URL
                    if not img_url.startswith('data:') and len(img_url) > 10:
                        # 修复端口号问题：如果URL是localhost但没有端口，添加8080端口
                        if img_url.startswith('http://localhost/') and ':8080' not in img_url:
                            img_url = img_url.replace('http://localhost/', 'http://localhost:8080/')
                            logger.debug(f"修复图片URL端口: {img_url}")
                        unique_images.append(img_url)
            
            if unique_images:
                logger.debug(f"用户 {user_id} 推文包含 {len(unique_images)} 张图片: {unique_images[:2]}...")  # 只显示前2个URL
            
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
                        initialized=True,
                        username=username  # 保存用户名到状态
                    )
                    
                    logger.info(f"用户 {username}(@{user_id}) 新推文已推送: {tweet_id}")
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
        
        if avg_error_rate > 0.3:  # 错误率超过30%，减少并发
            self.current_concurrent = max(self.min_concurrent, self.current_concurrent - 1)
            logger.info(f"错误率过高 ({avg_error_rate:.1%})，降低并发数: {old_concurrent} -> {self.current_concurrent}")
        elif avg_error_rate < 0.1 and success_rate > 0.8:  # 错误率低于10%且成功率高，增加并发
            self.current_concurrent = min(self.max_concurrent, self.current_concurrent + 1)
            logger.info(f"性能良好 (错误率: {avg_error_rate:.1%})，提高并发数: {old_concurrent} -> {self.current_concurrent}")
        
    async def poll_users_batch(self, user_batch: List[str]):
        """批量轮询用户"""
        batch_start = time.time()
        logger.info(f"开始处理批次: {len(user_batch)} 个用户 - {user_batch}")
        
        async with aiohttp.ClientSession() as session:
            tasks = [self.fetch_user_tweets(session, user_id) for user_id in user_batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            success_count = sum(1 for r in results if r is True)
            error_count = sum(1 for r in results if isinstance(r, Exception))
            batch_duration = time.time() - batch_start
            
            logger.info(f"批次完成: {len(user_batch)} 用户, {success_count} 个有新推文, {error_count} 个异常, 耗时: {batch_duration:.2f}秒")
            
            # 动态调整并发数
            self.adjust_concurrency(success_count, len(user_batch), error_count)
    
    async def run(self):
        """运行轮询引擎"""
        logger.info("启动简化轮询引擎...")
        
        # 初始化用户
        await self.initialize_users()
        
        # 发送测试推文
        await self.send_test_tweet()
        
        cycle_count = 0
        while True:
            try:
                cycle_count += 1
                cycle_start = time.time()
                logger.info(f"开始第 {cycle_count} 轮轮询...")
                
                users = self.state_manager.get_all_users()
                if not users:
                    logger.warning("没有用户需要轮询")
                    await asyncio.sleep(POLL_INTERVAL)
                    continue
                
                logger.info(f"本轮将处理 {len(users)} 个用户，并发数: {self.current_concurrent}")
                
                # 分批处理用户
                batch_count = 0
                for i in range(0, len(users), self.current_concurrent):
                    batch_count += 1
                    batch = users[i:i + self.current_concurrent]
                    logger.info(f"处理第 {batch_count} 批用户...")
                    await self.poll_users_batch(batch)
                    
                    # 批次间短暂延迟
                    if i + self.current_concurrent < len(users):
                        await asyncio.sleep(BATCH_DELAY)  # 减少批次间延迟从1秒到0.3秒
                
                # 保存状态
                save_start = time.time()
                self.state_manager.save_state()
                save_duration = time.time() - save_start
                
                cycle_duration = time.time() - cycle_start
                logger.info(f"第 {cycle_count} 轮轮询完成! 总耗时: {cycle_duration:.2f}秒, 状态保存耗时: {save_duration:.2f}秒")
                
                # 等待下一轮
                logger.info(f"等待 {POLL_INTERVAL} 秒后开始下一轮...")
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