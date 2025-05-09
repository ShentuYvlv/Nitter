#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import feedparser
import html
import time
import argparse
import re
import json
import os
import sys
from datetime import datetime
from typing import List, Dict, Any, Set
from bs4 import BeautifulSoup

# 确保输出正确显示中文
if sys.stdout.encoding != 'utf-8':
    try:
        # Windows系统设置
        if sys.platform.startswith('win'):
            import ctypes
            kernel32 = ctypes.windll.kernel32
            kernel32.SetConsoleOutputCP(65001)
    except:
        pass


class NitterScraper:
    def __init__(self, base_url: str = "http://localhost:8080", history_file: str = "tweet_history.json"):
        """
        初始化Nitter抓取器
        
        Args:
            base_url: Nitter实例的基础URL
            history_file: 保存推文历史记录的文件
        """
        self.base_url = base_url
        self.history_file = history_file
        self.tweet_history = self._load_history()
        
    def _load_history(self) -> Dict[str, Set[str]]:
        """
        加载推文历史记录
        
        Returns:
            包含用户名和推文ID集合的字典
        """
        if os.path.exists(self.history_file):
            try:
                with open(self.history_file, 'r', encoding='utf-8') as f:
                    # 从JSON加载历史记录，将集合列表转换为集合
                    history_data = json.load(f)
                    return {user: set(tweets) for user, tweets in history_data.items()}
            except (json.JSONDecodeError, IOError) as e:
                print(f"加载历史记录时出错: {e}")
                return {}
        return {}
    
    def _save_history(self) -> None:
        """保存推文历史记录到文件"""
        try:
            # 将集合转换为列表以便JSON序列化
            history_data = {user: list(tweets) for user, tweets in self.tweet_history.items()}
            with open(self.history_file, 'w', encoding='utf-8') as f:
                json.dump(history_data, f, ensure_ascii=False, indent=2)
        except IOError as e:
            print(f"保存历史记录时出错: {e}")
        
    def get_user_tweets(self, username: str, limit: int = 10, include_seen: bool = False) -> List[Dict[str, Any]]:
        """
        获取指定用户的最新推文
        
        Args:
            username: 用户名 (不含@符号)
            limit: 要获取的最大推文数量
            include_seen: 是否包含已经看过的推文
            
        Returns:
            包含推文信息的列表
        """
        rss_url = f"{self.base_url}/{username}/rss"
        
        try:
            print(f"正在获取 {username} 的RSS: {rss_url}")
            response = requests.get(rss_url, timeout=10)
            response.raise_for_status()
            
            # 确保正确的编码
            response.encoding = 'utf-8'
            
            # 解析RSS
            feed = feedparser.parse(response.text)
            
            if not feed.entries:
                print(f"没有找到 {username} 的推文")
                return []
            
            # 确保用户在历史记录中有一个条目
            if username not in self.tweet_history:
                self.tweet_history[username] = set()
                
            tweets = []
            new_tweet_count = 0
            
            for entry in feed.entries[:limit if include_seen else 30]:  # 获取更多，以便找到新推文
                tweet_id = entry.link.split("/")[-1].split("#")[0]
                
                # 如果不包括已看过的推文，且该推文已在历史记录中，则跳过
                if not include_seen and tweet_id in self.tweet_history[username]:
                    continue
                    
                # 解析日期时间
                try:
                    published_time = datetime.strptime(
                        entry.published, "%a, %d %b %Y %H:%M:%S %Z"
                    )
                except ValueError:
                    try:
                        published_time = datetime.strptime(
                            entry.published, "%a, %d %b %Y %H:%M:%S %z"
                        )
                    except ValueError:
                        # 如果无法解析日期，使用当前时间
                        published_time = datetime.now()
                        print(f"警告: 无法解析日期 '{entry.published}'，使用当前时间")
                
                # 从title获取推文内容并进行HTML解码
                content = html.unescape(entry.title)
                
                # 如果有description字段，可以提取其中的图片链接
                media_links = []
                if hasattr(entry, 'description'):
                    media_links = self._extract_media_links(entry.description)
                
                # 提取引用的内容
                quoted_content = ""
                if hasattr(entry, 'description'):
                    quoted_content = self._extract_quoted_content(entry.description)
                
                tweet = {
                    "id": tweet_id,
                    "author": username,
                    "content": content,
                    "quoted_content": quoted_content,
                    "media_links": media_links,
                    "published": published_time,
                    "link": entry.link,
                    "is_new": tweet_id not in self.tweet_history[username]
                }
                
                tweets.append(tweet)
                
                # 添加到历史记录
                if tweet["is_new"]:
                    self.tweet_history[username].add(tweet_id)
                    new_tweet_count += 1
                
                # 如果收集了足够的新推文，则停止
                if not include_seen and new_tweet_count >= limit:
                    break
                    
            # 保存更新后的历史记录
            self._save_history()
            
            # 如果是只获取新推文模式，且收集的推文少于限制数量，则按发布时间排序
            if not include_seen:
                tweets = sorted(tweets, key=lambda t: t["published"], reverse=True)[:limit]
                
            return tweets
            
        except requests.RequestException as e:
            print(f"获取 {username} 的推文时出错: {e}")
            return []
    
    def _extract_media_links(self, html_content: str) -> List[str]:
        """
        从HTML内容中提取媒体链接
        
        Args:
            html_content: 包含HTML标签的内容
            
        Returns:
            媒体链接列表
        """
        # 移除CDATA标记
        html_content = html_content.replace("<![CDATA[", "").replace("]]>", "")
        
        # 使用BeautifulSoup解析HTML
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # 找到所有图片标签
        img_tags = soup.find_all('img')
        img_links = [img.get('src') for img in img_tags if img.get('src')]
        
        return img_links
    
    def _extract_quoted_content(self, html_content: str) -> str:
        """
        从HTML内容中提取引用的推文内容
        
        Args:
            html_content: 包含HTML标签的内容
            
        Returns:
            引用的推文内容
        """
        # 移除CDATA标记
        html_content = html_content.replace("<![CDATA[", "").replace("]]>", "")
        
        # 使用BeautifulSoup解析HTML
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # 查找所有链接，可能包含引用的推文
        links = soup.find_all('a')
        
        # 寻找以localhost开头的链接，这些通常是引用推文的链接
        quoted_links = [a.text for a in links if a.text.startswith('localhost/') and '/status/' in a.text]
        
        if quoted_links:
            return f"引用推文: {quoted_links[0]}"
        else:
            return ""
    
    def _clean_html(self, html_content: str) -> str:
        """
        清理HTML内容，去除标签但保留文本
        
        Args:
            html_content: 包含HTML标签的内容
            
        Returns:
            清理后的纯文本内容
        """
        # 移除CDATA标记
        html_content = html_content.replace("<![CDATA[", "").replace("]]>", "")
        
        # 使用BeautifulSoup解析HTML
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # 获取纯文本
        text = soup.get_text(separator='\n')
        
        # 清理多余的空行
        text = re.sub(r'\n\s*\n', '\n\n', text).strip()
        
        return text
    
    def print_tweet(self, tweet: Dict[str, Any]) -> None:
        """
        在控制台打印推文
        
        Args:
            tweet: 包含推文信息的字典
        """
        new_marker = "[新]" if tweet.get("is_new", False) else ""
        
        print("=" * 60)
        print(f"作者: @{tweet['author']} {new_marker}")
        print(f"时间: {tweet['published'].strftime('%Y-%m-%d %H:%M:%S')}")
        print("-" * 60)
        print(f"{tweet['content']}")
        
        # 如果有引用内容，打印出来
        if tweet.get('quoted_content') and len(tweet['quoted_content']) > 0:
            print(f"\n{tweet['quoted_content']}")
        
        # 如果有媒体链接，打印出来
        if tweet.get('media_links') and len(tweet['media_links']) > 0:
            print("\n[媒体文件]:")
            for link in tweet['media_links']:
                print(f"- {link}")
                
        print("-" * 60)
        print(f"链接: {tweet['link']}")
        print("=" * 60)
        print()
    
    def fetch_and_print_user_tweets(self, username: str, limit: int = 10, include_seen: bool = False) -> None:
        """
        获取并打印指定用户的最新推文
        
        Args:
            username: 用户名 (不含@符号)
            limit: 要获取的最大推文数量
            include_seen: 是否包含已经看过的推文
        """
        tweets = self.get_user_tweets(username, limit, include_seen)
        
        if not tweets:
            print(f"未找到 @{username} 的新推文")
            return
        
        new_count = sum(1 for t in tweets if t.get("is_new", False))
        
        if include_seen:
            print(f"\n获取到 @{username} 的 {len(tweets)} 条最新推文:\n")
        else:
            print(f"\n获取到 @{username} 的 {new_count} 条新推文:\n")
        
        for tweet in tweets:
            self.print_tweet(tweet)
    
    def fetch_multiple_users(self, usernames: List[str], tweets_per_user: int = 5, include_seen: bool = False) -> None:
        """
        获取并打印多个用户的最新推文
        
        Args:
            usernames: 用户名列表
            tweets_per_user: 每个用户要获取的最大推文数量
            include_seen: 是否包含已经看过的推文
        """
        for username in usernames:
            print(f"\n正在获取 @{username} 的推文...")
            self.fetch_and_print_user_tweets(username, tweets_per_user, include_seen)
            time.sleep(1)  # 添加短暂延迟，避免请求过快
    
    def monitor_users(self, usernames: List[str], tweets_per_user: int = 5, interval: int = 300) -> None:
        """
        持续监控用户的新推文
        
        Args:
            usernames: 要监控的用户名列表
            tweets_per_user: 每个用户要显示的最大推文数量
            interval: 检查间隔（秒）
        """
        print(f"开始监控以下用户的新推文，每 {interval} 秒检查一次:")
        for username in usernames:
            print(f"- @{username}")
        
        try:
            while True:
                print(f"\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - 检查新推文...")
                self.fetch_multiple_users(usernames, tweets_per_user, include_seen=False)
                
                print(f"下一次检查将在 {interval} 秒后进行。按 Ctrl+C 停止监控。")
                time.sleep(interval)
        except KeyboardInterrupt:
            print("\n已停止监控。")
    
    def export_tweets_to_file(self, filename: str, usernames: List[str], limit_per_user: int = 20) -> None:
        """
        将用户推文导出到文件
        
        Args:
            filename: 输出文件名
            usernames: 用户名列表
            limit_per_user: 每个用户要导出的最大推文数量
        """
        all_tweets = []
        
        for username in usernames:
            print(f"正在获取 @{username} 的推文用于导出...")
            tweets = self.get_user_tweets(username, limit_per_user, include_seen=True)
            all_tweets.extend(tweets)
        
        # 按发布时间排序
        all_tweets = sorted(all_tweets, key=lambda t: t["published"], reverse=True)
        
        # 创建用于导出的推文数据
        export_data = []
        for tweet in all_tweets:
            # 创建推文数据的可导出副本
            export_tweet = {
                "id": tweet["id"],
                "author": tweet["author"],
                "content": tweet["content"],
                "published": tweet["published"].strftime("%Y-%m-%d %H:%M:%S"),
                "link": tweet["link"]
            }
            
            # 添加可选字段
            if tweet.get("media_links"):
                export_tweet["media_links"] = tweet["media_links"]
            
            if tweet.get("quoted_content"):
                export_tweet["quoted_content"] = tweet["quoted_content"]
            
            export_data.append(export_tweet)
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, ensure_ascii=False, indent=2)
            print(f"已成功将 {len(export_data)} 条推文导出到 {filename}")
        except IOError as e:
            print(f"导出推文时出错: {e}")
            
    def export_tweets_to_text(self, filename: str, usernames: List[str], limit_per_user: int = 20) -> None:
        """
        将用户推文导出到纯文本文件
        
        Args:
            filename: 输出文件名
            usernames: 用户名列表
            limit_per_user: 每个用户要导出的最大推文数量
        """
        all_tweets = []
        
        for username in usernames:
            print(f"正在获取 @{username} 的推文用于导出...")
            tweets = self.get_user_tweets(username, limit_per_user, include_seen=True)
            all_tweets.extend(tweets)
        
        # 按发布时间排序
        all_tweets = sorted(all_tweets, key=lambda t: t["published"], reverse=True)
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                for tweet in all_tweets:
                    f.write(f"作者: @{tweet['author']}\n")
                    f.write(f"时间: {tweet['published'].strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write(f"内容: {tweet['content']}\n")
                    
                    if tweet.get('quoted_content'):
                        f.write(f"{tweet['quoted_content']}\n")
                    
                    if tweet.get('media_links'):
                        f.write("媒体文件:\n")
                        for link in tweet['media_links']:
                            f.write(f"- {link}\n")
                    
                    f.write(f"链接: {tweet['link']}\n")
                    f.write("-" * 60 + "\n\n")
                    
            print(f"已成功将 {len(all_tweets)} 条推文导出到 {filename}")
        except IOError as e:
            print(f"导出推文时出错: {e}")


def main():
    parser = argparse.ArgumentParser(description="从Nitter实例抓取Twitter用户的最新推文")
    parser.add_argument(
        "--base-url", 
        default="http://localhost:8080", 
        help="Nitter实例的基础URL (默认: http://localhost:8080)"
    )
    parser.add_argument(
        "--users", 
        nargs="+", 
        required=True, 
        help="要抓取的Twitter用户名列表 (不含@符号)"
    )
    parser.add_argument(
        "--limit", 
        type=int, 
        default=5, 
        help="每个用户要获取的最大推文数量 (默认: 5)"
    )
    parser.add_argument(
        "--history-file",
        default="tweet_history.json",
        help="保存推文历史记录的文件 (默认: tweet_history.json)"
    )
    parser.add_argument(
        "--include-seen",
        action="store_true",
        help="包含已经看过的推文 (默认不包含)"
    )
    parser.add_argument(
        "--monitor",
        action="store_true",
        help="持续监控新推文 (使用 Ctrl+C 停止)"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=300,
        help="监控模式下的检查间隔（秒）(默认: 300)"
    )
    parser.add_argument(
        "--export",
        help="将推文导出到指定JSON文件 (例如: tweets.json)"
    )
    parser.add_argument(
        "--export-text",
        help="将推文导出到指定文本文件 (例如: tweets.txt)"
    )
    
    args = parser.parse_args()
    
    scraper = NitterScraper(base_url=args.base_url, history_file=args.history_file)
    
    if args.export:
        scraper.export_tweets_to_file(args.export, args.users, args.limit)
    elif args.export_text:
        scraper.export_tweets_to_text(args.export_text, args.users, args.limit)
    elif args.monitor:
        scraper.monitor_users(args.users, args.limit, args.interval)
    else:
        scraper.fetch_multiple_users(args.users, args.limit, args.include_seen)


if __name__ == "__main__":
    main()
