#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import time
import os
import sys
import argparse
from datetime import datetime
from typing import List, Dict, Any
from nitterScraper import NitterScraper

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

def load_following_list(file_path: str) -> List[str]:
    """
    从JSON文件加载关注列表
    
    Args:
        file_path: 关注列表JSON文件路径
        
    Returns:
        用户名列表
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        # 基于实际的following_list.json格式提取用户ID
        if isinstance(data, list):
            # 从示例可见，userId是用户名，username可能是昵称
            usernames = []
            for user in data:
                if 'userId' in user and user['userId']:
                    usernames.append(user['userId'])
            
            print(f"从关注列表中提取了 {len(usernames)} 个用户名")
            return usernames
            
        print(f"警告：无法从 {file_path} 识别用户列表格式，请检查文件结构")
        return []
    except (json.JSONDecodeError, IOError) as e:
        print(f"加载关注列表时出错: {e}")
        return []

def monitor_following(following_file: str, 
                     base_url: str = "http://localhost:8080",
                     batch_size: int = 10, 
                     interval: int = 300,
                     tweets_per_user: int = 1) -> None:
    """
    监控关注列表中所有用户的新推文
    
    Args:
        following_file: 关注列表文件路径
        base_url: Nitter实例的基础URL
        batch_size: 每批处理的用户数量
        interval: 每次完整检查的间隔时间（秒）
        tweets_per_user: 每个用户显示的最新推文数量
    """
    # 加载关注列表
    users = load_following_list(following_file)
    
    if not users:
        print(f"没有找到关注用户，请检查 {following_file} 文件格式")
        return
        
    total_users = len(users)
    print(f"已加载 {total_users} 个关注用户")
    
    # 创建抓取器
    scraper = NitterScraper(base_url=base_url)
    
    try:
        while True:
            start_time = time.time()
            print(f"\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - 开始检查所有关注用户的新推文...")
            
            new_tweets_found = 0
            
            # 分批处理用户
            for i in range(0, total_users, batch_size):
                batch_users = users[i:i+batch_size]
                print(f"\n处理批次 {i//batch_size + 1}/{(total_users+batch_size-1)//batch_size}: {len(batch_users)} 个用户")
                
                # 处理当前批次的用户
                for username in batch_users:
                    try:
                        tweets = scraper.get_user_tweets(username, tweets_per_user, include_seen=False)
                        
                        # 如果有新推文，打印出来
                        if tweets:
                            new_tweets_found += len(tweets)
                            print(f"\n发现 @{username} 的 {len(tweets)} 条新推文:")
                            for tweet in tweets:
                                scraper.print_tweet(tweet)
                        
                        # 短暂延迟，避免频繁请求
                        time.sleep(0.5)
                    except Exception as e:
                        print(f"处理用户 @{username} 时出错: {e}")
                
                # 批次之间的延迟，避免过多请求
                if i + batch_size < total_users:
                    print(f"等待 5 秒后处理下一批次...")
                    time.sleep(5)
            
            # 计算完成一轮检查所需的时间
            elapsed_time = time.time() - start_time
            
            if new_tweets_found > 0:
                print(f"\n本轮共发现 {new_tweets_found} 条新推文")
            else:
                print("\n本轮未发现新推文")
            
            # 计算下一轮检查的等待时间
            wait_time = max(5, interval - elapsed_time)
            next_check_time = datetime.now().timestamp() + wait_time
            next_check_str = datetime.fromtimestamp(next_check_time).strftime('%Y-%m-%d %H:%M:%S')
            
            print(f"完成全部 {total_users} 个用户的检查，耗时 {elapsed_time:.1f} 秒")
            print(f"下一轮检查将在 {wait_time:.1f} 秒后（{next_check_str}）开始。按 Ctrl+C 停止监控。")
            
            time.sleep(wait_time)
            
    except KeyboardInterrupt:
        print("\n已停止监控。")

def main():
    parser = argparse.ArgumentParser(description="监控关注列表中所有用户的新推文")
    parser.add_argument(
        "--following-file", 
        default="following_list.json", 
        help="关注列表文件路径 (默认: following_list.json)"
    )
    parser.add_argument(
        "--base-url", 
        default="http://localhost:8080", 
        help="Nitter实例的基础URL (默认: http://localhost:8080)"
    )
    parser.add_argument(
        "--batch-size", 
        type=int, 
        default=10, 
        help="每批处理的用户数量 (默认: 10)"
    )
    parser.add_argument(
        "--interval", 
        type=int, 
        default=600, 
        help="每次完整检查的间隔时间（秒）(默认: 600)"
    )
    parser.add_argument(
        "--tweets-per-user", 
        type=int, 
        default=1, 
        help="每个用户显示的最新推文数量 (默认: 1)"
    )
    
    args = parser.parse_args()
    
    monitor_following(
        following_file=args.following_file,
        base_url=args.base_url,
        batch_size=args.batch_size,
        interval=args.interval,
        tweets_per_user=args.tweets_per_user
    )

if __name__ == "__main__":
    main() 