#!/usr/bin/env python3
"""
测试UI功能
1. 测试自动滚动功能是否已禁用
2. 发送包含图片的测试推文
"""

import redis
import json
import time
from datetime import datetime

def send_test_tweets_with_images():
    """发送包含图片的测试推文"""
    print("=== 发送测试推文（包含图片）===")
    
    try:
        redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
        redis_client.ping()
        
        # 测试推文数据
        test_tweets = [
            {
                "id": f"ui_test_single_{int(time.time())}",
                "user_id": "ui_test",
                "username": "UI测试用户",
                "content": "测试单张图片点击放大功能",
                "html": "<p>测试单张图片点击放大功能</p>",
                "published_at": time.strftime("%a, %d %b %Y %H:%M:%S"),
                "url": "#",
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "images": json.dumps([
                    "https://pbs.twimg.com/media/test_single_image.jpg"
                ])
            },
            {
                "id": f"ui_test_multi_{int(time.time())}",
                "user_id": "ui_test",
                "username": "UI测试用户",
                "content": "测试多张图片点击放大功能",
                "html": "<p>测试多张图片点击放大功能</p>",
                "published_at": time.strftime("%a, %d %b %Y %H:%M:%S"),
                "url": "#",
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "images": json.dumps([
                    "https://pbs.twimg.com/media/test_multi_1.jpg",
                    "https://pbs.twimg.com/media/test_multi_2.jpg",
                    "https://pbs.twimg.com/media/test_multi_3.jpg"
                ])
            },
            {
                "id": f"ui_test_nopic_{int(time.time())}",
                "user_id": "ui_test",
                "username": "UI测试用户",
                "content": "测试无图片推文（验证自动滚动已禁用）",
                "html": "<p>测试无图片推文（验证自动滚动已禁用）</p>",
                "published_at": time.strftime("%a, %d %b %Y %H:%M:%S"),
                "url": "#",
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "images": json.dumps([])
            }
        ]
        
        print("发送测试推文...")
        for i, tweet in enumerate(test_tweets, 1):
            stream_id = redis_client.xadd("tweets", tweet)
            print(f"✅ 测试推文 {i} 发送成功: {stream_id}")
            print(f"   内容: {tweet['content']}")
            
            images = json.loads(tweet['images'])
            if images:
                print(f"   图片数量: {len(images)}")
                for j, img in enumerate(images, 1):
                    print(f"     图片{j}: {img}")
            else:
                print("   无图片")
            
            time.sleep(1)  # 间隔1秒发送
        
        print("\n📋 测试说明:")
        print("1. 自动滚动测试:")
        print("   - 新推文出现时，页面不应该自动滚动到顶部")
        print("   - 可以手动勾选'自动滚动'复选框来启用")
        
        print("\n2. 图片放大测试:")
        print("   - 点击任意图片应该打开放大模态框")
        print("   - 可以通过以下方式关闭模态框:")
        print("     * 点击右上角的 × 按钮")
        print("     * 点击图片外的黑色背景")
        print("     * 按 ESC 键")
        print("   - 图片应该居中显示，最大90%屏幕尺寸")
        
        print("\n3. 图片悬停效果:")
        print("   - 鼠标悬停在图片上时应该有轻微放大效果")
        print("   - 鼠标指针应该变为手型（pointer）")
        
        return True
        
    except Exception as e:
        print(f"❌ 发送测试推文失败: {e}")
        return False

def check_ui_features():
    """检查UI功能状态"""
    print("=== UI功能状态检查 ===")
    
    print("✅ 已实现的功能:")
    print("1. 自动滚动默认禁用")
    print("   - 'auto-scroll' 复选框默认未选中")
    print("   - 新推文不会强制页面滚动到顶部")
    
    print("\n2. 图片点击放大功能")
    print("   - 添加了图片模态框HTML结构")
    print("   - 添加了相关CSS样式")
    print("   - 图片添加了点击事件处理")
    print("   - 支持多种关闭方式")
    
    print("\n📋 使用说明:")
    print("1. 重启 sse_server.py 以加载新模板")
    print("2. 刷新浏览器页面")
    print("3. 发送测试推文验证功能")
    
    print("\n🎯 预期效果:")
    print("- 新推文出现时页面保持当前滚动位置")
    print("- 点击图片可以放大查看")
    print("- 图片放大时背景变黑，图片居中显示")
    print("- 可以通过多种方式关闭放大图片")

def interactive_test():
    """交互式测试"""
    print("=== 交互式UI测试 ===")
    
    print("请按照以下步骤测试:")
    print("1. 确保 sse_server.py 正在运行")
    print("2. 在浏览器中打开 http://localhost:5000")
    print("3. 观察'自动滚动'复选框是否未选中")
    print("4. 运行此脚本发送测试推文")
    print("5. 验证以下功能:")
    
    features = [
        "新推文出现时页面不自动滚动到顶部",
        "点击图片可以放大显示",
        "放大的图片居中显示，背景为黑色",
        "可以点击×按钮关闭放大图片",
        "可以点击背景关闭放大图片", 
        "可以按ESC键关闭放大图片",
        "图片悬停时有轻微放大效果"
    ]
    
    print("\n测试清单:")
    for i, feature in enumerate(features, 1):
        print(f"  {i}. {feature}")
    
    print(f"\n是否发送测试推文? (y/n): ", end="")
    choice = input().strip().lower()
    
    if choice in ['y', 'yes']:
        return send_test_tweets_with_images()
    else:
        print("跳过发送测试推文")
        return True

def main():
    """主函数"""
    print("UI功能测试工具")
    print("="*50)
    
    print("选择测试模式:")
    print("1. 检查功能状态")
    print("2. 发送测试推文")
    print("3. 交互式测试")
    print("4. 退出")
    
    try:
        choice = input("\n请选择 (1-4): ").strip()
        
        if choice == "1":
            check_ui_features()
        elif choice == "2":
            send_test_tweets_with_images()
        elif choice == "3":
            interactive_test()
        elif choice == "4":
            print("退出")
        else:
            print("无效选择")
            
    except KeyboardInterrupt:
        print("\n程序退出")
    except Exception as e:
        print(f"程序出错: {e}")

if __name__ == "__main__":
    main()
