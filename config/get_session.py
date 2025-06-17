#!/usr/bin/env python3
import requests
import json
import sys
import pyotp
import os
import traceback

# NOTE: pyotp and requests are dependencies
# > pip install pyotp requests aiohttp-socks

# 设置代理环境变量
os.environ['HTTP_PROXY'] = 'socks5://127.0.0.1:10808'
os.environ['HTTPS_PROXY'] = 'socks5://127.0.0.1:10808'
os.environ['ALL_PROXY'] = 'socks5://127.0.0.1:10808'

TW_CONSUMER_KEY = '3nVuSoBZnx6U4vzUxf5w'
TW_CONSUMER_SECRET = 'Bcs59EFbbsdF6Sl9Ng71smgStWEGwXXKSjYvPVt7qys'

def auth(username, password, otp_secret):
    """Twitter认证函数，使用代理连接"""
    # 创建代理配置
    proxies = {
        'http': 'socks5://127.0.0.1:10808',
        'https': 'socks5://127.0.0.1:10808',
    }
    
    # 设置会话和代理
    session = requests.Session()
    session.proxies = proxies
    
    print(f"已配置SOCKS5代理: {proxies['https']}")
    print(f"正在尝试认证用户: {username}")
    
    try:
        # 获取bearer token
        print("正在获取bearer token...")
        bearer_token_response = session.post(
            "https://api.twitter.com/oauth2/token",
            auth=(TW_CONSUMER_KEY, TW_CONSUMER_SECRET),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data='grant_type=client_credentials',
            timeout=30
        )
        
        if bearer_token_response.status_code != 200:
            print(f"获取bearer token失败: HTTP {bearer_token_response.status_code}")
            print(f"响应内容: {bearer_token_response.text}")
            return None
            
        bearer_token_req = bearer_token_response.json()
        bearer_token = bearer_token_req.get('access_token', '')
        
        if not bearer_token:
            print(f"bearer token为空或格式不对: {bearer_token_req}")
            bearer_token = ' '.join(str(x) for x in bearer_token_req.values())
            if not bearer_token:
                return None
        
        print(f"bearer token获取成功: {bearer_token[:15]}...")

        # 获取guest token
        print("正在获取guest token...")
        guest_token_response = session.post(
            "https://api.twitter.com/1.1/guest/activate.json",
            headers={'Authorization': f'Bearer {bearer_token}'},
            timeout=30
        )
        
        if guest_token_response.status_code != 200:
            print(f"获取guest token失败: HTTP {guest_token_response.status_code}")
            print(f"响应内容: {guest_token_response.text}")
            return None
            
        guest_token = guest_token_response.json().get('guest_token')

        if not guest_token:
            print("获取guest token失败，返回为空")
            return None
        
        print(f"guest token获取成功: {guest_token}")

        # 设置HTTP头
        twitter_header = {
            'Authorization': f'Bearer {bearer_token}',
            "Content-Type": "application/json",
            "User-Agent": "TwitterAndroid/10.21.0-release.0 (310210000-r-0) ONEPLUS+A3010/9 (OnePlus;ONEPLUS+A3010;OnePlus;OnePlus3;0;;1;2016)",
            "X-Twitter-API-Version": '5',
            "X-Twitter-Client": "TwitterAndroid",
            "X-Twitter-Client-Version": "10.21.0-release.0",
            "OS-Version": "28",
            "System-User-Agent": "Dalvik/2.1.0 (Linux; U; Android 9; ONEPLUS A3010 Build/PKQ1.181203.001)",
            "X-Twitter-Active-User": "yes",
            "X-Guest-Token": guest_token,
            "X-Twitter-Client-DeviceID": ""
        }

        session.headers.update(twitter_header)

        # 开始登录流程
        print("开始登录流程...")
        task1_response = session.post(
            'https://api.twitter.com/1.1/onboarding/task.json',
            params={
                'flow_name': 'login',
                'api_version': '1',
                'known_device_token': '',
                'sim_country_code': 'us'
            },
            json={
                "flow_token": None,
                "input_flow_data": {
                    "country_code": None,
                    "flow_context": {
                        "referrer_context": {
                            "referral_details": "utm_source=google-play&utm_medium=organic",
                            "referrer_url": ""
                        },
                        "start_location": {
                            "location": "deeplink"
                        }
                    },
                    "requested_variant": None,
                    "target_user_id": 0
                }
            }
        )
        
        if task1_response.status_code != 200:
            print(f"开始登录流程失败: HTTP {task1_response.status_code}")
            print(f"响应内容: {task1_response.text}")
            return None
            
        task1_json = task1_response.json()
        flow_token = task1_json.get('flow_token')
        
        if not flow_token:
            print(f"获取flow_token失败: {task1_json}")
            return None
            
        print(f"flow_token获取成功: {flow_token[:30]}...")
        session.headers['att'] = task1_response.headers.get('att', '')

        # 输入用户名
        print(f"输入用户名: {username}...")
        task2_response = session.post(
            'https://api.twitter.com/1.1/onboarding/task.json',
            json={
                "flow_token": flow_token,
                "subtask_inputs": [{
                    "enter_text": {
                        "suggestion_id": None,
                        "text": username,
                        "link": "next_link"
                    },
                    "subtask_id": "LoginEnterUserIdentifier"
                }]
            }
        )
        
        if task2_response.status_code != 200:
            print(f"输入用户名失败: HTTP {task2_response.status_code}")
            print(f"响应内容: {task2_response.text}")
            return None
            
        task2_json = task2_response.json()
        flow_token = task2_json.get('flow_token')
        
        if not flow_token:
            print(f"用户名输入后获取flow_token失败: {task2_json}")
            return None
            
        print(f"用户名输入成功, 新flow_token: {flow_token[:30]}...")

        # 输入密码
        print("输入密码...")
        task3_response = session.post(
            'https://api.twitter.com/1.1/onboarding/task.json',
            json={
                "flow_token": flow_token,
                "subtask_inputs": [{
                    "enter_password": {
                        "password": password,
                        "link": "next_link"
                    },
                    "subtask_id": "LoginEnterPassword"
                }],
            }
        )
        
        if task3_response.status_code != 200:
            print(f"输入密码失败: HTTP {task3_response.status_code}")
            print(f"响应内容: {task3_response.text}")
            return None
            
        task3_json = task3_response.json()
        
        # 检查是否被Twitter阻止登录
        for subtask in task3_json.get('subtasks', []):
            if subtask.get('subtask_id') == 'DenyLoginSubtask':
                # 尝试获取错误信息
                try:
                    cta = subtask.get('cta', {})
                    primary_text = cta.get('primary_text', {}).get('text', '')
                    secondary_text = cta.get('secondary_text', {}).get('text', '')
                    
                    print("登录被Twitter阻止")
                    print(f"原因: {primary_text}")
                    print(f"详情: {secondary_text}")
                    print("\n您需要等待一段时间后再尝试登录，或使用官方应用登录来解除限制。")
                    return None
                except Exception as e:
                    print(f"解析DenyLoginSubtask错误: {e}")
                    return None
        
        # 处理正常的登录流程
        for subtask in task3_json.get('subtasks', []):
            if "open_account" in subtask:
                print("登录成功!")
                return subtask["open_account"]
            elif "enter_text" in subtask and subtask.get('subtask_id') == 'LoginTwoFactorAuthChallenge':
                print("需要两步验证...")
                
                # 生成TOTP验证码
                try:
                    totp = pyotp.TOTP(otp_secret)
                    generated_code = totp.now()
                    print(f"已生成TOTP验证码: {generated_code}")
                except Exception as e:
                    print(f"生成TOTP验证码失败: {e}")
                    print("请确认您提供的2FA密钥是正确的Base32格式")
                    return None
                
                # 提交验证码
                task4_response = session.post(
                    "https://api.twitter.com/1.1/onboarding/task.json",
                    json={
                        "flow_token": task3_json.get("flow_token"),
                        "subtask_inputs": [
                            {
                                "enter_text": {
                                    "suggestion_id": None,
                                    "text": generated_code,
                                    "link": "next_link",
                                },
                                "subtask_id": "LoginTwoFactorAuthChallenge",
                            }
                        ],
                    }
                )
                
                if task4_response.status_code != 200:
                    print(f"提交验证码失败: HTTP {task4_response.status_code}")
                    print(f"响应内容: {task4_response.text}")
                    return None
                    
                task4_json = task4_response.json()
                
                # 检查验证结果
                for t4_subtask in task4_json.get("subtasks", []):
                    if "open_account" in t4_subtask:
                        print("两步验证成功!")
                        return t4_subtask["open_account"]
                
                print("两步验证失败，未找到open_account字段")
                print(f"完整响应: {json.dumps(task4_json, indent=2)}")
                return None
        
        # 如果没有找到预期的subtasks
        print(f"未找到预期的subtasks，登录流程异常")
        print(f"完整响应: {json.dumps(task3_json, indent=2)}")
        return None
        
    except requests.exceptions.ProxyError as pe:
        print(f"代理连接错误: {pe}")
        traceback.print_exc()
        return None
    except Exception as e:
        print(f"连接错误: {e}")
        traceback.print_exc()
        return None

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: python3 get_session.py <username> <password> <2fa secret> <path>")
        sys.exit(1)

    username = sys.argv[1]
    password = sys.argv[2]
    otp_secret = sys.argv[3]
    path = sys.argv[4]

    # 尝试安装必要的依赖
    try:
        # 检查PySocks是否已安装
        try:
            import socks
            print("PySocks已安装")
        except ImportError:
            import pip
            print("尝试安装PySocks...")
            pip.main(['install', 'PySocks'])
            print("PySocks安装成功")
    except Exception as e:
        print(f"PySocks安装失败: {e}")
        print("继续尝试，可能仍然无法使用代理")

    # 认证过程
    result = auth(username, password, otp_secret)
    if result is None:
        print("认证失败。")
        sys.exit(1)

    # 保存会话信息
    session_entry = {
        "oauth_token": result.get("oauth_token"),
        "oauth_token_secret": result.get("oauth_token_secret")
    }

    try:
        with open(path, "a") as f:
            f.write(json.dumps(session_entry) + "\n")
        print(f"认证成功! 会话信息已添加到 {path}")
        print(f"会话信息: {json.dumps(session_entry)}")
    except Exception as e:
        print(f"保存会话信息失败: {e}")
        sys.exit(1)

#2fa OTP BLGOLYSGQR3MRHPB        