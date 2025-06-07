import os
import json
import time
import asyncio
import logging
import argparse
import redis.asyncio as redis
from typing import List, Dict, Set, Optional, AsyncIterator
import sys
from datetime import datetime
import uuid

# 在Windows上设置正确的事件循环策略
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from fastapi import FastAPI, Request, HTTPException, Query, Depends, Body
from fastapi.responses import StreamingResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware

# 配置日志
logging.basicConfig(
    level=logging.DEBUG,  # 将日志级别改为DEBUG
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("sse_server")

# Redis配置
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_DB = int(os.environ.get("REDIS_DB", 0))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD", None)

# 推文流配置
TWEET_STREAM_KEY = "tweets"  # 与轮询引擎保持一致
LAST_STREAM_ID_KEY = "last_stream_id"
DEFAULT_FIRST_STREAM_ID = "$"  # 从最新消息开始

# 是否读取历史数据
READ_HISTORY = False  # 默认不读取历史数据

# 用户配置
PRIORITY_USERS_KEY = "priority_users"
NORMAL_USERS_KEY = "normal_users"
USER_INFO_KEY_PREFIX = "user_info:"  # 用于存储用户信息的键前缀

# 全局变量
GLOBAL_USERS = 0  # 初始化为0
USER_INFO_CACHE = {}  # 用于缓存用户信息

# 轮询配置
POLLING_CONFIG_KEY = "polling_config"  # Redis中存储轮询配置的键名
DEFAULT_POLLING_CONFIG = {
    "PRIORITY_POLL_INTERVAL": 10,  # 优先用户组轮询间隔（秒）
    "NORMAL_POLL_INTERVAL": 60,    # 普通用户组轮询间隔（秒）
    "BATCH_SIZE": 15,              # 每批处理的用户数量
    "REQUEST_TIMEOUT": 10,         # 请求超时时间（秒）
    "MAX_RETRIES": 5,              # 最大重试次数
    "REQUEST_RATE": 10,            # 每秒请求数
    "BURST_CAPACITY": 20           # 突发请求容量
}

# 创建FastAPI应用
app = FastAPI(title="推特实时推送服务")

# 添加CORS中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 创建HTML模板引擎
templates = Jinja2Templates(directory="templates")

# 创建静态文件服务
os.makedirs("static", exist_ok=True)
app.mount("/static", StaticFiles(directory="static"), name="static")

# 创建模板目录
os.makedirs("templates", exist_ok=True)

# 创建Redis连接池
async def get_redis():
    """获取Redis客户端"""
    try:
        client = redis.Redis(
            host=REDIS_HOST, 
            port=REDIS_PORT, 
            db=REDIS_DB,
            password=REDIS_PASSWORD,
            decode_responses=True
        )
        # 测试连接
        await client.ping()
        logger.info(f"成功连接到Redis服务器: {REDIS_HOST}:{REDIS_PORT}")
        yield client
    except Exception as e:
        logger.error(f"Redis连接错误: {str(e)}")
        # 重试连接
        for i in range(3):
            try:
                logger.info(f"尝试重新连接Redis (尝试 {i+1}/3)...")
                client = redis.Redis(
                    host=REDIS_HOST, 
                    port=REDIS_PORT, 
                    db=REDIS_DB,
                    password=REDIS_PASSWORD,
                    decode_responses=True
                )
                await client.ping()
                logger.info("重新连接成功")
                yield client
                return
            except Exception as e:
                logger.error(f"重新连接失败: {str(e)}")
                await asyncio.sleep(1)
        
        # 如果所有重试都失败了
        logger.critical("无法连接到Redis服务器，请检查Redis是否正在运行")
        raise
    finally:
        try:
            await client.close()
        except:
            pass

# 定义HTML模板
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>推特实时推送</title>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            background-color: #f7f9fa;
            color: #14171a;
            display: flex;
            flex-direction: column;
        }
        
        .header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
            border-bottom: 1px solid #e1e8ed;
            padding-bottom: 10px;
            width: 100%;
        }
        
        .header h1 {
            margin: 0;
            font-size: 24px;
            color: #1da1f2;
        }
        
        .status {
            font-size: 14px;
            color: #657786;
        }
        
        .status span {
            font-weight: bold;
        }
        
        .main-container {
            display: flex;
            width: 100%;
            gap: 20px;
        }
        
        .left-panel {
            flex: 7;
        }
        
        .right-panel {
            flex: 3;
            background-color: white;
            border-radius: 12px;
            padding: 16px;
            border: 1px solid #e1e8ed;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
            height: fit-content;
        }
        
        .config-panel {
            margin-bottom: 20px;
        }
        
        .config-panel h3 {
            margin-top: 0;
            color: #1da1f2;
            border-bottom: 1px solid #e1e8ed;
            padding-bottom: 8px;
        }
        
        .config-item {
            margin-bottom: 12px;
        }
        
        .config-item label {
            display: block;
            margin-bottom: 4px;
            font-size: 14px;
            color: #657786;
        }
        
        .config-item input {
            width: 100%;
            padding: 8px;
            border-radius: 8px;
            border: 1px solid #e1e8ed;
            font-size: 14px;
        }
        
        .config-item .description {
            font-size: 12px;
            color: #8899a6;
            margin-top: 4px;
        }
        
        .config-buttons {
            display: flex;
            justify-content: space-between;
            margin-top: 16px;
        }
        
        .tweet-container {
            margin-top: 20px;
        }
        
        .tweet {
            border: 1px solid #e1e8ed;
            border-radius: 12px;
            padding: 16px;
            margin-bottom: 16px;
            background-color: white;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
            transition: all 0.3s ease;
        }
        
        .tweet.new {
            animation: highlight 2s ease;
        }
        
        @keyframes highlight {
            0% { background-color: #f8f8b0; }
            100% { background-color: white; }
        }
        
        .tweet-header {
            display: flex;
            align-items: center;
            margin-bottom: 10px;
        }
        
        .user-info {
            margin-left: 10px;
        }
        
        .user-name {
            font-weight: bold;
            font-size: 16px;
            color: #14171a;
            margin: 0;
            text-decoration: none;
        }
        
        .user-id {
            color: #657786;
            font-size: 14px;
            margin: 0;
        }
        
        .tweet-content {
            font-size: 16px;
            line-height: 1.5;
            margin-bottom: 10px;
            word-wrap: break-word;
        }
        
        .tweet-images {
            margin: 12px 0;
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
        }
        
        .tweet-image {
            max-width: 100%;
            max-height: 400px;
            border-radius: 12px;
            object-fit: cover;
            cursor: pointer;
            transition: transform 0.2s ease;
            border: 1px solid #e1e8ed;
        }
        
        .tweet-image:hover {
            transform: scale(1.02);
        }
        
        /* 多图片布局 */
        .tweet-images:has(.tweet-image:nth-child(2)) .tweet-image {
            flex: 1;
            min-width: 0;
            max-width: calc(50% - 4px);
        }
        
        .tweet-images:has(.tweet-image:nth-child(3)) .tweet-image {
            max-width: calc(33.333% - 6px);
        }
        
        .tweet-images:has(.tweet-image:nth-child(4)) .tweet-image {
            max-width: calc(25% - 6px);
        }
        
        .tweet-footer {
            font-size: 14px;
            color: #657786;
            display: flex;
            justify-content: space-between;
        }
        
        .tweet-time {
            text-decoration: none;
            color: #657786;
        }
        
        .user-filter {
            margin-bottom: 20px;
        }
        
        #user-filter {
            padding: 8px;
            border-radius: 20px;
            border: 1px solid #e1e8ed;
            width: 100%;
            max-width: 300px;
            font-size: 14px;
        }
        
        .loading {
            text-align: center;
            padding: 20px;
            color: #657786;
        }
        
        .no-tweets {
            text-align: center;
            padding: 40px 0;
            color: #657786;
            font-size: 18px;
        }
        
        .controls {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
        }
        
        button {
            background-color: #1da1f2;
            color: white;
            border: none;
            border-radius: 20px;
            padding: 8px 16px;
            font-size: 14px;
            font-weight: bold;
            cursor: pointer;
            transition: background-color 0.3s;
        }
        
        button:hover {
            background-color: #1991db;
        }
        
        button:disabled {
            background-color: #9ad0f5;
            cursor: not-allowed;
        }
        
        button.secondary {
            background-color: #f5f8fa;
            color: #1da1f2;
            border: 1px solid #1da1f2;
        }
        
        button.secondary:hover {
            background-color: #e8f5fd;
        }

        .user-list {
            max-height: 200px;
            overflow-y: auto;
            border: 1px solid #e1e8ed;
            border-radius: 12px;
            margin-top: 10px;
            background-color: white;
        }
        
        .user-option {
            padding: 8px 16px;
            cursor: pointer;
            border-bottom: 1px solid #e1e8ed;
        }
        
        .user-option:hover {
            background-color: #f5f8fa;
        }
        
        .selected-users {
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
            margin-top: 10px;
        }
        
        .selected-user {
            background-color: #e8f5fd;
            border-radius: 16px;
            padding: 4px 12px;
            font-size: 14px;
            display: flex;
            align-items: center;
        }
        
        .remove-user {
            margin-left: 8px;
            cursor: pointer;
            color: #657786;
        }
        
        .remove-user:hover {
            color: #e0245e;
        }
        
        .debug-info {
            background-color: #f5f8fa;
            border: 1px solid #e1e8ed;
            border-radius: 12px;
            padding: 10px;
            margin-top: 20px;
            font-family: monospace;
            font-size: 12px;
            max-height: 200px;
            overflow-y: auto;
        }
        
        .connection-badge {
            display: inline-block;
            width: 10px;
            height: 10px;
            border-radius: 50%;
            margin-right: 5px;
        }
        
        .connection-badge.connected {
            background-color: #17bf63;
        }
        
        .connection-badge.disconnected {
            background-color: #e0245e;
        }
        
        .success-message {
            color: #17bf63;
            font-size: 14px;
            margin-top: 8px;
            display: none;
        }
        
        .error-message {
            color: #e0245e;
            font-size: 14px;
            margin-top: 8px;
            display: none;
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>推特实时推送</h1>
        <div class="status">
            状态: <span class="connection-badge" id="connection-badge"></span>
            <span id="connection-status">初始化...</span>
            <span id="user-count"></span>
        </div>
    </div>
    
    <div class="main-container">
        <div class="left-panel">
            <div class="controls">
                <div>
                    <button id="toggle-connection">暂停推送</button>
                    <button id="clear-tweets">清空推文</button>
                </div>
                <div>
                    <label for="auto-scroll">
                        <input type="checkbox" id="auto-scroll" checked> 自动滚动
                    </label>
                </div>
            </div>
            
            <div class="user-filter">
                <input type="text" id="user-filter" placeholder="搜索或添加用户...">
                <div class="user-list" id="user-list" style="display: none;"></div>
                <div class="selected-users" id="selected-users"></div>
            </div>
            
            <div class="tweet-container" id="tweet-container">
                <div class="loading">加载推文中...</div>
            </div>
        </div>
        
        <div class="right-panel">
            <div class="config-panel">
                <h3>轮询配置</h3>
                <div class="config-item">
                    <label for="PRIORITY_POLL_INTERVAL">优先用户组轮询间隔（秒）</label>
                    <input type="number" id="PRIORITY_POLL_INTERVAL" min="1" step="1">
                    <div class="description">优先用户组的轮询间隔时间，值越小更新越快</div>
                </div>
                <div class="config-item">
                    <label for="NORMAL_POLL_INTERVAL">普通用户组轮询间隔（秒）</label>
                    <input type="number" id="NORMAL_POLL_INTERVAL" min="1" step="1">
                    <div class="description">普通用户组的轮询间隔时间</div>
                </div>
                <div class="config-item">
                    <label for="BATCH_SIZE">批处理大小</label>
                    <input type="number" id="BATCH_SIZE" min="1" step="1">
                    <div class="description">每批处理的用户数量，影响并发量</div>
                </div>
                <div class="config-item">
                    <label for="REQUEST_TIMEOUT">请求超时时间（秒）</label>
                    <input type="number" id="REQUEST_TIMEOUT" min="1" step="1">
                    <div class="description">单个请求的超时时间</div>
                </div>
                <div class="config-item">
                    <label for="MAX_RETRIES">最大重试次数</label>
                    <input type="number" id="MAX_RETRIES" min="1" step="1">
                    <div class="description">请求失败后的最大重试次数</div>
                </div>
                <div class="config-item">
                    <label for="REQUEST_RATE">每秒请求数</label>
                    <input type="number" id="REQUEST_RATE" min="1" step="0.5">
                    <div class="description">每秒最大请求数量，影响并发量</div>
                </div>
                <div class="config-item">
                    <label for="BURST_CAPACITY">突发请求容量</label>
                    <input type="number" id="BURST_CAPACITY" min="1" step="1">
                    <div class="description">令牌桶的容量，允许短时间内的突发请求数</div>
                </div>
                <div class="config-buttons">
                    <button id="save-config">保存配置</button>
                    <button id="reset-config" class="secondary">重置默认</button>
                </div>
                <div id="config-success" class="success-message">配置已成功保存！</div>
                <div id="config-error" class="error-message">保存配置失败，请重试。</div>
            </div>
        </div>
    </div>
    
    <div class="debug-info" id="debug-info" style="display: none;"></div>
    
    <script>
        // 全局变量
        let eventSource = null;
        let allUsers = [];
        let selectedUsers = [];
        let paused = false;
        let tweets = [];
        const MAX_TWEETS = 100;
        let debugMode = false;
        let pollingConfig = {};
        
        // 获取DOM元素
        const tweetContainer = document.getElementById('tweet-container');
        const connectionStatus = document.getElementById('connection-status');
        const connectionBadge = document.getElementById('connection-badge');
        const userCount = document.getElementById('user-count');
        const toggleButton = document.getElementById('toggle-connection');
        const clearButton = document.getElementById('clear-tweets');
        const autoScrollCheckbox = document.getElementById('auto-scroll');
        const userFilter = document.getElementById('user-filter');
        const userList = document.getElementById('user-list');
        const selectedUsersContainer = document.getElementById('selected-users');
        const debugInfo = document.getElementById('debug-info');
        
        // 配置面板元素
        const saveConfigButton = document.getElementById('save-config');
        const resetConfigButton = document.getElementById('reset-config');
        const configSuccess = document.getElementById('config-success');
        const configError = document.getElementById('config-error');
        
        // 配置输入字段
        const configInputs = {
            PRIORITY_POLL_INTERVAL: document.getElementById('PRIORITY_POLL_INTERVAL'),
            NORMAL_POLL_INTERVAL: document.getElementById('NORMAL_POLL_INTERVAL'),
            BATCH_SIZE: document.getElementById('BATCH_SIZE'),
            REQUEST_TIMEOUT: document.getElementById('REQUEST_TIMEOUT'),
            MAX_RETRIES: document.getElementById('MAX_RETRIES'),
            REQUEST_RATE: document.getElementById('REQUEST_RATE'),
            BURST_CAPACITY: document.getElementById('BURST_CAPACITY')
        };
        
        // 加载轮询配置
        async function loadPollingConfig() {
            try {
                const response = await fetch('/api/polling-config');
                if (response.ok) {
                    pollingConfig = await response.json();
                    logDebug(`已加载轮询配置: ${JSON.stringify(pollingConfig)}`);
                    
                    // 更新配置输入字段
                    updateConfigInputs();
                } else {
                    const error = await response.json();
                    logDebug(`加载轮询配置失败: ${error.error}`);
                }
            } catch (error) {
                console.error('Failed to load polling config:', error);
                logDebug(`加载轮询配置出错: ${error.message}`);
            }
        }
        
        // 更新配置输入字段
        function updateConfigInputs() {
            for (const [key, input] of Object.entries(configInputs)) {
                if (pollingConfig[key] !== undefined) {
                    input.value = pollingConfig[key];
                }
            }
        }
        
        // 保存轮询配置
        async function savePollingConfig() {
            try {
                // 收集配置值
                const newConfig = {};
                for (const [key, input] of Object.entries(configInputs)) {
                    if (input.value) {
                        // 转换为数字
                        const value = key === 'REQUEST_RATE' ? 
                            parseFloat(input.value) : 
                            parseInt(input.value, 10);
                        
                        if (!isNaN(value) && value > 0) {
                            newConfig[key] = value;
                        }
                    }
                }
                
                // 发送到服务器
                const response = await fetch('/api/polling-config', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify(newConfig)
                });
                
                if (response.ok) {
                    pollingConfig = await response.json();
                    logDebug(`已保存轮询配置: ${JSON.stringify(pollingConfig)}`);
                    
                    // 显示成功消息
                    configSuccess.style.display = 'block';
                    configError.style.display = 'none';
                    
                    // 3秒后隐藏成功消息
                    setTimeout(() => {
                        configSuccess.style.display = 'none';
                    }, 3000);
                } else {
                    const error = await response.json();
                    logDebug(`保存轮询配置失败: ${error.error}`);
                    
                    // 显示错误消息
                    configError.textContent = `保存失败: ${error.error}`;
                    configError.style.display = 'block';
                    configSuccess.style.display = 'none';
                }
            } catch (error) {
                console.error('Failed to save polling config:', error);
                logDebug(`保存轮询配置出错: ${error.message}`);
                
                // 显示错误消息
                configError.textContent = `保存失败: ${error.message}`;
                configError.style.display = 'block';
                configSuccess.style.display = 'none';
            }
        }
        
        // 重置轮询配置
        function resetPollingConfig() {
            // 重置为默认值
            configInputs.PRIORITY_POLL_INTERVAL.value = 10;
            configInputs.NORMAL_POLL_INTERVAL.value = 60;
            configInputs.BATCH_SIZE.value = 15;
            configInputs.REQUEST_TIMEOUT.value = 10;
            configInputs.MAX_RETRIES.value = 5;
            configInputs.REQUEST_RATE.value = 10;
            configInputs.BURST_CAPACITY.value = 20;
            
            // 隐藏消息
            configSuccess.style.display = 'none';
            configError.style.display = 'none';
        }
        
        // 添加配置面板事件监听器
        saveConfigButton.addEventListener('click', savePollingConfig);
        resetConfigButton.addEventListener('click', resetPollingConfig);
        
        // 调试函数
        function logDebug(message) {
            if (debugMode) {
                const timestamp = new Date().toLocaleTimeString();
                const entry = document.createElement('div');
                entry.textContent = `[${timestamp}] ${message}`;
                debugInfo.appendChild(entry);
                
                // 保持最新消息可见
                debugInfo.scrollTop = debugInfo.scrollHeight;
                
                // 限制调试信息数量
                if (debugInfo.childNodes.length > 100) {
                    debugInfo.removeChild(debugInfo.firstChild);
                }
            }
        }
        
        // 切换调试模式（按Ctrl+D开启）
        document.addEventListener('keydown', function(e) {
            if (e.ctrlKey && e.key === 'd') {
                e.preventDefault();
                debugMode = !debugMode;
                debugInfo.style.display = debugMode ? 'block' : 'none';
                logDebug("调试模式 " + (debugMode ? "开启" : "关闭"));
            }
        });
        
        // 加载用户列表
        async function loadUsers() {
            try {
                const response = await fetch('/api/users');
                if (response.ok) {
                    allUsers = await response.json();
                    console.log('Loaded users:', allUsers.length);
                    logDebug(`已加载 ${allUsers.length} 个用户`);
                    
                    // 更新用户数量显示
                    userCount.textContent = `(${allUsers.length} 个用户)`;
                }
            } catch (error) {
                console.error('Failed to load users:', error);
                logDebug(`加载用户列表失败: ${error.message}`);
            }
        }
        
        // 处理用户筛选
        userFilter.addEventListener('input', () => {
            const filter = userFilter.value.toLowerCase();
            if (filter.length < 1) {
                userList.style.display = 'none';
                return;
            }
            
            const filtered = allUsers
                .filter(user => {
                    const username = user.username ? user.username.toLowerCase() : '';
                    const userId = user.user_id ? user.user_id.toLowerCase() : '';
                    return username.includes(filter) || userId.includes(filter);
                })
                .slice(0, 10);
            
            userList.innerHTML = '';
            
            if (filtered.length === 0) {
                userList.innerHTML = '<div class="user-option">没有匹配的用户</div>';
            } else {
                filtered.forEach(user => {
                    const option = document.createElement('div');
                    option.className = 'user-option';
                    option.textContent = `${user.username || user.user_id} (@${user.user_id})`;
                    option.addEventListener('click', () => {
                        if (!selectedUsers.includes(user.user_id)) {
                            selectedUsers.push(user.user_id);
                            updateSelectedUsers();
                            updateEventSource();
                        }
                        userFilter.value = '';
                        userList.style.display = 'none';
                    });
                    userList.appendChild(option);
                });
            }
            
            userList.style.display = 'block';
        });
        
        // 点击外部关闭用户列表
        document.addEventListener('click', (event) => {
            if (!userFilter.contains(event.target) && !userList.contains(event.target)) {
                userList.style.display = 'none';
            }
        });
        
        // 更新已选择的用户
        function updateSelectedUsers() {
            selectedUsersContainer.innerHTML = '';
            
            selectedUsers.forEach(userId => {
                const user = allUsers.find(u => u.user_id === userId) || {
                    username: userId,
                    user_id: userId
                };
                
                const element = document.createElement('div');
                element.className = 'selected-user';
                element.innerHTML = `
                    ${user.username || user.user_id} 
                    <span class="remove-user" data-id="${userId}">×</span>
                `;
                selectedUsersContainer.appendChild(element);
            });
            
            // 添加移除用户的事件
            document.querySelectorAll('.remove-user').forEach(btn => {
                btn.addEventListener('click', () => {
                    const userId = btn.getAttribute('data-id');
                    selectedUsers = selectedUsers.filter(id => id !== userId);
                    updateSelectedUsers();
                    updateEventSource();
                });
            });
        }
        
        // 连接到SSE流
        function connectToStream() {
            if (eventSource) {
                eventSource.close();
            }
            
            // 更新连接状态
            updateConnectionStatus('连接中...', 'connecting');
            
            let url = '/api/stream';
            if (selectedUsers.length > 0) {
                url += `?users=${selectedUsers.join(',')}`;
            }
            
            logDebug(`连接到SSE流: ${url}`);
            eventSource = new EventSource(url);
            
            eventSource.onopen = () => {
                updateConnectionStatus('已连接', 'connected');
                toggleButton.textContent = '暂停推送';
                paused = false;
                logDebug('SSE连接已打开');
            };
            
            eventSource.onerror = () => {
                updateConnectionStatus('连接错误', 'error');
                logDebug('SSE连接错误，尝试重新连接');
                // 尝试重新连接
                setTimeout(connectToStream, 5000);
            };
            
            eventSource.addEventListener('message', function(event) {
                try {
                    const data = JSON.parse(event.data);
                    
                    // 记录调试信息
                    logDebug(`接收到消息: ${event.data.substring(0, 100)}...`);
                    
                    if (data.type === 'heartbeat') {
                        // 心跳消息，更新状态
                        updateConnectionStatus('已连接', 'connected');
                        return;
                    } else if (data.type === 'connected') {
                        // 连接确认消息
                        updateConnectionStatus('已连接', 'connected');
                        logDebug(`连接确认: ${data.message}`);
                        return;
                    } else if (data.type === 'error') {
                        // 错误消息
                        logDebug(`SSE错误: ${data.message}`);
                        return;
                    } else if (data.type === 'closed') {
                        // 关闭消息
                        logDebug(`SSE关闭: ${data.message}`);
                        return;
                    }
                    
                    // 处理普通推文消息
                    // 添加新推文
                    tweets.unshift(data);
                    
                    // 限制推文数量
                    if (tweets.length > MAX_TWEETS) {
                        tweets = tweets.slice(0, MAX_TWEETS);
                    }
                    
                    // 更新显示
                    renderTweets();
                } catch (e) {
                    logDebug(`处理SSE消息时出错: ${e.message}`);
                    console.error('Error processing SSE message:', e);
                }
            });
        }
        
        // 更新连接状态
        function updateConnectionStatus(text, state) {
            connectionStatus.textContent = text;
            
            // 更新状态指示器
            if (state === 'connected') {
                connectionBadge.className = 'connection-badge connected';
                connectionStatus.style.color = '#17bf63';
            } else if (state === 'error') {
                connectionBadge.className = 'connection-badge disconnected';
                connectionStatus.style.color = '#e0245e';
            } else {
                connectionBadge.className = 'connection-badge';
                connectionStatus.style.color = '#657786';
            }
        }
        
        // 更新事件源
        function updateEventSource() {
            if (!paused) {
                connectToStream();
            }
        }
        
        // 格式化日期，处理无效日期
        function formatDate(dateStr) {
            if (!dateStr) return "未知日期";
            
            try {
                // 尝试处理常见的RFC 822/1123格式 (Twitter API常用格式)
                // 例如: "Wed, 09 May 2025 15:20:15 GMT"
                const date = new Date(dateStr);
                
                // 检查日期是否有效
                if (isNaN(date.getTime())) {
                    logDebug(`无效日期格式: ${dateStr}`);
                    return "未知日期";
                }
                
                // 格式化日期为本地时间
                const options = { 
                    year: 'numeric', 
                    month: 'short', 
                    day: 'numeric',
                    hour: '2-digit',
                    minute: '2-digit'
                };
                return date.toLocaleDateString(undefined, options);
            } catch (e) {
                logDebug(`日期解析错误: ${e.message}`);
                return "未知日期";
            }
        }
        
        // 渲染推文列表
        function renderTweets() {
            if (tweets.length === 0) {
                tweetContainer.innerHTML = '<div class="no-tweets">还没有任何推文</div>';
                return;
            }
            
            tweetContainer.innerHTML = '';
            
            // 过滤选定用户的推文
            let filteredTweets = tweets;
            if (selectedUsers.length > 0) {
                filteredTweets = tweets.filter(tweet => {
                    const tweetUserId = tweet.user_id || "";
                    return selectedUsers.includes(tweetUserId);
                });
            }
            
            if (filteredTweets.length === 0) {
                tweetContainer.innerHTML = '<div class="no-tweets">没有匹配的推文</div>';
                return;
            }
            
            // 创建推文元素
            filteredTweets.forEach((tweet, index) => {
                const tweetElement = document.createElement('div');
                tweetElement.className = 'tweet';
                if (index === 0) {
                    tweetElement.classList.add('new');
                }
                
                // 安全获取推文数据
                const userId = tweet.user_id || "未知用户";
                const username = tweet.username || tweet.user_id || "未知用户";  // 优先使用推文中的username
                const content = tweet.content || "无内容";
                const publishedAt = tweet.published_at || "";
                const url = tweet.url || "#";
                const images = tweet.images || [];  // 获取图片数组
                
                // 格式化日期
                const formattedDate = formatDate(publishedAt);
                
                // 如果推文数据中没有username，才从用户列表中查找
                let displayName = username;
                if (username === userId && allUsers) {
                    const userInfo = allUsers.find(u => u.user_id === userId);
                    displayName = userInfo ? (userInfo.username || userId) : userId;
                }
                
                // 构建图片HTML
                let imagesHtml = '';
                if (images && images.length > 0) {
                    imagesHtml = '<div class="tweet-images">';
                    images.forEach(imgUrl => {
                        if (imgUrl) {
                            imagesHtml += `<img src="${imgUrl}" alt="推文图片" class="tweet-image" loading="lazy" onerror="this.style.display='none'">`;
                        }
                    });
                    imagesHtml += '</div>';
                }
                
                tweetElement.innerHTML = `
                    <div class="tweet-header">
                        <div class="user-info">
                            <a href="https://twitter.com/${userId}" target="_blank" class="user-name">
                                ${displayName}
                            </a>
                            <p class="user-id">@${userId}</p>
                        </div>
                    </div>
                    <div class="tweet-content">${content}</div>
                    ${imagesHtml}
                    <div class="tweet-footer">
                        <a href="${url}" target="_blank" class="tweet-time">${formattedDate}</a>
                    </div>
                `;
                
                tweetContainer.appendChild(tweetElement);
            });
            
            // 自动滚动到顶部
            if (autoScrollCheckbox.checked) {
                window.scrollTo({ top: 0, behavior: 'smooth' });
            }
        }
        
        // 初始化
        async function init() {
            logDebug('初始化应用...');
            await loadUsers();
            await loadPollingConfig();
            
            // 连接事件流
            connectToStream();
            
            // 添加事件监听器
            toggleButton.addEventListener('click', () => {
                if (paused) {
                    connectToStream();
                    toggleButton.textContent = '暂停推送';
                } else {
                    if (eventSource) {
                        eventSource.close();
                        updateConnectionStatus('已暂停', 'paused');
                    }
                    toggleButton.textContent = '恢复推送';
                }
                paused = !paused;
                logDebug(`推送状态已变更为: ${paused ? '已暂停' : '已恢复'}`);
            });
            
            clearButton.addEventListener('click', () => {
                tweets = [];
                renderTweets();
                logDebug('已清空推文列表');
            });
        }
        
        // 启动应用
        init();
    </script>
</body>
</html>
"""

# 创建HTML模板文件
async def create_template_file():
    """创建HTML模板文件"""
    template_path = os.path.join("templates", "index.html")
    
    # 如果文件不存在，则创建
    if not os.path.exists(template_path):
        with open(template_path, "w", encoding="utf-8") as file:
            file.write(HTML_TEMPLATE)
        logger.info(f"创建HTML模板文件: {template_path}")

# 加载关注列表
def load_following_list():
    """从JSON文件加载关注用户列表"""
    try:
        following_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config", "following_list.json")
        if not os.path.exists(following_file):
            # 尝试在当前目录查找
            following_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "following_list.json")
            if not os.path.exists(following_file):
                logger.warning("未找到following_list.json文件")
                return []
        
        with open(following_file, 'r', encoding='utf-8') as f:
            following_list = json.load(f)
        logger.info(f"成功加载关注列表，共 {len(following_list)} 个用户")
        
        # 缓存用户信息
        for user in following_list:
            user_id = user.get("userId", "").lower()  # 确保用户ID是小写的
            if user_id:
                USER_INFO_CACHE[user_id] = {
                    "username": user.get("username", user_id),
                    "profile_image": user.get("profileImageUrlHttps", "")
                }
                logger.debug(f"缓存用户信息: {user_id} -> {user.get('username', user_id)}")
        
        logger.info(f"已缓存 {len(USER_INFO_CACHE)} 个用户的信息")
        return following_list
    except (json.JSONDecodeError, FileNotFoundError) as e:
        logger.error(f"无法加载关注列表: {str(e)}")
        return []

# 主页路由
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """主页"""
    return templates.TemplateResponse("index.html", {"request": request})

# 获取所有用户列表
@app.get("/api/users")
async def get_users(redis_client: redis.Redis = Depends(get_redis)):
    """获取所有用户列表"""
    users = []
    
    # 获取优先用户组
    priority_users = await redis_client.smembers(PRIORITY_USERS_KEY)
    normal_users = await redis_client.smembers(NORMAL_USERS_KEY)
    
    all_user_ids = list(priority_users) + list(normal_users)
    
    # 确保已加载用户信息缓存
    if not USER_INFO_CACHE:
        load_following_list()
    
    for user_id in all_user_ids:
        # 从缓存中获取用户信息
        user_info = USER_INFO_CACHE.get(user_id, {})
        username = user_info.get("username", user_id)
        profile_image = user_info.get("profile_image", "")
        
        users.append({
            "user_id": user_id,
            "username": username,
            "profile_image": profile_image,
            "priority": user_id in priority_users
        })
    
    return users

# 获取轮询配置
@app.get("/api/polling-config")
async def get_polling_config(redis_client: redis.Redis = Depends(get_redis)):
    """获取轮询配置"""
    try:
        # 从Redis获取配置
        config_json = await redis_client.get(POLLING_CONFIG_KEY)
        
        if config_json:
            # 解析JSON配置
            config = json.loads(config_json)
        else:
            # 如果不存在，使用默认配置并保存到Redis
            config = DEFAULT_POLLING_CONFIG
            await redis_client.set(POLLING_CONFIG_KEY, json.dumps(config))
        
        return config
    except Exception as e:
        logger.error(f"获取轮询配置出错: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"error": f"获取轮询配置失败: {str(e)}"}
        )

# 更新轮询配置
@app.post("/api/polling-config")
async def update_polling_config(
    config: Dict = Body(...),
    redis_client: redis.Redis = Depends(get_redis)
):
    """更新轮询配置"""
    try:
        # 验证配置参数
        valid_keys = set(DEFAULT_POLLING_CONFIG.keys())
        for key in config:
            if key not in valid_keys:
                return JSONResponse(
                    status_code=400,
                    content={"error": f"无效的配置参数: {key}"}
                )
            
            # 确保值为整数或浮点数
            if not isinstance(config[key], (int, float)) or config[key] <= 0:
                return JSONResponse(
                    status_code=400,
                    content={"error": f"参数 {key} 必须是正数"}
                )
        
        # 获取当前配置
        current_config_json = await redis_client.get(POLLING_CONFIG_KEY)
        if current_config_json:
            current_config = json.loads(current_config_json)
        else:
            current_config = DEFAULT_POLLING_CONFIG
        
        # 更新配置
        for key, value in config.items():
            current_config[key] = value
        
        # 保存到Redis
        await redis_client.set(POLLING_CONFIG_KEY, json.dumps(current_config))
        
        return current_config
    except Exception as e:
        logger.error(f"更新轮询配置出错: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"error": f"更新轮询配置失败: {str(e)}"}
        )

# SSE流端点
@app.get("/api/stream")
async def stream(
    request: Request,
    users: Optional[str] = Query(None),
    redis_client: redis.Redis = Depends(get_redis)
):
    """SSE流API端点"""
    logger.info(f"接收到SSE流连接请求，用户过滤: {users}")
    
    # 解析用户过滤参数
    user_filter = set()
    if users:
        user_filter = {user.strip() for user in users.split(",")}
        logger.info(f"过滤用户: {user_filter}")
    
    # 检查Redis连接
    try:
        await redis_client.ping()
        logger.info("Redis连接正常")
    except Exception as e:
        logger.error(f"Redis连接错误: {str(e)}")
        return StreamingResponse(
            content=[f"data: {{\"type\":\"error\",\"message\":\"Redis连接错误: {str(e)}\"}}\n\n"],
            media_type="text/event-stream"
        )
    
    # 检查Redis中是否有推文数据
    try:
        stream_length = await redis_client.xlen(TWEET_STREAM_KEY)
        logger.info(f"Redis推文流长度: {stream_length}")
    except Exception as e:
        logger.error(f"获取Redis流长度错误: {str(e)}")
    
    # 设置SSE响应头
    headers = {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",  # 禁用Nginx缓冲
    }
    
    # 返回流式响应
    return StreamingResponse(
        content=stream_tweets(request, redis_client, user_filter),
        media_type="text/event-stream",
        headers=headers
    )

async def stream_tweets(
    request: Request,
    redis_client: redis.Redis,
    user_filter: Set[str]
) -> AsyncIterator[str]:
    """推文流API端点"""
    client_id = str(uuid.uuid4())
    logger.info(f"客户端 {client_id} 连接到SSE流, 过滤用户: {list(user_filter) if user_filter else '全部'}")
    
    # 获取全局用户列表大小
    try:
        priority_count = await redis_client.scard(PRIORITY_USERS_KEY)
        normal_count = await redis_client.scard(NORMAL_USERS_KEY)
        total_users = priority_count + normal_count
        logger.info(f"已加载用户总数: {total_users} (优先: {priority_count}, 普通: {normal_count})")
        
        # 全局变量存储用户总数，供前端显示
        global GLOBAL_USERS
        GLOBAL_USERS = total_users
    except Exception as e:
        logger.error(f"获取用户数量出错: {str(e)}")
        total_users = 0
    
    # 发送连接确认消息
    connection_message = f"data: {{\"type\":\"connected\",\"message\":\"成功连接到SSE流，已加载{total_users}个用户\"}}\n\n"
    logger.debug(f"发送连接确认消息: {connection_message}")
    yield connection_message
    
    # 设置Redis客户端的最后读取ID
    try:
        # 检查是否应该读取历史数据
        if READ_HISTORY:
            # 从流的开始读取所有数据
            last_id = "0-0"
            logger.info(f"读取所有历史数据，起始ID: {last_id}")
        else:
            # 只读取新数据
            last_id = "$"
            logger.info(f"仅读取新数据，起始ID: {last_id}")
    except Exception as e:
        logger.error(f"获取最后ID出错: {str(e)}")
        last_id = "$"  # 默认只读取新数据
    
    # 连接保持时间（秒）
    connection_timeout = 3600  # 1小时
    connection_start = time.time()
    
    try:
        # 先尝试读取现有数据
        try:
            logger.debug(f"初始读取Redis流，起始ID: {last_id}")
            # 如果是从头读取，增加count参数，读取更多历史数据
            if last_id == "0-0":
                initial_response = await redis_client.xread(
                    {TWEET_STREAM_KEY: last_id},
                    count=100  # 增加读取数量，从10条增加到100条
                )
            else:
                # 如果只读取新数据，不需要设置count
                initial_response = await redis_client.xread(
                    {TWEET_STREAM_KEY: last_id},
                    block=0  # 不阻塞，立即返回
                )
            
            # 如果没有数据，尝试检查流是否存在
            if not initial_response:
                logger.warning(f"没有找到初始推文数据，检查流是否存在")
                # 检查流是否存在
                stream_exists = await redis_client.exists(TWEET_STREAM_KEY)
                if not stream_exists:
                    logger.warning(f"Redis流 {TWEET_STREAM_KEY} 不存在，可能是轮询引擎尚未写入数据")
                    # 发送警告消息给客户端
                    yield f"data: {{\"type\":\"warning\",\"message\":\"Redis流不存在，可能是轮询引擎尚未写入数据\"}}\n\n"
                else:
                    stream_length = await redis_client.xlen(TWEET_STREAM_KEY)
                    logger.info(f"Redis流 {TWEET_STREAM_KEY} 存在，长度为 {stream_length}")
                    if stream_length == 0:
                        yield f"data: {{\"type\":\"info\",\"message\":\"Redis流为空，等待新推文\"}}\n\n"
            else:
                logger.info(f"找到 {len(initial_response[0][1])} 条初始推文")
                
                # 处理初始数据
                for stream_name, messages in initial_response:
                    for message_id, fields in messages:
                        # 更新最后读取的消息ID
                        last_id = message_id
                        await redis_client.set(LAST_STREAM_ID_KEY, last_id)
                        
                        # 检查用户过滤
                        user_id = fields.get(b"user_id", b"").decode('utf-8') if isinstance(fields.get(b"user_id", b""), bytes) else fields.get("user_id", "")
                        # 确保用户ID是小写的
                        user_id = user_id.lower() if user_id else ""
                        
                        if user_filter and user_id not in user_filter:
                            continue
                        
                        # 构建推文数据
                        tweet_data = {}
                        # 处理字段，确保正确解码
                        for key, value in fields.items():
                            if isinstance(key, bytes):
                                key = key.decode('utf-8')
                            if isinstance(value, bytes):
                                try:
                                    value = value.decode('utf-8')
                                except UnicodeDecodeError:
                                    logger.warning(f"无法解码字段 {key}: {value}")
                                    value = str(value)
                            tweet_data[key] = value
                        
                        # 处理images字段，将JSON字符串转换为数组
                        if "images" in tweet_data and tweet_data["images"]:
                            try:
                                tweet_data["images"] = json.loads(tweet_data["images"])
                            except (json.JSONDecodeError, TypeError) as e:
                                logger.warning(f"解析images字段失败: {e}")
                                tweet_data["images"] = []
                        
                        # 添加用户信息
                        # 优先使用推文数据中的username字段，如果没有再从缓存中查找
                        if "username" not in tweet_data or not tweet_data["username"]:
                            if user_id in USER_INFO_CACHE:
                                tweet_data["username"] = USER_INFO_CACHE[user_id].get("username", user_id)
                                tweet_data["profile_image"] = USER_INFO_CACHE[user_id].get("profile_image", "")
                                logger.debug(f"从缓存补充用户信息: {user_id} -> {tweet_data['username']}")
                            else:
                                logger.debug(f"未找到用户信息: {user_id}")
                                # 尝试查找不区分大小写的匹配
                                for cached_id, info in USER_INFO_CACHE.items():
                                    if cached_id.lower() == user_id.lower():
                                        tweet_data["username"] = info.get("username", user_id)
                                        tweet_data["profile_image"] = info.get("profile_image", "")
                                        logger.debug(f"从缓存模糊匹配用户信息: {user_id} -> {tweet_data['username']}")
                                        break
                                else:
                                    # 如果都没有找到，使用user_id作为username
                                    tweet_data["username"] = user_id
                                    logger.debug(f"使用默认用户名: {user_id}")
                        else:
                            logger.debug(f"使用推文数据中的用户名: {user_id} -> {tweet_data['username']}")
                        
                        # 记录更多信息，帮助调试
                        logger.debug(f"发送初始推文: {tweet_data}")
                        
                        # 发送消息
                        tweet_json = json.dumps(tweet_data, ensure_ascii=False)
                        logger.debug(f"发送JSON数据: {tweet_json[:100]}...")
                        yield f"data: {tweet_json}\n\n"
        except Exception as e:
            logger.error(f"初始读取出错: {str(e)}", exc_info=True)  # 添加异常堆栈跟踪
            yield f"data: {{\"type\":\"error\",\"message\":\"读取数据时出错: {str(e)}\"}}\n\n"
        
        # 持续监听新消息
        while time.time() - connection_start < connection_timeout:
            # 检查客户端是否断开连接
            if await request.is_disconnected():
                logger.info(f"客户端 {client_id} 断开连接")
                break
                
            # 发送心跳消息
            heartbeat_message = f"data: {{\"type\":\"heartbeat\",\"time\":\"{datetime.now().isoformat()}\"}}\n\n"
            logger.debug(f"发送心跳消息: {heartbeat_message}")
            yield heartbeat_message
            
            # 从Redis流读取新消息
            logger.debug(f"从Redis流读取消息，起始ID: {last_id}")
            try:
                response = await redis_client.xread(
                    {TWEET_STREAM_KEY: last_id},
                    count=10,  # 每次最多读取10条消息
                    block=5000  # 阻塞5秒
                )
                
                if not response:
                    # 没有新消息，继续等待
                    logger.debug("没有新消息")
                    await asyncio.sleep(1)
                    continue
                    
                # 处理新消息
                logger.info(f"收到 {len(response[0][1])} 条新消息")
                for stream_name, messages in response:
                    for message_id, fields in messages:
                        # 更新最后读取的消息ID
                        last_id = message_id
                        await redis_client.set(LAST_STREAM_ID_KEY, last_id)
                        
                        # 检查用户过滤
                        user_id = fields.get(b"user_id", b"").decode('utf-8') if isinstance(fields.get(b"user_id", b""), bytes) else fields.get("user_id", "")
                        # 确保用户ID是小写的
                        user_id = user_id.lower() if user_id else ""
                        
                        if user_filter and user_id not in user_filter:
                            continue
                        
                        # 构建推文数据
                        tweet_data = {}
                        # 处理字段，确保正确解码
                        for key, value in fields.items():
                            if isinstance(key, bytes):
                                key = key.decode('utf-8')
                            if isinstance(value, bytes):
                                try:
                                    value = value.decode('utf-8')
                                except UnicodeDecodeError:
                                    logger.warning(f"无法解码字段 {key}: {value}")
                                    value = str(value)
                            tweet_data[key] = value
                        
                        # 处理images字段，将JSON字符串转换为数组
                        if "images" in tweet_data and tweet_data["images"]:
                            try:
                                tweet_data["images"] = json.loads(tweet_data["images"])
                            except (json.JSONDecodeError, TypeError) as e:
                                logger.warning(f"解析images字段失败: {e}")
                                tweet_data["images"] = []
                        
                        # 添加用户信息
                        # 优先使用推文数据中的username字段，如果没有再从缓存中查找
                        if "username" not in tweet_data or not tweet_data["username"]:
                            if user_id in USER_INFO_CACHE:
                                tweet_data["username"] = USER_INFO_CACHE[user_id].get("username", user_id)
                                tweet_data["profile_image"] = USER_INFO_CACHE[user_id].get("profile_image", "")
                                logger.debug(f"从缓存补充用户信息: {user_id} -> {tweet_data['username']}")
                            else:
                                logger.debug(f"未找到用户信息: {user_id}")
                                # 尝试查找不区分大小写的匹配
                                for cached_id, info in USER_INFO_CACHE.items():
                                    if cached_id.lower() == user_id.lower():
                                        tweet_data["username"] = info.get("username", user_id)
                                        tweet_data["profile_image"] = info.get("profile_image", "")
                                        logger.debug(f"从缓存模糊匹配用户信息: {user_id} -> {tweet_data['username']}")
                                        break
                                else:
                                    # 如果都没有找到，使用user_id作为username
                                    tweet_data["username"] = user_id
                                    logger.debug(f"使用默认用户名: {user_id}")
                        else:
                            logger.debug(f"使用推文数据中的用户名: {user_id} -> {tweet_data['username']}")
                        
                        # 记录更多信息，帮助调试
                        logger.debug(f"发送推文: {tweet_data}")
                        
                        # 发送消息
                        tweet_json = json.dumps(tweet_data, ensure_ascii=False)
                        logger.debug(f"发送JSON数据: {tweet_json[:100]}...")
                        yield f"data: {tweet_json}\n\n"
            except Exception as e:
                logger.error(f"读取Redis流出错: {str(e)}")
                await asyncio.sleep(1)
            
    except Exception as e:
        logger.error(f"流处理出错: {str(e)}")
        # 发送错误消息
        yield f"data: {{\"type\":\"error\",\"message\":\"{str(e)}\"}}\n\n"
    
    finally:
        # 发送关闭消息
        logger.info(f"客户端 {client_id} 断开连接")
        yield f"data: {{\"type\":\"closed\",\"message\":\"Stream closed\"}}\n\n"

@app.on_event("startup")
async def startup_event():
    """应用启动时执行的操作"""
    # 创建HTML模板文件
    await create_template_file()
    
    # 加载用户信息
    load_following_list()
    
    logger.info("推特实时推送服务启动")

@app.on_event("shutdown")
async def shutdown_event():
    """应用关闭时执行的操作"""
    logger.info("推特实时推送服务关闭")

# 主函数
def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="推特实时推送服务")
    
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="服务器主机地址 (默认: 0.0.0.0)"
    )
    
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="服务器端口 (默认: 8000)"
    )
    
    parser.add_argument(
        "--read-history",
        action="store_true",
        help="读取Redis流中的历史数据 (默认: 仅读取新数据)"
    )
    
    args = parser.parse_args()
    
    # 设置是否读取历史数据
    global READ_HISTORY
    READ_HISTORY = args.read_history
    
    try:
        import uvicorn
        # 添加检查端口是否被占用的逻辑
        import socket
        
        # 尝试检查端口是否可用
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.bind((args.host, args.port))
            s.close()
            logger.info(f"端口 {args.port} 可用，准备启动服务")
        except socket.error:
            logger.warning(f"端口 {args.port} 已被占用，尝试使用随机可用端口")
            # 尝试寻找可用端口
            s.bind(('0.0.0.0', 0))
            _, port = s.getsockname()
            s.close()
            args.port = port
            logger.info(f"将使用端口 {args.port}")
        
        # 使用Windows兼容的配置启动uvicorn
        uvicorn.run(
            app, 
            host=args.host, 
            port=args.port,
            log_level="info",
            loop="asyncio"  # 确保使用正确的事件循环
        )
    except ImportError:
        logger.error("未安装uvicorn，请使用pip安装: pip install uvicorn")
        exit(1)

if __name__ == "__main__":
    main() 