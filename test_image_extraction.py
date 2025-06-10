#!/usr/bin/env python3
"""
测试图片提取逻辑的脚本
用于验证修复后的图片去重功能
"""

import re
import json
from urllib.parse import urlparse

def normalize_image_url(url):
    """标准化图片URL，用于去重"""
    if not url:
        return ""
    
    # 修复端口号问题：如果URL是localhost但没有端口，添加8080端口
    if url.startswith('http://localhost/') and ':8080' not in url:
        url = url.replace('http://localhost/', 'http://localhost:8080/')
    
    # 移除URL中的查询参数进行去重比较（但保留原始URL）
    parsed = urlparse(url)
    normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    return normalized

def extract_images_from_description(description_text):
    """从描述文本中提取图片URL（模拟RSS解析）"""
    images = []
    
    if description_text:
        # 匹配img标签中的src属性
        img_pattern = r'<img[^>]+src=["\']([^"\']+)["\'][^>]*>'
        img_matches = re.findall(img_pattern, description_text, re.IGNORECASE)
        images.extend(img_matches)
        
        # 只有在没有从img标签找到图片时，才使用URL模式匹配作为备用方案
        if not img_matches:
            url_pattern = r'https?://[^\s<>"]+\.(?:jpg|jpeg|png|gif|webp)(?:\?[^\s<>"]*)?'
            url_matches = re.findall(url_pattern, description_text, re.IGNORECASE)
            images.extend(url_matches)
    
    return images

def deduplicate_images(images):
    """去重图片URL"""
    unique_images = []
    seen_normalized = set()
    
    for img_url in images:
        if not img_url or img_url.startswith('data:') or len(img_url) <= 10:
            continue
        
        # 修复端口号问题
        if img_url.startswith('http://localhost/') and ':8080' not in img_url:
            img_url = img_url.replace('http://localhost/', 'http://localhost:8080/')
            print(f"修复图片URL端口: {img_url}")
        
        # 使用标准化URL进行去重
        normalized = normalize_image_url(img_url)
        if normalized not in seen_normalized:
            seen_normalized.add(normalized)
            unique_images.append(img_url)
        else:
            print(f"发现重复图片URL: {img_url} (标准化: {normalized})")
    
    return unique_images

def test_image_extraction():
    """测试图片提取和去重功能"""
    
    # 测试用例1：包含重复图片的HTML描述
    test_description_1 = '''
    <p>这是一条推文</p>
    <img src="http://localhost/pic/media%2FGtA3pwYaoAA0tJa.jpg" alt="图片1">
    <p>一些文本</p>
    <img src="http://localhost:8080/pic/media%2FGtA3pwYaoAA0tJa.jpg" alt="图片1重复">
    <p>更多文本 http://localhost/pic/media%2FGtA3pwYaoAA0tJa.jpg</p>
    '''
    
    # 测试用例2：包含多张不同图片的HTML描述
    test_description_2 = '''
    <p>多图推文</p>
    <img src="http://localhost:8080/pic/media%2FGs-6x5zakAAFYXf.jpg" alt="图片1">
    <img src="http://localhost:8080/pic/media%2FGtB4pwYaoAA0tJb.jpg" alt="图片2">
    <p>文本中的URL: http://localhost/pic/media%2FGs-6x5zakAAFYXf.jpg</p>
    '''
    
    # 测试用例3：只有URL模式的描述（没有img标签）
    test_description_3 = '''
    <p>只有URL的推文</p>
    <p>图片链接: http://localhost/pic/media%2FGtC5pwYaoAA0tJc.jpg</p>
    <p>另一个: http://localhost:8080/pic/media%2FGtD6pwYaoAA0tJd.jpg</p>
    '''
    
    test_cases = [
        ("重复图片测试", test_description_1),
        ("多图测试", test_description_2),
        ("URL模式测试", test_description_3)
    ]
    
    for test_name, description in test_cases:
        print(f"\n=== {test_name} ===")
        print(f"描述内容: {description.strip()}")
        
        # 提取图片
        images = extract_images_from_description(description)
        print(f"提取到的图片: {images}")
        
        # 去重
        unique_images = deduplicate_images(images)
        print(f"去重后的图片: {unique_images}")
        
        # 转换为JSON（模拟存储到Redis）
        images_json = json.dumps(unique_images)
        print(f"JSON格式: {images_json}")
        
        # 解析JSON（模拟从Redis读取）
        parsed_images = json.loads(images_json)
        print(f"解析后的图片: {parsed_images}")
        
        print(f"原始数量: {len(images)}, 去重后数量: {len(unique_images)}")

if __name__ == "__main__":
    test_image_extraction()
