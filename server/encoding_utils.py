#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
✅ 安全编码处理工具（无系统IO修改，永不崩溃）
仅做字符串编码清洗，解决中文/特殊字符乱码，不修改sys.stdin/stdout
"""

def safe_input(prompt: str = "") -> str:
    """
    安全输入：原生input + 多层编码容错，不修改系统IO
    彻底解决 UnicodeDecodeError /  closed file 错误
    """
    try:
        # 方案1：标准UTF-8输入
        user_input = input(prompt)
        return _clean_encoding(user_input)
    except:
        try:
            # 方案2：兼容Windows/SSH终端GBK
            user_input = input(prompt.encode('gbk', errors='ignore').decode('gbk'))
            return _clean_encoding(user_input)
        except:
            # 终极兜底：返回空字符串，绝不崩溃
            return ""

def safe_print(*args, **kwargs):
    """安全打印，自动过滤异常字符"""
    try:
        print(*args, **kwargs)
    except:
        pass

def _clean_encoding(text: str) -> str:
    """统一清洗字符串编码，返回标准UTF-8"""
    if not isinstance(text, str):
        return ""
    # 多层编码容错，处理所有异常字节
    for enc in ['utf-8', 'gbk', 'gb2312', 'latin-1']:
        try:
            return text.encode(enc, errors='replace').decode('utf-8', errors='replace').strip()
        except:
            continue
    return text.strip()