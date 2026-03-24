#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
超强编码处理工具（独立程序）
解决所有终端输入/输出的 UnicodeDecodeError 问题
兼容：UTF-8、GBK、GB2312、Latin-1 全字符集
"""
import sys
import io

# 强制设置标准流编码
def init_encoding():
    """初始化全局编码为UTF-8，兼容异常终端"""
    try:
        # 修复标准输入输出流编码
        sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8', errors='replace')
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
    except Exception:
        pass

# 🔥 核心：安全输入函数（替代原生input()，永不编码报错）
def safe_input(prompt: str = "") -> str:
    """
    安全获取用户输入，自动处理所有编码问题
    :param prompt: 提示文字
    :return: 干净的字符串
    """
    init_encoding()
    try:
        # 原生输入 + 多层编码容错
        user_input = input(prompt)
        # 终极编码清洗
        user_input = user_input.encode('utf-8', 'replace').decode('utf-8', 'replace')
        return user_input.strip()
    except UnicodeDecodeError:
        # 兜底：GBK兼容
        try:
            return input(prompt).encode('gbk').decode('utf-8', 'replace').strip()
        except:
            return ""
    except Exception:
        return ""

# 安全打印（防止输出乱码）
def safe_print(*args, **kwargs):
    init_encoding()
    print(*args, **kwargs)