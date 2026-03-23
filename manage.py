#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
主管理脚本：终极修复版（指令全短横线 + 无报错）
支持指令：starter / config-cron / check-cron / cancel-cron / clear-all-cron / clean-old / extract / sync-db / sync-now / auto / help
"""
import os
import sys
import subprocess
import getpass
import logging
from datetime import datetime
from pathlib import Path

# ===================== 模块顶层导入settings =====================
try:
    from config.settings import *
    SETTINGS_LOADED = True
    IMPORT_ERROR_MSG = ""
except ImportError as e:
    SETTINGS_LOADED = False
    IMPORT_ERROR_MSG = str(e)
except Exception as e:
    SETTINGS_LOADED = False
    IMPORT_ERROR_MSG = str(e)

# ===================== 颜色常量 =====================
RED = '\033[0;31m'
GREEN = '\033[0;32m'
YELLOW = '\033[1;33m'
BOLD_RED = '\033[1;31m'
NC = '\033[0m'  # 重置颜色

# ===================== 日志初始化（关键：解决sync-now无日志问题） =====================
def init_logger(config):
    """初始化日志：同步过程记录到文件+控制台"""
    log_dir = Path(config["LOG_DIR"])
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"manage_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

# ===================== 核心工具函数 =====================
def run_shell_cmd(cmd, capture_output=False, logger=None):
    """执行shell命令，返回(输出, 错误, 退出码)，带日志记录"""
    if logger:
        logger.info(f"执行命令：{cmd}")
    try:
        if capture_output:
            result = subprocess.run(
                cmd, shell=True, capture_output=True, text=True,
                executable="/bin/bash", timeout=120  # 新增超时：2分钟
            )
            if logger:
                if result.stdout:
                    logger.info(f"命令输出：\n{result.stdout}")
                if result.stderr:
                    logger.error(f"命令错误输出：\n{result.stderr}")
            return result.stdout, result.stderr, result.returncode
        else:
            subprocess.run(
                cmd, shell=True, executable="/bin/bash"
            )
            return "", "", 0
    except subprocess.TimeoutExpired:
        error_msg = f"命令执行超时（超过120秒）：{cmd}"
        if logger:
            logger.error(error_msg)
        return "", error_msg, -1
    except Exception as e:
        error_msg = f"命令执行异常：{str(e)}"
        if logger:
            logger.error(error_msg)
        return "", error_msg, -1

def get_full_config():
    """读取配置（默认+settings覆盖）"""
    # 默认配置
    default_config = {
        "PYTHON_EXEC_PATH": sys.executable,  # 改用当前Python解释器，避免版本问题
        "MAIN_REPO_ROOT": os.getcwd(),
        "LOG_DIR": os.path.join(os.getcwd(), "logs"),
        "CRON_BACKUP_DIR": os.path.join(os.getcwd(), "logs"),
        "CRON_TASK_MARK": "# 节奏游戏项目定时任务",
        "CRON_TASKS": [
            f"0 2 * * * {sys.executable} {os.path.join(os.getcwd(), 'manage.py')} auto > {os.path.join(os.getcwd(), 'logs', 'auto_cron.log')} 2>&1"
        ],
        # 废弃旧同步脚本，指向csv_manage.py（核心修正）
        "SYNC_SCRIPT": os.path.join(os.getcwd(), "managers", "csv_manage.py"),
        "EXTRACT_SONG_SCRIPT": os.path.join(os.getcwd(), "scripts", "extract_song_data.py"),
        "CRON_MANAGE_SCRIPT": os.path.join(os.getcwd(), "managers", "cron_manage.py"),
        "CSV_MANAGE_SCRIPT": os.path.join(os.getcwd(), "managers", "csv_manage.py"),
        "CSV_ROOT_DIR": os.path.join(os.getcwd(), "data", "csv"),
        "ARCHIVE_DIR": os.path.join(os.getcwd(), "data", "csv", "archive"),
        "CSV_SOURCE_DIR": os.path.join(os.getcwd(), "data", "csv", "source"),
        "DB_CONFIG": {
            "host": "localhost", "port": 3306, "user": "root",
            "password": "", "database": "rhythmgame", "charset": "utf8mb4"
        },
        # Git同步新增默认配置（兼容settings）
        "CSV_REPO_URL": "https://github.com/zhengykbetter/rhythmgamebase.git",
        "CSV_REPO_BRANCH": "main",
        "CSV_REPO_LOCAL_PATH": "/opt/csv_repo",
        "PRIVATE_CSV_REPO_ROOT": "result"
    }

    # 用settings覆盖默认配置
    if SETTINGS_LOADED:
        print(f"{GREEN}✅ 成功读取settings.py配置{NC}")
        for key in default_config.keys():
            if key in locals() and locals()[key] is not None:
                default_config[key] = locals()[key]
    else:
        print(f"{YELLOW}ℹ️  未读取到settings.py，使用默认配置（{IMPORT_ERROR_MSG}）{NC}")

    # 创建必要目录
    for dir_path in [
        default_config["LOG_DIR"], default_config["CRON_BACKUP_DIR"],
        default_config["CSV_ROOT_DIR"], default_config["ARCHIVE_DIR"],
        default_config["CSV_SOURCE_DIR"],
        Path(default_config["CSV_REPO_LOCAL_PATH"]),  # 创建Git仓库目录
        os.path.dirname(default_config["CRON_MANAGE_SCRIPT"]),
        os.path.dirname(default_config["CSV_MANAGE_SCRIPT"])
    ]:
        Path(dir_path).mkdir(parents=True, exist_ok=True)

    return default_config

# ===================== 1. 初始化权限 =====================
def starter(config):
    """初始化脚本执行权限"""
    print(f"{YELLOW}===== 初始化脚本执行权限（仅需一次）====={NC}")
    logger = init_logger(config)
    # 主脚本赋权
    os.chmod(__file__, 0o755)
    logger.info(f"已赋予{__file__}执行权限")
    print(f"{GREEN}✅ 已赋予{__file__}执行权限{NC}")
    
    # scripts目录赋权
    scripts_dir = Path(config["MAIN_REPO_ROOT"]) / "scripts"
    if scripts_dir.exists():
        for fname in os.listdir(scripts_dir):
            if fname.endswith(".py"):
                fpath = scripts_dir / fname
                os.chmod(fpath, 0o755)
                logger.info(f"已赋予{fpath}执行权限")
                print(f"{GREEN}✅ 已赋予{fpath}执行权限{NC}")
    
    # managers目录赋权
    managers_dir = Path(config["MAIN_REPO_ROOT"]) / "managers"
    if managers_dir.exists():
        for fname in os.listdir(managers_dir):
            if fname.endswith(".py"):
                fpath = managers_dir / fname
                os.chmod(fpath, 0o755)
                logger.info(f"已赋予{fpath}执行权限")
                print(f"{GREEN}✅ 已赋予{fpath}执行权限{NC}")
    
    print(f"{GREEN}===== 权限初始化完成 ====={NC}")
    return True

# ===================== 2. Cron管理 =====================
def config_cron(config):
    """配置Cron任务"""
    logger = init_logger(config)
    starter(config)
    cron_script = Path(config["CRON_MANAGE_SCRIPT"])
    if not cron_script.exists():
        error_msg = f"cron_manage.py不存在：{cron_script}"
        logger.error(error_msg)
        print(f"{RED}❌ {error_msg}{NC}")
        return False
    stdout, stderr, returncode = run_shell_cmd(
        f"{config['PYTHON_EXEC_PATH']} {cron_script} config",
        capture_output=True, logger=logger
    )
    if returncode == 0:
        print(f"{GREEN}✅ Cron配置成功{NC}")
        return True
    else:
        print(f"{RED}❌ Cron配置失败：{stderr}{NC}")
        return False

def check_cron(config):
    """检查Cron配置"""
    logger = init_logger(config)
    cron_script = Path(config["CRON_MANAGE_SCRIPT"])
    if not cron_script.exists():
        error_msg = f"cron_manage.py不存在：{cron_script}"
        logger.error(error_msg)
        print(f"{RED}❌ {error_msg}{NC}")
        return False
    run_shell_cmd(f"{config['PYTHON_EXEC_PATH']} {cron_script} check", logger=logger)
    return True

def cancel_cron(config):
    """清除本项目Cron任务"""
    logger = init_logger(config)
    cron_script = Path(config["CRON_MANAGE_SCRIPT"])
    if not cron_script.exists():
        error_msg = f"cron_manage.py不存在：{cron_script}"
        logger.error(error_msg)
        print(f"{RED}❌ {error_msg}{NC}")
        return False
    stdout, stderr, returncode = run_shell_cmd(
        f"{config['PYTHON_EXEC_PATH']} {cron_script} cancel",
        capture_output=True, logger=logger
    )
    if returncode == 0:
        print(f"{GREEN}✅ 本项目Cron已清除{NC}")
        return True
    else:
        print(f"{RED}❌ 清除失败：{stderr}{NC}")
        return False

def clear_all_cron(config):
    """清除所有Cron任务（高危）"""
    logger = init_logger(config)
    print(f"{BOLD_RED}===== 危险操作警告 ====={NC}")
    print(f"{BOLD_RED}此命令将删除当前用户（{getpass.getuser()}）的所有Cron任务！{NC}")
    backup_path = Path(config["CRON_BACKUP_DIR"]) / f"cron_full_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    print(f"即将备份所有Cron任务到：{backup_path}")
    
    confirm = input(f"{BOLD_RED}请输入 'Y' 确认执行清除（输入其他则取消）：{NC}")
    if confirm.strip().upper() != "Y":
        logger.info("用户取消清除所有Cron操作")
        print(f"{GREEN}✅ 用户取消操作{NC}")
        return True

    # 备份
    run_shell_cmd(f"crontab -l 2>/dev/null > {backup_path}", capture_output=True, logger=logger)
    print(f"{GREEN}✅ 已备份到：{backup_path}{NC}")

    # 清除
    stdout, stderr, returncode = run_shell_cmd("crontab -r", capture_output=True, logger=logger)
    if returncode == 0:
        print(f"{GREEN}✅ 所有Cron已清除{NC}")
        cron_out, _, _ = run_shell_cmd("crontab -l 2>/dev/null", capture_output=True, logger=logger)
        if not cron_out:
            print(f"{GREEN}✅ 验证：无任何Cron任务{NC}")
        else:
            print(f"{YELLOW}ℹ️  残留任务：{cron_out}{NC}")
        return True
    else:
        print(f"{RED}❌ 清除失败：{stderr}{NC}")
        return False

# ===================== 3. CSV/DB管理（核心修复：sync-now） =====================
def clean_old(config):
    """清理旧CSV文件"""
    logger = init_logger(config)
    print(f"{YELLOW}===== 清理旧CSV文件 ====={NC}")
    csv_script = Path(config["CSV_MANAGE_SCRIPT"])
    if not csv_script.exists():
        error_msg = f"csv_manage.py不存在：{csv_script}"
        logger.error(error_msg)
        print(f"{RED}❌ {error_msg}{NC}")
        return False
    # 调用csv_manage.py clean-old
    cmd = f"{config['PYTHON_EXEC_PATH']} {csv_script} clean-old"
    stdout, stderr, returncode = run_shell_cmd(cmd, capture_output=True, logger=logger)
    if returncode == 0:
        print(f"{GREEN}✅ 旧CSV清理完成{NC}")
        if stdout:
            print(f"{GREEN}{stdout}{NC}")
        return True
    else:
        print(f"{RED}❌ 清理失败{NC}")
        if stderr:
            print(f"{RED}{stderr}{NC}")
        return False

def extract(config):
    """转换_raw CSV"""
    logger = init_logger(config)
    print(f"{YELLOW}===== 转换原始CSV ====={NC}")
    extract_script = Path(config["EXTRACT_SONG_SCRIPT"])
    if not extract_script.exists():
        error_msg = f"extract_song_data.py不存在：{extract_script}"
        logger.error(error_msg)
        print(f"{RED}❌ {error_msg}{NC}")
        return False
    stdout, stderr, returncode = run_shell_cmd(
        f"{config['PYTHON_EXEC_PATH']} {extract_script}",
        capture_output=True, logger=logger
    )
    if returncode == 0:
        print(f"{GREEN}✅ CSV转换完成{NC}")
        if stdout:
            print(f"{GREEN}{stdout}{NC}")
        return True
    else:
        print(f"{RED}❌ 转换失败{NC}")
        if stderr:
            print(f"{RED}{stderr}{NC}")
        return False

def sync_db(config):
    """同步CSV到数据库"""
    logger = init_logger(config)
    print(f"{YELLOW}===== 同步CSV到MySQL ====={NC}")
    csv_script = Path(config["CSV_MANAGE_SCRIPT"])
    if not csv_script.exists():
        error_msg = f"csv_manage.py不存在：{csv_script}"
        logger.error(error_msg)
        print(f"{RED}❌ {error_msg}{NC}")
        return False
    stdout, stderr, returncode = run_shell_cmd(
        f"{config['PYTHON_EXEC_PATH']} {csv_script} sync-db",
        capture_output=True, logger=logger
    )
    if returncode == 0:
        print(f"{GREEN}✅ 数据库同步完成{NC}")
        if stdout:
            print(f"{GREEN}{stdout}{NC}")
        return True
    else:
        print(f"{RED}❌ 同步失败{NC}")
        if stderr:
            print(f"{RED}{stderr}{NC}")
        return False

def sync_now(config):
    """手动同步远程CSV（核心修复：调用csv_manage.py sync-git）"""
    logger = init_logger(config)
    print(f"{YELLOW}===== 同步远程CSV ====={NC}")
    # 核心修正：废弃旧的sync_csv_from_remote.py，调用csv_manage.py的sync-git
    csv_script = Path(config["CSV_MANAGE_SCRIPT"])
    if not csv_script.exists():
        error_msg = f"csv_manage.py不存在：{csv_script}"
        logger.error(error_msg)
        print(f"{RED}❌ {error_msg}{NC}")
        return False
    
    # 执行Git同步：csv_manage.py sync-git
    cmd = f"{config['PYTHON_EXEC_PATH']} {csv_script} sync-git"
    stdout, stderr, returncode = run_shell_cmd(cmd, capture_output=True, logger=logger)
    
    # 详细输出结果（解决无日志问题）
    if returncode == 0:
        print(f"{GREEN}✅ 远程CSV同步完成{NC}")
        if stdout:
            print(f"{GREEN}{stdout}{NC}")
        return True
    else:
        print(f"{RED}❌ 同步失败{NC}")
        # 输出详细错误信息
        if stderr:
            print(f"{RED}{stderr}{NC}")
        if stdout:
            print(f"{YELLOW}ℹ️  同步输出：{stdout}{NC}")
        logger.error(f"sync-now失败，返回码：{returncode}")
        return False

# ===================== 4. 全自动流程（修正执行顺序） =====================
def auto_run(config):
    """全自动执行：sync-now → extract → clean-old → sync-db（逻辑更合理）"""
    logger = init_logger(config)
    print(f"{YELLOW}===== 开始全自动执行 ====={NC}")
    success = True
    try:
        # 修正执行顺序：先同步Git → 再提取 → 再清理 → 最后同步DB
        print("-" * 40 + "\n【1/4】同步远程CSV")
        if not sync_now(config):
            success = False
            print(f"{RED}❌ 同步远程CSV失败，跳过后续步骤{NC}")
            return False
        
        print("-" * 40 + "\n【2/4】转换原始CSV")
        if not extract(config):
            success = False
        
        print("-" * 40 + "\n【3/4】清理旧CSV文件")
        if not clean_old(config):
            success = False
        
        print("-" * 40 + "\n【4/4】同步CSV到数据库")
        if not sync_db(config):
            success = False
        
        if success:
            print(f"{GREEN}🎉 全自动执行完成！{NC}")
        else:
            print(f"{YELLOW}⚠️  全自动执行完成，但部分步骤有警告/失败{NC}")
        return success
    except Exception as e:
        error_msg = f"全自动执行异常：{str(e)}"
        logger.error(error_msg)
        print(f"{RED}❌ {error_msg}{NC}")
        return False

# ===================== 5. 帮助信息 =====================
def show_help():
    """展示帮助（全短横线指令）"""
    print(f"{YELLOW}===== 项目管理脚本指令说明 ====={NC}")
    print("用法：python3 manage.py [指令]")
    print("\n【1. 初始化/权限】")
    print("  starter            - 初始化脚本执行权限（仅需一次）")
    print("\n【2. Cron管理】")
    print("  config-cron        - 配置定时任务（每天2点执行auto）")
    print("  check-cron         - 检查当前Cron配置")
    print("  cancel-cron        - 清除本项目Cron任务")
    print(f"  clear-all-cron     - {BOLD_RED}清除所有Cron任务（危险！）{NC}")
    print("\n【3. CSV/DB管理】")
    print("  clean-old          - 清理旧CSV文件（保留_raw）")
    print("  extract            - 转换原始CSV为目标格式")
    print("  sync-db            - 同步CSV到MySQL")
    print("  sync-now           - 手动同步远程CSV（GitHub→本地）")
    print("\n【4. 全自动】")
    print("  auto               - 一键执行：sync-now→extract→clean-old→sync-db")
    print("\n【5. 其他】")
    print("  help               - 查看此帮助信息")

# ===================== 主入口 =====================
def main():
    if len(sys.argv) < 2:
        show_help()
        sys.exit(1)

    # 读取配置
    config = get_full_config()
    logger = init_logger(config)
    logger.info(f"执行指令：{sys.argv[1]}")

    # 指令映射（全短横线）
    command_map = {
        "starter": lambda: starter(config),
        "config-cron": lambda: config_cron(config),
        "check-cron": lambda: check_cron(config),
        "cancel-cron": lambda: cancel_cron(config),
        "clear-all-cron": lambda: clear_all_cron(config),
        "clean-old": lambda: clean_old(config),
        "extract": lambda: extract(config),
        "sync-db": lambda: sync_db(config),
        "sync-now": lambda: sync_now(config),  # 核心修复的函数
        "auto": lambda: auto_run(config),
        "help": show_help
    }

    # 执行指令
    command = sys.argv[1]
    if command in command_map:
        try:
            success = command_map[command]()
            sys.exit(0 if success else 1)
        except Exception as e:
            logger.error(f"指令执行异常：{str(e)}")
            print(f"{RED}❌ 指令执行异常：{str(e)}{NC}")
            sys.exit(1)
    else:
        error_msg = f"未知指令：{command}"
        logger.error(error_msg)
        print(f"{RED}❌ {error_msg}{NC}")
        show_help()
        sys.exit(1)

if __name__ == "__main__":
    main()