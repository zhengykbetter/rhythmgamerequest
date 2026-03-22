#!/bin/bash
# 主项目一键管理脚本（类Makefile，配置分离）
# 用法：
# ./manage.sh config-cron   # 配置定时任务
# ./manage.sh check-cron    # 检查cron
# ./manage.sh sync-now      # 手动同步
# ./manage.sh cancel-cron   # 取消本项目的crontab任务（保留其他任务）
# ./manage.sh help          # 查看帮助

# ===================== 从Python配置文件读取参数 =====================
# 读取CRON配置文件路径
CRON_CONFIG_FILE=$(python3 -c "import sys; sys.path.append('.'); from config.settings import CRON_CONFIG_FILE; print(CRON_CONFIG_FILE)")
# 读取同步脚本路径
SYNC_SCRIPT="./scripts/sync_csv_from_remote.py"
# 读取日志目录
LOG_DIR=$(python3 -c "import sys; sys.path.append('.'); from config.settings import LOG_DIR; print(LOG_DIR)")
# 读取Python执行路径
PYTHON_PATH=$(python3 -c "import sys; sys.path.append('.'); from config.settings import PYTHON_EXEC_PATH; print(PYTHON_EXEC_PATH)")

# 本项目crontab任务特征（用于精准匹配/删除，基于主仓库路径）
PROJECT_CRONTAB_MARK="/opt/main_repo/"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# 创建日志目录
mkdir -p ${LOG_DIR}

# ===================== 核心函数 =====================
config_cron() {
    echo -e "${YELLOW}===== 开始配置定时任务（从${CRON_CONFIG_FILE}导入）=====${NC}"
    # 备份原有crontab
    crontab -l > ${LOG_DIR}/cron_backup_$(date +%Y%m%d_%H%M%S).log 2>/dev/null
    echo -e "${GREEN}✅ 已备份原有crontab到${LOG_DIR}/cron_backup_$(date +%Y%m%d_%H%M%S).log${NC}"
    
    # 导入新配置
    crontab ${CRON_CONFIG_FILE}
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ 定时任务配置成功！${NC}"
        echo -e "${YELLOW}当前定时任务配置：${NC}"
        crontab -l
    else
        echo -e "${RED}❌ 定时任务配置失败！请检查${CRON_CONFIG_FILE}文件格式${NC}"
        exit 1
    fi
}

check_cron() {
    echo -e "${YELLOW}===== 当前服务器定时任务配置 =====${NC}"
    crontab -l
    if [ $? -ne 0 ]; then
        echo -e "${RED}❌ 暂无定时任务配置！${NC}"
    fi
    
    echo -e "\n${YELLOW}===== 开源的定时任务配置（${CRON_CONFIG_FILE}）=====${NC}"
    cat ${CRON_CONFIG_FILE}
}

sync_now() {
    echo -e "${YELLOW}===== 开始手动执行CSV同步 =====${NC}"
    ${PYTHON_PATH} ${SYNC_SCRIPT}
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ CSV手动同步完成！日志请查看${LOG_DIR}/sync_csv_$(date +%Y%m%d).log${NC}"
    else
        echo -e "${RED}❌ CSV手动同步失败！请查看控制台输出或日志${NC}"
        exit 1
    fi
}

cancel_cron() {
    echo -e "${YELLOW}===== 开始取消本项目的crontab任务 =====${NC}"
    # 1. 备份当前crontab（带时间戳，避免误删）
    BACKUP_FILE="${LOG_DIR}/cron_backup_before_cancel_$(date +%Y%m%d_%H%M%S).log"
    crontab -l > ${BACKUP_FILE} 2>/dev/null
    echo -e "${GREEN}✅ 已备份当前crontab到${BACKUP_FILE}${NC}"

    # 2. 检查是否有本项目的定时任务
    CRON_CONTENT=$(crontab -l 2>/dev/null)
    if echo "${CRON_CONTENT}" | grep -q "${PROJECT_CRONTAB_MARK}"; then
        # 3. 过滤掉本项目的任务，保留其他任务
        NEW_CRON_CONTENT=$(echo "${CRON_CONTENT}" | grep -v "${PROJECT_CRONTAB_MARK}")
        
        # 4. 重新导入过滤后的crontab（保留其他任务）
        echo "${NEW_CRON_CONTENT}" | crontab -
        if [ $? -eq 0 ]; then
            echo -e "${GREEN}✅ 本项目的crontab任务已成功取消！${NC}"
            echo -e "${YELLOW}当前剩余定时任务：${NC}"
            crontab -l || echo -e "${RED}❌ 暂无剩余定时任务${NC}"
        else
            echo -e "${RED}❌ 取消本项目crontab任务失败！已恢复原有配置${NC}"
            # 恢复备份
            crontab ${BACKUP_FILE}
            exit 1
        fi
    else
        echo -e "${YELLOW}ℹ️  未检测到本项目的crontab任务，无需取消${NC}"
    fi
}

show_help() {
    echo -e "${YELLOW}===== 主项目一键管理脚本 =====${NC}"
    echo "用法：./manage.sh [命令]"
    echo "命令列表："
    echo "  config-cron   - 配置定时任务（从开源的cron_config导入）"
    echo "  check-cron    - 检查当前定时任务配置"
    echo "  sync-now      - 手动执行CSV同步（拉取私有仓库+复制到主仓库）"
    echo "  cancel-cron   - 取消本项目的crontab任务（保留服务器其他任务）"
    echo "  help          - 查看帮助"
}

# ===================== 主逻辑 =====================
case "$1" in
    config-cron)
        config_cron
        ;;
    check-cron)
        check_cron
        ;;
    sync-now)
        sync_now
        ;;
    cancel-cron)
        cancel_cron
        ;;
    help|*)
        show_help
        ;;
esac