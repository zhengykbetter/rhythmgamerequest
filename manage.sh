#!/bin/bash
# 主项目一键管理脚本（类Makefile，简化命令记忆）
# 用法：
# ./manage.sh config-cron   # 配置定时任务（从开源的cron_config导入）
# ./manage.sh check-cron    # 检查当前定时任务配置
# ./manage.sh sync-now      # 手动执行CSV同步（拉取+复制）
# ./manage.sh help          # 查看帮助

# 配置项（可根据实际路径调整）
CRON_CONFIG_FILE="./scripts/cron_config"
SYNC_SCRIPT="./scripts/sync_csv_from_remote.py"
LOG_DIR="./logs"
PYTHON_PATH="/usr/bin/python3"

# 颜色输出（便于区分信息）
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 创建日志目录
mkdir -p ${LOG_DIR}

# 函数1：配置定时任务
config_cron() {
    echo -e "${YELLOW}===== 开始配置定时任务（从${CRON_CONFIG_FILE}导入）=====${NC}"
    # 备份原有crontab
    crontab -l > ${LOG_DIR}/cron_backup_$(date +%Y%m%d).log 2>/dev/null
    echo -e "${GREEN}✅ 已备份原有crontab到${LOG_DIR}/cron_backup_$(date +%Y%m%d).log${NC}"
    
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

# 函数2：检查定时任务
check_cron() {
    echo -e "${YELLOW}===== 当前服务器定时任务配置 =====${NC}"
    crontab -l
    if [ $? -ne 0 ]; then
        echo -e "${RED}❌ 暂无定时任务配置！${NC}"
    fi
    
    echo -e "\n${YELLOW}===== 开源的定时任务配置（${CRON_CONFIG_FILE}）=====${NC}"
    cat ${CRON_CONFIG_FILE}
}

# 函数3：手动同步CSV
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

# 函数4：帮助信息
show_help() {
    echo -e "${YELLOW}===== 主项目一键管理脚本 =====${NC}"
    echo "用法：./manage.sh [命令]"
    echo "命令列表："
    echo "  config-cron   - 配置定时任务（从开源的cron_config导入）"
    echo "  check-cron    - 检查当前定时任务配置"
    echo "  sync-now      - 手动执行CSV同步（拉取私有仓库+复制到主仓库）"
    echo "  help          - 查看帮助"
}

# 主逻辑：根据参数执行对应函数
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
    help|*)
        show_help
        ;;
esac