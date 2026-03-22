#!/bin/bash
# 主项目一键管理脚本（类Makefile，配置分离）
# 用法：
# ./manage.sh starter        # 初始化：给所有脚本赋予执行权限（首次部署必做）
# ./manage.sh config-cron   # 配置定时任务（从settings.py读取配置）
# ./manage.sh check-cron    # 检查cron
# ./manage.sh sync-now      # 手动同步
# ./manage.sh cancel-cron   # 取消本项目的crontab任务（保留其他任务）
# ./manage.sh help          # 查看帮助

# ===================== 基础配置：确保Python路径正确 =====================
export PYTHONPATH=$(pwd):$PYTHONPATH
# 临时配置文件路径（动态生成cron配置用）
TEMP_CRON_FILE="./scripts/temp_cron_config"

# ===================== 从settings.py读取所有配置 =====================
# 读取Python执行路径
PYTHON_PATH=$(python3 -c "from config.settings import PYTHON_EXEC_PATH; print(PYTHON_EXEC_PATH)" 2>/dev/null)
# 读取同步脚本路径
SYNC_SCRIPT=$(python3 -c "from config.settings import MAIN_REPO_ROOT; print(MAIN_REPO_ROOT / 'scripts' / 'sync_csv_from_remote.py')" 2>/dev/null)
# 读取日志目录
LOG_DIR=$(python3 -c "from config.settings import LOG_DIR; print(LOG_DIR)" 2>/dev/null)
# 读取cron备份目录
CRON_BACKUP_DIR=$(python3 -c "from config.settings import CRON_BACKUP_DIR; print(CRON_BACKUP_DIR)" 2>/dev/null)
# 读取cron任务列表（按行输出）
CRON_TASKS=$(python3 -c "from config.settings import CRON_TASKS; [print(task) for task in CRON_TASKS]" 2>/dev/null)
# 读取cron任务特征标记
CRON_TASK_MARK=$(python3 -c "from config.settings import CRON_TASK_MARK; print(CRON_TASK_MARK)" 2>/dev/null)

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# ===================== 核心函数 =====================
# 初始化脚本权限
starter() {
    echo -e "${YELLOW}===== 开始初始化脚本执行权限 =====${NC}"
    # 给manage.sh本身加权限
    chmod +x ./manage.sh
    echo -e "${GREEN}✅ 已给manage.sh赋予执行权限${NC}"
    
    # 给同步脚本加权限
    if [ -f "${SYNC_SCRIPT}" ]; then
        chmod +x ${SYNC_SCRIPT}
        echo -e "${GREEN}✅ 已给${SYNC_SCRIPT}赋予执行权限${NC}"
    fi
    
    # 给CSV→DB脚本加权限（可选）
    CSV_TO_DB_SCRIPT=$(python3 -c "from config.settings import MAIN_REPO_ROOT; print(MAIN_REPO_ROOT / 'scripts' / 'csv_to_db.py')" 2>/dev/null)
    if [ -f "${CSV_TO_DB_SCRIPT}" ]; then
        chmod +x ${CSV_TO_DB_SCRIPT}
        echo -e "${GREEN}✅ 已给${CSV_TO_DB_SCRIPT}赋予执行权限${NC}"
    fi
    
    # 创建日志目录
    if [ -n "${LOG_DIR}" ]; then
        mkdir -p ${LOG_DIR}
        echo -e "${GREEN}✅ 已创建日志目录：${LOG_DIR}${NC}"
    else
        mkdir -p ./logs
        echo -e "${YELLOW}ℹ️  配置读取失败，已创建默认日志目录：./logs${NC}"
        LOG_DIR="./logs"
        CRON_BACKUP_DIR="./logs"
    fi
    
    echo -e "${GREEN}===== 脚本权限初始化完成 =====${NC}"
}

# 配置cron（完全读取settings.py，无硬编码）
config_cron() {
    # 先执行starter确保权限
    starter
    
    echo -e "${YELLOW}===== 开始配置定时任务（从settings.py读取）=====${NC}"
    
    # 1. 验证配置是否读取成功
    if [ -z "${CRON_TASKS}" ]; then
        echo -e "${RED}❌ 读取cron配置失败！请检查settings.py中的CRON_TASKS配置${NC}"
        exit 1
    fi
    
    # 2. 创建cron备份目录
    mkdir -p ${CRON_BACKUP_DIR}
    # 备份原有crontab
    BACKUP_FILE="${CRON_BACKUP_DIR}/cron_backup_$(date +%Y%m%d_%H%M%S).log"
    crontab -l > ${BACKUP_FILE} 2>/dev/null
    echo -e "${GREEN}✅ 已备份原有crontab到${BACKUP_FILE}${NC}"
    
    # 3. 过滤原有crontab：保留非本项目的任务
    EXISTING_CRONTAB=$(crontab -l 2>/dev/null | grep -v "${CRON_TASK_MARK}")
    
    # 4. 动态生成新的cron配置文件
    echo "${EXISTING_CRONTAB}" > ${TEMP_CRON_FILE}
    # 添加本项目的cron任务
    echo -e "\n# 节奏游戏项目定时任务（自动生成，请勿手动修改）" >> ${TEMP_CRON_FILE}
    echo "${CRON_TASKS}" >> ${TEMP_CRON_FILE}
    
    # 5. 导入新的cron配置
    crontab ${TEMP_CRON_FILE}
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ 定时任务配置成功！${NC}"
        # 删除临时文件
        rm -f ${TEMP_CRON_FILE}
        # 展示当前配置
        echo -e "${YELLOW}当前定时任务配置（仅本项目）：${NC}"
        crontab -l | grep -A 100 "${CRON_TASK_MARK}"
    else
        echo -e "${RED}❌ 定时任务配置失败！已恢复原有配置${NC}"
        # 恢复备份
        crontab ${BACKUP_FILE}
        # 删除临时文件
        rm -f ${TEMP_CRON_FILE}
        exit 1
    fi
}

# 检查cron配置
check_cron() {
    echo -e "${YELLOW}===== 当前服务器定时任务配置（本项目）=====${NC}"
    crontab -l | grep -A 100 "${CRON_TASK_MARK}" || echo -e "${RED}❌ 未检测到本项目定时任务${NC}"
    
    echo -e "\n${YELLOW}===== settings.py中的cron配置 =====${NC}"
    echo "${CRON_TASKS}" || echo -e "${RED}❌ 读取settings.py配置失败${NC}"
}

# 手动同步CSV
sync_now() {
    # 执行starter确保权限
    starter
    
    echo -e "${YELLOW}===== 开始手动执行CSV同步 =====${NC}"
    # 确保日志目录存在
    mkdir -p ${LOG_DIR}
    
    ${PYTHON_PATH} ${SYNC_SCRIPT}
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ CSV手动同步完成！日志请查看${LOG_DIR}/sync_csv_$(date +%Y%m%d).log${NC}"
    else
        echo -e "${RED}❌ CSV手动同步失败！请查看控制台输出或日志${NC}"
        exit 1
    fi
}

# 取消本项目cron任务
cancel_cron() {
    echo -e "${YELLOW}===== 开始取消本项目的crontab任务 =====${NC}"
    # 备份当前crontab
    BACKUP_FILE="${CRON_BACKUP_DIR}/cron_backup_before_cancel_$(date +%Y%m%d_%H%M%S).log"
    crontab -l > ${BACKUP_FILE} 2>/dev/null
    echo -e "${GREEN}✅ 已备份当前crontab到${BACKUP_FILE}${NC}"

    # 检查是否有本项目的定时任务
    CRON_CONTENT=$(crontab -l 2>/dev/null)
    if echo "${CRON_CONTENT}" | grep -q "${CRON_TASK_MARK}"; then
        # 过滤掉本项目的任务，保留其他任务
        NEW_CRON_CONTENT=$(echo "${CRON_CONTENT}" | grep -v "${CRON_TASK_MARK}")
        
        # 重新导入过滤后的crontab
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

# 帮助信息
show_help() {
    echo -e "${YELLOW}===== 主项目一键管理脚本 =====${NC}"
    echo "用法：./manage.sh [命令]"
    echo "命令列表："
    echo "  starter       - 初始化：给所有脚本赋予执行权限（首次部署必做）"
    echo "  config-cron   - 配置定时任务（从settings.py读取配置）"
    echo "  check-cron    - 检查当前定时任务配置"
    echo "  sync-now      - 手动执行CSV同步（拉取私有仓库+复制到主仓库）"
    echo "  cancel-cron   - 取消本项目的crontab任务（保留服务器其他任务）"
    echo "  help          - 查看帮助"
}

# ===================== 主逻辑 =====================
case "$1" in
    starter)
        starter
        ;;
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