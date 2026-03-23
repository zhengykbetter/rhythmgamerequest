Welcome to my project!

| 命令                      | 功能说明                                           |
| ------------------------- | -------------------------------------------------- |
| `./manage.sh help`        | 查看所有可用命令                                   |
| `./manage.sh config-cron` | 配置定时任务                                       |
| `./manage.sh check-cron`  | 检查当前服务器定时任务配置                         |
| `./manage.sh sync-now`    | 手动执行 CSV 同步                                  |
| `./manage.sh cancel-cron` | 取消本项目定时任务（保留服务器其他任务，自动备份） |



| 指令名称         | 功能描述                           | 使用场景 & 注意事项                                          |
| ---------------- | ---------------------------------- | ------------------------------------------------------------ |
| `starter`        | 初始化脚本执行权限（仅需执行一次） | 给 `scripts/`、`managers/` 目录下所有 `.py` 脚本赋权，**首次部署必执行** |
| `config-cron`    | 配置 Cron 定时任务                 | 默认配置「每天凌晨 2 点执行 `auto` 指令」；覆盖旧的本项目 Cron 任务，执行前自动备份 Cron |
| `check-cron`     | 检查当前 Cron 配置                 | 展示服务器上本项目的 Cron 任务 + `settings.py` 中的配置，验证配置是否生效 |
| `cancel-cron`    | 仅清除本项目的 Cron 任务           | 保留服务器上其他非本项目的 Cron 任务，安全清理本项目定时任务 |
| `clear-all-cron` | 清除当前用户所有 Cron 任务         | ⚠️ 危险操作！删除所有 Cron（含非本项目）；执行前强制警告 + 自动备份，需输入 `Y` 确认 |
| `clean_old`      | 清理旧 CSV 文件                    | 保留 `_raw` 后缀的源文件，删除 7 天前的处理后文件；清理前自动归档旧文件 |
| `extract`        | 转换 `_raw` CSV 为目标格式         | 调用 `extract_song_data.py`，将原始 CSV 处理为数据库可导入的格式 |
| `sync_db`        | 同步 CSV 数据到 MySQL              | 将处理后的 CSV 数据写入 MySQL；依赖 `settings.py` 中的 `DB_CONFIG` 数据库配置 |
| `sync-now`       | 手动同步远程 CSV 文件              | 调用 `sync_csv_from_remote.py`，拉取最新的远程原始 CSV 文件  |
| `auto`           | 全自动执行流程                     | 一键执行：`clean_old → extract → sync_db → sync_now`；Cron 定时任务默认执行此指令 |
| `help`           | 查看所有指令帮助                   | 展示完整指令列表及功能说明；执行 `python3 manage.py` 无参数时默认执行此指令 |

配置问题参见help.md

欢迎提issues