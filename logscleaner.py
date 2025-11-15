import datetime
import os
import time
import logging
from typing import List

# ==================== 核心配置（精准指定+删除原文件+创建空文件） ====================
CONFIG = {
    # 待清理的3个卫星日志文件（精准路径）
    "target_log_files": [
        "/DATA/logs/FY4B_FullDisk/fy4b_fulldisk_download.log",
        "/DATA/logs/GK2A_Color/gk2a_color_download.log",
        "/DATA/logs/GK2A_Infrared/gk2a_infrared_download.log"
    ],
    # 卫星日志清理规则
    "target_clean_interval_hours": 1,  # 每1小时清理一次
    # 自身日志配置（删除原文件+创建空文件）
    "self_log_subdir": "LogCleaner",   # 自身日志子文件夹（/DATA/logs/LogCleaner）
    "self_log_filename": "log_cleaner.log",
    "self_clean_interval_days": 1,     # 自身日志每天清理一次
    # 通用配置
    "root_log_dir": "/DATA/logs",      # 根日志目录
    "force_create_log_file": True      # 若文件不存在，自动创建空文件（确保清理目标存在）
}

# 计算自身日志完整路径
SELF_LOG_DIR = os.path.join(CONFIG["root_log_dir"], CONFIG["self_log_subdir"])
CONFIG["self_log_fullpath"] = os.path.join(SELF_LOG_DIR, CONFIG["self_log_filename"])

# ==================== 自身日志配置（仅输出到文件，无终端输出） ====================
def setup_self_logger():
    logger = logging.getLogger("LogCleaner")
    logger.setLevel(logging.INFO)
    logger.propagate = False  # 禁用父logger，避免重复输出

    # 自动创建自身日志子目录和空文件（确保自身日志可正常写入）
    os.makedirs(SELF_LOG_DIR, exist_ok=True)
    if not os.path.exists(CONFIG["self_log_fullpath"]) and CONFIG["force_create_log_file"]:
        create_empty_file(CONFIG["self_log_fullpath"])
        logger.info(f"自动创建自身日志空文件：{CONFIG['self_log_fullpath']}")

    # 仅保留文件handler：所有日志仅写入文件，无终端输出
    file_handler = logging.FileHandler(CONFIG["self_log_fullpath"], encoding="utf-8")
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s UTC - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    ))
    logger.addHandler(file_handler)

    return logger

self_logger = setup_self_logger()

# ==================== 核心工具函数（删除原文件+创建空文件） ====================
def create_empty_file(file_path: str) -> bool:
    """在指定路径创建空文件（若父目录不存在则自动创建）"""
    try:
        parent_dir = os.path.dirname(file_path)
        os.makedirs(parent_dir, exist_ok=True)
        with open(file_path, "w", encoding="utf-8") as f:
            f.write("")  # 创建空文件
        return True
    except Exception as e:
        self_logger.error(f"创建空文件失败：{file_path}，错误信息：{str(e)}", exc_info=True)
        return False

def validate_log_file(file_path: str) -> bool:
    """验证日志文件是否存在，不存在则创建空文件"""
    if os.path.exists(file_path):
        return True
    if CONFIG["force_create_log_file"]:
        self_logger.warning(f"日志文件不存在，自动创建空文件：{file_path}")
        return create_empty_file(file_path)
    self_logger.warning(f"日志文件不存在且未开启自动创建，跳过：{file_path}")
    return False

def clean_log_file_by_replace(file_path: str) -> bool:
    """清理日志文件：删除原文件+创建同名空文件"""
    self_logger.info(f"开始清理日志文件：{file_path}")
    self_logger.info(f"清理逻辑：删除原文件 → 创建同名空文件")

    try:
        # 步骤1：删除原文件（若存在）
        if os.path.exists(file_path):
            os.remove(file_path)
            self_logger.info(f"已成功删除原文件：{file_path}")
        else:
            self_logger.info(f"原文件不存在，直接创建空文件：{file_path}")

        # 步骤2：创建同名空文件（保持路径和文件名不变）
        if create_empty_file(file_path):
            self_logger.info(f"已成功创建同名空文件：{file_path}")
            self_logger.info(f"日志文件清理完成：{file_path}")
            return True
        else:
            self_logger.error(f"创建同名空文件失败，日志文件清理失败：{file_path}")
            return False

    except Exception as e:
        self_logger.error(f"日志文件清理失败：{file_path}，错误信息：{str(e)}", exc_info=True)
        return False

# ==================== 卫星日志清理（每1小时执行：删除+创建空文件） ====================
def clean_target_logs():
    """清理3个卫星日志文件（每1小时一次，删除原文件+创建空文件）"""
    self_logger.info("=" * 80)
    self_logger.info(f"开始卫星日志清理（每{CONFIG['target_clean_interval_hours']}小时一次）")
    self_logger.info(f"当前UTC时间：{datetime.datetime.now(datetime.UTC).strftime('%Y-%m-%d %H:%M:%S')}")
    self_logger.info(f"清理逻辑：删除原文件 → 创建同名空文件（路径和文件名不变）")
    self_logger.info("=" * 80)

    # 验证并确保所有目标文件可清理（不存在则创建空文件）
    valid_files = []
    for file_path in CONFIG["target_log_files"]:
        if validate_log_file(file_path):
            valid_files.append(file_path)
        else:
            self_logger.warning(f"日志文件无法就绪，跳过清理：{file_path}")

    if not valid_files:
        self_logger.info("无有效日志文件可清理，本次卫星日志清理任务结束")
        self_logger.info("=" * 80 + "\n")
        return

    # 逐个清理文件（删除+创建空文件）
    success_count = 0
    fail_count = 0
    for file_path in valid_files:
        if clean_log_file_by_replace(file_path):
            success_count += 1
        else:
            fail_count += 1
        self_logger.info("-" * 50)

    # 清理总结
    self_logger.info("=" * 80)
    self_logger.info("卫星日志清理任务完成")
    self_logger.info(f"有效文件数：{len(valid_files)} | 清理成功：{success_count} | 清理失败：{fail_count}")
    next_clean_time = datetime.datetime.now(datetime.UTC) + datetime.timedelta(hours=CONFIG["target_clean_interval_hours"])
    self_logger.info(f"下次卫星日志清理时间：{next_clean_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    self_logger.info("=" * 80 + "\n")

# ==================== 自身日志清理（每天执行：删除+创建空文件） ====================
def clean_self_logs():
    """清理脚本自身日志（每天一次，删除原文件+创建空文件）"""
    # 确保自身日志文件存在
    if not validate_log_file(CONFIG["self_log_fullpath"]):
        self_logger.error("自身日志文件无法就绪，跳过清理")
        return

    # 每天仅清理一次（避免重复）
    today = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d")
    last_clean_date = getattr(clean_self_logs, "_last_clean_date", None)
    
    if last_clean_date == today:
        self_logger.info(f"今日已清理自身日志（{today}），跳过本次")
        return
    
    self_logger.info("=" * 80)
    self_logger.info(f"开始自身日志清理（每日一次）")
    self_logger.info(f"自身日志文件：{CONFIG['self_log_fullpath']}")
    self_logger.info(f"清理逻辑：删除原文件 → 创建同名空文件（路径和文件名不变）")
    self_logger.info("=" * 80)

    if clean_log_file_by_replace(CONFIG["self_log_fullpath"]):
        self_logger.info("自身日志清理成功")
        clean_self_logs._last_clean_date = today  # 记录今日已清理
    else:
        self_logger.error("自身日志清理失败")
    
    self_logger.info("=" * 80 + "\n")

# ==================== 主程序（完整逻辑：删除+创建空文件+仅日志输出） ====================
def main():
    # 初始化自身日志清理日期记录
    clean_self_logs._last_clean_date = None

    self_logger.info("=" * 80)
    self_logger.info("日志定时清理脚本启动（删除原文件+创建空文件）")
    self_logger.info("=" * 30 + " 核心说明 " + "=" * 30)
    self_logger.info("清理逻辑：删除原始日志文件，在原路径创建同名空文件")
    self_logger.info("卫星日志：每1小时清理一次，路径和文件名保持不变")
    self_logger.info("自身日志：每天清理一次，路径和文件名保持不变")
    self_logger.info("所有日志仅输出到文件，无终端输出")
    self_logger.info("=" * 30 + " 任务配置 " + "=" * 30)
    self_logger.info("1. 卫星日志（删除+创建空文件）：")
    for idx, file in enumerate(CONFIG["target_log_files"], 1):
        self_logger.info(f"     {idx}. {file}")
    self_logger.info("2. 自身日志（删除+创建空文件）：")
    self_logger.info(f"     - 存储路径：{CONFIG['self_log_fullpath']}")
    self_logger.info(f"     - 清理频率：每天一次")
    self_logger.info("=" * 80 + "\n")

    try:
        # 启动时执行首次清理（卫星日志+自身日志）
        self_logger.info("启动初始化：执行首次日志清理...")
        clean_target_logs()  # 首次清理卫星日志
        clean_self_logs()    # 首次清理自身日志

        # 主循环：每1小时调度一次
        while True:
            # 计算下次卫星日志清理时间
            now = datetime.datetime.now(datetime.UTC)
            next_target_clean = now + datetime.timedelta(hours=CONFIG["target_clean_interval_hours"])
            today = now.strftime("%Y-%m-%d")
            self_log_clean_status = "已清理" if clean_self_logs._last_clean_date == today else "未清理"
            
            self_logger.info("等待下次任务...")
            self_logger.info(f"  - 下次卫星日志清理：{next_target_clean.strftime('%Y-%m-%d %H:%M:%S')} UTC")
            self_logger.info(f"  - 自身日志清理：每日自动触发（今日状态：{self_log_clean_status}）")

            # 休眠1小时（每10秒检查一次，支持手动中断）
            sleep_total_seconds = CONFIG["target_clean_interval_hours"] * 3600
            for _ in range(sleep_total_seconds // 10):
                time.sleep(10)

            # 到点执行卫星日志清理
            clean_target_logs()

            # 检查是否需要执行自身日志清理（每日一次）
            clean_self_logs()

    except KeyboardInterrupt:
        self_logger.info("\n" + "=" * 80)
        self_logger.info("脚本被手动中断，正在退出...")
        self_logger.info("所有日志文件清理操作已生效，未执行的任务将终止")
        self_logger.info("=" * 80)
    except Exception as e:
        self_logger.critical(f"脚本运行异常：{str(e)}", exc_info=True)
        self_logger.critical("脚本将退出，请排查错误后重新启动")

if __name__ == "__main__":
    main()