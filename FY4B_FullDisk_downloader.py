import datetime
import requests
import os
import time
import logging
import json
from typing import Optional, Tuple, Dict, List, Any
from requests.exceptions import RequestException

# ==================== 核心配置（对齐GK-2A，下载当前时间-1h的图片） ====================
SAVE_BASE_DIR = "/DATA/Gallery/FY4B/FullDisk"  # 与GK-2A目录结构一致
LOG_DIR = "/DATA/logs/FY4B_FullDisk"  # 独立日志目录（对齐GK-2A日志规范）
DOWNLOAD_HISTORY_FILE = os.path.join(LOG_DIR, "fy4b_download_history.json")  # 独立历史记录（同GK-2A格式）

# FY-4B核心配置（重点：下载当前UTC时间-1小时的15分钟整点图片）
CONFIG = {
    "name": "FY4B_FullDisk",
    "url_template": "https://img.nsmc.org.cn/CLOUDIMAGE/FY4B/AGRI/GCLR/DISK/FY4B-_AGRI--_N_DISK_1050E_L2-_GCLR_MULT_NOM_{start_time}_{end_time}_1000M_V0001.JPG",
    "time_interval": 15,  # 观测间隔15分钟（仅00/15/30/45分，匹配原始链接）
    "download_offset_hours": 1,  # 下载当前时间-1小时的图片（核心配置）
    "min_file_size": 1024 * 500,  # 最小500KB（与GK-2A文件验证规则一致）
    "check_interval": 300,  # 实时检查间隔5分钟（同GK-2A检查频率）
    "max_recover_days": 0.5,  # 仅下载12小时内的图片（0.5天=12小时）
    "file_magic": b"\xFF\xD8"  # JPG文件头验证（与GK-2A文件验证规则一致）
}

# 通用配置（完全复制GK-2A的通用参数，无差异）
MAX_RETRIES = 3
RETRY_DELAY = 5
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
HEADERS = {"User-Agent": USER_AGENT}
PROGRESS_BAR_INTERVAL = 60
PROGRESS_BAR_LENGTH = 50
FAILED_RETRY_DELAY = 600  # 失败10分钟后重试（同GK-2A重试机制）
DOWNLOAD_PROGRESS_INTERVAL = 2
CHECK_INTERVAL = 10  # 主循环检查间隔（同GK-2A，减少CPU占用）

# 全局变量（同GK-2A命名规范）
failed_tasks: Dict[str, float] = {}

# ==================== 日志配置（完全对齐GK-2A日志格式） ====================
def setup_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = os.path.join(LOG_DIR, "fy4b_fulldisk_download.log")
    
    # 日志格式与GK-2A完全一致（UTC时间+级别+信息）
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s UTC - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    ))
    
    logger.addHandler(file_handler)
    return logger

logger = setup_logger()

# ==================== 核心工具函数（重点修改下载判断逻辑，精准匹配当前时间-1h） ====================
def create_required_dirs():
    """创建目录（同GK-2A的目录创建逻辑，含日志说明）"""
    try:
        os.makedirs(SAVE_BASE_DIR, exist_ok=True)
        logger.info(f"保存目录准备就绪：{SAVE_BASE_DIR}（有效文件永久保留，不自动删除，同GK-2A规则）")
        
        os.makedirs(LOG_DIR, exist_ok=True)
        logger.info(f"日志目录准备就绪：{LOG_DIR}（同GK-2A日志存储规范）")
    except Exception as e:
        logger.critical(f"创建目录失败！{str(e)}", exc_info=True)
        raise

def load_download_history() -> Dict[str, Any]:
    """加载历史记录（同GK-2A的历史记录格式，字段完全一致）"""
    default_history = {
        "downloaded": [],  # 已下载时间戳（YYYYMMDDHHMM，同GK-2A格式）
        "last_download_time": (datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=CONFIG["max_recover_days"])).strftime("%Y-%m-%d %H:%M:%S"),
        "last_check_time": ""
    }
    
    if not os.path.exists(DOWNLOAD_HISTORY_FILE):
        logger.info("下载历史文件不存在，使用默认配置（同GK-2A初始化逻辑）")
        save_download_history(default_history)
        return default_history
    
    try:
        with open(DOWNLOAD_HISTORY_FILE, "r", encoding="utf-8") as f:
            history = json.load(f)
        
        # 兼容旧格式（同GK-2A的兼容逻辑）
        for key in default_history.keys():
            if key not in history:
                history[key] = default_history[key]
        
        logger.info(f"成功加载下载历史，已记录 {len(history['downloaded'])} 个任务（同GK-2A历史管理）")
        return history
    except Exception as e:
        logger.error(f"加载历史记录失败，使用默认配置：{str(e)}")
        save_download_history(default_history)
        return default_history

def save_download_history(history: Dict[str, Any]):
    """保存历史记录（同GK-2A的存储逻辑，JSON格式+缩进）"""
    try:
        with open(DOWNLOAD_HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(history, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"保存历史记录失败：{str(e)}", exc_info=True)

def generate_url_by_time(publish_dt: datetime.datetime) -> Tuple[str, str]:
    """生成URL（严格匹配原始链接的15分钟观测周期规律）"""
    start_time = publish_dt.strftime("%Y%m%d%H%M00")  # 开始时间=观测时间（精确到分钟）
    end_dt = publish_dt + datetime.timedelta(minutes=14, seconds=59)  # 结束时间=开始时间+14分59秒
    end_time = end_dt.strftime("%Y%m%d%H%M%S")
    url = CONFIG["url_template"].format(start_time=start_time, end_time=end_time)
    time_stamp = publish_dt.strftime("%Y%m%d%H%M")  # 时间戳=观测时间（YYYYMMDDHHMM）
    return url, time_stamp

def is_download_ready(publish_dt: datetime.datetime) -> Tuple[bool, str]:
    """判断是否可下载：观测时间 ≤ 当前UTC时间-1小时，且在12小时内"""
    # 计算当前时间-1小时的阈值
    threshold_dt = datetime.datetime.now(datetime.UTC) - datetime.timedelta(hours=CONFIG["download_offset_hours"])
    # 12小时内的阈值（最早可下载的观测时间）
    earliest_dt = datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=CONFIG["max_recover_days"])
    
    publish_time_str = publish_dt.strftime("%Y-%m-%d %H:%M UTC")
    threshold_str = threshold_dt.strftime("%Y-%m-%d %H:%M UTC")
    earliest_str = earliest_dt.strftime("%Y-%m-%d %H:%M UTC")
    current_time_str = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    
    # 双重判断：1. 观测时间在12小时内；2. 观测时间 ≤ 当前时间-1小时
    if earliest_dt <= publish_dt <= threshold_dt:
        logger.info(
            f"图片已达下载条件（当前时间-1小时）：\n"
            f"  - 观测时间：{publish_time_str}\n"
            f"  - 当前时间-1小时阈值：{threshold_str}\n"
            f"  - 12小时内最早观测时间：{earliest_str}\n"
            f"  - 当前UTC时间：{current_time_str}"
        )
        return True, threshold_str
    else:
        if publish_dt < earliest_dt:
            logger.info(f"图片已超出12小时范围，跳过：观测时间{publish_time_str} < 最早阈值{earliest_str}")
        else:
            logger.info(f"图片未达下载条件（当前时间-1小时）：观测时间{publish_time_str} > 阈值{threshold_str}")
        return False, threshold_str

def is_downloaded(time_stamp: str, history: Dict[str, Any]) -> bool:
    """检查是否已下载（优先文件存在性，同GK-2A的去重逻辑）"""
    save_path = generate_save_path(time_stamp)
    if os.path.exists(save_path) and is_file_valid(save_path):
        logger.info(f"文件已存在且有效，视为已下载：{save_path}（永久保留，不重复下载，同GK-2A规则）")
        add_download_history(time_stamp, history)
        return True
    return time_stamp in history["downloaded"]

def add_download_history(time_stamp: str, history: Dict[str, Any]):
    """添加历史记录（仅清理12小时内记录，同GK-2A的历史清理逻辑）"""
    if time_stamp not in history["downloaded"]:
        history["downloaded"].append(time_stamp)
        # 保留12小时内的历史记录（实际文件永久保留）
        twelve_hours_ago = (datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=CONFIG["max_recover_days"])).strftime("%Y%m%d%H%M")
        history["downloaded"] = [t for t in history["downloaded"] if t >= twelve_hours_ago]
        logger.info(f"历史记录清理：保留最近12小时任务记录，文件永久保留在 {SAVE_BASE_DIR}（同GK-2A规则）")
    
    history["last_download_time"] = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M:%S")
    history["last_check_time"] = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M:%S")
    save_download_history(history)

def generate_save_path(time_stamp: str) -> str:
    """生成保存路径（文件名格式同GK-2A，含卫星+观测时间戳）"""
    filename = f"FY4B_FullDisk_{time_stamp}.jpg"
    return os.path.join(SAVE_BASE_DIR, filename)

# ==================== 进度条逻辑（完全复制GK-2A的进度条样式和计算逻辑） ====================
def get_next_check_window(current_time: datetime.datetime) -> Tuple[Optional[datetime.datetime], int, int]:
    next_check_time = current_time + datetime.timedelta(seconds=CONFIG["check_interval"])
    seconds_remaining = int((next_check_time - current_time).total_seconds())
    total_seconds = CONFIG["check_interval"]
    return next_check_time, seconds_remaining, total_seconds

def generate_progress_bar(seconds_remaining: int, total_seconds: int) -> str:
    if total_seconds <= 0:
        return "[==================================================] 00:00"
    
    seconds_remaining = min(seconds_remaining, total_seconds)
    elapsed_seconds = total_seconds - seconds_remaining
    progress = elapsed_seconds / total_seconds
    
    filled_length = int(PROGRESS_BAR_LENGTH * progress)
    filled_length = max(0, min(filled_length, PROGRESS_BAR_LENGTH))
    empty_length = PROGRESS_BAR_LENGTH - filled_length
    
    if seconds_remaining >= 3600:
        hours = seconds_remaining // 3600
        mins = (seconds_remaining % 3600) // 60
        secs = seconds_remaining % 60
        time_str = f"{hours:02d}:{mins:02d}:{secs:02d}"
    else:
        mins = seconds_remaining // 60
        secs = seconds_remaining % 60
        time_str = f"{mins:02d}:{secs:02d}"
    
    return f"[{'=' * filled_length}{' ' * empty_length}] {time_str}"

def generate_download_progress_bar(downloaded: int, total: int) -> str:
    if total <= 0:
        return "[==================================================] 0% (未知大小)"
    
    progress = downloaded / total
    filled_length = int(PROGRESS_BAR_LENGTH * progress)
    empty_length = PROGRESS_BAR_LENGTH - filled_length
    percent = int(progress * 100)
    
    downloaded_size = downloaded / 1024
    total_size = total / 1024
    if total_size >= 1024:
        downloaded_size /= 1024
        total_size /= 1024
        size_str = f"{downloaded_size:.2f}MB / {total_size:.2f}MB"
    else:
        size_str = f"{downloaded_size:.2f}KB / {total_size:.2f}KB"
    
    return f"[{'=' * filled_length}{' ' * empty_length}] {percent}% ({size_str})"

# ==================== 下载核心函数（完全对齐GK-2A的下载逻辑） ====================
def is_file_valid(save_path: str) -> bool:
    """验证文件有效性（同GK-2A的验证规则：大小+文件头）"""
    if not os.path.exists(save_path):
        return False
    
    file_size = os.path.getsize(save_path)
    if file_size < CONFIG["min_file_size"]:
        logger.warning(f"文件不完整（{file_size}字节 < {CONFIG['min_file_size']}字节），属于错误文件：{save_path}")
        if os.path.exists(save_path):
            os.remove(save_path)
            logger.info(f"已删除不完整的错误文件：{save_path}（同GK-2A清理规则）")
        return False
    
    with open(save_path, "rb") as f:
        magic = f.read(len(CONFIG["file_magic"]))
        if magic != CONFIG["file_magic"]:
            logger.warning(f"无效JPG文件（文件头：{magic.hex()}），属于错误文件：{save_path}")
            os.remove(save_path)
            logger.info(f"已删除非JPG格式的错误文件：{save_path}（同GK-2A清理规则）")
            return False
    
    logger.info(f"文件验证有效：{save_path}（永久保留，同GK-2A规则）")
    return True

def download_file(url: str, time_stamp: str, history: Dict[str, Any], is_recover: bool = False) -> bool:
    """下载图片（完全复制GK-2A的下载逻辑：重试+进度条+错误处理）"""
    prefix = "[补下载]" if is_recover else ""
    save_path = generate_save_path(time_stamp)
    
    logger.info(f"FY-4B {prefix} - 开始处理：{url}")
    logger.info(f"FY-4B {prefix} - 保存路径：{save_path}（有效文件永久保留）")
    if is_recover:
        logger.info(f"FY-4B {prefix} - 观测时间戳：{time_stamp}（UTC）")

    if os.path.exists(save_path) and is_file_valid(save_path):
        logger.info(f"FY-4B {prefix} - 文件已存在且有效，跳过下载：{save_path}（同GK-2A去重规则）")
        add_download_history(time_stamp, history)
        return True

    response = None
    for retry in range(MAX_RETRIES):
        try:
            response = requests.get(
                url, headers=HEADERS, timeout=60, stream=True
            )
            
            if response.status_code != 200:
                logger.error(f"FY-4B {prefix} - 下载失败（HTTP {response.status_code}）：{url}")
                continue

            total_size = int(response.headers.get("content-length", 0))
            downloaded_size = 0
            last_progress_time = 0

            with open(save_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=16384):
                    if chunk:
                        f.write(chunk)
                        downloaded_size += len(chunk)
                        
                        current_time = time.time()
                        if (current_time - last_progress_time >= DOWNLOAD_PROGRESS_INTERVAL) or (downloaded_size == total_size):
                            progress_bar = generate_download_progress_bar(downloaded_size, total_size)
                            logger.info(f"FY-4B {prefix} - 下载进度：{progress_bar}")
                            last_progress_time = current_time

            if is_file_valid(save_path):
                current_utc = datetime.datetime.now(datetime.UTC)
                logger.info(f"FY-4B {prefix} - 下载成功：{save_path}（有效文件，永久保留）")
                logger.info(f"FY-4B {prefix} - 当前UTC时间：{current_utc.strftime('%Y-%m-%d %H:%M:%S')}")
                logger.info(f"FY-4B {prefix} - " + "=" * 50)
                add_download_history(time_stamp, history)
                return True
            else:
                logger.warning(f"FY-4B {prefix} - 下载的文件为错误文件：{save_path}")
        
        except KeyboardInterrupt:
            logger.info(f"\nFY-4B {prefix} - 下载被手动中断，清理未完成的临时文件...")
            if os.path.exists(save_path) and os.path.getsize(save_path) < CONFIG["min_file_size"]:
                os.remove(save_path)
                logger.info(f"FY-4B {prefix} - 已删除未完成的临时文件：{save_path}（同GK-2A清理规则）")
            raise
        
        except RequestException as e:
            logger.error(f"FY-4B {prefix} - 下载失败（重试 {retry+1}/{MAX_RETRIES}）：{str(e)}")
            if retry < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY + retry * 2)
        except Exception as e:
            logger.error(f"FY-4B {prefix} - 下载异常（重试 {retry+1}/{MAX_RETRIES}）：{str(e)}", exc_info=True)
            if retry < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY + retry * 2)
        finally:
            if response:
                response.close()

    logger.error(f"FY-4B {prefix} - 所有重试失败：{url}")
    return False

# ==================== 补下载执行函数（12小时内，仅下载当前时间-1h之前的图片） ====================
def generate_missing_images(history: Dict[str, Any]) -> List[Tuple[str, str]]:
    """生成补下载列表：12小时内 + 观测时间 ≤ 当前时间-1小时 + 15分钟整点"""
    missing_images = []
    last_check_time_str = history["last_check_time"]
    
    # 核心阈值：当前时间-1小时 + 12小时内
    threshold_dt = datetime.datetime.now(datetime.UTC) - datetime.timedelta(hours=CONFIG["download_offset_hours"])
    earliest_dt = datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=CONFIG["max_recover_days"])
    
    if not last_check_time_str:
        start_dt = earliest_dt
    else:
        try:
            start_dt = datetime.datetime.strptime(last_check_time_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=datetime.timezone.utc)
            if start_dt < earliest_dt:
                start_dt = earliest_dt
        except:
            start_dt = earliest_dt
            logger.warning("最后检查时间格式错误，使用12小时前作为补下载起始时间（同GK-2A兼容逻辑）")
    
    current_dt = datetime.datetime.now(datetime.UTC)
    logger.info(f"补下载扫描条件：\n  - 时间范围：{earliest_dt.strftime('%Y-%m-%d %H:%M UTC')} 至 {threshold_dt.strftime('%Y-%m-%d %H:%M UTC')}\n  - 观测间隔：15分钟整点（00/15/30/45分）")
    
    # 生成符合条件的所有15分钟整点观测时间
    start_min = start_dt.minute
    nearest_min = (start_min // 15) * 15  # 取00/15/30/45分
    publish_dt = start_dt.replace(minute=nearest_min, second=0, microsecond=0)
    if publish_dt < start_dt:
        publish_dt += datetime.timedelta(minutes=CONFIG["time_interval"])
    
    # 遍历所有符合条件的观测时间
    while publish_dt <= threshold_dt:
        url, time_stamp = generate_url_by_time(publish_dt)
        if not is_downloaded(time_stamp, history):
            missing_images.append((url, time_stamp))
        
        publish_dt += datetime.timedelta(minutes=CONFIG["time_interval"])
    
    logger.info(f"补下载扫描发现 {len(missing_images)} 个未下载的符合条件图片")
    return missing_images

def execute_recover_download(history: Dict[str, Any]):
    """执行补下载（同GK-2A的补下载逻辑）"""
    global failed_tasks
    logger.info("=" * 80)
    logger.info("开始执行FY-4B全圆盘云图补下载（12小时内 + 当前时间-1h之前的图片）")
    logger.info("=" * 80)

    total_recover = 0
    total_success = 0
    total_fail = 0

    try:
        missing_images = generate_missing_images(history)
        for url, time_stamp in missing_images:
            total_recover += 1
            
            if time_stamp in failed_tasks:
                logger.info(f"图片已在失败队列中，跳过：{time_stamp}（UTC）")
                continue
            
            if download_file(url, time_stamp, history, is_recover=True):
                total_success += 1
            else:
                failed_tasks[time_stamp] = time.time()
                total_fail += 1
                logger.info(f"补下载失败，10分钟后重试：{time_stamp}（UTC）（同GK-2A重试规则）")
                logger.info("=" * 50)

    except KeyboardInterrupt:
        logger.info(f"\n补下载过程被手动中断，已完成 {total_success}/{total_recover} 个任务（未删除任何文件）")
        raise

    logger.info("=" * 80)
    logger.info("FY-4B全圆盘云图补下载执行完成")
    logger.info(f"总扫描未下载任务数：{total_recover}")
    logger.info(f"补下载成功数：{total_success}（文件均永久保留）")
    logger.info(f"补下载失败数（将重试）：{total_fail}")
    logger.info("=" * 80)

# ==================== 主程序（重点修改实时监控逻辑，精准匹配当前时间-1h） ====================
def main():
    current_utc = datetime.datetime.now(datetime.UTC)
    beijing_time = current_utc + datetime.timedelta(hours=8)
    logger.info("=" * 80)
    logger.info("FY-4B全圆盘云图自动下载脚本启动（完全对齐GK-2A规则，下载当前时间-1h的图片）")
    logger.info(f"当前UTC时间：{current_utc.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"当前北京时间：{beijing_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"保存目录：{SAVE_BASE_DIR}（有效文件永久保留，仅错误文件和临时文件会被删除）")
    logger.info(f"日志目录：{LOG_DIR}")
    logger.info(f"核心配置：观测间隔{CONFIG['time_interval']}分钟（00/15/30/45分） | 下载当前时间-{CONFIG['download_offset_hours']}小时的图片 | 补下载范围12小时内")
    logger.info("核心规则：已下载的有效文件绝不自动删除，仅清理错误文件和未完成的临时文件（同GK-2A）")
    logger.info("=" * 80)

    try:
        create_required_dirs()
    except Exception as e:
        logger.critical(f"初始化失败：{str(e)}", exc_info=True)
        return

    download_history = load_download_history()
    global failed_tasks
    failed_tasks = {}

    # 优先执行补下载（12小时内 + 当前时间-1h之前的图片）
    try:
        execute_recover_download(download_history)
    except KeyboardInterrupt:
        logger.info("\n" + "=" * 80)
        logger.info(f"脚本被手动中断，当前UTC：{current_utc.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("脚本已退出（未删除任何有效文件）")
        logger.info("=" * 80)
        return

    # 初始化运行时变量
    next_progress_refresh = time.time() + PROGRESS_BAR_INTERVAL
    next_check_time = time.time()  # 首次立即检查

    logger.info("\n开始实时监控FY-4B图片发布（下载当前时间-1h的15分钟整点图片）...")
    while True:
        try:
            current_utc = datetime.datetime.now(datetime.UTC)
            current_timestamp = time.time()

            # 进度条刷新（同GK-2A样式）
            if current_timestamp >= next_progress_refresh:
                logger.info("=" * 20 + " 进度刷新 " + "=" * 20)
                next_window, seconds_remaining, total_seconds = get_next_check_window(current_utc)
                if next_window:
                    progress_bar = generate_progress_bar(seconds_remaining, total_seconds)
                    logger.info(f"FY-4B | 距离下次检查：{progress_bar} | 下次检查：{next_window.strftime('%Y-%m-%d %H:%M:%S')} UTC")
                else:
                    logger.info("FY-4B | 无法计算下次检查时间")
                logger.info(f"当前未完成重试任务数：{len(failed_tasks)}")
                logger.info(f"核心规则：有效文件永久保留在 {SAVE_BASE_DIR}，仅错误文件和临时文件会被删除")
                logger.info("=" * 50)
                
                next_progress_refresh = current_timestamp + PROGRESS_BAR_INTERVAL

            # 失败任务重试（仅12小时内，同GK-2A）
            if failed_tasks:
                tasks_to_remove = []
                for time_stamp, first_fail_time in failed_tasks.items():
                    if current_timestamp - first_fail_time >= FAILED_RETRY_DELAY:
                        try:
                            publish_dt = datetime.datetime.strptime(time_stamp, "%Y%m%d%H%M").replace(tzinfo=datetime.timezone.utc)
                            # 仅重试12小时内的失败任务
                            earliest_dt = datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=CONFIG["max_recover_days"])
                            if publish_dt >= earliest_dt:
                                retry_url, _ = generate_url_by_time(publish_dt)
                            else:
                                logger.info(f"失败任务已超出12小时范围，不再重试：{time_stamp}（UTC）")
                                tasks_to_remove.append(time_stamp)
                                continue
                        except:
                            logger.error(f"重试失败：时间戳格式错误，跳过：{time_stamp}")
                            tasks_to_remove.append(time_stamp)
                            continue

                        if download_file(retry_url, time_stamp, download_history):
                            logger.info(f"重试成功：{time_stamp}（UTC）（文件永久保留）")
                        else:
                            logger.error(f"重试失败，已超出12小时范围，不再重试：{time_stamp}（UTC）")
                        
                        tasks_to_remove.append(time_stamp)

                for time_stamp in tasks_to_remove:
                    del failed_tasks[time_stamp]

            # 实时检查并下载：找当前时间-1小时对应的15分钟整点观测图片
            if current_timestamp >= next_check_time:
                # 计算当前时间-1小时的时间点
                target_dt = current_utc - datetime.timedelta(hours=CONFIG["download_offset_hours"])
                # 取target_dt对应的15分钟整点（00/15/30/45分）
                target_min = (target_dt.minute // 15) * 15
                publish_dt = target_dt.replace(minute=target_min, second=0, microsecond=0)
                
                # 双重验证：1. 观测时间在12小时内；2. 是15分钟整点
                earliest_dt = datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=CONFIG["max_recover_days"])
                if earliest_dt <= publish_dt <= target_dt:
                    url, time_stamp = generate_url_by_time(publish_dt)
                    if not is_downloaded(time_stamp, download_history):
                        # 直接下载（已满足当前时间-1h条件）
                        if not download_file(url, time_stamp, download_history):
                            failed_tasks[time_stamp] = current_timestamp
                            logger.info(f"首次下载失败，10分钟后重试：{time_stamp}（UTC）")
                            logger.info("=" * 50)
                    else:
                        logger.info(f"图片已下载，跳过：{time_stamp}（UTC）")
                else:
                    logger.info(f"无符合条件的图片：观测时间{publish_dt.strftime('%Y-%m-%d %H:%M UTC')} 超出12小时范围或未达当前时间-1h")
                
                # 更新下次检查时间
                next_check_time = current_timestamp + CONFIG["check_interval"]

            # 定期检查（减少CPU占用，同GK-2A）
            time.sleep(CHECK_INTERVAL)

        except KeyboardInterrupt:
            current_utc = datetime.datetime.now(datetime.UTC)
            logger.info("\n" + "=" * 80)
            logger.info(f"脚本被手动中断，当前UTC：{current_utc.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"未完成重试任务：{len(failed_tasks)}个")
            logger.info(f"重要提示：所有已下载的有效文件均已保留在 {SAVE_BASE_DIR}，未删除任何有效文件")
            logger.info("=" * 80)
            break
        except Exception as e:
            current_utc = datetime.datetime.now(datetime.UTC)
            logger.error("=" * 80)
            logger.error(f"主循环异常，当前UTC：{current_utc.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.error(f"错误信息：{str(e)}", exc_info=True)
            logger.error("重要提示：异常未影响已下载的有效文件，所有有效文件均已保留")
            logger.error("=" * 80)
            next_progress_refresh = time.time() + PROGRESS_BAR_INTERVAL
            time.sleep(CONFIG["check_interval"] // 2)

    logger.info("FY-4B全圆盘云图下载脚本已退出（所有有效文件均永久保留）")

if __name__ == "__main__":
    main()