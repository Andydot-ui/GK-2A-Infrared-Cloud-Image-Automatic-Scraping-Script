import datetime
import requests
import os
import time
import logging
import json
from typing import Optional, Tuple, Dict, List, Any
from requests.exceptions import RequestException

# ==================== 核心配置（仅彩色图片） ====================
SAVE_BASE_DIR = "/DATA/Gallery/GK-2A/Color"  # 直接保存到彩色图片目录
LOG_DIR = "/DATA/logs/GK2A_Color"  # 独立日志目录
DOWNLOAD_HISTORY_FILE = os.path.join(LOG_DIR, "color_download_history.json")  # 独立历史记录

# 彩色图片专属配置
CONFIG = {
    "name": "GK2A_Color",
    "url_template": "https://nmsc.kma.go.kr/IMG/GK2A/AMI/PRIMARY/L1B/COMPLETE/FD/{ym}/{dd}/{hh}/gk2a_ami_le1b_rgb-true_fd010ge_{ymd_hh_nn}.srv.png",
    "publish_mins": [10, 30, 50],  # 每小时10/30/50分发布
    "download_delay_mins": 20,      # 发布后20分钟下载
    "min_file_size": 1024 * 100,   # 最小100KB
    "file_magic": b"\x89PNG"       # PNG文件头验证
}

# 通用配置
MAX_RETRIES = 3
RETRY_DELAY = 5
CHECK_INTERVAL = 10
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
HEADERS = {"User-Agent": USER_AGENT}
PROGRESS_BAR_INTERVAL = 60
PROGRESS_BAR_LENGTH = 50
FAILED_RETRY_DELAY = 600
DOWNLOAD_PROGRESS_INTERVAL = 2
MAX_RECOVER_DAYS = 30

# 全局变量
failed_tasks: Dict[str, float] = {}

# ==================== 日志配置（独立日志） ====================
def setup_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = os.path.join(LOG_DIR, "gk2a_color_download.log")
    
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s UTC - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    ))
    
    logger.addHandler(file_handler)
    return logger

logger = setup_logger()

# ==================== 工具函数 ====================
def create_required_dirs():
    """创建保存目录和日志目录"""
    try:
        os.makedirs(SAVE_BASE_DIR, exist_ok=True)
        logger.info(f"彩色图片保存目录准备就绪：{SAVE_BASE_DIR}")
        
        os.makedirs(LOG_DIR, exist_ok=True)
        logger.info(f"日志目录准备就绪：{LOG_DIR}")
    except Exception as e:
        logger.critical(f"创建目录失败！{str(e)}", exc_info=True)
        raise

def load_download_history() -> Dict[str, Any]:
    """加载独立的彩色图片下载历史"""
    default_history = {
        "downloaded": [],  # 已下载的发布时间列表（UTC）
        "last_download_time": (datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    }
    
    if not os.path.exists(DOWNLOAD_HISTORY_FILE):
        logger.info("彩色图片下载历史文件不存在，使用默认配置（补下载最近1天）")
        save_download_history(default_history)
        return default_history
    
    try:
        with open(DOWNLOAD_HISTORY_FILE, "r", encoding="utf-8") as f:
            history = json.load(f)
        
        # 兼容旧格式
        if "downloaded" not in history:
            history["downloaded"] = []
        if "last_download_time" not in history:
            history["last_download_time"] = default_history["last_download_time"]
        
        logger.info(f"成功加载彩色图片下载历史，已下载 {len(history['downloaded'])} 张")
        return history
    except Exception as e:
        logger.error(f"加载彩色图片下载历史失败，使用默认配置：{str(e)}")
        save_download_history(default_history)
        return default_history

def save_download_history(history: Dict[str, Any]):
    """保存独立的彩色图片下载历史"""
    try:
        with open(DOWNLOAD_HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(history, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"保存彩色图片下载历史失败：{str(e)}", exc_info=True)

def is_downloaded(pt_str: str, history: Dict[str, Any]) -> bool:
    """检查彩色图片是否已下载"""
    return pt_str in history["downloaded"]

def add_download_history(pt_str: str, history: Dict[str, Any]):
    """添加彩色图片到下载历史（仅清理历史记录，不删除文件）"""
    if pt_str not in history["downloaded"]:
        history["downloaded"].append(pt_str)
        # 仅清理历史记录列表（保留30天内的记录），不删除实际文件
        thirty_days_ago = (datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=MAX_RECOVER_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
        history["downloaded"] = [p for p in history["downloaded"] if p >= thirty_days_ago]
        logger.info(f"彩色图片 - 历史记录清理：保留最近30天记录，实际文件不删除")
    
    history["last_download_time"] = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M:%S")
    save_download_history(history)

def generate_missing_publish_times(history: Dict[str, Any]) -> List[datetime.datetime]:
    """生成未下载的彩色图片发布时间"""
    publish_mins = CONFIG["publish_mins"]
    missing_times = []
    
    last_dt_str = history["last_download_time"]
    try:
        last_dt = datetime.datetime.strptime(last_dt_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=datetime.timezone.utc)
    except:
        last_dt = datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=1)
        logger.warning("彩色图片 - 最后下载时间格式错误，使用1天前作为起始时间")
    
    current_dt = datetime.datetime.now(datetime.UTC)
    start_dt = max(last_dt, current_dt - datetime.timedelta(days=MAX_RECOVER_DAYS))
    
    logger.info(f"彩色图片 - 补下载时间范围：{start_dt.strftime('%Y-%m-%d %H:%M:%S')} UTC 至 {current_dt.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    
    current_check_dt = start_dt.replace(minute=0, second=0, microsecond=0)
    while current_check_dt <= current_dt:
        for min_val in publish_mins:
            publish_dt = current_check_dt.replace(minute=min_val, second=0, microsecond=0)
            download_ready_dt = publish_dt + datetime.timedelta(minutes=CONFIG["download_delay_mins"])
            if publish_dt >= start_dt and download_ready_dt <= current_dt:
                missing_times.append(publish_dt)
        current_check_dt += datetime.timedelta(hours=1)
    
    missing_times = list(sorted(list(set(missing_times))))
    logger.info(f"彩色图片 - 共发现 {len(missing_times)} 个未下载的发布时间点")
    return missing_times

# ==================== 进度条逻辑 ====================
def calculate_download_time(publish_min: int, delay_mins: int) -> int:
    return (publish_min + delay_mins) % 60

def get_next_download_window(current_time: datetime.datetime) -> Tuple[Optional[datetime.datetime], int, int]:
    publish_mins = CONFIG["publish_mins"]
    delay_mins = CONFIG["download_delay_mins"]
    next_window_start = None
    min_seconds_remaining = float("inf")

    for pt_min in publish_mins:
        dt_min = calculate_download_time(pt_min, delay_mins)
        pt_candidate = current_time.replace(minute=pt_min, second=0, microsecond=0)
        
        if pt_candidate < current_time:
            pt_candidate += datetime.timedelta(hours=1)
        
        dt_hour = pt_candidate.hour
        if pt_candidate.minute + delay_mins >= 60:
            dt_hour = (dt_hour + 1) % 24
        
        dt_candidate = pt_candidate.replace(hour=dt_hour, minute=dt_min, second=0, microsecond=0)
        window_start = dt_candidate - datetime.timedelta(seconds=60)
        
        if window_start < current_time:
            window_start += datetime.timedelta(hours=1)
        
        seconds_remaining = int((window_start - current_time).total_seconds())
        if seconds_remaining < 0:
            seconds_remaining = 0
        
        if seconds_remaining < min_seconds_remaining:
            min_seconds_remaining = seconds_remaining
            next_window_start = window_start

    if not next_window_start:
        return None, 0, 0

    total_seconds = int((next_window_start - current_time).total_seconds())
    return next_window_start, min_seconds_remaining, total_seconds

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

# ==================== 下载核心函数 ====================
def is_file_valid(save_path: str) -> bool:
    """验证彩色图片文件有效性（仅删除错误文件）"""
    if not os.path.exists(save_path):
        return False
    file_size = os.path.getsize(save_path)
    if file_size < CONFIG["min_file_size"]:
        logger.warning(f"彩色图片 - 文件不完整（{file_size}字节 < {CONFIG['min_file_size']}字节）：{save_path}")
        # 仅删除不完整的错误文件，有效文件不删除
        if os.path.exists(save_path):
            os.remove(save_path)
            logger.info(f"彩色图片 - 已删除不完整的错误文件：{save_path}")
        return False
    
    # 验证PNG文件头（仅删除非PNG格式的错误文件）
    with open(save_path, "rb") as f:
        magic = f.read(len(CONFIG["file_magic"]))
        if magic != CONFIG["file_magic"]:
            logger.warning(f"彩色图片 - 无效PNG文件（文件头：{magic.hex()}）：{save_path}")
            os.remove(save_path)
            logger.info(f"彩色图片 - 已删除非PNG格式的错误文件：{save_path}")
            return False
    
    return True

def generate_url_and_save_path(pt: datetime.datetime) -> tuple[str, str]:
    """生成彩色图片URL和保存路径"""
    ym = pt.strftime("%Y%m")
    dd = pt.strftime("%d")
    hh = pt.strftime("%H")
    ymd_hh_nn = pt.strftime("%Y%m%d%H%M")
    
    url = CONFIG["url_template"].format(ym=ym, dd=dd, hh=hh, ymd_hh_nn=ymd_hh_nn)
    filename = os.path.basename(url)
    save_path = os.path.join(SAVE_BASE_DIR, filename)
    return url, save_path

def download_file(url: str, save_path: str, pt: datetime.datetime, pt_str: str, history: Dict[str, Any], is_recover: bool = False) -> bool:
    """下载彩色图片（仅删除临时文件和错误文件）"""
    prefix = "[补下载]" if is_recover else ""
    logger.info(f"彩色图片 {prefix} - 开始下载：{url}")
    logger.info(f"彩色图片 {prefix} - 保存路径：{save_path}")
    if is_recover:
        logger.info(f"彩色图片 {prefix} - 发布时间：{pt_str} UTC")

    response = None
    for retry in range(MAX_RETRIES):
        try:
            response = requests.get(
                url, headers=HEADERS, timeout=30, stream=True
            )
            
            # 检查HTTP状态码
            if response.status_code != 200:
                logger.error(f"彩色图片 {prefix} - 下载失败（HTTP {response.status_code}）：{url}")
                continue

            total_size = int(response.headers.get("content-length", 0))
            downloaded_size = 0
            last_progress_time = 0

            with open(save_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded_size += len(chunk)
                        
                        current_time = time.time()
                        if (current_time - last_progress_time >= DOWNLOAD_PROGRESS_INTERVAL) or (downloaded_size == total_size):
                            progress_bar = generate_download_progress_bar(downloaded_size, total_size)
                            logger.info(f"彩色图片 {prefix} - 下载进度：{progress_bar}")
                            last_progress_time = current_time

            if is_file_valid(save_path):
                current_utc = datetime.datetime.now(datetime.UTC)
                logger.info(f"彩色图片 {prefix} - 下载成功：{save_path}（文件永久保留，不自动删除）")
                logger.info(f"彩色图片 {prefix} - 当前UTC时间：{current_utc.strftime('%Y-%m-%d %H:%M:%S')}")
                logger.info(f"彩色图片 {prefix} - " + "=" * 50)
                add_download_history(pt_str, history)
                return True
            else:
                logger.warning(f"彩色图片 {prefix} - 下载的文件无效：{save_path}")
        
        except KeyboardInterrupt:
            logger.info(f"\n彩色图片 {prefix} - 下载被手动中断，清理临时文件...")
            # 仅删除未下载完成的临时文件，已完成的有效文件不删除
            if os.path.exists(save_path) and os.path.getsize(save_path) < CONFIG["min_file_size"]:
                os.remove(save_path)
                logger.info(f"彩色图片 {prefix} - 已删除未完成的临时文件：{save_path}")
            raise
        
        except RequestException as e:
            logger.error(f"彩色图片 {prefix} - 下载失败（重试 {retry+1}/{MAX_RETRIES}）：{str(e)}")
            if retry < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY + retry * 2)
        except Exception as e:
            logger.error(f"彩色图片 {prefix} - 下载异常（重试 {retry+1}/{MAX_RETRIES}）：{str(e)}", exc_info=True)
            if retry < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY + retry * 2)
        finally:
            if response:
                response.close()

    logger.error(f"彩色图片 {prefix} - 所有重试失败：{url}")
    return False

def check_download_window(current_time: datetime.datetime) -> Optional[datetime.datetime]:
    """检查当前是否在彩色图片下载窗口内"""
    current_min = current_time.minute
    current_hour = current_time.hour
    publish_mins = CONFIG["publish_mins"]
    delay_mins = CONFIG["download_delay_mins"]

    for pt_min in publish_mins:
        dt_min = calculate_download_time(pt_min, delay_mins)
        dt_hour = current_hour if (pt_min + delay_mins) < 60 else (current_hour + 1) % 24

        dt = current_time.replace(hour=dt_hour, minute=dt_min, second=0, microsecond=0)
        if dt < current_time:
            dt += datetime.timedelta(days=1)

        window_start = dt - datetime.timedelta(seconds=60)
        window_end = dt + datetime.timedelta(seconds=60)

        if window_start <= current_time <= window_end:
            pt_hour = current_hour if (pt_min + delay_mins) < 60 else (current_hour - 1) % 24
            pt = current_time.replace(hour=pt_hour, minute=pt_min, second=0, microsecond=0)
            if pt > current_time:
                pt -= datetime.timedelta(days=1)
            return pt

    return None

# ==================== 补下载执行函数 ====================
def execute_recover_download(history: Dict[str, Any]):
    """执行彩色图片补下载"""
    global failed_tasks
    logger.info("=" * 80)
    logger.info("开始执行GK-2A彩色图片补下载")
    logger.info("=" * 80)

    total_recover = 0
    total_success = 0
    total_fail = 0

    try:
        missing_times = generate_missing_publish_times(history)
        for pt in missing_times:
            pt_str = pt.strftime("%Y-%m-%d %H:%M:%S")
            total_recover += 1
            
            if is_downloaded(pt_str, history):
                logger.info(f"彩色图片 - 已下载，跳过：{pt_str} UTC（文件永久保留）")
                continue
            
            url, save_path = generate_url_and_save_path(pt)
            if os.path.exists(save_path):
                # 若文件已存在，即使不在历史记录中，也视为已下载（不重复下载）
                logger.info(f"彩色图片 - 文件已存在，视为已下载：{save_path}（文件永久保留）")
                add_download_history(pt_str, history)
                total_success += 1
                continue
            
            if download_file(url, save_path, pt, pt_str, history, is_recover=True):
                total_success += 1
            else:
                failed_tasks[pt_str] = time.time()
                total_fail += 1
                logger.info(f"彩色图片 - 补下载失败，10分钟后重试：{pt_str} UTC")
                logger.info(f"彩色图片 - " + "=" * 50)

    except KeyboardInterrupt:
        logger.info(f"\n补下载过程被手动中断，已完成 {total_success}/{total_recover} 个任务")
        raise

    logger.info("=" * 80)
    logger.info("GK-2A彩色图片补下载执行完成")
    logger.info(f"总扫描未下载任务数：{total_recover}")
    logger.info(f"补下载成功数：{total_success}")
    logger.info(f"补下载失败数（将重试）：{total_fail}")
    logger.info("=" * 80)

# ==================== 主程序 ====================
def main():
    current_utc = datetime.datetime.now(datetime.UTC)
    logger.info("=" * 80)
    logger.info("GK-2A彩色图片自动下载脚本启动")
    logger.info(f"当前UTC时间：{current_utc.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"保存目录：{SAVE_BASE_DIR}（已下载文件永久保留，不自动删除）")
    logger.info(f"日志目录：{LOG_DIR}")
    logger.info(f"补下载最大时间范围：{MAX_RECOVER_DAYS}天")
    logger.info("=" * 80)

    try:
        create_required_dirs()
    except Exception as e:
        logger.critical(f"初始化失败：{str(e)}", exc_info=True)
        return

    # 加载独立历史记录
    download_history = load_download_history()
    global failed_tasks
    failed_tasks = {}

    # 优先执行补下载
    try:
        execute_recover_download(download_history)
    except KeyboardInterrupt:
        logger.info("\n" + "=" * 80)
        logger.info(f"脚本被手动中断，当前UTC：{current_utc.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("脚本已退出")
        logger.info("=" * 80)
        return

    # 初始化运行时变量
    next_progress_refresh = time.time() + PROGRESS_BAR_INTERVAL

    logger.info("\n开始实时监控彩色图片下载窗口...")
    while True:
        try:
            current_utc = datetime.datetime.now(datetime.UTC)
            current_timestamp = time.time()

            # 进度条刷新
            if current_timestamp >= next_progress_refresh:
                logger.info("=" * 20 + " 进度刷新 " + "=" * 20)
                next_window, seconds_remaining, total_seconds = get_next_download_window(current_utc)
                if next_window:
                    progress_bar = generate_progress_bar(seconds_remaining, total_seconds)
                    logger.info(f"彩色图片 | 距离下次下载窗口：{progress_bar} | 下次窗口：{next_window.strftime('%Y-%m-%d %H:%M:%S')} UTC")
                else:
                    logger.info(f"彩色图片 | 无法计算下次下载窗口")
                logger.info(f"当前未完成重试任务数：{len(failed_tasks)}")
                logger.info(f"提示：已下载的有效文件永久保留在 {SAVE_BASE_DIR}")
                logger.info("=" * 50)
                
                next_progress_refresh = current_timestamp + PROGRESS_BAR_INTERVAL

            # 失败任务重试
            if failed_tasks:
                tasks_to_remove = []
                for pt_str, first_fail_time in failed_tasks.items():
                    if current_timestamp - first_fail_time >= FAILED_RETRY_DELAY:
                        pt = datetime.datetime.strptime(pt_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=datetime.timezone.utc)
                        url, save_path = generate_url_and_save_path(pt)
                        
                        if os.path.exists(save_path):
                            logger.info(f"彩色图片 - 重试时发现文件已存在，视为成功：{save_path}（文件永久保留）")
                            add_download_history(pt_str, download_history)
                            tasks_to_remove.append(pt_str)
                            continue
                        
                        if download_file(url, save_path, pt, pt_str, download_history):
                            logger.info(f"彩色图片 - 重试成功：{pt_str} UTC（文件永久保留）")
                        else:
                            logger.error(f"彩色图片 - 重试失败，跳过：{pt_str} UTC")
                        
                        tasks_to_remove.append(pt_str)

                for pt_str in tasks_to_remove:
                    del failed_tasks[pt_str]

            # 实时下载逻辑
            pt = check_download_window(current_utc)
            if pt:
                pt_str = pt.strftime("%Y-%m-%d %H:%M:%S")
                task_key = pt_str

                if is_downloaded(pt_str, download_history) or task_key in failed_tasks:
                    continue

                url, save_path = generate_url_and_save_path(pt)
                if os.path.exists(save_path):
                    logger.info(f"彩色图片 - 文件已存在，视为已下载：{save_path}（文件永久保留）")
                    add_download_history(pt_str, download_history)
                    continue

                if not download_file(url, save_path, pt, pt_str, download_history):
                    failed_tasks[task_key] = current_timestamp
                    logger.info(f"彩色图片 - 首次下载失败，10分钟后重试：{pt_str} UTC")
                    logger.info(f"彩色图片 - " + "=" * 50)

            time.sleep(CHECK_INTERVAL)

        except KeyboardInterrupt:
            current_utc = datetime.datetime.now(datetime.UTC)
            logger.info("\n" + "=" * 80)
            logger.info(f"脚本被手动中断，当前UTC：{current_utc.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"未完成重试任务：{len(failed_tasks)}个")
            logger.info(f"提示：已下载的有效文件永久保留在 {SAVE_BASE_DIR}，未删除任何有效文件")
            logger.info("=" * 80)
            break
        except Exception as e:
            current_utc = datetime.datetime.now(datetime.UTC)
            logger.error("=" * 80)
            logger.error(f"主循环异常，当前UTC：{current_utc.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.error(f"错误信息：{str(e)}", exc_info=True)
            logger.error("=" * 80)
            next_progress_refresh = time.time() + PROGRESS_BAR_INTERVAL
            time.sleep(CHECK_INTERVAL * 3)

    logger.info("GK-2A彩色图片下载脚本已退出（已下载文件永久保留）")

if __name__ == "__main__":
    main()