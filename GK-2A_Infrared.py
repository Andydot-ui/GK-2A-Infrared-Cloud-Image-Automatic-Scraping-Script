import os
import time
import datetime
import requests
from pathlib import Path

class GK2ADownloader:
    def __init__(self):
        # 创建保存目录
        self.save_dir = Path("/volume1/Cloud/图片/GK-2A/红外增强图片")
        self.save_dir.mkdir(parents=True, exist_ok=True)
        
        # 真彩图下载时间配置
        self.color_update_minutes = [00, 20, 40]  # 真彩图更新时间
        self.download_delay_minutes = 20  # 下载延迟时间
        
    def get_previous_update_time(self, update_minutes):
        """获取前一个更新时间点"""
        # 使用带时区的时间
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        current_hour = now_utc.hour
        current_minute = now_utc.minute
        
        # 找到当前时间之前最近的更新时间点
        prev_minutes = [m for m in update_minutes if m <= current_minute]
        if prev_minutes:
            target_minute = max(prev_minutes)
            target_time = now_utc.replace(minute=target_minute, second=0, microsecond=0)
        else:
            # 如果当前时间早于今天的所有更新时间，则使用前一小时的最后一个更新时间
            prev_hour = now_utc - datetime.timedelta(hours=1)
            target_minute = max(update_minutes)
            target_time = prev_hour.replace(minute=target_minute, second=0, microsecond=0)
            
        return target_time
    
    def get_next_download_time(self):
        """获取下一个下载时间点（发布时间+延迟）"""
        # 使用带时区的时间
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        
        # 找到下一个发布时间点
        next_update_time = self.get_next_update_time(self.color_update_minutes)
        
        # 计算下载时间（发布时间+延迟）
        download_time = next_update_time + datetime.timedelta(minutes=self.download_delay_minutes)
        
        return download_time
    
    def get_next_update_time(self, update_minutes):
        """获取下一个更新时间点"""
        # 使用带时区的时间
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        current_minute = now_utc.minute
        current_hour = now_utc.hour
        
        # 找到当前时间之后最近的更新时间点
        next_minutes = [m for m in update_minutes if m > current_minute]
        if next_minutes:
            target_minute = min(next_minutes)
            target_time = now_utc.replace(minute=target_minute, second=0, microsecond=0)
        else:
            # 如果当前时间晚于当前小时的所有更新时间，则使用下一小时的第一个更新时间
            next_hour = now_utc + datetime.timedelta(hours=1)
            target_minute = min(update_minutes)
            target_time = next_hour.replace(minute=target_minute, second=0, microsecond=0)
            
        return target_time
    
    def generate_url(self, target_time):
        """生成下载URL"""
        yyyymm = target_time.strftime("%Y%m")
        dd = target_time.strftime("%d")
        hh = target_time.strftime("%H")
        nn = target_time.strftime("%M")
        yyyymmddhhnn = target_time.strftime("%Y%m%d%H%M")
        
        url = f"https://nmsc.kma.go.kr/IMG/GK2A/AMI/PRIMARY/L1B/COMPLETE/FD/{yyyymm}/{dd}/{hh}/gk2a_ami_le1b_enhc-color-ir105_fd020ge_{yyyymmddhhnn}.srv.png"
        return url
    
    def get_filename(self, target_time):
        """生成文件名"""
        yyyymmddhhnn = target_time.strftime("%Y%m%d%H%M")
        return f"gk2a_ami_le1b_enhc-color-ir105_fd020ge_{yyyymmddhhnn}.srv.png"
    
    def download_image(self, url, filepath):
        """下载图像"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            with open(filepath, 'wb') as f:
                f.write(response.content)
            
            print(f"成功下载: {filepath}")
            file_size = os.path.getsize(filepath) / 1024  # KB
            print(f"文件大小: {file_size:.1f} KB")
            return True
            
        except requests.exceptions.RequestException as e:
            print(f"下载失败 {url}: {e}")
            return False
    
    def check_and_download(self):
        """检查并下载最新图像"""
        # 获取上一个更新时间点
        update_time = self.get_previous_update_time(self.color_update_minutes)
        
        # 检查是否已经过了延迟时间
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        download_time = update_time + datetime.timedelta(minutes=self.download_delay_minutes)
        
        if now_utc < download_time:
            # 还没到下载时间，等待
            wait_seconds = (download_time - now_utc).total_seconds()
            print(f"等待 {wait_seconds/60:.1f} 分钟直到下载时间: {download_time.strftime('%Y-%m-%d %H:%M')} UTC")
            return False
        
        # 生成URL和文件路径
        url = self.generate_url(update_time)
        filename = self.get_filename(update_time)
        filepath = self.save_dir / filename
        
        # 检查文件是否已存在
        if filepath.exists():
            file_size = os.path.getsize(filepath) / 1024
            print(f"文件已存在: {filename} ({file_size:.1f} KB)")
            return True
        
        # 下载图像
        print(f"尝试下载: {filename} (发布于 {update_time.strftime('%Y-%m-%d %H:%M')} UTC)")
        success = self.download_image(url, filepath)
        
        if success:
            # 如果下载成功，检查文件是否有效（大小合理）
            file_size = os.path.getsize(filepath)
            if file_size < 1024:  # 小于1KB可能是错误页面
                print("下载的文件可能无效，删除...")
                os.remove(filepath)
                return False
        return success
    
    def calculate_sleep_time(self):
        """计算需要休眠的时间"""
        # 获取下一个下载时间点
        next_download = self.get_next_download_time()
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        
        # 计算需要休眠的秒数
        sleep_seconds = (next_download - now_utc).total_seconds()
        
        return max(sleep_seconds, 60)  # 最少休眠1分钟
    
    def run(self):
        """主运行循环"""
        print("GK-2A真彩气象云图下载器开始运行...")
        print("真彩图发布时间: 每小时的10, 30, 50分 (UTC)")
        print(f"下载延迟: {self.download_delay_minutes} 分钟")
        print(f"实际下载时间: 每小时的25, 45, 05分 (UTC)")
        print(f"保存路径: {self.save_dir}")
        print("程序将24小时不间断运行")
        print("按 Ctrl+C 停止程序")
        
        try:
            while True:
                print(f"\n[{datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC] 检查更新...")
                
                self.check_and_download()
                
                sleep_time = self.calculate_sleep_time()
                next_check = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=sleep_time)
                print(f"下一次下载时间: {next_check.strftime('%Y-%m-%d %H:%M:%S')} UTC")
                print(f"休眠 {sleep_time/60:.1f} 分钟...")
                
                time.sleep(sleep_time)
                
        except KeyboardInterrupt:
            print("\n程序被用户中断")
        except Exception as e:
            print(f"程序运行出错: {e}")
            print("60秒后重新启动...")
            time.sleep(60)
            self.run()

if __name__ == "__main__":
    downloader = GK2ADownloader()
    downloader.run()