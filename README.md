# GK-2A-Infrared-Cloud-Image-Automatic-Scraping-Script
用python脚本自动爬取GK-2A的红外增强卫星云图，可部署在服务器上

爬取示例：

GK-2A红外图片（10.5微米）

<img width="1375" height="1409" alt="gk2a_ami_le1b_ir105_fd020ge_202511141840 srv" src="https://github.com/user-attachments/assets/a072d95d-8837-4453-b6d6-ad844b6ce7ce" />

GK-2A全彩图片

<img width="1375" height="1409" alt="gk2a_ami_le1b_rgb-true_fd010ge_202511150230 srv" src="https://github.com/user-attachments/assets/11ccadae-5d22-4214-aa9d-ed1e76ed1bb1" />

FY4B全盘彩色图片

![FY4B_FullDisk_202511150445(1)](https://github.com/user-attachments/assets/282771f5-66ce-4021-862f-40633783a984)


在运行前请确保你已经安装python环境
# 安装依赖库
打开电脑终端，输入以下命令更新python和源（国外用户可忽略这一步）

python -m pip install --upgrade pip

pip config set global.index-url https://mirrors.tuna.tsinghua.edu.cn/pypi/web/simple

再输入以下命令安装依赖库

pip install requests
# 运行脚本
你可以选择以下两种运行方式：

1.临时运行（当你关闭终端后该程序结束运行，适用于windows，mac，linux）

python3 /你的路径/name.py

或者你用旧版本：python /你的路径/name.py

2.长期运行（关闭终端后脚本将始终在后台运行，直到关闭主机，适用于linux，mac）

nohup python3 /你的路径/name.py

或者你用旧版本：nohup python /你的路径/GK-2A_Infrared.py
