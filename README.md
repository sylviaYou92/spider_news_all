# 资讯平台Spider
资讯平台Spider用来实现资讯平台的数据收集和结构化存储。本项目是基于centos7+mysql+python2.7+scrapy框架实现的。

## 目录

## 项目结构说明
spider_news_all\
├──spider_news_all\
│   ├── commands\
│   │   ├──crawlAll.py 一次性启动所有爬虫\
│   ├── spiders 各网站爬虫\
│   │   ├── xxx.py xxx网站爬取脚本\
│   ├── config.py 数据库配置文件\
│   ├── items.py 爬取字段设置文件\
│   ├── pipelines.py 数据管道\
│   ├── settings.py scrapy配置文件\
├──misc\
│   ├── requirements.txt 项目所需依赖\
├──README.md 项目说明文档\

## 使用说明
### 安装项目依赖
本项目所需的依赖均写在misc/requirements.txt中，在目录下执行以下命令即可安装所需依赖：
```shell
pip install -r requirements.txt
```
### 启动爬虫
本项目支持启动单个爬虫和所有爬虫，在项目目录下执行以下命令即可启动爬虫：
```shell
scrapy crawlAll #启动所有爬虫
```
```shell
scrapy crawl xxx #启动单个爬虫
scrapy crawl infoq #以启动infoq为例
```
### 设置定时爬虫
本项目通过linux crontab实现定时爬虫，目前设置每30分钟全量执行一次爬虫：
```shell
*/30 * * * * cd /usr/local/opt/spider_news_all/spider_news_all && scrapy crawlAll > /usr/local/opt/spider_log/out.log 2>&1
```
### 添加新的爬虫
将新的爬虫文件(.py)放在spiders/中即可，爬虫会在下个定时中自动加入执行。