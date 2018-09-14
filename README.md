# 资讯平台Spider
---
资讯平台Spider用来实现资讯平台的数据收集和结构化存储。本项目是基于centos7+mysql+python2.7+scrapy框架实现的。

## 目录

## 项目结构说明
---
spider_news_all\
├──spider_news_all\
│   ├── commands 
│   │   ├──crawlAll.py 一次性启动所有爬虫\
│   ├── spiders 各网站爬虫\
│   │   ├── xxx.py xxx网站爬取脚本\
│   ├── config.py 数据库配置文件\
│   ├── items.py 爬取字段设置文件\
│   ├── pipelines.py 数据管道\
│   ├── settings.py scrapy配置文件\
├──misc
│   ├── requirements.txt 项目所需依赖\
├──README.md 项目说明文档\

## 使用说明
---
### 安装项目依赖

```shell
pip install -r requirements.txt
```
