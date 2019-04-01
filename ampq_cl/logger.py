# !/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Wjy
import logging.config
import os
import platform


def get_logger(app_name, log_path, debug=False):
    """
    提供日志模块
    :param app_name: str 项目名
    :param log_path: str 日志路径
    :param debug: str 否是记录
    :return:
    """
    __PREFIX = "D:\\tyxb_tiantong_logs" if platform.system() == "Windows" else "%s/%s/%s/%s" % (
        log_path.rstrip("/"), app_name.strip("/"), os.environ["HOSTNAME"], os.getpid())

    # 根据系统修改文件生成路径
    __LOG_PATH_DEBUG = r"%s\debug.log" % __PREFIX if platform.system() == "Windows" else "%s/debug.log" % __PREFIX
    __LOG_PATH_INFO = r"%s\info.log" % __PREFIX if platform.system() == "Windows" else "%s/info.log" % __PREFIX
    __LOG_PATH_WARN = r"%s\warn.log" % __PREFIX if platform.system() == "Windows" else "%s/warn.log" % __PREFIX
    __LOG_PATH_ERROR = r"%s\error.log" % __PREFIX if platform.system() == "Windows" else "%s/error.log" % __PREFIX

    # 判断目录是否存在
    if not os.path.exists(__PREFIX):
        os.makedirs(__PREFIX)

    # 鏃ュ織閰嶇疆
    __LOGGING_CONFIG = {
        "version": 1,
        "disable_existing_loggers": True,
        "formatters": {
            "standard": {
                "format": "[%(asctime)s] %(levelname)s::(%(process)d %(thread)d)::%(module)s(%(funcName)s:%(lineno)d): %(message)s"
            },
        },
        "handlers": {
            "error": {
                "class": "logging.handlers.TimedRotatingFileHandler",
                "level": "ERROR",
                "formatter": "standard",
                "filename": __LOG_PATH_ERROR + "_file",
                "when": "H",
                "interval": 1
            },
            "warn": {
                "class": "logging.handlers.TimedRotatingFileHandler",
                "level": "WARN",
                "formatter": "standard",
                "filename": __LOG_PATH_WARN + "_file",
                "when": "H",
                "interval": 1
            },
            "info": {
                "class": "logging.handlers.TimedRotatingFileHandler",
                "level": "INFO",
                "formatter": "standard",
                "filename": __LOG_PATH_INFO + "_file",
                "when": "H",
                "interval": 1
            },
            "debug": {
                "class": "logging.handlers.TimedRotatingFileHandler",
                "level": "DEBUG",
                "formatter": "standard",
                "filename": __LOG_PATH_DEBUG + "_file",
                "when": "H",
                "interval": 1
            }
        },
        "loggers": {
            "default": {
                "handlers": ["debug", "info", "warn", "error"],
                "level": "INFO",
                "propagate": True
            },
            "enable_debug": {
                "handlers": ["debug", "info", "warn", "error"],
                "level": "DEBUG",
                "propagate": True
            }
        }
    }

    logging.config.dictConfig(__LOGGING_CONFIG)
    if debug:
        return logging.getLogger("enable_debug")
    else:
        return logging.getLogger("default")
