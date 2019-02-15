#!/usr/bin/env python
# -*- coding:utf-8 -*-
# Author: WangJY

from os.path import dirname, join
from pip.req import parse_requirements

# 包管理工具
from setuptools import (
    find_packages,
    setup
)

with open(join(dirname(__file__), './VERSION.txt'), 'rb') as f:
    version = f.read().decode('ascii').strip()
    '''
        作为一个合格的模块，应该有版本号
    '''

setup(
    name='mini_scrapy',  # 模块名
    version=version,  # 版本号
    description='A mini spider frameword, like scrapy',  # 描述
    packages=find_packages(exclude=[]),  # 包含目录中所有包，exclude=['哪些文件除外']
    author='WangJY',  # 作者
    author_email='wangjunyan456@gmail.com',  # 作者邮箱
    license='Apache License v2',
    package_data={'': ['*.*']},  # {'包名': ['正则']}
    url='#',
    install_requires=[str(ir.req) for ir in parse_requirements("requirements.txt",session=False)],  # 所需的运行环境
    zip_safe=False,
    classifiers=[
        'Programming Language :: Python',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: Unix',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
)