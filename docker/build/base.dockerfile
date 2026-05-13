## Base image for building the project
FROM  ubuntu:20.04
## 如果要使用显卡GPU 就需要使用nvidia的基础镜像（cuda）
## 同理华为昇腾NPU也需要使用昇腾的基础镜像

ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=Asia/Shanghai

SHELL ["/bin/bash", "-c"]

RUN apt-get clean && \
    apt-get autoclean
COPY apt/sources.list /etc/apt/

RUN apt-get update && \
    apt-get install -y \
    libssl-dev gcc g++ make gdb \
    curl \
    build-essential \
    libboost-all-dev \
    vim \
    libzmq3-dev \
    libgoogle-glog-dev \
    cmake \
    libbsd-dev

## 第三方源码库与docker构建环境交互一般逻辑
## 安装eventpp和simdjson：先拷贝本地代码到docker中，再执行安装脚本
COPY install/eventpp /tmp/install/eventpp
RUN /tmp/install/eventpp/install_eventpp.sh 

COPY install/simdjson /tmp/install/simdjson
RUN /tmp/install/simdjson/install_simdjson.sh 








