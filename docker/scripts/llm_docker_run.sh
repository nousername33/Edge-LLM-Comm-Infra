#!/usr/bin/env bash
MONITOR_HOME_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/../.." && pwd )"

## 获取系统用户信息 保证容器内外用户一致 避免权限问题
local_host="$(hostname)"
user="${USER}"
uid="$(id -u)"
group="$(id -g -n)"
gid="$(id -g)"

## 每次运行前先清理之前的容器
echo "stop and rm docker" 
docker stop llm > /dev/null
docker rm -v -f llm > /dev/null

echo "start docker"
docker run -it -d \
--name llm \
-e DISPLAY=$display \
--privileged=true \
-e DOCKER_USER="${user}" \
-e USER="${user}" \
-e DOCKER_USER_ID="${uid}" \
-e DOCKER_GRP="${group}" \
-e DOCKER_GRP_ID="${gid}" \
-e XDG_RUNTIME_DIR=$XDG_RUNTIME_DIR \
-v ${MONITOR_HOME_DIR}:/work \ ## 将当前项目目录挂载到docker
-v ${XDG_RUNTIME_DIR}:${XDG_RUNTIME_DIR} \
--net host \
llm:v1.0
