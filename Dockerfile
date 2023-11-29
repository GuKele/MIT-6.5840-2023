# 1.选择基础镜像： 在 Dockerfile 中使用 FROM 命令选择一个适当的基础镜像作为起点。例如，选择一个包含特定操作系统和工具的基础镜像。
FROM golang:1.21.0

# 2.设置工作目录： 使用 WORKDIR 命令设置容器内的工作目录，使后续命令的执行路径在该目录下。
WORKDIR /

# 3.安装依赖软件： 更换镜像源，使用适当的包管理工具（如 apt-get、yum、pip 等）安装所需的依赖软件和工具。

# Install Ubuntu packages.
# Please add packages in alphabetical order.
ARG DEBIAN_FRONTEND=noninteractive
# RUN cp /etc/apt/sources.list /etc/apt/sources.list.bak

# RUN sed -i 's#http://archive.ubuntu.com/#http://mirrors.tuna.tsinghua.edu.cn/#' /etc/apt/sources.list
# RUN sed -i 's/us.archive.ubuntu.com/mirrors.aliyun.com/g' /etc/apt/sources.list; \
# sed -i 's/cn.archive.ubuntu.com/mirrors.aliyun.com/g' /etc/apt/sources.list; \
# sed -i 's/archive.ubuntu.com/mirrors.aliyun.com/g' /etc/apt/sources.list; \
# sed -i 's/security.ubuntu.com/mirrors.aliyun.com/g' /etc/apt/sources.list;

RUN apt-get -y update && \
    apt -y upgrade && \
    apt-get -y install \
      build-essential \
      doxygen \
      git \
      vim \
      curl \
      gdb \
      fish && \
    echo -e "export LC_ALL="zh_CN.utf8" \n LANG="zh_CH.utf8" \n echo $LANG $LC_ALL" >> /etc/profile && \
    go env -w GO111MODULE=on && \
    go env -w GOPROXY=https://goproxy.cn,direct

# curl -L https://get.oh-my.fish | fish && \
# go env -w GOPROXY=https://mirrors.aliyun.com/goproxy/,direct
# go的tools例如gopls、gotests、delve等等，直接用go插件来安装

# 4.复制文件： 使用 COPY 命令将本地文件或目录复制到镜像中的指定路径。

# 5.设置环境变量： 使用 ENV 命令设置环境变量，以供容器内的程序使用。
# ENV GOPATH=/go

# 6.运行命令： 使用 RUN 命令运行命令，例如编译代码、执行安装操作等。

# 7.容器启动命令： 使用 CMD 或 ENTRYPOINT 命令设置容器启动时要执行的命令。
CMD bash

# 8.开放端口： 使用 EXPOSE 命令声明容器将监听的端口。

# 首先要介绍一下匿名挂载和具名挂载: 挂载就是宿主机和容器公用同一文件从而实现数据同步与持久。
#    匿名挂载就是挂载时不指定宿主机文件路径<容器卷>，只指定容器内数据卷的路径文件名，这样容器内的数据卷初始化后是不包含初始数据（空的），而与宿主机那个文件进行挂载由docker自己来决定，可以用docker inspect命令查看。
#    具名挂载是指挂载时还指定宿主文件路径的挂载<宿主机卷:容器卷>，这样可以初始化而且也方便你在宿主机直接修改数据。
# 9.挂载卷：在Dokcerfile中使用VOLUME创建的挂载点是匿名挂载，并且使用该镜像的创造的每一个容器都必须挂载，其实不太方便。
# 如果需要在挂载点中提供初始数据，可以使用 -v 参数或 --mount 参数将主机目录或文件与容器内部的目录关联，实现数据共享和持久化。
# 建议在run构建容器时-v挂载项目代码，这样就是持久的挂载。而start容器时--mount挂载是临时性的挂载。
# VOLUME [<容器内数据卷路径1>, <容器内数据卷路径2>...]

# 10.其他配置操作： 根据实际需要，您可以在 Dockerfile 中编写其他命令，以便构建出符合预期的镜像。
