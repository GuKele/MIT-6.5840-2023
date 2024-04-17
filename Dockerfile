FROM golang:1.21.0

WORKDIR /

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get -y update && \
    apt -y upgrade && \
    apt-get -y install \
      build-essential \
      doxygen \
      git \
      vim \
      curl \
      gdb && \
    echo -e "export LC_ALL="zh_CN.utf8" \n LANG="zh_CH.utf8" \n echo $LANG $LC_ALL" >> /etc/profile && \
    go env -w GO111MODULE=on && \
    go env -w GOPROXY=https://goproxy.cn,direct

CMD bash
