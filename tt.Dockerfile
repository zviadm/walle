FROM ubuntu:18.04

# Compilers and Build tools
WORKDIR /root
RUN apt-get update && apt-get install -y --no-install-recommends \
		autoconf \
		automake \
		ca-certificates \
		g++ \
		gcc \
		git \
		libc6-dev \
		libtool \
		make \
		pkg-config \
		python

ADD https://dl.google.com/go/go1.14.linux-amd64.tar.gz ./
RUN tar -xvzf go1.14.linux-amd64.tar.gz \
	&& mv go go1.14 \
	&& rm go1.14.linux-amd64.tar.gz

# ThirdParty dependencies. Sort dependencies: Slowest->Fastest.
RUN apt-get install -y --no-install-recommends \
		libjemalloc-dev \
		libsnappy-dev
RUN git clone --depth 1 https://github.com/wiredtiger/wiredtiger.git --branch mongodb-4.5.0 --single-branch
RUN cd wiredtiger \
	&& sh ./autogen.sh \
	&& env \
		CFLAGS="-fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free" \
		LDFLAGS="-ljemalloc" \
		./configure -with-builtins=snappy -disable-shared \
	&& make install

# Extra tools.
RUN apt-get update && apt-get install -y --no-install-recommends \
	iptables \
	less \
	silversearcher-ag

# Environment variables.
ENV GOPATH=/root/.cache/goroot:/root/src
ENV PATH=$PATH:/root/.cache/goroot/bin
