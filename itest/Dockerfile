FROM ubuntu:18.04

# APT dependencies for building compilers, build tools.
RUN apt-get update && apt-get install -y --no-install-recommends \
		g++ \
		gcc \
		libc6-dev \
		make \
		pkg-config \
		autoconf \
		automake \
		git \
		python
WORKDIR /root

# Compilers and build tools
ADD https://dl.google.com/go/go1.14.linux-amd64.tar.gz ./
RUN tar -xvzf go1.14.linux-amd64.tar.gz \
	&& mv go go1.4 \
	&& rm go1.14.linux-amd64.tar.gz

# ThirdParty dependencies. Sort dependencies: Slowest->Fastest.
RUN apt-get install -y --no-install-recommends libsnappy-dev
ADD http://source.wiredtiger.com/releases/wiredtiger-3.2.1.tar.bz2 ./
RUN tar -xvjf wiredtiger-3.2.1.tar.bz2 \
	&& cd wiredtiger-3.2.1 \
	&& ./configure -with-builtins=snappy -disable-shared \
	&& make \
	&& make install

# Extra tools.
RUN apt-get update && apt-get install -y --no-install-recommends \
	git \
	silversearcher-ag

# Environment variables.
ENV GOPATH=/root/.cache/goroot:/root/src