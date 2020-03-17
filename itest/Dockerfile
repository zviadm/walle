FROM ubuntu:18.04

# APT Dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
		g++ \
		gcc \
		libc6-dev \
		make \
		pkg-config \
		libsnappy-dev \
	&& rm -rf /var/lib/apt/lists/*

WORKDIR /root

# Compilers/Tools
ADD https://dl.google.com/go/go1.14.linux-amd64.tar.gz ./
RUN tar -xvzf go1.14.linux-amd64.tar.gz \
	&& mv go go1.4 \
	&& rm go1.14.linux-amd64.tar.gz

# ThirdParty Dependencies. Sort dependencies: Slowest->Fastest.
ADD http://source.wiredtiger.com/releases/wiredtiger-3.2.1.tar.bz2 ./
RUN tar -xvjf wiredtiger-3.2.1.tar.bz2 \
	&& cd wiredtiger-3.2.1 \
	&& ./configure -with-builtins=snappy -disable-shared \
	&& make \
	&& cp ./libwiredtiger.la /usr/local/lib/libwiredtiger.la \
	&& cp ./ext/compressors/snappy/libwiredtiger_snappy.la /usr/local/lib/libwiredtiger_snappy.la