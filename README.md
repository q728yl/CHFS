# CHFS
基于Inode的分布式文件系统

CHFS是类似GFS的分布式文件系统，基于	inode	，支持	mkdir,	symlink,	create,	read,	write

使用	redo-log	实现	Atomicity

实现基于	raft	协议的	Key-Value	存储

实现	MapReduce	工具，通过	RPC	调用实现分布式系统上的单词数量统计功能。
