# project 1

有的笔记是自己写的，有的是缝合的。

## 一些基本概念

### ProtoBuf

ProtoBuf 是结构数据序列化方法，可简单类比于 XML。感觉就和我以前写 java 返回 json 是一个类型，只不过更轻、更快。需要双方维护一个协议约束文件，以.proto结尾。

### RPC 和 gRPC

RPC 是远程过程调用，广义上来讲，所有本应用程序外的调用都可以归类为 RPC，不管是分布式服务，第三方服务的 HTTP 接口，还是读写 Redis 的一次请求。RESTfull 就是一种实现 RPC 的方式。

gRPC 是一款 RPC 框架，使用 Protobuf 进行数据编码，提高数据压缩率；使用 HTTP2.0 弥补了 HTTP1.1 的不足；同样在调用方和服务方使用协议约定文件，提供参数可选，为版本兼容留下缓冲空间。

## 目标

### 实现简单数据库

`kv/storage/standalone_storage/standalone_storage.go`

do all read/write operations through `engine_util` provided methods.

### Implement service handlers

## 杂记

`t := i.(T)` 是类型断言，i 是接口变量，T 为转换的目标类型。`t, ok := i.(T)` 更有好，正常 true,不正常 false。

engines.go 里头有一个将 *WriteBatch 写到 engine 的 kv db 里头的函数。