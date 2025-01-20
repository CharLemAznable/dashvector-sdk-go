### dashvector-sdk-go

![GitHub release (latest by date)](https://img.shields.io/github/v/release/CharLemAznable/dashvector-sdk-go)

[![MIT Licence](https://badges.frapsoft.com/os/mit/mit.svg?v=103)](https://opensource.org/licenses/mit-license.php)
![GitHub code size](https://img.shields.io/github/languages/code-size/CharLemAznable/dashvector-sdk-go)

[阿里云 向量检索服务](https://help.aliyun.com/product/2510217.html) DashVector SDK for Go.

#### 创建客户端

```go
client := dashvector.NewClient(ctx)
```

#### 创建Collection

```go
_, _ = client.CreateServing(ctx, collectionName, 
    dashvector.WithDimension(4))
```

#### 创建Partition

```go
collection := client.GetCollection(collectionName)
_, _ = collection.CreateServing(ctx, partitionName)
```

#### 创建Doc

```go
// 可直接使用默认Partition
_, _ = collection.Insert(ctx,
    dashvector.WithDocument(
        dashvector.WithVector(0.1, 0.2, 0.3, 0.4),
    ))

// 也可使用自定义Partition
partition := collection.GetPartition(partitionName)
_, _ = partition.Insert(ctx,
    dashvector.WithDocument(
        dashvector.WithVector(0.1, 0.2, 0.3, 0.4),
    ))
```

#### 检索Doc

```go
// 可直接使用默认Partition
queryResponse, _ := collection.Query(ctx,
    dashvector.QueryWithVector(0.1, 0.2, 0.3, 0.4),
    dashvector.QueryWithTopk(10),
    dashvector.QueryWithIncludeVector(true))

// 也可使用自定义Partition
partition := collection.GetPartition(partitionName)
queryResponse, _ := partition.Query(ctx,
    dashvector.QueryWithVector(0.1, 0.2, 0.3, 0.4),
    dashvector.QueryWithTopk(10),
    dashvector.QueryWithIncludeVector(true))
```
