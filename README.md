## mysql 同步组件(Kafka)

### 介绍
用于将mysql的数据同步到其他的数据源，如不同类型的关系数据库，nosql数据库，Elasticsearch等。

结合了kafka的mysql-connect组件，可快速实现同步。

PS. 暂时实现了mysql->elasticsearch(7.10)的同步功能

### 准备
kafka 版本 2.13-2.6.0 [官网](https://kafka.apache.org/)

[kafka-connect-mysql](https://github.com/confluentinc/kafka-connect-jdbc)



