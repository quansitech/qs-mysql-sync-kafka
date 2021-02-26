## mysql 同步组件(Kafka)

### 介绍
用于将mysql的数据同步到其他的数据源，如不同类型的关系数据库，nosql数据库，Elasticsearch等。

结合了kafka的mysql-connect组件，可快速实现同步。

PS. 暂时实现了mysql->elasticsearch(7.10)的同步功能，暂不支持集群模式。

### 准备
kafka 版本 2.13-2.6.0 [官网](https://kafka.apache.org/)

[kafka-connect-mysql](https://github.com/confluentinc/kafka-connect-jdbc)

mysql监控表

```sql
CREATE TABLE `es_sync` (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT primary key,
  `data_type` varchar(30) COLLATE utf8mb4_general_ci NOT NULL COMMENT '数据类型',
  `op` varchar(30) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '操作: update delete insert等',
  `data_key` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '字段值',
  `create_date` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

//设置全覆盖索引，提升查询性能
ALTER TABLE `kb_es_sync` ADD KEY `idx_full_cover` (`id`,`create_date`,`data_type`,`op`,`data_key`);
```



### 安装

```shell
npm install @quansitech/qs-mysql-sync-kafka
```



### 设置

1. 启动kafka及kafka-connect-mysql (方法自行查找相关文档)。
2. 创建mysql监控表，kafka-connect-mysql使用timestamp+incrementing模式同步内容到kafka的topic。
3. 监控需要同步的表，当表发生变动时利用增删改触发器向es_sync表插入一条数据。
   + data_type： 是对应的模块名，封装着初始化、增、删、改等具体的同步逻辑，也可以是更个性化的逻辑，根据具体需求灵活设置。
   + op：就是对应的模块具体方法，如上面提到的增、删、改等操作。
   + data_key：是传递给op方法的值。
4. 编写具体的模块同步逻辑。
5. 执行初始化，并启动consumer模式，开始监控同步。



### 配置及接口说明

组件由三个类组成，分别是

1. MysqlEsSync 负责提供mysql->es同步的调用方法。
2. SyncClient  组件的核心模块，负责topic信息的接收，根据信息调度不同的处理模块。
3. Utils 组件的工具类



+ ##### 类接口详解

  + MysqlEsSync 

    1. getInstance(opt)  对象实例化入口(单例)

       >参数说明:
       >
       >opt object类型
       >
       >```javascript
       >const defaultOpt = {
       >    mysqlConnectionLimit : 10, //mysql连接数
       >    mysqlHost: '', //mysql服务器地址
       >    mysqlPort: 3306, //mysql服务器端口
       >    mysqlUser: '', //mysql用户名
       >    mysqlPassword: '', //mysql用户密码
       >    mysqlDatabase: '', //要连接的mysql数据库
       >    esHost: '', //es服务器地址
       >    esPort: '', //es服务器端口号
       >    bulkFetchRow: 500 //批量同步每次同步的数据行数
       >};
       >```
       >
    
    2. deleteEsIndex(index) 删除Es索引
    
       >参数说明:
       >
       >index string类型 索引名称
    
    3. createEsIndex(index, mapping, settings) 创建Es索引
    
       > 参数说明:
       >
       > index string类型 索引名称
       >
       > mapping object类型 索引的mapping配置
       >
       > settings object类型 索引的settings配置
    
    4. deleteEsDoc(id, index) 删除Es索引指定doc记录
    
       >参数说明:
       >
       >id string类型 doc id值
       >
       >index string类型 索引名称
    
    5. bulkPutEsFromMysql(sql, docKey, index, pipeline) 从mysql批量同步数据指指定的Es索引
    
       >参数说明:
       >
       >sql string类型 sql查询语句
       >
       >docKey string类型 作为doc主键的数据库字段
       >
       >index string类型 索引名称
       >
       >pipeline string类型 指定向索引插入数据时需要执行的数据处理管道的管道名称
    
    6. bulkPutEsFromMysqlById(sql, docKey, index, idColumn, pipeline) 从mysql批量同步数据指指定的Es索引（数据量很大情况下用这个方法效率更高）
    
       >参数说明:
       >
       >sql string类型 sql查询语句
       >
       >docKey string类型 作为doc主键的数据库字段
       >
       >index string类型 索引名称
       >
       >idColumn string类型 mysql数据表主键，用于分页排序
       >
       >pipeline string类型 指定向索引插入数据时需要执行的数据处理管道的管道名称
    
    7. deletePolicy(name) 删除Es的enrich策略
    
       >参数说明:
       >
       >name string类型 enrich策略名
    
    8. createPolicy(name, body) 创建Es的enrich策略
    
       >参数说明:
       >
       >name string类型 enrich策略名
       >
       >body object类型 策略配置
    
    9. executePolicy(name) 执行Es的enrich策略
    
       >参数说明:
       >
       >name string类型 enrich策略名
    
    10. createPipeline(id, body) 创建Es 管道
    
        >参数说明:
        >
        >id string类型 管道名
        >
        >body object类型 管道配置
    
    11. deletePipeline(id) 删除Es 管道
    
        >参数说明:
        >
        >id string类型 管道名
    
    12. esUpdateByQuery(index, pipeline, body) 更新Es符合条件的索引记录
    
        >参数说明:
        >
        >index string类型 索引名称
        >
        >pipeline string类型 更新执行的管道的管道名
        >
        >body object类型 需要执行更新操作的记录搜索设置
    
  + SyncClient

    1.  constructor(opt, modules) 构造函数

       >参数说明:
       >
       >opt object类型 配置参数
       >
       >```javascript
       >const defaultOpt = {
       >    kafkaTopic: '', //要监听的topic名
       >    kafkaHost: '', //kafka 服务器地址
       >    adminEmail: '', //如发生错误需要发送邮件，则可在此设置邮件，多个用逗号隔开
       >    nodemailerOpt: { 
       >        host: '', //邮件服务器
       >        port: 465, //邮件服务器端口号
       >        secure: true, //是否使用安全连接
       >        user: '', //邮件账号
       >        password: '' //账号密码
       >    }
       >};
       >```
       >
       >modules array类型 模块数组
       >
       >```javascript
       >modules = [
       >    require('./sync-module/book') //引入模块的js
       >]
       >```

       

    2. init(param) 数据初始化

       > 参数说明:
       >
       > param object类型 设置项
       >
       > ```javascript
       > param = {
       >     module: '' //指定初始化的模块名
       > }
       > 
       > { module: 'all' } //表示初始化所有模块
       > { module: [ 'book' ]} //表示仅初始化book模块
       > ```

    3. startConsumer(param) 启动consumer

       > 参数说明：
       >
       > param object类型 设置项
       >
       > ```javascript
       > param = {
       >     offset: 100 //指定topic偏移量
       > }
       > ```

  + Utils

    1. parseParam() 将命令行的参数转换成对象配置

       > ```javascript
       > //node index.js init --module=all
       > const param = Utils.parseParam();
       > 
       > console.log(param);
       > // { module: 'all' }
       > 
       > //node index.js init --module=book,borrow
       > const param = Utils.parseParam();
       > 
       > console.log(param);
       > // { module: ['book', 'borrow'] }
       > ```

    2. chunkRun(chunkSize, arr, runFn)  Es最大支持65536 的terms查询设置，如果需要批量同步的sql数据大于该值，则需要切分，该方法可以自动帮我们完成数组的切分

       >参数说明：
       >
       >chunkSize 整数类型 切分大小
       >
       >arr 数组类型 需要被切分的数组
       >
       >runFn 匿名函数 切分后的数组会作为参数传入该匿名函数执行

+ ##### 模块接口说明

  1. name 模块名称
  2. init() 执行初始化时会调用该方法
  3. 其余的方法可根据需要自定义，但必须与 es_sync表里的op字段对应，否则报错。es_sync表的data_key记录会通过第一个参数传入

  

### 简单用例

假设我们要同步一张书籍表book

```sql
CREATE TABLE `book` (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT primary key,
  `book_name` varchar(30) NOT NULL,
  `isbn` varchar(30) NOT NULL,
  `author` varchar(30) NOT NULL,
  `price` varchar(10) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
```

创建增删改触发器

```sql
CREATE TRIGGER `tri_book_delete_after` AFTER DELETE ON `book` FOR EACH ROW BEGIN
    insert into es_sync(`data_type`, `op`, `data_key`) values('book', 'delete', old.id);
END;

CREATE TRIGGER `tri_book_insert_after` AFTER INSERT ON `book` FOR EACH ROW BEGIN
    insert into es_sync(`data_type`, `op`, `data_key`) values('book', 'insert', new.id);
END

CREATE TRIGGER `tri_book_update_after` AFTER UPDATE ON `book` FOR EACH ROW BEGIN
    insert into es_sync(`data_type`, `op`, `data_key`) values('book', 'update', new.id);
END
```

编写同步模块 book.js

```javascript
const MysqlSync = require('@quansitech/qs-mysql-sync-kafka');

const MysqlEsSync = MysqlSync.MysqlEsSync;

const opt = {
    mysqlHost: '127.0.0.1',
    mysqlPort: 3306,
    mysqlUser: 'db_user',
    mysqlPassword: '123456',
    mysqlDatabase: 'demo',
    esHost: '127.0.0.1',
    esPort: '9200'
};
const mesClient = MysqlEsSync.getInstance(opt);

const indexName = 'book';

const indexMapping = {
    properties: {
        id: { type: "keyword" },
        book_name: { type: "keyword" },
        isbn: { type: 'keyword' },
        author: {
            type: 'keyword',
        },
        price: {
            type: 'float'
        }
    }
};

const fetchSql = (id = '') => {
    let sql = `select id,book_name,isbn,author,price from book`;
    if(id){
        sql = `${sql} where id='${id}'`;
    }
    return sql;
};

module.exports = {
    name: 'book',
    init: async function(){
        await mesClient.deleteEsIndex(indexName);

        await mesClient.createEsIndex(indexName, indexMapping);
        
        await mesClient.bulkPutEsFromMysqlById(fetchSql(), 'id', indexName, 'id');
    },
    insert: async function(id){
        try{
            await mesClient.bulkPutEsFromMysql(fetchSql(id), 'id', indexName);
        }
        catch(e){
            throw e;
        }
    },
    update: async function(id){
        try{
            await mesClient.bulkPutEsFromMysql(fetchSql(id), 'id', indexName);
        }
        catch(e){
            throw e;
        }
    },
    delete: async function(id){
        try{
            await mesClient.deleteEsDoc(id, indexName);
        }
        catch(e){
            throw e;
        }
    }
}
```

编写启动代码 index.js

```javascript
const MysqlSync = require('@quansitech/qs-mysql-sync-kafka');

const SyncClient = MysqlSync.SyncClient;
const utils = MysqlSync.Utils;

const opt = {
    kafkaTopic: 'demo.es_sync',
    kafkaHost: '127.0.0.1:9092'
};
const syncClient = new SyncClient(opt, [
    require('./book'),
]);


const param = utils.parseParam();
switch(process.argv[2]){
    case 'init':
        syncClient.init(param).then(() => { process.exit(0) });
        break;
    case 'consumer':
        syncClient.startConsumer(param);
        break;
}
```

执行初始化

```shell
node index.js init --module=all
```

启动kafka consumer

```sh
node index.js consumer
```



### 高级用法

+ #### 模块方法的延迟执行

  ```javascript
  customOp(id, startSync = false) 
  //当customOp返回true时，核心获取后会将该操作放入延迟队列。当所有topic的消息都过完一遍后再发起延迟队列的执行。执行延迟队列的方法时，startSync参数会传入true。
  ```

  看说明可能比较难理解，我们举个具体的例子来说明。

  有两张数据库表book, borrow，book是书籍资料表，borrow 是借阅记录表。假设这两张表数据量非常大，我们想将他们同步到ES来统计作者的被借阅次数汇总，借此来分析作者的受欢迎程度。

  

  book表

  ```sql
  CREATE TABLE `book` (
    `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT primary key,
    `book_name` varchar(30) NOT NULL,
    `isbn` varchar(30) NOT NULL,
    `author` varchar(30) NOT NULL,
    `price` varchar(10) NOT NULL
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
  ```

  

  borrow表

  ```sql
  CREATE TABLE `borrow` (
    `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT primary key,
    `book_id` varchar(30) NOT NULL,
    `reader_id` varchar(30) NOT NULL,
    `borrow_time` timestamp(3) NOT NULL
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
  ```

  (触发器参照简单用例，这里不再提供代码)

  

  将这两张表同步到ES，但你会发现ES不支持关联查询，而ES提供了一种enrich的策略类型结合pipeline来将两个不同的索引组合的方法。

  

  book模块

  ```javascript
  const MysqlSync = require('@quansitech/qs-mysql-sync-kafka');
  
  const MysqlEsSync = MysqlSync.MysqlEsSync;
  
  const opt = {
      mysqlHost: '127.0.0.1',
      mysqlPort: 3306,
      mysqlUser: 'db_user',
      mysqlPassword: '123456',
      mysqlDatabase: 'demo',
      esHost: '127.0.0.1',
      esPort: '9200'
  };
  const mesClient = MysqlEsSync.getInstance(opt);
  
  const indexName = 'book';
  
  const indexMapping = {
      properties: {
          id: { type: "keyword" },
          book_name: { type: "keyword" },
          isbn: { type: 'keyword' },
          author: {
              type: 'keyword',
          },
          price: {
              type: 'float'
          }
      }
  };
  
  const fetchSql = (id = '') => {
      let sql = `select id,book_name,isbn,author,price from book`;
      if(id){
          sql = `${sql} where id='${id}'`;
      }
      return sql;
  };
  
  const updateByQueryBody = id => {
      return {
          query: {
              term: {
                  book_id: {
                      value: id
                  }
              }
          }
      };
  };
  
  module.exports = {
      name: 'book',
      init: async function(){
          await mesClient.deleteEsIndex(indexName);
  
          await mesClient.createEsIndex(indexName, indexMapping);
          
          await mesClient.bulkPutEsFromMysqlById(fetchSql(), 'id', indexName, 'id');
      },
      insert: async function(id){
          try{
              await mesClient.bulkPutEsFromMysql(fetchSql(id), 'id', indexName);
          }
          catch(e){
              throw e;
          }
      },
      update: async function(id){
          try{
              await mesClient.bulkPutEsFromMysql(fetchSql(id), 'id', indexName);
              
              //book索引发生变化，必须重启policy来重建enrich索引
              await mesClient.executePolicy('book_lookup');
              
              //触发borrow数据的更新
              await mesClient.esUpdateByQuery('borrow', 'borrow_pipeline', updateByQueryBody(id));
          }
          catch(e){
              throw e;
          }
      },
      delete: async function(id){
          try{
              await mesClient.deleteEsDoc(id, indexName);
          }
          catch(e){
              throw e;
          }
      }
  }
  ```

  

  borrow模块

  ```javascript
  const MysqlSync = require('@quansitech/qs-mysql-sync-kafka');
  
  const MysqlEsSync = MysqlSync.MysqlEsSync;
  
  const indexName = 'borrow';
  const pipelineId = 'borrow_pipeline';
  const policyName = 'book_lookup';
  
  const opt = {
      mysqlHost: '127.0.0.1',
      mysqlPort: 3306,
      mysqlUser: 'db_user',
      mysqlPassword: '123456',
      mysqlDatabase: 'demo',
      esHost: '127.0.0.1',
      esPort: '9200'
  };
  const mesClient = MysqlEsSync.getInstance(opt);
  
  const indexMapping = {
      properties: {
          id: { type: "keyword" },
          reader_id: { type: "keyword" },
          book_id: { type: "keyword" },
          borrow_time: { type: "date" }
      }
  }
  
  const pipelineBody = {
      processors: [
          {
              enrich: {
                  policy_name: policyName,
                  field : "book_id",
                  target_field: "book_data"
              }
          }
      ]
  };
  
  const policyBody = {
      match: {
          indices: 'book',
          match_field: 'id',
          enrich_fields: [ 'isbn', 'book_name','author', 'price']
      }
  };
  
  const fetchSql = (id = '', bookId = '' ) => {
      let sql = `SELECT
      id, 
      reader_id, 
      book_id, 
      borrow_time
      FROM borrow where 1=1`;
      if(id){
          sql = `${sql} and id='${id}'`;
      }
      if(bookId){
          sql = `${sql} and book_id='${bookId}'`;
      }
      return sql;
  };
  
  module.exports = {
      name: 'borrow',
      init: async function(){
          await mesClient.deleteEsIndex(indexName);
  
          await mesClient.createEsIndex(indexName, indexMapping);
  
          await mesClient.deletePipeline(pipelineId);
  
          await mesClient.deletePolicy(policyName);
  
          await mesClient.createPolicy(policyName, policyBody);
  
          await mesClient.executePolicy(policyName);
  
          await mesClient.createPipeline(pipelineId, pipelineBody);
          
          await mesClient.bulkPutEsFromMysqlById(fetchSql(), 'id', indexName, 'id', pipelineId);
      },
      insert: async function(id){
          try{
              await mesClient.bulkPutEsFromMysql(fetchSql(id), 'id', indexName, pipelineId);
          }
          catch(e){
              throw e;
          }
      },
      update: async function(id){
          try{
              await mesClient.bulkPutEsFromMysql(fetchSql(id), 'id', indexName, pipelineId);
          }
          catch(e){
              throw e;
          }
      },
      delete: async function(id){
          try{
              await mesClient.deleteEsDoc(id, indexName);
          }
          catch(e){
              throw e;
          }
      }
  }
  ```

  book模块的update操作会触发enrich policy的重启，而这个操作很耗时。如果有成千上万的book数据更新，那肯定是处理不过来的。这种场景就可以使用延迟执行的特性，现在改造下book模块。

  ```javascript
  .
  .
  .
  //改成可批量更新的模式
  const fetchSql = (id = '') => {
      let sql = `SELECT
      id, 
      reader_id, 
      book_id, 
      borrow_time
      FROM borrow where 1=1`;
      if(id && typeof id == 'string'){
          sql = `${sql} where id='${id}'`;
      }
      else if(id && typeof id == 'object'){
          sql = `${sql} where id in ('${id.join("','")}')`;
      }
      return sql;
  };
  
  //改成可批量更新的模式
  const updateByQueryBody = id => {
      if(id && typeof id == 'string'){
          return {
              query: {
                  term: {
                      book_id: {
                          value: id
                      }
                  }
              }
          };
      }
      else if(id && typeof id == 'object'){
          return {
              query: {
                  terms: {
                      book_id: id
                  }
              }
          };
      }
  };
  
  //用于收集需要延迟更新的id
  let updateIds = [];
  module.exports = {
  .
  .
  .
  	update: async function(id, startSync = false){
          try{
              //先不执行,仅仅收集id
              if(startSync == false){
                  if(!updateIds.includes(id)){
                      updateIds.push(id);
                  }
                  //返回true 告诉核心模块该方法需要延迟执行
                  return true;
              }
              else{ //startSync=true  所有消息收集完毕，开始执行
                  await mesClient.bulkPutEsFromMysql(fetchSql(updateIds), 'id', indexName, pipelineId);
  
                  await mesClient.executePolicy('book_lookup');
      
                  //es terms查询最多支持到65536的数据大小，需要切块执行更新
                  await utils.chunkRun(60000, updateIds, async chunkIds => {
                      await mesClient.esUpdateByQuery('borrow', 'borrow_pipeline', updateByQueryBody(chunkIds));
                  });
                  
                  updateIds = [];
              }
          }
          catch(e){
              throw e;
          }
          
      },
  .
  .
  .
  }
  ```

  经改造后， update操作仅在消息收集完的最后一刻执行一次，这样就大大提高了程序的效率和稳定性。

