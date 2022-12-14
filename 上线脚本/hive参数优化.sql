--设置计算引擎
set hive.execution.engine=mr;  --默认引擎
set hive.execution.engine=spark;  --编排很好mr
set hive.execution.engine=tez;  --基于内存

--开启并发
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=8;   --并发线程数调整，默认为8


--Mapjoin,将小表在join时读进内存(使用需要注意内存溢出的问题)
set hive.auto.convert.join = true;
set hive.mapjoin.smalltable.filesize=25000000;   --默认25MB，建议不要超过1GN
set hive.ignore.mapjoin.hint = false;  


--开启矢量，一次处理1024条数据
set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;

--Join 数据倾斜优化
set hive.skewjoin.key=100000; --在编写 Join 查询语句时，如果确定是由于 join 出现的数据倾斜，那么请做如下设置：join 的键对应的记录条数超过这个值则会进行分拆，值根据具体数据量设置
set hive.optimize.skewjoin=false;  --如果是 join 过程出现倾斜应该设置为 true
set hive.skewjoin.mapjoin.map.tasks=10000; --如果开启了，在 Join 过程中 Hive 会将计数超过阈值 hive.skewjoin.key（默认 100000）的倾斜 key 对应的行临时写进文件中，然后再启动另一个 job 做 map join 生成结果。通过hive.skewjoin.mapjoin.map.tasks 参数还可以控制第二个 job 的 mapper 数量，默认 10000。


--临时表相关
set hive.exec.temporary.table.storage=memory;  --将临时表读入内存。可选参数memory,ssd,default

--with as 固化写入内存
-- 这个参数在默认情况下是-1（关闭的）；当开启（大于0），比如设置为2，则如果with..as语句被引用2次及以上时，会把with..as语句生成的table物化，从而做到with..as语句只执行一次，来提高效率
set hive.optimize.cte.materialize.threshold=2; 

--是否开启自动使用索引
set hive.optimize.index.filter=true;

--开启本地模式
set hive.exec.mode.local.auto=true;
set hive.exec.mode.local.auto.inputbytes.max=50000000;  --设置本地运行的最大数据输入量，当输入的数据量小于这个值时使用本地模式  默认为134217728，即128M，单位是Byte


--Map Reduce数量相关
--数据分片大小 (分片的数量决定map的数量)
--计算公式: splitSize = Math.max(minSize, Math.min(maxSize, blockSize))
set mapreduce.input.fileinputformat.split.maxsize=750000000;


--调整Join顺序，让多次Join产生的中间数据尽可能小，选择不同的Join策略
set hive.cbo.enable=true;


--数据统计：  新创建的表/分区是否自动计算统计数据
set hive.stats.autogather=true;
set hive.compute.query.using.stats=true;
set hive.stats.fetch.column.stats=true;
set hive.stats.fetch.partition.stats=true;



--通过命令行方式开启hive的事务(注意，仅支持orc文件类型)
set hive.support.concurrency = true;
set hive.enforce.bucketing = true;
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.txn.manager = org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.compactor.initiator.on = true;
set hive.compactor.worker.threads = 1; 

create table dim_Product  --按照上面的方式之一开启Hive事务后，创建如下的支持事务的分桶表：
(
 product_sk int ,
 product_code int ,
 product_name varchar(128),
 product_category varchar(256),
 version varchar(32),
 effective_date date,
 expiry_date date
)
clustered by (product_sk ) into 8 buckets  -- 在Hive中只有分桶表支持事务
stored as orc tblproperties('transactional'='true');  -- 设置属性transactional'='true'开启事务支持


---- 开启CBO   CBO(Cost based Optimizer)可以自动优化HQL中多个JOIN的顺序，并选择合适的JOIN算法。
set hive.cbo.enable=true; 
set hive.compute.query.using.stats=true; 
set hive.stats.fetch.column.stats=true; 
set hive.stats.fetch.partition.stats=true;