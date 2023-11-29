# 一、Why We Need？
* 连接物理意义上分割的一些机器，能够实现数据共享
* 通过并行，增加服务的容量和扩展性
* 增加容错性
* 实现安全性：对于比如说密码管理系统，可以将需要存储的密码隔离开，分布式存储在多个机器上，这样就不用对单台计算机进行特别严格的接口管理。

# 二、Historical Context
* 局域网分布式系统（DNS、AFS等，1980s）
* 数据中心和大型网站（大公司内部的服务系统，如WebSearch、Shopping等，1990s）
* 云计算（建设分布式基础设施，个人也能搭建分布式高性能系统，2000s）

# 三、Challenges
* 高并发带来的不确定性
* 系统的部分失效问题，如网络分区等
* 实际上并没有那么理论去实现性能的便利

# 四、Focus：基础设施
* Storage
* Computation
* Communication
* 尽可能抽象分布式系统，让他看起来像个单机系统

# 五、Main Topics：
* 容错：Fault Tolerance
* 高可用性（使用复制）
* 可恢复性（日志或事务）
* 一致性：Consistency
* 性能：Performace
* 吞吐量 ThroughPut
* 延迟 Latency
