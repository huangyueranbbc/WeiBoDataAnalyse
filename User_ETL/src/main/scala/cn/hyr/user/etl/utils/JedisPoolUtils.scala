package cn.hyr.user.etl.utils

import java.util

import redis.clients.jedis.HostAndPort

object JedisPoolUtils extends Serializable {

  @transient private var jedis: JedisClusterProxy = _

  def initJedisCluster(): Unit = {
    if (jedis == null) {
      val jedisClusterNodes = new util.HashSet[HostAndPort]
      jedisClusterNodes.add(new HostAndPort("192.168.2.128", 7001))
      jedisClusterNodes.add(new HostAndPort("192.168.2.129", 7001))
      jedisClusterNodes.add(new HostAndPort("192.168.2.129", 7002))
      jedis = new JedisClusterProxy(jedisClusterNodes)
      val hook = new Thread {
        override def run(): Unit = jedis.close()
      }
      sys.addShutdownHook(hook.run())
    }
  }

  def getPool: JedisClusterProxy = {
    assert(jedis != null)
    jedis
  }
}