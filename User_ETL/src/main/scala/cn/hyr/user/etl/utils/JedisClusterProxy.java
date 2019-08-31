package cn.hyr.user.etl.utils;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.io.Serializable;
import java.util.Set;

/*******************************************************************************
 * @date 2019-08-31 15:50
 * @author: <a href=mailto:huangyr>黄跃然</a>
 * @Description:
 ******************************************************************************/
public class JedisClusterProxy extends JedisCluster implements Serializable {
    public JedisClusterProxy(HostAndPort node) {
        super(node);
    }

    public JedisClusterProxy(HostAndPort node, int timeout) {
        super(node, timeout);
    }

    public JedisClusterProxy(HostAndPort node, int timeout, int maxAttempts) {
        super(node, timeout, maxAttempts);
    }

    public JedisClusterProxy(HostAndPort node, GenericObjectPoolConfig poolConfig) {
        super(node, poolConfig);
    }

    public JedisClusterProxy(HostAndPort node, int timeout, GenericObjectPoolConfig poolConfig) {
        super(node, timeout, poolConfig);
    }

    public JedisClusterProxy(HostAndPort node, int timeout, int maxAttempts, GenericObjectPoolConfig poolConfig) {
        super(node, timeout, maxAttempts, poolConfig);
    }

    public JedisClusterProxy(HostAndPort node, int connectionTimeout, int soTimeout, int maxAttempts, GenericObjectPoolConfig poolConfig) {
        super(node, connectionTimeout, soTimeout, maxAttempts, poolConfig);
    }

    public JedisClusterProxy(HostAndPort node, int connectionTimeout, int soTimeout, int maxAttempts, String password, GenericObjectPoolConfig poolConfig) {
        super(node, connectionTimeout, soTimeout, maxAttempts, password, poolConfig);
    }

    public JedisClusterProxy(Set<HostAndPort> nodes) {
        super(nodes);
    }

    public JedisClusterProxy(Set<HostAndPort> nodes, int timeout) {
        super(nodes, timeout);
    }

    public JedisClusterProxy(Set<HostAndPort> nodes, int timeout, int maxAttempts) {
        super(nodes, timeout, maxAttempts);
    }

    public JedisClusterProxy(Set<HostAndPort> nodes, GenericObjectPoolConfig poolConfig) {
        super(nodes, poolConfig);
    }

    public JedisClusterProxy(Set<HostAndPort> nodes, int timeout, GenericObjectPoolConfig poolConfig) {
        super(nodes, timeout, poolConfig);
    }

    public JedisClusterProxy(Set<HostAndPort> jedisClusterNode, int timeout, int maxAttempts, GenericObjectPoolConfig poolConfig) {
        super(jedisClusterNode, timeout, maxAttempts, poolConfig);
    }

    public JedisClusterProxy(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts, GenericObjectPoolConfig poolConfig) {
        super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, poolConfig);
    }

    public JedisClusterProxy(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts, String password, GenericObjectPoolConfig poolConfig) {
        super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, password, poolConfig);
    }
}
