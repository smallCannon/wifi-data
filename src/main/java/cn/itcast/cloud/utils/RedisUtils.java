package cn.itcast.cloud.utils;

import java.io.Serializable;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisUtils implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -5787638302480086925L;
	private static  JedisPool jedisPool ;
	
	static{
		if(jedisPool == null ){
			GenericObjectPoolConfig config = new GenericObjectPoolConfig();
			config.setMaxIdle(30);
			config.setMaxTotal(50);
			config.setMaxWaitMillis(300000);
			config.setMinIdle(10);
			jedisPool = new JedisPool(config, "node-1", 6379,100000);
		}
	}
	
	
	public static Jedis getJedis(){
		Jedis resource = jedisPool.getResource();
		return resource;
	}
	
	
	/**
	 * 往redis数据库当中插入一些匹配的黑名单，例如，手机号，手机mac地址，虚拟身份账号等
	 */
	public static void redisInit(){
		Jedis jedis = getJedis();
		//iumac 黑名单开始
		jedis.set("240385448408780", "black");
		jedis.set("46377912253896", "black");
		jedis.set("207037906141705", "black");
		jedis.set("228847639986922", "black");
		jedis.set("240385445917628", "black");
		jedis.set("251077918332690", "black");
		jedis.set("242364149550040", "black");
		jedis.set("149708392568862", "black");
		jedis.set("4407158967550", "black");
		jedis.set("240385455225023", "black");
		
		// 手机号黑名单 开始
		jedis.set("13944818394", "black");
		jedis.set("13646764042", "black");
		jedis.set("15143164728", "black");
		jedis.set("15504464311", "black");
		jedis.set("13756099785", "black");
		jedis.set("18643118036", "black");
		jedis.set("13331693296", "black");
		jedis.set("18404319911", "black");
		jedis.set("18704414544", "black");
		jedis.set("18704414544", "black");
		
		//虚拟身份账号黑名单
		jedis.set("3392872202", "black");
		jedis.set("2643706415", "black");
		jedis.set("2386194837", "black");
		jedis.set("350280526", "black");
		jedis.set("27581738", "black");
		jedis.set("117195175", "black");
		jedis.set("1119689847", "black");
		jedis.set("1206874218", "black");
		jedis.set("1242991575", "black");
		jedis.set("1152921504669480549", "black");
		jedis.close();
	}

	
	
	

}
