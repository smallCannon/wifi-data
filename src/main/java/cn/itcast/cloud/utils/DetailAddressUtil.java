package cn.itcast.cloud.utils;

import java.io.Serializable;
import java.net.URL;

import cn.itcast.cloud.domain.Address;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;


public class DetailAddressUtil implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1606497155988946028L;
	private static  JSONObject object = new JSONObject();
    
	
	
	  public static void main(String[] args) {    
          
          String add = getAdd("116.3539", "40.08065");  
          System.out.println(add);
          
          String addressToLati = addressToLati("北京市昌平区回龙观东大街");
          System.out.println(addressToLati);
      }    
        
	  /**
	   * 根据经纬度，获取详细地址
	   * @param log
	   * @param lat
	   * @return
	   */
      public static String getAdd(String log, String lat ){
          //lat 小  log  大    
          //参数解释: 纬度,经度 type 001 (100代表道路，010代表POI，001代表门址，111可以同时显示前三项)    
          String urlString = "http://gc.ditu.aliyun.com/regeocoding?l="+lat+","+log+"&type=010";    
          String res = "";       
          try {       
              URL url = new URL(urlString);      
              java.net.HttpURLConnection conn = (java.net.HttpURLConnection)url.openConnection();      
              conn.setDoOutput(true);      
              conn.setRequestMethod("POST");      
              java.io.BufferedReader in = new java.io.BufferedReader(new java.io.InputStreamReader(conn.getInputStream(),"UTF-8"));      
              String line;      
             while ((line = in.readLine()) != null) {      
                 res += line+"\n";      
           }      
              in.close();      
          } catch (Exception e) {      
              System.out.println("error in wapaction,and e is " + e.getMessage());      
          }     
          object.clear();
          Address parseObject = object.parseObject(res, Address.class);
          String firstPart = parseObject.getAddrList()[0].getAdmName();
          String[] split = firstPart.split(",");
          if(null != split && split.length > 0 ){
        	  if(split[0].trim().equals(split[1].trim())){
        		  firstPart = split[1]+split[2];
        	  }else{
        		  firstPart = firstPart.replace(",", "");
        	  }
          }
          String addressDetail = firstPart+parseObject.getAddrList()[0].getName();
          return addressDetail;      
      }    
	
	
      /**
       * 根据详细地址，获取经纬度,返回一个json格式的字符串，{"lon":116.35397,"level":5,"address":"","cityName":"","alevel":3,"lat":40.08065} 
       *具体变成经纬度，还得需要自己获取
       */
      public static String  addressToLati(String address){
    	//  http://gc.ditu.aliyun.com/geocoding?a=苏州市
    	  String res = "";       
          try {       
              URL url = new URL( "http://gc.ditu.aliyun.com/geocoding?a="+address);      
              java.net.HttpURLConnection conn = (java.net.HttpURLConnection)url.openConnection();      
              conn.setDoOutput(true);      
              conn.setRequestMethod("POST");      
              java.io.BufferedReader in = new java.io.BufferedReader(new java.io.InputStreamReader(conn.getInputStream(),"UTF-8"));      
              String line;      
             while ((line = in.readLine()) != null) {      
                 res += line+"\n";      
           }      
              in.close();      
          } catch (Exception e) {      
              System.out.println("error in wapaction,and e is " + e.getMessage());      
          }     
          System.out.println(res);    
         
          return res;   
      }
	
      
      
      
	
}
