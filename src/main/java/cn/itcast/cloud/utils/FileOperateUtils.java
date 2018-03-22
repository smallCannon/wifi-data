package cn.itcast.cloud.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.io.FileUtils;

public class FileOperateUtils implements Serializable {

	
	private static ConcurrentLinkedQueue<String> concurrentLinkedQueue =  new ConcurrentLinkedQueue<String>();
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -4763341832497838848L;

	/**
	 * 
	 */
	
	
	

	public static  synchronized void writeLine(File file,String fileContent){
		concurrentLinkedQueue.clear();
		concurrentLinkedQueue.add(fileContent);
		try {
			FileUtils.writeLines(file, concurrentLinkedQueue, "\r", true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static BufferedWriter writer;
	private static File file;
	//数据存储的根路径
	//private String basePath="F:\\wifiDatas";
	
	private static  String basePath="/export/data";
	
	private static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
	
	public static void  meargeToLargeFile(String dataType, String stringByField) throws Exception{
		String format2 = format.format(new Date());
		if(! new File(basePath+File.separator+dataType+File.separator+format2).exists()){
			new File(basePath+File.separator+dataType+File.separator+format2).mkdirs();
		}
		file = new File(basePath+File.separator+dataType+File.separator+format2+File.separator+dataType+".txt");
		if(!file.isFile()){
			file.createNewFile();
		}
		writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true)));
		writer.write(stringByField);
		writer.newLine();
		writer.flush();
		writer.close();
		//文件内容大于127.5M的时候进行上传HDFS
		/*if(FileUtils.sizeOf(file) > 133693440 ){
			//将文件上传到hdfs对应的位置去
			AppendToHdfs.appendToHdfs(file,dataType );
		}
		*/
		if(FileUtils.sizeOf(file) > 5120 ){
			//将文件上传到hdfs对应的位置去
			AppendToHdfs.appendToHdfs(file,dataType );
		}
		
		
		
		
	}
	
	private static String[] dataTypeArray = new String[]{"YT1013","YT1020","YT1023","YT1033","YT1034"};
	
	public static void uploadYestData(){
		//将昨日的所有数据都上传到hdfs上面去
		for (String string : dataTypeArray) {
			File file  = new File(basePath+File.separator+ string+File.separator+DateUtils.getBeforeDay(-1)+File.separator+string+".txt");
			if(file.isFile()&& file.exists()){
				try {
					AppendToHdfs.appendToHdfs(file,string);
					file.delete();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		
		
	}
	
	
	
	
	
	

}
