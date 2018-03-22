package cn.itcast.cloud;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 *  告警触发bolt ，通过数据匹配redis当中的数据，每条数据匹配redis当中的黑名单，进行实时告警
 */
public class wifiWarnongBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String dataLine = tuple.getStringByField("dataLine");
        if (StringUtils.isNotEmpty(dataLine) && StringUtils.isNotBlank(dataLine)) {
            String[] split = dataLine.split("\001");
            if (dataLine.length() > 0 && dataLine != null) {
                switch (split.length) {
                    case 25:
                        //终端mac获取记录
                        //获取iumac黑名单
                        collector.emit(new Values(dataLine,"YT1013"));
                        break;
                    case 22:
                        //虚拟身份
                        //  手机号，账户名黑名单
                        String mobile = split[0];
                        collector.emit(new Values(dataLine,"YT1020"));
                        break;
                    case 51:
                        //匹配iumac黑名单
                        //终端上下线记录
                        collector.emit(new Values(dataLine,"YT1023"));
                        break;
                    case 21:
                        // 匹配iumac黑名单
                        //搜索关键字记录
                        collector.emit(new Values(dataLine,"YT1033"));
                    case 24:
                        //网页访问内容
                        //匹配iumac黑名单
                        collector.emit(new Values(dataLine,"YT1034"));
                        break;
                    default:
                        //异常数据，需要处理异常数据，将异常数据保存起来，统计每个设备异常数据的量，适时进行设备检修
                        //保存到一个文件里面去
                        //FileOperateUtils.writeLine(file, wifiDataLine);
                        break;
                }
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("wifiData", "dataType"));
    }
}
