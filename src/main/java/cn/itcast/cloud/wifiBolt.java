package cn.itcast.cloud;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.File;
import java.util.Map;

public class wifiBolt extends BaseBasicBolt {

    private File file;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        file = new File("");
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            // 取出从kafkaSpout中的数据
        String wifiData = tuple.getValue(4).toString();
        if (StringUtils.isNotEmpty(wifiData)) {
            String[] split = wifiData.split("@zolen@");
           // 取出的数据有效
            if (null != split && split.length > 0) {
                switch (split.length) {
                    // 终端mac的记录
                    case 25:
                        basicOutputCollector.emit(new Values(wifiData.replace("@zolen@", "\001")));
                        break;
                    case 22:
                        //虚拟身份
                        basicOutputCollector.emit(new Values(wifiData.replace("@zolen@", "\001")));
                        break;
                    case 51:
                        //终端上下线记录
                        basicOutputCollector.emit(new Values(wifiData.replace("@zolen@", "\001")));
                        break;
                    case 21:
                        //搜索关键字记录
                        basicOutputCollector.emit(new Values(wifiData.replace("@zolen@", "\001")));
                        break;
                    case 24:
                        //网页访问记录
                        basicOutputCollector.emit(new Values(wifiData.replace("@zolen@", "\001")));
                        break;
                    default:
                        // 异常的数据
                       // FileOperateUtils.writeLine(file, wifiDataLine);
                        break;


                }
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("dataLine"));
    }
}
