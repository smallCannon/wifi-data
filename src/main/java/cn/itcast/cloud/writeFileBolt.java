package cn.itcast.cloud;

import cn.itcast.cloud.utils.FileOperateUtils;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * 定时检测数据 将数据写到文件中
 */
public class writeFileBolt extends BaseBasicBolt {


    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config config = new Config();
        // 每过五秒
        config.put(config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 5);
        return config;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            //判断当前tuple是否是来自于系统的定时器
        if (tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)) {
            FileOperateUtils.uploadYestData();
        } else {
            String dataType = tuple.getStringByField("dataType");
            System.out.println("数据类型为：" + dataType);
            String stringFields = tuple.getStringByField("wifiData");
            try {
                FileOperateUtils.meargeToLargeFile(dataType, stringFields);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
