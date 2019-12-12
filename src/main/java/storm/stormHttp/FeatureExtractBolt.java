package storm.stormHttp;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.Map;

public class FeatureExtractBolt extends BaseRichBolt {
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    @Override
    public void execute(Tuple tuple) {
        System.out.println("execute:"+tuple);
        String content = tuple.getString(0);
        System.out.println("content:");
        CloseableHttpClient httpClient =  HttpClients.createDefault();
        HttpGet httpGet = new HttpGet("http://192.168.40.20:8088/?content="+content);
        try {
            CloseableHttpResponse closeableHttpResponse = httpClient.execute(httpGet);
            HttpEntity httpEntity = closeableHttpResponse.getEntity();
            System.out.println("zhauntai:"+closeableHttpResponse.getStatusLine());
            if(httpEntity!=null){
                System.out.println("Response content length:"+ httpEntity.getContentLength());
                String responseString = new String(EntityUtils.toString(httpEntity));
                responseString = new String(responseString.getBytes("ISO-8859-1"),"utf-8");
                System.out.println("转码后的结果="+responseString);

            }
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
