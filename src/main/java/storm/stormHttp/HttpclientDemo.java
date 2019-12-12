package storm.stormHttp;

import org.apache.http.HttpEntity;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

public class HttpclientDemo {
    public static void main(String[] args) throws Exception{
        CloseableHttpClient httpClient = HttpClients.createDefault();
        String content = "中秋节快乐";
        System.out.println(content);
        HttpGet httpGet =new HttpGet("http://192.168.40.20:8088/?content="+content);
        CloseableHttpResponse response = httpClient.execute(httpGet);
        HttpEntity httpEntity =response.getEntity();
        System.out.println(response.getStatusLine());
        if(httpEntity!=null){
            System.out.println("response content length:"+ httpEntity.getContentLength());
            String responString = new String(EntityUtils.toString(httpEntity));
            System.out.println("转码前："+responString);
            responString = new String(responString.getBytes("ISO-8859-1"),"utf-8");
            System.out.println("转码后："+responString);
        }
    }
}
