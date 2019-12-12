package storm.stormHbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

public class HbaseGetInfo {


    public static final String tableName = "storm_hbase_music_h";
    public static Configuration conf = HBaseConfiguration.create();
    private static HTable table;

    public static void selectRowKeyNew(String tableName,String rowKey) throws IOException {
        System.out.println("************************1");
        Connection connect = ConnectionFactory.createConnection(conf);
        TableName name = TableName.valueOf(tableName);
        Table table = connect.getTable(name);
        System.out.println("************************2");
        Get g = new Get(rowKey.getBytes());
        System.out.println("************************3");
        Result rs = table.get(g);
        System.out.println("************************4");

        System.out.println("==>"+new String(rs.getRow()));
        for(Cell kv: rs.rawCells()){
            System.out.println("---------------"+new String(kv.getRow())+"-------------------");
            System.out.println("Column Family:"+ new String(kv.getFamily()));
            System.out.println("column  :"+ new String(kv.getQualifier()));
            System.out.println("value  :"+ new String(kv.getValue()));
        }


    }
    public static void main(String[] args){
        conf.set("hbase.master","192.168.40.20:60000");
        conf.set("hbase.zookeeper.quorum","192.168.40.20,192.168.40.21,192.168.40.22");


        System.out.println("[1]=========");
        try {
            selectRowKeyNew(tableName,"1001");
            System.out.println("[2]=========");
        } catch (IOException e) {
            System.out.println("[3]=========");
            e.printStackTrace();
        }
    }
}
