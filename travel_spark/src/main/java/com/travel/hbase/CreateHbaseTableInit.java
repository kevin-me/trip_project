package com.travel.hbase;

import com.travel.common.Constants;
import com.travel.utils.HbaseTools;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class CreateHbaseTableInit {
    public static void main(String[] args) throws IOException {
        //实现hbase的表的预分区的操作

        String[] tableNames = new String[]{"order_info","renter_info","driver_info","opt_alliance_business"};

        //获取预分区的key

        byte [][] byteNum = new byte[8][];
        for(int i =0;i< 8 ;i++){

            //0000  0001  0002  0003  0004  0005  0006  0007
            String leftPad = StringUtils.leftPad(i + "", 4, "0");
            byteNum[i]  = Bytes.toBytes(leftPad + "|");
        }

        Connection hbaseConn = HbaseTools.getHbaseConn();
        Admin admin = hbaseConn.getAdmin();

        for (String tableName : tableNames) {

            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(Constants.DEFAULT_DB_FAMILY);
            hTableDescriptor.addFamily(hColumnDescriptor);
            admin.createTable(hTableDescriptor,byteNum);
        }
        admin.close();
        hbaseConn.close();


    }

}
