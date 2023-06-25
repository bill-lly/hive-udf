package com.tangshiwei.udtf;


import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

@org.apache.hadoop.hive.ql.exec.Description(name = "string_int_count", value = "Count the number of occurrences of " +
        "each digit in a comma-separated string consisting of pure numbers")
public class GenericUDTFStringIntCount extends GenericUDTF {
    private static final Logger log = LoggerFactory.getLogger(GenericUDTFStringIntCount.class);

    /**
     * 初始化
     *
     * @param argOIs 入参个数
     * @return
     */
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        log.debug("initializing GenericUDTFStringIntCount");
        //TODO 1 约束函数传入参数的个数,只能为一个
        if (argOIs.getAllStructFieldRefs().size() != 1) {
            throw new UDFArgumentLengthException("Function explode_json_arrays_sorted has to input at least one " +
                    "column...");
        }

        //TODO 2 约束函数传入参数的类型
        String typeName = argOIs.getAllStructFieldRefs().get(0).getFieldObjectInspector().getTypeName();
        if (!"string".equals(typeName)) {
            log.error("Got" + typeName + "instead of string...");
            throw new UDFArgumentTypeException(0, "explode_json_array Function，The type of the first parameter can " +
                    "only be String...," + "but \"" + typeName + "\" is found");
        }

        //TODO 3 约束函数返回值的类型
        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldNames.add("key"); //列别名
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
        fieldNames.add("value"); //列别名
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);

        log.debug("done initializing GenericUDTFStringIntCount");
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    /**
     * 处理逻辑
     * 计算每个数字元素出现的次数
     * 例:
     * 原始数据:
     * row:0,1,2,3,4,5,1,2
     * 炸开后:
     * key   value
     * row     1      2
     * row     2      2
     * row     3      1
     * row     4      1
     * row     5      1
     *
     * @param args object array of arguments
     */
    @Override
    public void process(Object[] args) throws HiveException {
        IntWritable[] result = new IntWritable[2];
        if (args[0] != null) {
            String value = args[0].toString();
            String[] split = value.split(",");
            HashMap<Integer, Integer> hashMap = new HashMap<>();
            for (String s : split) {
                int i = Integer.valueOf(s).intValue();
                if (i != 0) {
                    if (hashMap.containsKey(i)) {
                        hashMap.put(i, hashMap.get(i) + 1);
                    } else {
                        hashMap.put(i, 1);
                    }
                }
            }

            if (hashMap.size()!=0){
                Iterator<Integer> iterator = hashMap.keySet().iterator();
                while (iterator.hasNext()) {
                    Integer key = iterator.next();
                    result[0] = new IntWritable(key);
                    result[1] = new IntWritable(hashMap.get(key));
                    forward(result);
                }
            } else {
                log.warn("点位信息无清洁信息，均为0");
                result[0] = new IntWritable(0);
                result[1] = new IntWritable(0);
                forward(result);
            }
        }else {
            log.error("点位信息为空，数据异常");
            forward(result);
        }
    }

    @Override
    public void close() throws HiveException {

    }
}

