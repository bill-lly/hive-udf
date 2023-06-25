package com.tangshiwei.udtf;


import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@org.apache.hadoop.hive.ql.exec.Description(name = "explode_json_arrays_by_index", value = "_FUNC_(JsonArrays... a) - " +
        "separates the elements of json array a into multiple rows; If columns of different lengths are inputed, " +
        "the number of rows in the longest column will returned." +
        "The others will return \'{}\';")
public class GenericUDTFExplodeJSONArraysByIndex extends GenericUDTF {
    private static final Logger log = LoggerFactory.getLogger(GenericUDTFExplodeJSONArraysByIndex.class);

    /**
     * 初始化
     *
     * @param argOIs 入参个数
     * @return
     */
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        log.debug("initializing GenericUDTFExplodeJSONArraysSorted");
        //TODO 1 约束函数传入参数的个数,至少一个
        if (argOIs.getAllStructFieldRefs().size() < 1) {
            throw new UDFArgumentLengthException("Function explode_json_arrays_sorted has to input at least one " +
                    "column...");
        }

        //TODO 2 约束函数传入参数的类型
        List<? extends StructField> structFieldRefs = argOIs.getAllStructFieldRefs();

        for (int i = 0; i < structFieldRefs.size(); i++) {
            String typeName = structFieldRefs.get(0).getFieldObjectInspector().getTypeName();
            if (!"string".equals(typeName)) {
                log.error("Got" + typeName + "instead of string...");
                throw new UDFArgumentTypeException(i, "explode_json_arrays_sorted Function，The type of the" + i +
                        "th" + "arg can only be String...," + "but \"" + typeName + "\" is found");
            }
        }

        //TODO 3 约束函数返回值的类型
        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();
        for (int i = 0; i < structFieldRefs.size()+1; i++) {
            fieldNames.add("col_" + (i + 1)); //列别名
            fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);  // 列类型
        }

        log.debug("done initializing GenericUDTFExplodeJSONArraysSorted");
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    /**
     * 处理逻辑
     * 将多个jsonArray字段,按照顺序炸开相同的行数,空值赋{}
     * 例:
     * 原始数据
     * row
     * col_1:[{k:v,...},{k:v,...},{k:v,...},{k:v,...}]
     * col_2:[{k:v,...},{k:v,...}]
     * 炸开后:
     *         row_1  ,  row_2  ,  row_3  ,  row_4
     * col_1:{k:v,...},{k:v,...},{k:v,...},{k:v,...}
     * col_2:{k:v,...},{k:v,...},{}       ,{}
     * index:    0    ,    1    ,    2    ,    3
     *
     * @param args object array of arguments
     */
    @Override
    public void process(Object[] args) throws HiveException {
        // 结果数组，长度等于返回的字段数，+1字段用于记录index
        String[] result = new String[args.length + 1];
        // 把每一个传入的字段解析为jsonArray然后存放到集合中
        ArrayList<JSONArray> jsonArrayList = new ArrayList<>();
        // 记录每个元素的长度
        int[] lengths = new int[args.length];
        // TODO 初始化
        // 解析jsonArrya并记录每一个jsonArray的长度，并将jsonArray放入集合，脏数据清洗赋值"[]"
        for (int i = 0; i < args.length; i++) {
            JSONArray jsonArray;
            try {
                jsonArray = new JSONArray(args[i].toString());
            } catch (JSONException jsonException) {
                log.error("Have Drity Data...，is not in jsonArray format,input data : " + args[i].toString() + " ;");
                try {
                    jsonArray = new JSONArray("[" + args[i].toString() + "]");
                } catch (JSONException je) {
                    log.error("Input data is not in json format and cannot be parsed");
                    jsonArray = new JSONArray("[]");
                }
            } catch (NullPointerException ne) {
                log.error(ne + ",Input Null Data...;Null Date is args[" + i + "]");
                jsonArray = new JSONArray("[]");
            }
            jsonArrayList.add(jsonArray);
            lengths[i] = jsonArray.length();
        }
        // 求出所有输入jsonArray的最大长度
        int[] sortLengths = Arrays.stream(lengths).sorted().toArray();
        int maxLengthJsonArray = sortLengths[sortLengths.length - 1];
        if (maxLengthJsonArray > 0) {
            // TODO 遍历返回
            for (int j = 0; j < maxLengthJsonArray; j++) {
                for (int i = 0; i < jsonArrayList.size(); i++) {
                    String json;
                    // 遍历取第i个jsaonArray的第j个json
                    try {
                        json = jsonArrayList.get(i).get(j).toString();
                    } catch (JSONException je) {
                        json = "{}";
                    }
                    result[i] = json;
                }
                // 上游回传数据的index有可能是错误的，单独增加一个字段记录实际index,便于作为炸开后数据的联合主键使用
                result[result.length - 1] = String.valueOf(j);
                forward(result);
            }
        }else {
            // 入参都是null,无法炸开,index记录0并返回
            result[result.length - 1] = String.valueOf(0);
            forward(result);
        }
    }

    @Override
    public void close() throws HiveException {

    }
}

