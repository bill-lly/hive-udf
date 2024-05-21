package com.tangshiwei.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Iterator;

@Description(name = "get_json_same_prefix_key_value", value = "获取json相同前缀key的value值,返回json字符串")
public class GenericUDFGetJsonSamePrefixKeyValue extends GenericUDF {
    private static final Logger LOG = LoggerFactory.getLogger(GenericUDFGetJsonSamePrefixKeyValue.class);

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        //TODO 1.检查入参个数
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException("Please input two arg");
        }
        //TODO 2.检查入参类型
        if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE && ((PrimitiveObjectInspector) arguments[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING
        && arguments[1].getCategory() != ObjectInspector.Category.PRIMITIVE && ((PrimitiveObjectInspector) arguments[1]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING
        ) {
            throw new UDFArgumentTypeException(1, "Please input string arg");
        }
        //TODO 3.约束返回值类型 Map
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments[0].get() == null || arguments[1].get() == null) {
            return null;
        } else {
            String jsonStr = arguments[0].get().toString();
            String keyPrefixStr = arguments[1].get().toString();
            JSONObject resultJson = new JSONObject();
            try {
                JSONObject jsonObject = new JSONObject(jsonStr);
                Iterator<String> keys = jsonObject.keys();
                while (keys.hasNext()) {
                    String key = keys.next();
                    if(key.startsWith(keyPrefixStr)) {
                        Object value = jsonObject.get(key);
                        resultJson.put(key, value.toString());
                    }
                }
                return resultJson.toString();
            } catch (Exception e) {
                LOG.error("Error data,The Exception is:" + e);
                return null;
            }
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString(GenericUDFGetJsonSamePrefixKeyValue.class.getName(), children);
    }


    //测试
//    public String evaluateTest(String arguments0, String arguments1){
//        if (arguments0 == null || arguments1 == null) {
//            return null;
//        } else {
//            String jsonStr = arguments0;
//            String keyPrefixStr = arguments1;
//            JSONObject resultJson = new JSONObject();
//            try {
//                JSONObject jsonObject = new JSONObject(jsonStr);
//                Iterator<String> keys = jsonObject.keys();
//                while (keys.hasNext()) {
//                    String key = keys.next();
//                    if(key.startsWith(keyPrefixStr)) {
//                        Object value = jsonObject.get(key);
//                        resultJson.put(key, value.toString());
//                    }
//                }
//                return resultJson.toString();
//            } catch (Exception e) {
//                LOG.error("Error data,The Exception is:" + e);
//                return null;
//            }
//        }
//    }
//
//    public static void main(String[] args) {
//        GenericUDFGetJsonSamePrefixKeyValue fun = new GenericUDFGetJsonSamePrefixKeyValue();
//        String result = fun.evaluateTest("{\n" +
//                "\t\"batteryCellNumber\": 16,\n" +
//                "\t\"batteryCellTemperature0\": 40,\n" +
//                "\t\"batteryCellTemperature1\": 41,\n" +
//                "\t\"batteryCurrent\": 6.075,\n" +
//                "\t\"batteryHalfVoltage\": 26412\n" +
//                "}", "batteryCellTemperature");
//        System.out.println(result);
//    }
}
