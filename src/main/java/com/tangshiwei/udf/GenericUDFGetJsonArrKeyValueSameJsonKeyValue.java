package com.tangshiwei.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Map;
import java.util.TreeMap;

/*获取json数组中key value所在json指定key对应value值
case:
  输入:参数1:"[{"coefficient": 1.0, "checkLevelName": "GOOD"}, {"coefficient": 0.8, "checkLevelName": "ORDINARY"}, {"coefficient": 0.6, "checkLevelName": "POOR"}, {"coefficient": 0.4, "checkLevelName": "INVALID"}]"
      参数2: "queryKey"
      参数3: "queryValue"
      参数4: "targetKey"
  输出:1.0
*/
@org.apache.hadoop.hive.ql.exec.Description(name = "get_json_array_kv_same_json_kv", value = "")
public class GenericUDFGetJsonArrKeyValueSameJsonKeyValue extends GenericUDF {
    /**
     * 判断传入参数类型
     *
     * @param arguments
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        //TODO 1.检查入参个数
        if (arguments.length == 4) {
            //TODO 2.检查入参类型
            if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE && ((PrimitiveObjectInspector) arguments[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING
                    && arguments[1].getCategory() != ObjectInspector.Category.PRIMITIVE && ((PrimitiveObjectInspector) arguments[1]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING
                    && arguments[2].getCategory() != ObjectInspector.Category.PRIMITIVE && ((PrimitiveObjectInspector) arguments[2]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING
                    && arguments[3].getCategory() != ObjectInspector.Category.PRIMITIVE && ((PrimitiveObjectInspector) arguments[3]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
                throw new UDFArgumentTypeException(1, "Please input string arg");
            }
        } else {
            throw new UDFArgumentLengthException("Please input four args");
        }
        //TODO 3.约束返回值类型
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        Object jsonArray = deferredObjects[0].get();
        Object queryKey = deferredObjects[1].get();
        Object queryValue = deferredObjects[2].get();
        Object targetKey = deferredObjects[3].get();
        if (jsonArray == null || queryKey == null || queryValue == null || targetKey == null) {
            return null;
        }

        //传入参数字段类型转换
        String jsonArrayStr = jsonArray.toString();
        String queryKeyStr = queryKey.toString();
        String queryValueStr = queryValue.toString();
        String targetKeyStr = targetKey.toString();

        //解析json
        JSONArray array = null;
        try {
            array = new JSONArray(jsonArrayStr);
        } catch (JSONException jsonException) {
            return null;
        }

        String targetValueStr = null;
        for (int i = 0; i < array.length(); i++) {
            JSONObject jsonObject = array.getJSONObject(i);
            if(jsonObject.get(queryKeyStr).toString().equals(queryValueStr)){
                targetValueStr = jsonObject.get(targetKeyStr).toString();
                break;
            }
        }
        return targetValueStr;
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString(GenericUDFGetJsonArrKeyValueSameJsonKeyValue.class.getName(), children);
    }


    //单元测试
//    private String evaluateTest(String jsonArray, String queryKey, String queryValue, String targetKey) {
//
//        if (jsonArray == null || queryKey == null || queryValue == null || targetKey == null) {
//            return null;
//        }
//
//        //传入参数字段类型转换
//        String jsonArrayStr = jsonArray.toString();
//        String queryKeyStr = queryKey.toString();
//        String queryValueStr = queryValue.toString();
//        String targetKeyStr = targetKey.toString();
//
//        //解析json
//        JSONArray array = null;
//        try {
//            array = new JSONArray(jsonArrayStr);
//        } catch (JSONException jsonException) {
//            return null;
//        }
//
//        String targetValueStr = null;
//        for (int i = 0; i < array.length(); i++) {
//            JSONObject jsonObject = array.getJSONObject(i);
//            if(jsonObject.get(queryKeyStr).toString().equals(queryValueStr)){
//                targetValueStr = jsonObject.get(targetKeyStr).toString();
//                break;
//            }
//        }
//        return targetValueStr;
//    }
//
//    public static void main(String[] args) {
//        String s = "[{\"coefficient\": 1.0, \"checkLevelName\": \"GOOD\"}, {\"coefficient\": 2.0, \"checkLevelName\": \"GOOD\"}, {\"coefficient\": 0.8, \"checkLevelName\": \"ORDINARY\"}, {\"coefficient\": 0.6, \"checkLevelName\": \"POOR\"}, {\"coefficient\": 0.4, \"checkLevelName\": \"INVALID\"}]";
//        GenericUDFGetJsonArrKeyValueSameJsonKeyValue fun = new GenericUDFGetJsonArrKeyValueSameJsonKeyValue();
//        String result = fun.evaluateTest(s, "checkLevelName", "GOOD", "coefficient");
//        System.out.println(result);
//    }
}
