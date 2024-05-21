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

/*获取json数组中同一个key所有value值
case:
  输入:参数1:"[{"group": "firmware", "artifact": "system", "requirements": {"version": "=M6-05-1-c"}}, {"group": "app",
            "artifact": "app", "requirements": {"version": "=v2.27.17.M"}}, {"group": "hardware", "artifact": "mcu",
            "requirements": {"version": "=V0.25.79-V0.5042.45"}}, {"group": "hardware", "artifact": "router", "requirements":
            {"version": "=v2.4.6-router"}}, {"group": "conveyor", "artifact": "conveyor", "requirements": {"version": "=t2.2
            .0"}}]"
      参数2: "$.requirements.version"
      参数3(可选，默认","): ","
  输出:=V3-16-0-75-a,=v2.18.8.75,=V1.9.30-75-4TH,=v2.3.6-router-1,=t1.1.1
*/
@org.apache.hadoop.hive.ql.exec.Description(name = "get_jsonarray_key_all_values", value = "_FUNC_(String jsonArray," +
        "String path,String separator)")
public class GenericUDFGetJsonArrayKeyAllValue extends GenericUDF {
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
        if (arguments.length == 3) {
            //TODO 2.检查入参类型
            if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE && ((PrimitiveObjectInspector) arguments[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING
                    && arguments[1].getCategory() != ObjectInspector.Category.PRIMITIVE && ((PrimitiveObjectInspector) arguments[1]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING
                    && arguments[2].getCategory() != ObjectInspector.Category.PRIMITIVE && ((PrimitiveObjectInspector) arguments[1]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
                throw new UDFArgumentTypeException(1, "Please input string arg");
            }
        } else if (arguments.length == 2) {
            //TODO 2.检查入参类型
            if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE && ((PrimitiveObjectInspector) arguments[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING
                    && arguments[1].getCategory() != ObjectInspector.Category.PRIMITIVE && ((PrimitiveObjectInspector) arguments[1]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
                throw new UDFArgumentTypeException(1, "Please input string arg");
            }
        } else {
            throw new UDFArgumentLengthException("Please input only two args or three args");
        }
        //TODO 3.约束返回值类型
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        Object jsonArray = deferredObjects[0].get();
        Object path = deferredObjects[1].get();
        Object separator;
        // 只传入2个参数获取不到分隔符,处理索引越界,给分隔符赋默认值:","
        try {
            separator = deferredObjects[2].get();
        }catch (Exception e){
            separator=",";
        }
        if (jsonArray == null || path == null || !isValidPathFormat(path.toString())) {
            return null;
        }
        JSONArray array;
        try {
            array = new JSONArray(jsonArray.toString());
        } catch (JSONException jsonException) {
            return null;
        }
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < array.length(); i++) {
            JSONObject jsonObject = array.getJSONObject(i);
            String value = getValueByPath(jsonObject, path.toString());

            if (value != null) {
                result.append(value);
                if (i < array.length() - 1) {
                    result.append(separator.toString());
                }
            }
        }
        return result.toString();
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString(GenericUDFGetJsonArrayKeyAllValue.class.getName(), children);
    }

    // path格式校验
    public static boolean isValidPathFormat(String path) {
        String regex = "^\\$\\.([a-zA-Z0-9]+\\.)*[a-zA-Z0-9]+$";
        return path.matches(regex);
    }

    private static String getValueByPath(JSONObject jsonObject, String path) throws JSONException {
        String[] keys = path.substring(2).split("\\.");

        for (String key : keys) {
            if (jsonObject.has(key)) {
                Object value = jsonObject.get(key);

                if (value instanceof JSONObject) {
                    jsonObject = (JSONObject) value;
                } else if (value instanceof JSONArray) {
                    JSONArray jsonArray = (JSONArray) value;

                    StringBuilder arrayResult = new StringBuilder();
                    for (int i = 0; i < jsonArray.length(); i++) {
                        if (i > 0) {
                            arrayResult.append(",");
                        }
                        arrayResult.append(jsonArray.get(i));
                    }

                    return arrayResult.toString();
                } else {
                    return value.toString();
                }
            } else {
                return null;
            }
        }

        return null;
    }

    //单元测试
//    private Object evaluateTest(String jsonArray, String path, Object separator) {
//
//        if (jsonArray == null || path == null || !isValidPathFormat(path)) {
//            return null;
//        }
//        JSONArray array;
//        try {
//            array = new JSONArray(jsonArray);
//        } catch (JSONException jsonException) {
//            return null;
//        }
//        StringBuilder result = new StringBuilder();
//        for (int i = 0; i < array.length(); i++) {
//            JSONObject jsonObject = array.getJSONObject(i);
//            String value = getValueByPath(jsonObject, path);
//
//            if (value != null) {
//                result.append(value);
//                if (i < array.length() - 1) {
//                    result.append(separator.toString());
//                }
//            }
//        }
//        return result.toString();
//
//    }
//
//    public static void main(String[] args) {
//        String s = "[{\"group\": \"firmware\", \"artifact\": \"system\", \"requirements\": {\"version\": " +
//                "\"=V3-16-0-75-a\"}}, {\"group\": \"app\", \"artifact\": \"app\", \"requirements\": {\"version\": " +
//                "\"=v2.18.8.75\"}}, {\"group\": \"hardware\", \"artifact\": \"mcu\", \"requirements\": {\"version\": " +
//                "\"=V1.9.30-75-4TH\"}}, {\"group\": \"hardware\", \"artifact\": \"router\", \"requirements\": " +
//                "{\"version\": \"=v2.3.6-router-1\"}}, {\"group\": \"conveyor\", \"artifact\": \"conveyor\", " +
//                "\"requirements\": {\"version\": \"=t1.1.1\"}}]";
//        String s1 = "[{\"cleanActionName\":2}]";
//        GenericUDFGetJsonArrayKeyAllValue fun = new GenericUDFGetJsonArrayKeyAllValue();
//        String result = (String) fun.evaluateTest(s1, "$.cleanActionName", null);
//        System.out.println(result);
//    }
}
