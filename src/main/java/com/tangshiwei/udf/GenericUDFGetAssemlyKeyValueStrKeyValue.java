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
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/*解析拼装键值对字符串,通过key获取value
case:
  输入:参数1: 拼装字符串"assemlyKeyValueStr" 例如:"incidentId#DW006:itemId#1132:crewId#6524"
      参数2: 各个键值对之间分隔符"betweenKVSeparator"
      参数3: 键值对键和值之间的分隔符"kvSeparator"
      参数4: 需要获取的值的键"key"
  输出:金额amount
*/
@Description(name = "get_assemly_key_value_str_key_value", value = "")
public class GenericUDFGetAssemlyKeyValueStrKeyValue extends GenericUDF {
    private static final Logger LOG = LoggerFactory.getLogger(GenericUDFGetAssemlyKeyValueStrKeyValue.class);

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
        Object assemlyKeyValue = deferredObjects[0].get();
        Object betweenKVSeparator = deferredObjects[1].get();
        Object kvSeparator = deferredObjects[2].get();
        Object key = deferredObjects[3].get();
        if (assemlyKeyValue == null || betweenKVSeparator == null || kvSeparator == null || key == null) {
            return null;
        }

        String assemlyKeyValueStr = assemlyKeyValue.toString();
        String betweenKVSeparatorStr = betweenKVSeparator.toString();
        String kvSeparatorStr = kvSeparator.toString();
        String keyStr = key.toString();

        Map<String, String> keyValueMap = new HashMap<>();
        String[] keyValueArr = assemlyKeyValueStr.split(betweenKVSeparatorStr);
        String[] keyValue;
        for (int keyValueArrIndex = 0; keyValueArrIndex < keyValueArr.length; keyValueArrIndex++){
            keyValue = keyValueArr[keyValueArrIndex].split(kvSeparatorStr);
            keyValueMap.put(keyValue[0], keyValue[1]);
        }
        return keyValueMap.get(keyStr);
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString(GenericUDFGetAssemlyKeyValueStrKeyValue.class.getName(), children);
    }

    //测试
//    public String evaluateTest(Object assemlyKeyValue, Object betweenKVSeparator, Object kvSeparator, Object key) throws HiveException {
//        if (assemlyKeyValue == null || betweenKVSeparator == null || kvSeparator == null || key == null) {
//            return null;
//        }
//
//        String assemlyKeyValueStr = assemlyKeyValue.toString();
//        String betweenKVSeparatorStr = betweenKVSeparator.toString();
//        String kvSeparatorStr = kvSeparator.toString();
//        String keyStr = key.toString();
//
//        Map<String, String> keyValueMap = new HashMap<>();
//        String[] keyValueArr = assemlyKeyValueStr.split(betweenKVSeparatorStr);
//        String[] keyValue;
//        for (int keyValueArrIndex = 0; keyValueArrIndex < keyValueArr.length; keyValueArrIndex++){
//            keyValue = keyValueArr[keyValueArrIndex].split(kvSeparatorStr);
//            keyValueMap.put(keyValue[0], keyValue[1]);
//        }
//        return keyValueMap.get(keyStr);
//    }
//
//    public static void main(String[] args) throws HiveException {
//        GenericUDFGetAssemlyKeyValueStrKeyValue fun = new GenericUDFGetAssemlyKeyValueStrKeyValue();
//        String result = fun.evaluateTest("incidentId#DW007:itemId#1132:crewId#6524", ":", "#", "itemId");
//        System.out.println(result);
//    }
}
