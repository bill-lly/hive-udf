package com.tangshiwei.udf;

import org.json.JSONObject;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

@Description(name = "json_to_map", value = "_FUNC_(arguments) - This function is convert the json string  to " +
        "map<string,string>")
public class GenericUDFJsonToMap extends GenericUDF {
    private static final Logger LOG = LoggerFactory.getLogger(GenericUDFJsonToMap.class);

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        //TODO 1.检查入参个数
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("Please input only one arg");
        }
        //TODO 2.检查入参类型
        if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE && ((PrimitiveObjectInspector) arguments[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING
        ) {
            throw new UDFArgumentTypeException(1, "Please input string arg");
        }
        //TODO 3.约束返回值类型 Map
        return ObjectInspectorFactory.getStandardMapObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector, PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Map map = null;

        if (arguments[0].get() == null) {
            return map;
        } else {
            String str = arguments[0].get().toString();
            try {
                JSONObject jsonObject = new JSONObject(str);
                map = new HashMap<String, String>();
                Iterator<String> keys = jsonObject.keys();
                while (keys.hasNext()) {
                    String key = keys.next();
                    Object value = jsonObject.get(key);
                    map.put(key, value.toString());
                }
                return map;
            } catch (Exception e) {
                LOG.error("Error data,The Exception is:" + e);
                return map;
            }
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString(GenericUDFJsonToMap.class.getName(), children);
    }

}
