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
import org.json.JSONObject;
/*获取json数组中同一个key所有value值
case:
  输入:参数1:"[{"cleanActionId":33,"cleanActionName":"内部擦拭"},{"cleanActionId":41,"cleanActionName":"更换纸品"}]"
      参数2: "cleanActionName"
  输出:内部擦拭,更换纸品
*/
public class GenericUDFGetJsonArrayKeyAllValue extends GenericUDF {
    /**
     * 判断传入参数类型
     * @param arguments
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        //TODO 1.检查入参个数
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException("Please input only one arg");
        }
        //TODO 2.检查入参类型
        if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE && ((PrimitiveObjectInspector) arguments[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING
        && arguments[1].getCategory() != ObjectInspector.Category.PRIMITIVE && ((PrimitiveObjectInspector) arguments[1]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING){
            throw new UDFArgumentTypeException(1,"Please input string arg");
        }
        //TODO 3.约束返回值类型
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        Object arg_0 = deferredObjects[0].get();
        Object arg_1 = deferredObjects[1].get();
        if(arg_0 == null || arg_1 == null){
            return null;
        }
        String jsonArrStr = arg_0.toString();
        String jsonKey = arg_1.toString();
        if(jsonArrStr == "" || jsonKey == ""){
            return null;
        }
        StringBuilder builder = new StringBuilder(10240);
        JSONArray jsonArray = new JSONArray(jsonArrStr);
        for (int i = 0; i < jsonArray.length(); i++) {
            String jsonStr = jsonArray.getString(i);
            JSONObject jsonObject = new JSONObject(jsonStr);
            builder.append(jsonObject.get(jsonKey)).append(",");
        }
        builder.delete(builder.length() - 1, builder.length());
        return builder.toString();
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString(GenericUDFGetJsonArrayKeyAllValue.class.getName(),children);
    }

    //单元测试
//    private Object evaluateTest(String arg_0, String arg_1) {
//        if(arg_0 == null || arg_1 == null){
//            return null;
//        }
//        String jsonArrStr = arg_0.toString();
//        String jsonKey = arg_1.toString();
//        if(jsonArrStr == "" || jsonKey == ""){
//            return null;
//        }
//        StringBuilder builder = new StringBuilder(10240);
//        JSONArray jsonArray = new JSONArray(jsonArrStr);
//        for (int i = 0; i < jsonArray.length(); i++) {
//            String jsonStr = jsonArray.getString(i);
//            JSONObject jsonObject = new JSONObject(jsonStr);
//            builder.append(jsonObject.get(jsonKey)).append(",");
//        }
//        builder.delete(builder.length() - 1, builder.length());
//        return builder.toString();
//    }
//
//    public static void main(String[] args) {
//        GenericUDFGetJsonArrayKeyAllValue fun = new GenericUDFGetJsonArrayKeyAllValue();
//        String result = (String) fun.evaluateTest(null, "");
//        System.out.println(result);
//    }
}
