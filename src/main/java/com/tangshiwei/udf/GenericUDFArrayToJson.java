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

@Description(name = "array_to_json", value = "数组转成json,第一个参数为数组字符串,格式必须是'[...]',如果传多个参数,从第二个参数开始一次作为json的key,如果传入一个参数,默认json的key为key1、key2...")
public class GenericUDFArrayToJson extends GenericUDF {
    private static final Logger LOG = LoggerFactory.getLogger(GenericUDFArrayToJson.class);

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        //TODO 1.检查入参个数
        if (arguments.length == 0) {
            throw new UDFArgumentLengthException("Please input at least one argument");
        }
        //TODO 2.检查入参类型
        for(int argIndex = 0; argIndex < arguments.length; argIndex++){
            if (arguments[argIndex].getCategory() != ObjectInspector.Category.PRIMITIVE && ((PrimitiveObjectInspector) arguments[argIndex]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
                throw new UDFArgumentTypeException(1, "Please input string arg");
            }
        }
        //TODO 3.约束返回值类型string
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments[0].get() != null && arguments[0].get().toString().matches("^\\[+.*\\]$")) {
            JSONObject resultJson = new JSONObject();
            String arrStr = arguments[0].get().toString();
            String regEx = "[\\[\\]\"\' ]";
            arrStr = arrStr.replaceAll(regEx, "");
            String[] arrStrSplit = arrStr.split(",");
            if(arrStrSplit.length > 1 || (arrStrSplit.length == 1 && !arrStrSplit[0].equals(""))) {
                if (arguments.length >= 2) {
                    if(arrStrSplit.length >= arguments.length - 1) {
                        for (int argIndex = 1; argIndex < arguments.length; argIndex++) {
                            resultJson.put(arguments[argIndex].get().toString(), arrStrSplit[argIndex - 1]);
                        }
                    } else {
                        LOG.error("传入的key值个数超过array元素的个数");
                        return null;
                    }
                } else {
                    for (int arrStrSplitIndex = 0; arrStrSplitIndex < arrStrSplit.length; arrStrSplitIndex++) {
                        resultJson.put("k" + (arrStrSplitIndex + 1), arrStrSplit[arrStrSplitIndex]);
                    }
                }
                return resultJson.toString();
            } else {
                LOG.error("传入array元素的个数为0");
                return null;
            }
        } else {
            LOG.error("传入的array参数为空或格式不正确,示例[a1,a2,...]");
            return null;
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString(GenericUDFArrayToJson.class.getName(), children);
    }

    //测试
//    public String evaluateTest(String[] arguments) throws HiveException {
//        if (arguments[0] != null && arguments[0].matches("^\\[+.*\\]$")) {
//            JSONObject resultJson = new JSONObject();
//            String arrStr = arguments[0].toString();
//            String regEx = "[\\[\\]\"\' ]";
//            arrStr = arrStr.replaceAll(regEx, "");
//            String[] arrStrSplit = arrStr.split(",");
//            if(arrStrSplit.length >1 || (arrStrSplit.length == 1 && !arrStrSplit[0].equals(""))) {
//                if (arguments.length >= 2) {
//                    if(arrStrSplit.length >= arguments.length - 1) {
//                        for (int argIndex = 1; argIndex < arguments.length; argIndex++) {
//                            resultJson.put((String) arguments[argIndex], arrStrSplit[argIndex - 1]);
//                        }
//                    } else {
//                        LOG.error("传入的key值个数超过array元素的个数");
//                        return null;
//                    }
//                } else {
//                    for (int arrStrSplitIndex = 0; arrStrSplitIndex < arrStrSplit.length; arrStrSplitIndex++) {
//                        resultJson.put("k" + (arrStrSplitIndex + 1), arrStrSplit[arrStrSplitIndex]);
//                    }
//                }
//                return resultJson.toString();
//            } else {
//                LOG.error("传入array元素的个数为0");
//                return null;
//            }
//        } else {
//            LOG.error("传入的array参数为空或格式不正确,示例[a1,a2,...]");
//            return null;
//        }
//    }
//
//    public static void main(String[] args) throws HiveException {
//        GenericUDFArrayToJson fun = new GenericUDFArrayToJson();
//        String[] arguments = new String[]{};
//        String result = fun.evaluateTest(arguments);
//        System.out.println(result);
//    }
}
