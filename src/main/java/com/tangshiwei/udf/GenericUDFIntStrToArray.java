package com.tangshiwei.udf;

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
import java.util.ArrayList;

@org.apache.hadoop.hive.ql.exec.Description(name = "str_to_array", value = "_FUNC_(arguments) - This function is " +
        "convert the string \"0101001...\" to array<int>  [0,1,0,1,0,0,1,...]")
public class GenericUDFIntStrToArray extends GenericUDF {
    private static final Logger LOG = LoggerFactory.getLogger(GenericUDFIntStrToArray.class);

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
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("Please input only one arg");
        }
        //TODO 2.检查入参类型
        if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE && ((PrimitiveObjectInspector) arguments[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING
        ) {
            throw new UDFArgumentTypeException(1, "Please input string arg");
        }
        //TODO 3.约束返回值类型 Array<int>
        return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
    }


    /**
     * 处理逻辑  string 0101001 -> Array<int>=[0,1,0,1,0,0,1]
     *
     * @param arguments
     * @return
     * @throws HiveException
     */
    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments[0].get() == null) {
            return null;
        } else {
            String str = arguments[0].get().toString();
            // 把010101转化为int[] ints = {0,1,0,1,0,1};
            ArrayList<Integer> integers = new ArrayList<>();
            for (int i = 0; i < str.length(); i++) {
                integers.add((Integer) (str.charAt(i) - '0'));
            }
            return integers;
        }
    }

    /**
     * 帮助信息
     *
     * @param children
     * @return
     */
    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString(GenericUDFIntStrToArray.class.getName(), children);
    }
}
