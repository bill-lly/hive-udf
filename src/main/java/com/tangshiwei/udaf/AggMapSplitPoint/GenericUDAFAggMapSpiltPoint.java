package com.tangshiwei.udaf.AggMapSplitPoint;


import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import java.io.Serializable;
import java.util.Collection;

/**
 * 未完成
 */
@Description(name = "letters", value = "_FUNC_(expr) - 返回该列中所有字符串的字符总数")
public class GenericUDAFAggMapSpiltPoint extends AbstractGenericUDAFResolver {
    public GenericUDAFAggMapSpiltPoint() {
    }
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {
        // 限定参数为一个
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly one argument is expected.");
        }

        ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(parameters[0]);
        // 输入类型为基本数据类型
        if (oi.getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                    "Argument must be PRIMITIVE, but "
                            + oi.getCategory().name()
                            + " was passed.");
        }

        PrimitiveObjectInspector inputOI = (PrimitiveObjectInspector) oi;
        // 只能输入STRING类型
        if (inputOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentTypeException(0,
                    "Argument must be String, but "
                            + inputOI.getPrimitiveCategory().name()
                            + " was passed.");
        }
        return new GenericUDAFAggMapSpiltPointEvaluator();
    }
}
