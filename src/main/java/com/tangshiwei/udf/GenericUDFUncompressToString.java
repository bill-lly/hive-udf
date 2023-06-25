package com.tangshiwei.udf;


import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;

@org.apache.hadoop.hive.ql.exec.Description(name = "uncompress_to_string", value = "_FUNC_(arg) - This function first" +
        " decrypts data using Base64 decoding and then decompresses it using Gzip.")
public class GenericUDFUncompressToString extends GenericUDF {
    private static final Logger log = LoggerFactory.getLogger(GenericUDFUncompressToString.class);

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
            throw new UDFArgumentTypeException(1, "Please input String arg");
        }
        //TODO 3.约束返回值类型
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    /**
     * 处理逻辑
     *
     * @param arguments
     * @return
     * @throws HiveException
     */
    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object o = arguments[0].get();
        if (o == null) {
            return "";
        } else {
            byte[] bytes = String.valueOf(o).getBytes(StandardCharsets.ISO_8859_1);
            try {
                return uncompressToString(bytes);
            } catch (Exception e) {
                log.error("java.util.zip.ZipException: invalid distance too far back");
                return "";
            }
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
        return getStandardDisplayString(GenericUDFUncompressToString.class.getName(), children);
    }

    public static String uncompressToString(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        try {
            GZIPInputStream ungzip = new GZIPInputStream(in);
            byte[] buffer = new byte[256];
            int n;
            while ((n = ungzip.read(buffer)) >= 0) {
                out.write(buffer, 0, n);
            }
            return out.toString(StandardCharsets.UTF_8.name());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
