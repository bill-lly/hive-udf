package com.tangshiwei.udaf.AggMapSplitPoint;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMkCollectionEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * 未完成
 */
public class GenericUDAFAggMapSpiltPointEvaluator extends GenericUDAFEvaluator implements Serializable {
    private static final long serialVersionUID = 1l;

    enum BufferType {SET, LIST}

    // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
    private transient ObjectInspector inputOI;
    // For Other
    private transient ListObjectInspector outputOI;
    private transient StandardListObjectInspector loi;
    private transient ListObjectInspector internalMergeOI;
    private BufferType bufferType;


    //needed by kyro
    public GenericUDAFAggMapSpiltPointEvaluator() {
    }

    public GenericUDAFAggMapSpiltPointEvaluator(BufferType bufferType) {
        this.bufferType = bufferType;
    }

    int total = 0;

    // 初始化方法
    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
            throws HiveException {

        assert (parameters.length == 1);
        super.init(m, parameters);

        //map阶段读取sql列，输入为String基础数据格式
        if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
            inputOI = parameters[0];
        } else {
            // 其余阶段，输入和输出均为 List 数据格式
            // 输入
            // 输出
            outputOI = (ListObjectInspector) parameters[0];
            if (!(parameters[0] instanceof ListObjectInspector)) {
                //no map aggregation.
                inputOI = ObjectInspectorUtils.getStandardObjectInspector(parameters[0]);
                return ObjectInspectorFactory.getStandardListObjectInspector(inputOI);
            } else {
                internalMergeOI = (ListObjectInspector) parameters[0];
                inputOI = internalMergeOI.getListElementObjectInspector();
                loi = (StandardListObjectInspector)
                        ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI);
                return loi;
            }
        }

        // 指定各个阶段输出数据格式都为 List 类型
        outputOI = (ListObjectInspector) ObjectInspectorFactory.getReflectionObjectInspector(List.class,
                ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
        return outputOI;

    }


    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
        return new AggMapSpiltPointBuffer();
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
        ((GenericUDAFAggMapSpiltPointEvaluator.AggMapSpiltPointBuffer) agg).container.clear();
    }

    /**
     * PARTIAL1(map)和 COMPLETE(only map)阶段调用
     *
     * @param agg
     * @param parameters The objects of parameters.
     * @throws HiveException
     */
    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters)
            throws HiveException {
        assert (parameters.length == 1);
        Object p = parameters[0];
        if (p != null) {
            AggMapSpiltPointBuffer aggPointBuffer = (AggMapSpiltPointBuffer) agg;
            AverageValue(p.toString(), aggPointBuffer);
        }
    }

    /**
     * PARTIAL2(combiner) 和 FINAL(reduce) 阶段调用
     *
     * @param agg
     * @param partial The partial aggregation result.
     * @throws HiveException
     */
    @Override
    public void merge(AggregationBuffer agg, Object partial)
            throws HiveException {


    }
    
    /**
     * PARTIAL1(map) 和 PARTIAL2(combiner)阶段调用
     *
     * @param agg
     * @return
     * @throws HiveException
     */
    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {

        return null;
    }

    /**
     * 最终返回结果
     *
     * @param agg
     * @return
     * @throws HiveException
     */
    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {

        return null;
    }

    /**
     * 合并数据的具体实现方法
     *
     * @param value
     * @param agg
     */
    public void AverageValue(String value, AggMapSpiltPointBuffer agg) {
        int[] arr = new int[value.length()];

        for (int i = 0; i < value.length(); i++) {
            arr[i] = (int) (value.charAt(i) - '0');
        }
        Collection<Object> result = new ArrayList<>();
        Collection<Object> container = agg.container;
        Object[] objects = container.toArray();
        for (int i = 0; i < objects.length; i++) {
            result.add(arr[i]+(int)objects[i]);
        }
        agg = (AggMapSpiltPointBuffer) result;
    }

    public class AggMapSpiltPointBuffer extends AbstractAggregationBuffer {
        private Collection<Object> container;

        public AggMapSpiltPointBuffer() {
            if (bufferType == BufferType.LIST) {
                container = new ArrayList<Object>();
            } else if (bufferType == BufferType.SET) {
                container = new LinkedHashSet<Object>();
            } else {
                throw new RuntimeException("Buffer type unknown");
            }
        }
    }

    public static void main(String[] args) {
        Object[] objects={1,2,3};
        int[] ints = new int[3];
        for (int i = 0; i < objects.length; i++) {
            Object object = objects[i];
            int int1 = (int) objects[i];
            ints[i]=(int)objects[i];
        }
        for (int in : ints) {
            System.out.println(in);
        }
    }
}
