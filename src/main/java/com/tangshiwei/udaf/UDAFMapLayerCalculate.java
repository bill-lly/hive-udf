package com.tangshiwei.udaf;


import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;

@org.apache.hadoop.hive.ql.exec.Description(name = "map_layer_calculate", value = "_FUNC_(arguments) Calculate layer " +
        "results based on point data.")
public class UDAFMapLayerCalculate extends org.apache.hadoop.hive.ql.exec.UDAF {
    private static final Logger LOG = LoggerFactory.getLogger(UDAFMapLayerCalculate.class);

    public static class Evaluator implements UDAFEvaluator {
        // 记录中间和最终结果
        private HashMap<Integer, Integer> sumMapPoint;

        public Evaluator() {
            super();
            init();
        }

        /**
         * 初始化方法
         */
        public void init() {
            sumMapPoint = new HashMap<Integer, Integer>();
            reset();
        }

        /**
         * 逻辑处理,反复调用
         *
         * @param value
         * @return
         */
        public boolean iterate(int key, int value) {
            if (key == 0) {
                return true;
            }
            sumMapPoint.put(key, value);
            return true;
        }

        /**
         * 当前阶段结束时执行的方法，返回的是部分聚合的结果（map、combiner）
         *
         * @return
         */
        public HashMap<Integer, Integer> terminatePartial() {
            return sumMapPoint;
        }

        /**
         * 合并数据
         *
         * @param other
         * @return
         */
        public boolean merge(HashMap<Integer, Integer> other) {
            if (other == null) {
                return true;
            } else {
                sumMapPoint.putAll(other);
            }
            return true;
        }

        /**
         * group by的时候返回当前分组的最终结果
         *
         * @param
         * @return
         * @throws
         */
        public String terminate() {
            Set<Integer> keySet = sumMapPoint.keySet();
            Integer[] keys = keySet.toArray(new Integer[keySet.size()]);
            Arrays.sort(keys);
            for (int i = keys.length - 2; i >= 0; i--) {
                sumMapPoint.put(keys[i], sumMapPoint.get(keys[i]) + sumMapPoint.get(keys[i + 1]));
            }
            return sumMapPoint.toString();
        }

        private void reset() {
            if (sumMapPoint != null) {
                sumMapPoint.clear();
            }
        }
    }
}
