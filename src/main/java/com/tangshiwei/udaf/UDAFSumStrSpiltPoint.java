package com.tangshiwei.udaf;


import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

@org.apache.hadoop.hive.ql.exec.Description(name = "",value = "_FUNC_(arguments) converts the \"010101\" string " +
        "into the array [0,1,0,1,0,1] and sums the data in the same group by index aggregate," +
        " And returns the string: 0,1,0,1,0,1,")
public class UDAFSumStrSpiltPoint extends UDAF {
    private static final Logger LOG = LoggerFactory.getLogger(UDAFSumStrSpiltPoint.class);

    public static class Evaluator implements UDAFEvaluator {
        // 记录中间和最终结果
        private ArrayList<Integer> sumMapPoint;

        public Evaluator() {
            super();
            init();
        }

        /**
         * 初始化方法
         */
        public void init() {
            reset();
        }

        /**
         * 逻辑处理,反复调用
         *
         * @param value
         * @return
         */
        public boolean iterate(String value) {
            if (value == null || value.length() == 0) {
                return true;
            }
            // 把010101转化为 Integer[] ints = {0,1,0,1,0,1};
            Integer[] ints = new Integer[value.length()];
            for (int i = 0; i < value.length(); i++) {
                ints[i] = (Integer) (value.charAt(i) - '0');
            }
            // 第一次把 sumMapPoint 替换为 ints
            if (sumMapPoint == null) {
                sumMapPoint = new ArrayList<Integer>();
                for (Integer i : ints) {
                    sumMapPoint.add(i);
                }
            } else {
                // 否则进行向量求和
                ArrayList<Integer> result = new ArrayList<>();
                for (int i = 0; i < ints.length; i++) {
                    result.add(sumMapPoint.get(i) + ints[i]);
                }
                sumMapPoint = result;
            }
            return true;
        }

        /**
         * 当前阶段结束时执行的方法，返回的是部分聚合的结果（map、combiner）
         *
         * @return
         */
        public ArrayList<Integer> terminatePartial() {
            return sumMapPoint;
        }

        /**
         * 合并数据
         *
         * @param other
         * @return
         */
        public boolean merge(ArrayList<Integer> other) {
            if (other == null) {
                return true;
            } else {
                ArrayList<Integer> result = new ArrayList<>();
                for (int i = 0; i < other.size(); i++) {
                    result.add(getSumMapPoint(sumMapPoint, i) + other.get(i));
                }
                sumMapPoint = result;
            }
            return true;
        }

        private Integer getSumMapPoint(ArrayList<Integer> sumMapPoint, Integer i) {
            if (sumMapPoint == null || sumMapPoint.size() == 0) {
                return 0;
            }
            return sumMapPoint.get(i);
        }

        /**
         * group by的时候返回当前分组的最终结果
         *
         * @param
         * @return
         * @throws
         */
        public String terminate() {
            StringBuilder sb = new StringBuilder();
            for (Integer integer : sumMapPoint) {
                sb.append(integer).append(",");
            }
            return sb.toString();
        }

        private void reset() {
            if (sumMapPoint != null) {
                sumMapPoint.clear();
            }
        }
    }
}
