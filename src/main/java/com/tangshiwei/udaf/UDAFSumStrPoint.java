package com.tangshiwei.udaf;


import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

@org.apache.hadoop.hive.ql.exec.Description(name="sum_str_point",value = "_FUNC_(arguments) converts \"0,1,0,1,0,1,\"the string into" +
        " the array  [1,0,1,0,1], according to the array and array index aggregate sum,And returns the string \"0," +
        "1,0,1,0,1,\"")
public class UDAFSumStrPoint extends UDAF {
    private static final Logger LOG = LoggerFactory.getLogger(UDAFSumStrPoint.class);

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
            if (value == null) {
                return true;
            }
            // 把字符串0,11,30,21,0,1转化为 Integer[] integers = {0,1,0,1,0,1};
            ArrayList<Integer> integers = new ArrayList<>();
            String[] split = value.split(",");
            for (int i = 0; i < split.length; i++) {
                integers.add(Integer.parseInt(split[i]));
            }
            // 第一次把 sumMapPoint 替换为 ints
            if (sumMapPoint == null) {
                sumMapPoint = integers;
            } else {
                // 判断数据是否为脏数据
                if (sumMapPoint.size() != integers.size()) {
                    throw new RuntimeException("原数据长度为: " + sumMapPoint.size() + " ,输入聚合数据长度为: " + integers.size() +
                            " ,脏数据无法聚合");
                }
                // 否则进行向量求和
                ArrayList<Integer> result = new ArrayList<>();
                for (int i = 0; i < integers.size(); i++) {
                    result.add(sumMapPoint.get(i) + integers.get(i));
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
