package com.tangshiwei.udaf;


import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

@org.apache.hadoop.hive.ql.exec.Description(name = "sum_array_by_index", value = "_FUNC_(arguments) Sum up multiple " +
        "one-dimensional arrays as one-dimensional vectors.")
public class UDAFSumArrayByIndex extends UDAF {
    private static final Logger LOG = LoggerFactory.getLogger(UDAFSumArrayByIndex.class);
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
        public boolean iterate(ArrayList<Integer> value) {
            if (value == null) {
                return true;
            }
            // 第一次把 sumMapPoint 替换为 ints
            if (sumMapPoint == null) {
                sumMapPoint = new ArrayList<Integer>();
                for (Integer i : value) {
                    sumMapPoint.add(i);
                }
            } else {
                // 否则进行向量求和
                ArrayList<Integer> result = new ArrayList<>();
                for (int i = 0; i < value.size(); i++) {
                    result.add(sumMapPoint.get(i) + value.get(i));
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
        public ArrayList<Integer> terminate() {
            return sumMapPoint;
        }

        private void reset() {
            if (sumMapPoint != null) {
                sumMapPoint.clear();
            }
        }
    }
}
