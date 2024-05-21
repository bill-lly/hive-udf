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
import org.json.JSONException;
import org.json.JSONObject;

import java.math.BigDecimal;
import java.util.*;

/*ICS收益结算根据工作时间获取price
case:
  输入:参数1: "[{"price": 2, "workTime": 5}, {"price": 3, "workTime": 6}]"
      参数2: 工作时间"workTime"
      参数3: 补时单价"supplementTimePrice"
      参数4: 工单最高金额"maximumAmount"
  输出:金额amount
*/
@org.apache.hadoop.hive.ql.exec.Description(name = "ics_exec_settl_get_amount_by_work_time", value = "")
public class ICSExecSettlGetAmountByWorkTime extends GenericUDF {
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
        if (arguments.length == 4) {
            //TODO 2.检查入参类型
            if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE && ((PrimitiveObjectInspector) arguments[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING
                    && arguments[1].getCategory() != ObjectInspector.Category.PRIMITIVE && ((PrimitiveObjectInspector) arguments[1]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.INT
                    && arguments[2].getCategory() != ObjectInspector.Category.PRIMITIVE && ((PrimitiveObjectInspector) arguments[2]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.DOUBLE
                    && arguments[3].getCategory() != ObjectInspector.Category.PRIMITIVE && ((PrimitiveObjectInspector) arguments[3]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.DOUBLE) {
                throw new UDFArgumentTypeException(1, "Please input string arg");
            }
        } else {
            throw new UDFArgumentLengthException("Please input four args");
        }
        //TODO 3.约束返回值类型
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        Object workTimePriceJsonArray = deferredObjects[0].get();
        Object workTime = deferredObjects[1].get();
        Object supplTimePrice = deferredObjects[2].get();
        Object maximumAmount = deferredObjects[3].get();
        if (workTimePriceJsonArray == null || workTime == null || supplTimePrice == null || maximumAmount == null) {
            return null;
        }

        int worTimeInt = Integer.parseInt(workTime.toString());
        double supplTimePriceF = Double.parseDouble(supplTimePrice.toString());
        double maximumAmountF = Double.parseDouble(maximumAmount.toString());
        //解析json
        JSONArray array = null;
        try {
            array = new JSONArray(workTimePriceJsonArray.toString());
        } catch (JSONException jsonException) {
            return null;
        }
        Map<Integer, Double> workTimePriceMap = new TreeMap();
        for (int i = 0; i < array.length(); i++) {
            JSONObject jsonObject = array.getJSONObject(i);
            double priceValue = Double.parseDouble(jsonObject.get("price").toString());
            int workTimeValue = (Integer)jsonObject.get("workTime");
            workTimePriceMap.put(workTimeValue, priceValue);
        }

        //如果executeTime < 最小结算工时，结算结果 = 0
        Integer minkey = (Integer) getMapMinKey(workTimePriceMap);
        if(worTimeInt < minkey) {
            return 0.0d;
        }

        //如果executeTime > 最大结算工时, 结算结果 = 最大区间金额 + （executeTime-最大结算工时）* 补时单价
        Integer maxkey = (Integer) getMapMaxKey(workTimePriceMap);
        if(worTimeInt > maxkey) {
            double amount = workTimePriceMap.get(maxkey) + (worTimeInt - maxkey) * supplTimePriceF;
            if(amount <= maximumAmountF){
                return getDecimalPlace(amount, 2);
            } else {
                return getDecimalPlace(maximumAmountF,2);
            }
        }

        //最小结算工时<= executeTime <= 最大结算工时， 结算结果 = 区间规定的钱
        double amount = 0.0d;
        Set<Integer> keys = workTimePriceMap.keySet();
        Iterator<Integer> iterator = keys.iterator();
        Integer key = null;
        while(iterator.hasNext()){
            key = iterator.next();
            if(key <= worTimeInt) {
                amount = workTimePriceMap.get(key);
            } else {
                break;
            }
        }

        if(amount <= maximumAmountF){
            return getDecimalPlace(amount, 2);
        } else {
            return getDecimalPlace(maximumAmountF, 2);
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString(ICSExecSettlGetAmountByWorkTime.class.getName(), children);
    }


    //四舍五入取后n位小数
    public double getDecimalPlace(double d, int place){
        BigDecimal b = new BigDecimal(d);
        double result = b.setScale(place, BigDecimal.ROUND_HALF_UP).doubleValue();
        return result;
    }


    public Object getMapMinKey(Map map){
        Set<Integer> set = map.keySet();
        Object[] obj = set.toArray();
        Arrays.sort(obj);
        //最小key
        Integer minKey = (Integer) obj[0];
        return minKey;
    }

    public Object getMapMaxKey(Map map){
        Set<Integer> set = map.keySet();
        Object[] obj = set.toArray();
        Arrays.sort(obj);
        //最小key
        Integer maxKey = (Integer) obj[obj.length - 1];
        return maxKey;
    }

    //单元测试
//    private Double evaluateTest(String workTimePriceJsonArray, Integer workTime, Double supplTimePrice, Double maximumAmount) {
//
//        if (workTimePriceJsonArray == null || workTime == null || supplTimePrice == null || maximumAmount == null) {
//            return null;
//        }
//
//        Integer worTimeInt = (Integer) workTime;
//        double supplTimePriceF = Double.parseDouble(supplTimePrice.toString());
//        double maximumAmountF = Double.parseDouble(maximumAmount.toString());
//        //解析json
//        JSONArray array = null;
//        try {
//            array = new JSONArray(workTimePriceJsonArray.toString());
//        } catch (JSONException jsonException) {
//            return null;
//        }
//        Map<Integer, Double> workTimePriceMap = new TreeMap();
//        for (int i = 0; i < array.length(); i++) {
//            JSONObject jsonObject = array.getJSONObject(i);
//            double priceValue = Double.parseDouble(jsonObject.get("price").toString());
//            int workTimeValue = (Integer)jsonObject.get("workTime");
//            workTimePriceMap.put(workTimeValue, priceValue);
//        }
//
//        //如果executeTime < 最小结算工时，结算结果 = 0
//        Integer minkey = (Integer) getMapMinKey(workTimePriceMap);
//        if(worTimeInt < minkey) {
//            return 0.0d;
//        }
//
//        //如果executeTime > 最大结算工时, 结算结果 = 最大区间金额 + （executeTime-最大结算工时）* 补时单价
//        Integer maxkey = (Integer) getMapMaxKey(workTimePriceMap);
//        if(worTimeInt > maxkey) {
//            double amount = workTimePriceMap.get(maxkey) + (worTimeInt - maxkey) * supplTimePriceF;
//            if(amount <= maximumAmountF){
//                return getDecimalPlace(amount, 2);
//            } else {
//                return getDecimalPlace(maximumAmountF,2);
//            }
//        }
//
//        //最小结算工时<= executeTime <= 最大结算工时， 结算结果 = 区间规定的钱
//        double amount = 0.0d;
//        Set<Integer> keys = workTimePriceMap.keySet();
//        Iterator<Integer> iterator = keys.iterator();
//        Integer key = null;
//        while(iterator.hasNext()){
//            key = iterator.next();
//            if(key <= worTimeInt) {
//                amount = workTimePriceMap.get(key);
//            } else {
//                break;
//            }
//        }
//
//        if(amount <= maximumAmountF){
//            return getDecimalPlace(amount, 2);
//        } else {
//            return getDecimalPlace(maximumAmountF, 2);
//        }
//    }
//
//    public static void main(String[] args) {
//        String s = "[{\"price\": 3.127, \"workTime\": 7}, {\"price\": 2.564, \"workTime\": 5}, {\"price\": 4.369, \"workTime\": 10}]";
//        ICSExecSettlGetAmountByWorkTime fun = new ICSExecSettlGetAmountByWorkTime();
//        double result = fun.evaluateTest(s, 7, 1.045, 10.099);
//        System.out.println(result);
//    }
}
