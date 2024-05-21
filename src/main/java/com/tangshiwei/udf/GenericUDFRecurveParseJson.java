package com.tangshiwei.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

@Description(name = "递归解析json", value = "递归获取json中所有的key-value值")
public class GenericUDFRecurveParseJson extends GenericUDF {
    private static final Logger LOG = LoggerFactory.getLogger(GenericUDFRecurveParseJson.class);

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
        //TODO 3.约束返回值类型 Map
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    private static String upperStrFirstLetter(String str) {
        char[] cs=str.toCharArray();
        cs[0]-=32;
        return String.valueOf(cs);
    }

    public JSONObject parseJSON(JSONObject data,JSONObject result, List<String> newKeyList){
        Set<String> parentKey = data.keySet();//获取所有的key
        for(String key :parentKey){
            Object sunData = data.get(key);//通过key获取value
            newKeyList.add(upperStrFirstLetter(key));//连接每一层key生成newkey
            if(sunData instanceof JSONObject){
                parseJSON((JSONObject)sunData,result, newKeyList);//递归解析子层JSON
            }else{
                result.put(String.join("",newKeyList),sunData);//如果value不是json类型那么直接输出
                newKeyList.remove(newKeyList.size() - 1);
            }
        }
        if(newKeyList.size() >= 1) {
            newKeyList.remove(newKeyList.size() - 1);
        }
        return result;//输出解析后的数据
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments[0].get() == null) {
            return null;
        } else {
            JSONObject result = new JSONObject();
            List newKey = new ArrayList();
            String str = arguments[0].get().toString();
            JSONObject inputJsonObject = new JSONObject(str);
            result = parseJSON(inputJsonObject, result, newKey);
            return result.toString();
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString(GenericUDFRecurveParseJson.class.getName(), children);
    }

    //test
//    public String evaluateTest(String arguments) {
//        if (arguments == null) {
//            return null;
//        } else {
//            JSONObject result = new JSONObject();
//            String str = arguments;
//            List newKey = new ArrayList();
//            JSONObject inputJsonObject = new JSONObject(str);
//            result = parseJSON(inputJsonObject, result, newKey);
//            return result.toString();
//        }
//    }
//
//    public static void main(String[] args) {
//        GenericUDFRecurveParseJson fun = new GenericUDFRecurveParseJson();
//        String result = fun.evaluateTest("{\n" +
//                "  \"version\": \"1.0.0\",\n" +
//                "  \"odom\": {\n" +
//                "    \"position\": {\n" +
//                "      \"x\": 0.0,\n" +
//                "      \"y\": 0.0,\n" +
//                "      \"z\": 0.0\n" +
//                "    },\n" +
//                "    \"orientation\": {\n" +
//                "      \"x\": 0.0,\n" +
//                "      \"y\": 0.0,\n" +
//                "      \"z\": 0.0,\n" +
//                "      \"w\": 0.0\n" +
//                "    },\n" +
//                "    \"v\": 0.0,\n" +
//                "    \"w\": 0.0\n" +
//                "  },\n" +
//                "  \"unbiasedImuPry\": {\n" +
//                "    \"pitch\": 0.0,\n" +
//                "    \"roll\": 0.0\n" +
//                "  },\n" +
//                "  \"time\": {\n" +
//                "    \"startup\": 1691411849,\n" +
//                "    \"taskStart\": 0,\n" +
//                "    \"current\": 1692775361\n" +
//                "  },\n" +
//                "  \"network\": {\n" +
//                "    \"wifiIntensityLevel\": 0,\n" +
//                "    \"mobileIntensityLevel\": 0,\n" +
//                "    \"wifiTraffic\": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],\n" +
//                "    \"mobileTraffic\": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],\n" +
//                "    \"wifiSpeed\": 0,\n" +
//                "    \"wifiSpeedRX\": 0,\n" +
//                "    \"wifiSpeedTX\": 0,\n" +
//                "    \"mobileSpeed\": 0,\n" +
//                "    \"mobileSpeedRX\": 0,\n" +
//                "    \"mobileSpeedTX\": 0\n" +
//                "  },\n" +
//                "  \"robotStatus\": {\n" +
//                "    \"location\": {\n" +
//                "      \"locationStatus\": 0,\n" +
//                "      \"locationMapName\": \"\",\n" +
//                "      \"locationMapOriginX\": 0.0,\n" +
//                "      \"locationMapOriginY\": 0.0,\n" +
//                "      \"locationMapResolution\": 0.0,\n" +
//                "      \"locationMapGridWidth\": 0,\n" +
//                "      \"locationMapGridHeight\": 0,\n" +
//                "      \"locationX\": 0.0,\n" +
//                "      \"locationY\": 0.0,\n" +
//                "      \"locationYaw\": 0.0,\n" +
//                "      \"locationX1\": 0.0,\n" +
//                "      \"locationY1\": 0.0,\n" +
//                "      \"locationYaw1\": 0.0\n" +
//                "    },\n" +
//                "    \"commonStatus\": {\n" +
//                "      \"sleepMode\": 0,\n" +
//                "      \"rebooting\": false,\n" +
//                "      \"manualControlling\": false,\n" +
//                "      \"rampAssistStatus\": 0,\n" +
//                "      \"otaStatus\": 0,\n" +
//                "      \"autoMode\": false,\n" +
//                "      \"emergencyStop\": false,\n" +
//                "      \"manualCharging\": false,\n" +
//                "      \"manualWorking\": false,\n" +
//                "      \"wakeupMode\": 0,\n" +
//                "      \"maintainMode\": 0\n" +
//                "    },\n" +
//                "    \"scheduleStatus\": {\n" +
//                "      \"schedulerPauseFlags\": 0,\n" +
//                "      \"schedulerArranger\": \"\"\n" +
//                "    },\n" +
//                "    \"scanMap\": {\n" +
//                "      \"scaningMapStatus\": 0,\n" +
//                "      \"scaningMapName\": \"\"\n" +
//                "    },\n" +
//                "    \"recordPath\": {\n" +
//                "      \"recordPathStatus\": 0,\n" +
//                "      \"recordPathName\": \"\"\n" +
//                "    },\n" +
//                "    \"navigation\": {\n" +
//                "      \"naviStatus\": 0,\n" +
//                "      \"naviInstanceId\": \"\",\n" +
//                "      \"naviMapName\": \"\",\n" +
//                "      \"naviPosName\": \"\",\n" +
//                "      \"naviPosType\": 0,\n" +
//                "      \"naviPosFunction\": 0\n" +
//                "    },\n" +
//                "    \"task\": {\n" +
//                "      \"taskStatus\": 0,\n" +
//                "      \"taskInstanceId\": \"\",\n" +
//                "      \"multiTaskName\": \"\",\n" +
//                "      \"multiTaskListCount\": 0,\n" +
//                "      \"multiTaskLoopCount\": 0,\n" +
//                "      \"taskQueueName\": \"\",\n" +
//                "      \"taskQueueListCount\": 0,\n" +
//                "      \"taskQueueLoopCount\": 0,\n" +
//                "      \"taskQueueMapName\": \"\",\n" +
//                "      \"multiTaskListIndex\": 0,\n" +
//                "      \"multiTaskLoopIndex\": 0,\n" +
//                "      \"taskQueueListIndex\": 0,\n" +
//                "      \"taskQueueLoopIndex\": 0,\n" +
//                "      \"taskQueueProgress\": 0.0,\n" +
//                "      \"subTaskProgress\": 0.0,\n" +
//                "      \"subTaskType\": 0,\n" +
//                "      \"expectCleaningType\": 0,\n" +
//                "      \"currentCleaningType\": 0\n" +
//                "    },\n" +
//                "    \"elevator\": {\n" +
//                "      \"takeElevatorStatus\": 0,\n" +
//                "      \"takeElevatorFrom\": \"\",\n" +
//                "      \"takeElevatorTo\": \"\",\n" +
//                "      \"takeElevatorState\": \"\"\n" +
//                "    },\n" +
//                "    \"station\": {\n" +
//                "      \"stationStatus\": 0,\n" +
//                "      \"stationState\": 0,\n" +
//                "      \"stationNumInQueue\": 0,\n" +
//                "      \"stationAvailableItems\": 0,\n" +
//                "      \"stationSupplyingItems\": 0,\n" +
//                "      \"stationFinishedItems\": 0,\n" +
//                "      \"stationPosName\": \"\",\n" +
//                "      \"stationPosType\": 0,\n" +
//                "      \"stationPosFunction\": 0\n" +
//                "    }\n" +
//                "  },\n" +
//                "  \"electronicSystem\": {\n" +
//                "    \"batteryVoltage\": \"\",\n" +
//                "    \"chargerVoltage\": \"\",\n" +
//                "    \"chargerCurrent\": \"\",\n" +
//                "    \"batteryCurrent\": \"\",\n" +
//                "    \"battery\": \"\",\n" +
//                "    \"wheelDriverData8\": \"\",\n" +
//                "    \"wheelDriverData9\": \"\",\n" +
//                "    \"wheelDriverDatae\": \"\",\n" +
//                "    \"wheelDriverDataf\": \"\",\n" +
//                "    \"wheelDriverData10\": \"\",\n" +
//                "    \"wheelDriverData11\": \"\",\n" +
//                "    \"wheelDriverData12\": \"\",\n" +
//                "    \"wheelDriverData13\": \"\",\n" +
//                "    \"hybridDriverData32\": \"\",\n" +
//                "    \"hybridDriverData33\": \"\",\n" +
//                "    \"hybridDriverData34\": \"\",\n" +
//                "    \"hybridDriverData35\": \"\",\n" +
//                "    \"hybridDriverData36\": \"\",\n" +
//                "    \"hybridDriverData37\": \"\",\n" +
//                "    \"hybridDriverData38\": \"\",\n" +
//                "    \"hybridDriverData39\": \"\"\n" +
//                "  },\n" +
//                "  \"cleaningSystem\": {\n" +
//                "    \"rollingBrushMotorWorking\": \"\",\n" +
//                "    \"brushMotorWorking\": \"\",\n" +
//                "    \"leftBrushMotorWorking\": \"\",\n" +
//                "    \"sprayMotor\": \"\",\n" +
//                "    \"fanLevel\": \"\",\n" +
//                "    \"squeegeeDown\": \"\",\n" +
//                "    \"frontRollingBrushMotorCurrent\": \"\",\n" +
//                "    \"rearRollingBrushMotorCurrent\": \"\",\n" +
//                "    \"rollingBrushMotorFront\": \"\",\n" +
//                "    \"rollingBrushMotorAfter\": \"\",\n" +
//                "    \"brushSpinLevel\": \"\",\n" +
//                "    \"sideBrushSpinLevel\": \"\",\n" +
//                "    \"brushDownPosition\": \"\",\n" +
//                "    \"waterLevel\": \"\",\n" +
//                "    \"leftBrushSpinLevel\": \"\",\n" +
//                "    \"filterLevel\": \"\",\n" +
//                "    \"sprayDetergent\": \"\",\n" +
//                "    \"valve\": \"\",\n" +
//                "    \"cleanWaterLevel\": \"\",\n" +
//                "    \"sewageLevel\": \"\",\n" +
//                "    \"rollingBrushMotorFrontFeedBack\": \"\",\n" +
//                "    \"rollingBrushMotorAfterFeedBack\": \"\",\n" +
//                "    \"leftSideBrushCurrentFeedBack\": \"\",\n" +
//                "    \"rightSideBrushCurrentFeedBack\": \"\",\n" +
//                "    \"xdsDriverInfo\": \"\",\n" +
//                "    \"brushDownPositionFeedBack\": \"\",\n" +
//                "    \"suctionPressureVoltage\": \"\",\n" +
//                "    \"leftSideBrushMotorCurrent\": \"\",\n" +
//                "    \"rightSideBrushMotorCurrent\": \"\",\n" +
//                "    \"sprayMotorCurrent\": \"\",\n" +
//                "    \"vacuumMotorCurrent\": \"\",\n" +
//                "    \"squeegeeLiftMotorCurrent\": \"\",\n" +
//                "    \"filterMotorCurrent\": \"\"\n" +
//                "  }\n" +
//                "}");
//        System.out.println(result);
//    }
}
