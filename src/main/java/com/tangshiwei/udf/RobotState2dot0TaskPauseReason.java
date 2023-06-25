package com.tangshiwei.udf;

import com.tangshiwei.utils.URLCoderUtil;
import lombok.SneakyThrows;
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

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;


@org.apache.hadoop.hive.ql.exec.Description(name = "robot_state2dot0_task_pause_reason", value = "_FUNC_(arguments) - This function is used to process the transformation of the status value reported by the cloud platform into the specific reason description when the robot task is suspended.")
public class RobotState2dot0TaskPauseReason extends GenericUDF {
    private static final Logger LOG = LoggerFactory.getLogger(RobotState2dot0TaskPauseReason.class);
    /**
     * 判断传入参数类型
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
        ){
            throw new UDFArgumentTypeException(1,"Please input string arg");
        }
        //TODO 3.约束返回值类型
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }


    /**
     * 处理逻辑
     * @param arguments
     * @return
     * @throws HiveException
     */
    @SneakyThrows
    @Override
    public Object evaluate(DeferredObject[] arguments)  {
        Object arg = arguments[0].get();
        if(arg == null){
            return "";
        }
        String argStr = arg.toString();
        if (argStr.equals("")) {
            return "";
        }
        int taskPauseReasonValue = Integer.parseInt(argStr);
        String parseResult = taskPauseReasonValueCover(taskPauseReasonValue);
        return parseResult;
    }

    /**
     * 处理函数
     * @param taskPauseReasonValue
     * @return
     * @throws HiveException
     */
    public static String taskPauseReasonValueCover(int taskPauseReasonValue) {
        StringBuilder builder = new StringBuilder(10240);
        if((taskPauseReasonValue & 1) != 0) {
            builder.append("急停").append(",");
        }
        if((taskPauseReasonValue & 2) != 0) {
            builder.append("手动模式").append(",");
        }
        if((taskPauseReasonValue & 4) != 0) {
            builder.append("脚踏").append(",");
        }
        if((taskPauseReasonValue & 8) != 0) {
            builder.append("手动充电").append(",");
        }
        if((taskPauseReasonValue & 16) != 0) {
            builder.append("手动作业").append(",");
        }
        if((taskPauseReasonValue & 32) != 0) {
            builder.append("手动暂停").append(",");
        }
        if((taskPauseReasonValue & 64) != 0) {
            builder.append("静音模式/勿扰模式").append(",");
        }
        if((taskPauseReasonValue & 256) != 0) {
            builder.append("OTA 升级").append(",");
        }
        if((taskPauseReasonValue & 512) != 0) {
            builder.append("远程唤醒模式").append(",");
        }
        if((taskPauseReasonValue & 1024) != 0) {
            builder.append("远程控制").append(",");
        }
        if((taskPauseReasonValue & 65536) != 0) {
            builder.append("告警").append(",");
        }
        if((taskPauseReasonValue & 131072) != 0) {
            builder.append("告警").append(",");
        }
        if((taskPauseReasonValue & 262144) != 0) {
            builder.append("告警").append(",");
        }
        if((taskPauseReasonValue & 524288) != 0) {
            builder.append("告警").append(",");
        }
        if((taskPauseReasonValue & 1048576) != 0) {
            builder.append("告警").append(",");
        }
        if((taskPauseReasonValue & 2097152) != 0) {
            builder.append("告警").append(",");
        }
        if((taskPauseReasonValue & 16777216) != 0) {
            builder.append("调度").append(",");
        }
        builder.delete(builder.length() - 1, builder.length());
        return builder.toString();
    }

    /**
     * 帮助信息
     * @param children
     * @return
     */
    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString(RobotState2dot0TaskPauseReason.class.getName(),children);
    }

     //单元测试
//    private Object evaluateTest(String arg) {
//        Object argOject = arg;
//        if(argOject == null){
//            return "";
//        }
//        String argStr = argOject.toString();
//        if (argStr.equals("")) {
//            return "";
//        }
//        int taskPauseReasonValue = Integer.parseInt(argStr);
//        String parseResult = taskPauseReasonValueCover(taskPauseReasonValue);
//        return parseResult;
//    }
//
//    public static void main(String[] args) {
//        RobotState2dot0TaskPauseReason fun = new RobotState2dot0TaskPauseReason();
//        String s = "1024";
//        String result = (String) fun.evaluateTest(s);
//        System.out.println(result);
//    }
}