package com.tangshiwei.udf;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Description;
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
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import java.io.*;
import java.security.*;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.Enumeration;

@Description(name = "decrypt_privacy_user",value = "对隐私用户进行解密")
public class GenericUDFDecryptPrivacyUser extends GenericUDF {
    private static final Logger LOG = LoggerFactory.getLogger(GenericUDFDecryptPrivacyUser.class);
    // PFX文件的密码
    private static final String password = "UEO28ve2pHFPo7EmhptPb";
    private static final String filepath = "hdfs://hdfs-cluster//user/hive/jars/resources/secret-key-prod.pfx";
    private static final InputStream pfxInputStream;
    private static final FileSystem fs;
    private static final PublicKey publicKey;

    static {

        //TODO 读取密钥
        try {
            // 创建Hadoop配置对象
            Configuration conf = new Configuration();

            // 获取Hadoop文件系统对象
            fs = FileSystem.get(conf);

            // 创建文件路径对象
            Path filePath = new Path(filepath);

            // 打开输入流
            pfxInputStream = fs.open(filePath);

            // 初始化KeyStore
            KeyStore keyStore = KeyStore.getInstance("PKCS12");

            keyStore.load(pfxInputStream, password.toCharArray());

            // 假设只有一个别名，并获取第一个别名
            Enumeration<String> aliases = keyStore.aliases();
            String keyAlias = "";
            if (aliases.hasMoreElements()) {
                keyAlias = aliases.nextElement();
            }
            X509Certificate x509Certificate = (X509Certificate) keyStore.getCertificate(keyAlias);
            publicKey = x509Certificate.getPublicKey();

            // TODO 关闭资源
            pfxInputStream.close();
            fs.close();

        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (CertificateException e) {
            throw new RuntimeException(e);
        } catch (KeyStoreException e) {
            throw new RuntimeException(e);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
    /**
     * 判断传入参数类型&数量
     * 约束返回值类型
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
        if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE && ((PrimitiveObjectInspector) arguments[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentTypeException(1, "Please input string arg");
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
        if (null == arguments[0].get()) {
            return null;
        }
        try {
            // Base64解码加密数据并解密
            byte[] decryptData = doDecrypt(publicKey, Base64.getDecoder().decode(arguments[0].get().toString()));
            return new String(decryptData);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public ObjectInspector initializeAndFoldConstants(ObjectInspector[] arguments) throws UDFArgumentException {
        return super.initializeAndFoldConstants(arguments);
    }

    /**
     * 帮助信息
     *
     * @param children
     * @return
     */
    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString(GenericUDFDecryptPrivacyUser.class.getName(), children);
    }

    /**
     * 解密方法
     *
     * @param key        密钥
     * @param cipherData 待解密字节数组
     * @return
     */
    private static byte[] doDecrypt(Key key, byte[] cipherData) {
        if (key == null) {
            throw new IllegalArgumentException("解密私钥不能为空");
        }
        Cipher cipher = null;
        try {
            cipher = Cipher.getInstance("RSA");
            cipher.init(Cipher.DECRYPT_MODE, key);
            return cipher.doFinal(cipherData);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("无此解密算法");
        } catch (InvalidKeyException e) {
            throw new RuntimeException("解密私钥非法,请检查");
        } catch (IllegalBlockSizeException e) {
            System.out.println(e);
            throw new RuntimeException("密文长度非法");
        } catch (BadPaddingException e) {
            throw new RuntimeException("密文数据已损坏");
        } catch (NoSuchPaddingException e) {
            e.printStackTrace();
            return null;
        }
    }
}
