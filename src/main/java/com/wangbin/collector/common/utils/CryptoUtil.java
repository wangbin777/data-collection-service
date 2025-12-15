package com.wangbin.collector.common.utils;

import lombok.extern.slf4j.Slf4j;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;

/**
 * 加密解密工具类
 */
@Slf4j
public class CryptoUtil {

    private static final String AES_ALGORITHM = "AES";
    private static final String RSA_ALGORITHM = "RSA";
    private static final String SHA256_ALGORITHM = "SHA-256";
    private static final String MD5_ALGORITHM = "MD5";

    private static final int AES_KEY_SIZE = 128;
    private static final int RSA_KEY_SIZE = 2048;

    private CryptoUtil() {
        // 工具类，防止实例化
    }

    // ==================== MD5 ====================

    /**
     * MD5加密
     */
    public static String md5(String data) {
        try {
            MessageDigest md = MessageDigest.getInstance(MD5_ALGORITHM);
            byte[] digest = md.digest(data.getBytes(StandardCharsets.UTF_8));
            return bytesToHex(digest);
        } catch (Exception e) {
            log.error("MD5加密失败", e);
            return null;
        }
    }

    /**
     * MD5加密（加盐）
     */
    public static String md5WithSalt(String data, String salt) {
        try {
            return md5(data + salt);
        } catch (Exception e) {
            log.error("MD5加盐加密失败", e);
            return null;
        }
    }

    // ==================== SHA256 ====================

    /**
     * SHA256加密
     */
    public static String sha256(String data) {
        try {
            MessageDigest md = MessageDigest.getInstance(SHA256_ALGORITHM);
            byte[] digest = md.digest(data.getBytes(StandardCharsets.UTF_8));
            return bytesToHex(digest);
        } catch (Exception e) {
            log.error("SHA256加密失败", e);
            return null;
        }
    }

    /**
     * SHA256加密（加盐）
     */
    public static String sha256WithSalt(String data, String salt) {
        try {
            return sha256(data + salt);
        } catch (Exception e) {
            log.error("SHA256加盐加密失败", e);
            return null;
        }
    }

    /**
     * SHA256多次加密
     */
    public static String sha256Multiple(String data, int times) {
        try {
            String result = data;
            for (int i = 0; i < times; i++) {
                result = sha256(result);
            }
            return result;
        } catch (Exception e) {
            log.error("SHA256多次加密失败", e);
            return null;
        }
    }

    // ==================== Base64 ====================

    /**
     * Base64编码
     */
    public static String base64Encode(String data) {
        try {
            return Base64.getEncoder().encodeToString(data.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            log.error("Base64编码失败", e);
            return null;
        }
    }

    /**
     * Base64编码（字节数组）
     */
    public static String base64Encode(byte[] data) {
        try {
            return Base64.getEncoder().encodeToString(data);
        } catch (Exception e) {
            log.error("Base64编码失败", e);
            return null;
        }
    }

    /**
     * Base64解码
     */
    public static String base64Decode(String data) {
        try {
            byte[] decoded = Base64.getDecoder().decode(data);
            return new String(decoded, StandardCharsets.UTF_8);
        } catch (Exception e) {
            log.error("Base64解码失败", e);
            return null;
        }
    }

    /**
     * Base64解码（字节数组）
     */
    public static byte[] base64DecodeToBytes(String data) {
        try {
            return Base64.getDecoder().decode(data);
        } catch (Exception e) {
            log.error("Base64解码失败", e);
            return null;
        }
    }

    // ==================== AES ====================

    /**
     * 生成AES密钥
     */
    public static String generateAesKey() {
        try {
            KeyGenerator keyGen = KeyGenerator.getInstance(AES_ALGORITHM);
            keyGen.init(AES_KEY_SIZE);
            SecretKey secretKey = keyGen.generateKey();
            return base64Encode(secretKey.getEncoded());
        } catch (Exception e) {
            log.error("生成AES密钥失败", e);
            return null;
        }
    }

    /**
     * AES加密
     */
    public static String aesEncrypt(String data, String key) {
        try {
            SecretKeySpec secretKey = new SecretKeySpec(base64DecodeToBytes(key), AES_ALGORITHM);
            Cipher cipher = Cipher.getInstance(AES_ALGORITHM);
            cipher.init(Cipher.ENCRYPT_MODE, secretKey);
            byte[] encrypted = cipher.doFinal(data.getBytes(StandardCharsets.UTF_8));
            return base64Encode(encrypted);
        } catch (Exception e) {
            log.error("AES加密失败", e);
            return null;
        }
    }

    /**
     * AES解密
     */
    public static String aesDecrypt(String encryptedData, String key) {
        try {
            SecretKeySpec secretKey = new SecretKeySpec(base64DecodeToBytes(key), AES_ALGORITHM);
            Cipher cipher = Cipher.getInstance(AES_ALGORITHM);
            cipher.init(Cipher.DECRYPT_MODE, secretKey);
            byte[] decrypted = cipher.doFinal(base64DecodeToBytes(encryptedData));
            return new String(decrypted, StandardCharsets.UTF_8);
        } catch (Exception e) {
            log.error("AES解密失败", e);
            return null;
        }
    }

    // ==================== RSA ====================

    /**
     * 生成RSA密钥对
     */
    public static KeyPair generateRsaKeyPair() {
        try {
            KeyPairGenerator keyPairGen = KeyPairGenerator.getInstance(RSA_ALGORITHM);
            keyPairGen.initialize(RSA_KEY_SIZE);
            return keyPairGen.generateKeyPair();
        } catch (Exception e) {
            log.error("生成RSA密钥对失败", e);
            return null;
        }
    }

    /**
     * 获取公钥字符串
     */
    public static String getPublicKeyString(PublicKey publicKey) {
        return base64Encode(publicKey.getEncoded());
    }

    /**
     * 获取私钥字符串
     */
    public static String getPrivateKeyString(PrivateKey privateKey) {
        return base64Encode(privateKey.getEncoded());
    }

    /**
     * 从字符串加载公钥
     */
    public static PublicKey loadPublicKey(String publicKeyStr) {
        try {
            byte[] keyBytes = base64DecodeToBytes(publicKeyStr);
            X509EncodedKeySpec keySpec = new X509EncodedKeySpec(keyBytes);
            KeyFactory keyFactory = KeyFactory.getInstance(RSA_ALGORITHM);
            return keyFactory.generatePublic(keySpec);
        } catch (Exception e) {
            log.error("加载RSA公钥失败", e);
            return null;
        }
    }

    /**
     * 从字符串加载私钥
     */
    public static PrivateKey loadPrivateKey(String privateKeyStr) {
        try {
            byte[] keyBytes = base64DecodeToBytes(privateKeyStr);
            PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(keyBytes);
            KeyFactory keyFactory = KeyFactory.getInstance(RSA_ALGORITHM);
            return keyFactory.generatePrivate(keySpec);
        } catch (Exception e) {
            log.error("加载RSA私钥失败", e);
            return null;
        }
    }

    /**
     * RSA公钥加密
     */
    public static String rsaEncrypt(String data, String publicKeyStr) {
        try {
            PublicKey publicKey = loadPublicKey(publicKeyStr);
            Cipher cipher = Cipher.getInstance(RSA_ALGORITHM);
            cipher.init(Cipher.ENCRYPT_MODE, publicKey);
            byte[] encrypted = cipher.doFinal(data.getBytes(StandardCharsets.UTF_8));
            return base64Encode(encrypted);
        } catch (Exception e) {
            log.error("RSA公钥加密失败", e);
            return null;
        }
    }

    /**
     * RSA私钥解密
     */
    public static String rsaDecrypt(String encryptedData, String privateKeyStr) {
        try {
            PrivateKey privateKey = loadPrivateKey(privateKeyStr);
            Cipher cipher = Cipher.getInstance(RSA_ALGORITHM);
            cipher.init(Cipher.DECRYPT_MODE, privateKey);
            byte[] decrypted = cipher.doFinal(base64DecodeToBytes(encryptedData));
            return new String(decrypted, StandardCharsets.UTF_8);
        } catch (Exception e) {
            log.error("RSA私钥解密失败", e);
            return null;
        }
    }

    /**
     * RSA私钥签名
     */
    public static String rsaSign(String data, String privateKeyStr) {
        try {
            PrivateKey privateKey = loadPrivateKey(privateKeyStr);
            Signature signature = Signature.getInstance("SHA256withRSA");
            signature.initSign(privateKey);
            signature.update(data.getBytes(StandardCharsets.UTF_8));
            byte[] signed = signature.sign();
            return base64Encode(signed);
        } catch (Exception e) {
            log.error("RSA签名失败", e);
            return null;
        }
    }

    /**
     * RSA公钥验签
     */
    public static boolean rsaVerify(String data, String sign, String publicKeyStr) {
        try {
            PublicKey publicKey = loadPublicKey(publicKeyStr);
            Signature signature = Signature.getInstance("SHA256withRSA");
            signature.initVerify(publicKey);
            signature.update(data.getBytes(StandardCharsets.UTF_8));
            return signature.verify(base64DecodeToBytes(sign));
        } catch (Exception e) {
            log.error("RSA验签失败", e);
            return false;
        }
    }

    // ==================== 通用方法 ====================

    /**
     * 生成随机字符串
     */
    public static String generateRandomString(int length) {
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder sb = new StringBuilder();
        SecureRandom random = new SecureRandom();

        for (int i = 0; i < length; i++) {
            int index = random.nextInt(chars.length());
            sb.append(chars.charAt(index));
        }

        return sb.toString();
    }

    /**
     * 生成设备认证密码
     */
    public static String generateDevicePassword(String productKey, String deviceName, String deviceSecret) {
        try {
            String content = "productKey=" + productKey + "&deviceName=" + deviceName +
                    "&timestamp=" + System.currentTimeMillis();
            return sha256WithSalt(content, deviceSecret);
        } catch (Exception e) {
            log.error("生成设备认证密码失败", e);
            return null;
        }
    }

    /**
     * 验证设备密码
     */
    public static boolean verifyDevicePassword(String password, String productKey,
                                               String deviceName, String deviceSecret,
                                               long timestamp, long timeout) {
        try {
            // 检查时间戳是否有效
            long currentTime = System.currentTimeMillis();
            if (Math.abs(currentTime - timestamp) > timeout) {
                return false;
            }

            String content = "productKey=" + productKey + "&deviceName=" + deviceName +
                    "&timestamp=" + timestamp;
            String expectedPassword = sha256WithSalt(content, deviceSecret);

            return password.equals(expectedPassword);
        } catch (Exception e) {
            log.error("验证设备密码失败", e);
            return false;
        }
    }

    /**
     * 生成消息签名
     */
    public static String generateMessageSign(String message, String secret) {
        try {
            String content = message + secret;
            return sha256(content);
        } catch (Exception e) {
            log.error("生成消息签名失败", e);
            return null;
        }
    }

    /**
     * 验证消息签名
     */
    public static boolean verifyMessageSign(String message, String sign, String secret) {
        try {
            String expectedSign = generateMessageSign(message, secret);
            return sign != null && sign.equals(expectedSign);
        } catch (Exception e) {
            log.error("验证消息签名失败", e);
            return false;
        }
    }

    /**
     * 字节数组转十六进制字符串
     */
    private static String bytesToHex(byte[] bytes) {
        StringBuilder hexString = new StringBuilder();
        for (byte b : bytes) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }
}