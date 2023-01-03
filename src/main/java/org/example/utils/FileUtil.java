package org.example.utils;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.List;

@Slf4j
public class FileUtil {

    private static final String TAG = "FileUtil-";

    /**
     * 文件路径
     */
    public static final String FILE_PATH = "/securitypatrolpad";

    /**
     * 添加内容进入文件
     *
     * @param fileName 文件名称
     * @param content  内容
     * @param path     路径
     * @return boolean
     */
    public static boolean addContentToFile(String fileName, String content, String path) {
        boolean isSuccess = false;
        BufferedWriter bufferedWriter = null;
        try {
            //创建文件夹
            File dirs = new File(path);
            if (!dirs.exists()) {
                if (!dirs.mkdirs()) {
                    log.error(TAG + "addContentToFile: mkdirs failed");
                    return false;
                }
            }

            //文件不存在就创建
            File file = new File(path + "/" + fileName);
            if (!file.exists()) {
                if (!file.createNewFile()) {
                    log.error(TAG + "addContentToFile: createNewFile failed");
                    return false;
                }
            }

            boolean isAdd = true;
            if (file.length() > 20 * 1024 * 1024) {
                //大于20mb更新文件
                isAdd = false;
            }
            bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, isAdd)));
            bufferedWriter.write(content + "\r\n");
            isSuccess = true;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (bufferedWriter != null) {
                    bufferedWriter.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return isSuccess;
    }

    /**
     * 从输入流中保存文件
     *
     * @param fileName 文件名
     * @param is       输入流
     * @return 是否保存成功
     */
    public static boolean saveFile(String fileName, InputStream is, String path) {
        log.info(TAG + "saveFile fileName = " + fileName + " path = " + path);
        FileOutputStream fos = null;
        boolean isSuccess = false;
        try {
            //创建文件夹
            File dirs = new File(path);
            if (!dirs.exists()) {
                if (!dirs.mkdirs()) {
                    log.error(TAG + "saveFile: mkdirs failed");
                    return false;
                }
            }

            //已存在的文件不重复创建
            File file = new File(path + "/" + fileName);
            if (!file.exists()) {
                if (!file.createNewFile()) {
                    log.error(TAG + "saveFile: createNewFile failed");
                    return false;
                }
            }

            //写入流
            fos = new FileOutputStream(file);
            byte[] buffer = new byte[1024 * 4];
            int len = 0;
            while ((len = is.read(buffer)) != -1) {
                fos.write(buffer, 0, len);
            }

            //文件没长度就不成功
            long length = file.length();
            if (length != 0) {
                isSuccess = true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeStream(is, fos);
        }
        return isSuccess;
    }

    public interface OnDownloadFileProgressListener {
        void onProgress(int p);
    }

    /**
     * 关闭流
     *
     * @param is 输入流
     * @param os 输出流
     */
    private static void closeStream(InputStream is, OutputStream os) {
        try {
            if (os != null) {
                os.flush();
                os.close();
            }
            if (is != null) {
                is.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭流
     *
     * @param stream 流
     */
    public static void closeStream(Closeable stream) {
        try {
            if (stream != null) {
                stream.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 文件是否存在
     *
     * @param fileName 文件名
     * @param path     路径
     * @return 文件是否存在
     */
    public static boolean has(String fileName, String path) {
        File file = new File(path + "/" + fileName);
        return file.exists() && file.length() > 0;
    }

    /**
     * 文件是否存在
     *
     * @param path 路径
     * @return 文件是否存在
     */
    public static boolean isHas(String path) {
        File file = new File(path);
        return file.exists() && file.length() > 0;
    }

    /**
     * 删除目录下的所有文件，不删除目录下的文件夹
     *
     * @param path 目录路径
     */
    public static boolean deleteAll(String path) {
        boolean result = true;

        File dir = new File(path);
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files == null) {
                return true;
            }
            //遍历文件
            for (File file : files) {
                if (!file.isDirectory()) {
                    result = result && file.delete();
                }
            }
        }

        return result;
    }

    /**
     * 删除指定文件
     *
     * @param path 文件路径
     * @return 是否删除成功
     */
    public static boolean delete(String path) {
        File file = new File(path);
        if (file.exists()) {
            return file.delete();
        }
        return false;
    }

    /**
     * 删除图片 路径以,隔开
     *
     * @param images String
     */
    public static void deletePic(String images) {
        if (images != null && !images.equals("")) {
            //图片路径
            String[] imagesArray = images.split(",");
            for (String imagePath : imagesArray) {
                delete(imagePath);
            }
        }
    }

    /**
     * 获取文件或文件夹大小
     * @param file File 文件对象
     * @return long 字节
     */
    public static long getFileSize(File file)
    {
        long size = 0L;
        try {
            // 不存在返回 -1
            if (!file.exists()) {
                return -1;
            }

            // 如果是文件则返回文件大小
            if (file.isFile()) {
                return file.length();
            }

            // 是目录，递归计算子目录和所有文件的大小
            File[] filesList = file.listFiles();
            if (filesList == null) {
                return 0L;
            }

            for (File ff : filesList) {
                if (ff.isDirectory()) {
                    size += getFileSize(ff);
                } else {
                    size += ff.length();
                }
            }
        } catch (Exception e) {
            log.error(TAG + "getFileSize Exception", e);
            e.printStackTrace();
        }
        return size;
    }

    /**
     * 保存文本内容到文件
     *
     * @param fileName 文件名
     * @param subDir 子目录
     * @param lines List
     *
     * @return 是否保存成功
     */
    public static boolean saveToFile(String fileName, String subDir, List<String> lines) {
        boolean isSuccess = false;
        String path = FILE_PATH + "/" + subDir;
        String fullName = path + "/" + fileName;
        PrintWriter writer = null;
        try {
            //创建文件夹
            File dirs = new File(path);
            if (!dirs.exists()) {
                if (!dirs.mkdirs()) {
                    log.error(TAG + "saveToFile: mkdirs failed");
                    return false;
                }
            }

            //已存在的文件不重复创建
            File file = new File(fullName);
            if (!file.exists()) {
                if (!file.createNewFile()) {
                    log.error(TAG + "saveToFile: createNewFile failed");
                    return false;
                }
            }

            writer = new PrintWriter(fullName, "UTF-8");
            for (String s : lines) {
                writer.println(s);
            }
            isSuccess = true;
        } catch (Exception e) {
            log.error(TAG + "saveToFile Exception", e);
        } finally {
            closeStream(writer);
        }
        return isSuccess;
    }
}
