package com.qf.ca.cadp.common.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.qf.ca.cadp.common.exception.BusinessException;
import com.qf.ca.cadp.common.exception.ErrorConstant;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.dozer.Mapper;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BaseUtils {
	
	private static ObjectMapper mapper;
	private static Base64.Decoder base64decoder = Base64.getDecoder();
	private static Base64.Encoder base64encoder = Base64.getEncoder();
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static <T> Collection<T> mapCollection(Collection source, Class<T> clazz, Mapper mapper) {
		Iterator iter = source.iterator();
		try {
			Collection<T> dest = (Collection<T>) source.getClass().newInstance();
			while(iter.hasNext()){
				dest.add(mapper.map(iter.next(), clazz));
			}
			return dest;
		} catch (InstantiationException | IllegalAccessException e) {
			throw new BusinessException(ErrorConstant.GENERAL_EXCEPTION, e.getMessage(), e);
		}
	}
	
	@SuppressWarnings("rawtypes")
	public static <T> List<T> toTypeList(Class<T> clz, String content) {
		try {
			JavaType type = BaseUtils.getObjectMapper().getTypeFactory().constructCollectionType(List.class, clz);
			List<T> obj = BaseUtils.getObjectMapper().readValue(content, type);
			return obj;
		} catch (IOException e) {
			return null;
		}
	}
	
	public static ObjectMapper getObjectMapper() {
		if(mapper == null) {
			mapper = new ObjectMapper();
			mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		}
		return mapper;
	}

	public static <T> T json2Obj(String body, Class<T> clz) throws IOException {
		return getObjectMapper().readValue(body, clz);
	}

	public static <T> T json2Obj(String body, JavaType javaType) throws IOException {
		return getObjectMapper().readValue(body, javaType);
	}
	
	public static <T> T json2Obj(String body, TypeReference typeReference) throws IOException {
		return getObjectMapper().readValue(body, typeReference);
	}

	public static String obj2Json(Object obj) throws JsonProcessingException {
		return getObjectMapper().writeValueAsString(obj);
	}
	
/*	private static Map<String, Condition> conditionMap = new HashMap<String, Condition>();

	public static Condition getConditionFromStr(String filterExp) {
		Condition condition = conditionMap.get(filterExp);
		if(condition == null) {
			condition = ConditionUtils.fromString(filterExp);
			conditionMap.put(filterExp, condition);
		}
		return condition;
	}*/
	
	private static Map<String, DateTimeFormatter> dateFormatterMap = new HashMap<>();
	
	public static DateTimeFormatter getTimeFormatFromStr(String fmt) {
		DateTimeFormatter format = dateFormatterMap.get(fmt);
		if(format == null) {
			format = DateTimeFormatter.ofPattern(fmt);
			dateFormatterMap.put(fmt, format);
		}
		return format;
	}

	public static LocalDateTime getDateTimeFromInstant(Long millis) {
		return Instant.ofEpochMilli(millis).atZone(ZoneId.systemDefault()).toLocalDateTime();
	}
	
	public static byte[] joinByteArray(byte[] splitter, byte[] ... args) {
		List<byte[]> keyList = Arrays.asList(args);
		int size = 0;
		for(byte[] key : keyList) {
			size += key.length;
		}
		byte[] destByte = null;
		if(ArrayUtils.isNotEmpty(splitter)) {
			destByte = new byte[size + (keyList.size()-1) * splitter.length];
		} else {
			destByte = new byte[size];
		}
		int pos = 0;
		for(int i = 0 ; i<keyList.size() ; i++) {
			System.arraycopy(keyList.get(i), 0, destByte, pos, keyList.get(i).length);
			pos += keyList.get(i).length;
			if(i < keyList.size()-1 && ArrayUtils.isNotEmpty(splitter)) {
				System.arraycopy(splitter, 0, destByte, pos, splitter.length);
				pos += splitter.length;
			}
		}
		return destByte;
	}

	public static String getFillString(String prefix,String suffix,char fill,int totalLength){
		int c = 0;
		int needLength = totalLength - (prefix.length()+suffix.length());

		StringBuilder sb = new StringBuilder();
		sb.append(prefix);
		while(c < needLength){
			sb.append(fill);
			c++;
		}
		sb.append(suffix);
		return sb.toString();
	}

	/**
	 * 毫秒时间戳转化为指定LocalDateTime
	 * @date 2019/7/24
	 * @param timestamp
	 * @return java.time.LocalDateTime
	 */
	public static LocalDateTime timestamToDatetime(long timestamp){
		Instant instant = Instant.ofEpochMilli(timestamp);
		return LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
	}

	public static void copyProperties(Object source, Object dest) throws Exception {
		// 获取属性
		BeanInfo sourceBean = Introspector.getBeanInfo(source.getClass(),Object.class);
		PropertyDescriptor[] sourceProperty = sourceBean.getPropertyDescriptors();
		BeanInfo destBean = Introspector.getBeanInfo(dest.getClass(),Object.class);
		PropertyDescriptor[] destProperty = destBean.getPropertyDescriptors();
		try {
			for (int i = 0; i < sourceProperty.length; i++) {
				for (int j = 0; j < destProperty.length; j++) {
					if (sourceProperty[i].getName().equals(destProperty[j].getName())  && sourceProperty[i].getPropertyType() == destProperty[j].getPropertyType()) {
						// 调用source的getter方法和dest的setter方法
						destProperty[j].getWriteMethod().invoke(dest,sourceProperty[i].getReadMethod().invoke(source));
						break;
					}
				}
			}
		} catch (Exception e) {
			throw new Exception("属性复制失败:" + e.getMessage());
		}
	}

	public static List<String> findAllRegexMatch(String regex, String content, int groupIndex){
		ArrayList<String> result = new ArrayList<>();
		if(StringUtils.isBlank(regex) || StringUtils.isBlank(content)){
			return result;
		}
		Matcher m = Pattern.compile(regex).matcher(content);
		while(m.find()){
			result.add(m.group(groupIndex));
		}
		return result;
	}
	
	public static String base64Encode(String src) {
		return base64encoder.encodeToString(src.getBytes());		
	}
	
	public static String base64Decode(String src) {
		return new String(base64decoder.decode(src));
	}
	
	public static Integer toInteger(String value, Integer defaultValue){
        if (null == value) {
            return defaultValue;
        }
        try {
            return Integer.valueOf(value.trim());
        }catch (Exception e){

        }
        return defaultValue;
    }
	
	public static Long toLong(String value, Long defaultValue){
        if (null == value) {
            return defaultValue;
        }
        try {
            return Long.valueOf(value.trim());            		
        }catch (Exception e){

        }
        return defaultValue;
    }
	
	public static Double toDouble(String value, Double defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        try { 
        	return Double.valueOf(value.trim());            
        } catch (Exception e) {
        	
        }
        return defaultValue;
    }
	
	public static Boolean toBool(String value, Boolean defaultValue) {
        if (value == null) {
            return defaultValue;
        }        
        if (BaseUtils.toInteger(value, 1) != 0) {
        	return true;
        }
        return false;
    }

	public static String[] extractWords(String sentence) {
		if(StringUtils.isNotBlank(sentence)){
			return StringUtils.replaceAll(sentence, "\\W+", ",").split(",");
		} else {
			return null;
		}
	}
}
