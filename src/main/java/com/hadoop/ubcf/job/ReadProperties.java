package com.hadoop.ubcf.job;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ReadProperties {
	
    private final static String file = System.getProperty("user.dir");
    private static Map<String, String> properties = new HashMap<>();
    
    public static void init(){
    	read(findProperties(file));
    }
    
    //根据key获取值
    public static String get(String key){
    	if(properties.containsKey(key)) {
    		return properties.get(key).trim();
    	}else {
			return null;
		}
    }
    
    
    public static List<String> findProperties(String path) {
    	List<String> p = new ArrayList<>();
        File file = new File(path);
        LinkedList<File> list = new LinkedList<>();

        if (file.exists()) {
            if (null == file.listFiles()) {
                return p;
            }
            list.addAll(Arrays.asList(file.listFiles()));
            while (!list.isEmpty()) {
                File[] files = list.removeFirst().listFiles();
                if (null == files) {
                    continue;
                }
                for (File f : files) {
                    if (f.isDirectory()) {
                        list.add(f);
                    } else {
                    	String name = f.getName();
                    	if(name.indexOf(".")>0) {
                    		int lastIndex = name.lastIndexOf(".");
                    		String suffix = name.substring(lastIndex+1, name.length());
                    		if("properties".equals(suffix)) 
                    			p.add( f.getAbsolutePath());
                    	}
                    }
                }
            }
        } else {
            System.out.println("文件不存在!");
        }
        return p;
    }
    
	public static void read(List<String> paths) {
		paths.forEach(path -> {
			try (
					FileReader fr = new FileReader(new File(path));
					BufferedReader br = new BufferedReader(fr);
					) {
				String line = null;
				while((line=br.readLine())!=null) {
					if(line.indexOf("#") == 0 || "".equals(line)) {
						continue;
					}
					int index = line.indexOf("=");
					String key = line.substring(0, index);
					String value = line.substring(index+1, line.length());
					properties.put(key, value);
				}
			} catch (Exception e) {
			}
		});
	}
}
