package com.zjpl.edw_java.conf;

import java.io.InputStream;
import java.util.Properties;

/**
 * 1.配置管理类
 * 功能：1.从properties读取配置文件，提供通过key获取对应的value
 */
public class ConfigurationManager {

    /**
     * Properties 对象使用private来修饰，就代表了其是私有的
     * 那么外界的对象，就不能直接通过ConfigurationManager这种方法来获取properties对象
     * 之所以这么做就为了防止外界的代码不小修改了properties对象中的某个key对应的value
     */
    private static Properties properties = new Properties();

    /**
     * 静态代码块
     * java 中，每一个类第一次用的时候，就会被java虚拟机(JVM)中的类加载器,去从磁盘中的.class
     * 文件中加载出来，然后为每一个构建一个class对象，就代表了这个类
     *
     * 每个类在第一次加载的时候，就会进行自身的初始化，那么类初始化的时候，会执行哪些操作的呢？
     * 就由每个类内部的static{}构成的静态代码块决定，我们自己就可以在类中开发静态代码块类第一次
     * 使用的时候，就会加载，加载的时候就会初始化，初始化类的时候就会执行类的静态代码块
     *
     * 因此，对于我们的配置组件，就在静态代码块中，编写读写静态代码块的代码
     * 这样的话，第一次外界代码调用这个ConfigurationMananger类的静态方法的时候，就会加载文件中的数据
     * 而且，放在静态代码块中，还有一个好处，就是类的初始化在整个JVM生命周期中，有且仅有一次，也就是
     * 说配置文件只会加载一次，然后以后就是重复使用，效率比较高，不用反复加载多次
     */
    static {
        try{
            InputStream in= ConfigurationManager.class.getClassLoader().getResourceAsStream("application.properties");
            //调用Properties的load(),给它传入一个文件的InputStream输入流
            properties.load(in);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 获取指定key对应的value
     * @param key
     * @return
     */
    public static String getProperties(String key){
        return properties.getProperty(key);
    }

    /**
     * 获取整数的value
     * @param key
     * @return
     */
    public static Integer getInteger(String key){
        String value =getProperties(key);
        try{
            return Integer.valueOf(value);
        }catch (Exception e){
            e.printStackTrace();
        }
        return 0;
    }
}
