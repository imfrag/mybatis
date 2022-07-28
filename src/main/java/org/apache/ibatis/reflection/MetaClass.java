/**
 *    Copyright 2009-2017 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.ibatis.reflection;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;

import org.apache.ibatis.reflection.invoker.GetFieldInvoker;
import org.apache.ibatis.reflection.invoker.Invoker;
import org.apache.ibatis.reflection.invoker.MethodInvoker;
import org.apache.ibatis.reflection.property.PropertyTokenizer;

/**
 * @author Clinton Begin
 */
public class MetaClass {

  private final ReflectorFactory reflectorFactory;
  private final Reflector reflector;

  private MetaClass(Class<?> type, ReflectorFactory reflectorFactory) {
    this.reflectorFactory = reflectorFactory;
    this.reflector = reflectorFactory.findForClass(type);
  }

  // 通过静态方法调用私有构造方法
  public static MetaClass forClass(Class<?> type, ReflectorFactory reflectorFactory) {
    return new MetaClass(type, reflectorFactory);
  }

  // 获取属性的MetaClass
  public MetaClass metaClassForProperty(String name) {
    // 根据传入的属性名，获取相应get方法的返回类型
    Class<?> propType = reflector.getGetterType(name);
    return MetaClass.forClass(propType, reflectorFactory);
  }

  private MetaClass metaClassForProperty(PropertyTokenizer prop) {
    Class<?> propType = getGetterType(prop);
    return MetaClass.forClass(propType, reflectorFactory);
  }

  public String findProperty(String name) {
    StringBuilder prop = buildProperty(name, new StringBuilder());
    return prop.length() > 0 ? prop.toString() : null;
  }

  public String findProperty(String name, boolean useCamelCaseMapping) {
    // 使用驼峰命名映射，移除"_"符号
    if (useCamelCaseMapping) {
      name = name.replace("_", "");
    }
    return findProperty(name);
  }

  public String[] getGetterNames() {
    return reflector.getGetablePropertyNames();
  }

  public String[] getSetterNames() {
    return reflector.getSetablePropertyNames();
  }

  public Class<?> getSetterType(String name) {
    PropertyTokenizer prop = new PropertyTokenizer(name);
    if (prop.hasNext()) {
      MetaClass metaProp = metaClassForProperty(prop.getName());
      return metaProp.getSetterType(prop.getChildren());
    } else {
      return reflector.getSetterType(prop.getName());
    }
  }

  // 获取给定属性名对应的get方法返回类型
  public Class<?> getGetterType(String name) {
    PropertyTokenizer prop = new PropertyTokenizer(name);
    // 返回子表达式的返回类型
    // 如items[0].name，则应该返回Item类型中name属性对应的get方法的返回类型
    if (prop.hasNext()) {
      MetaClass metaProp = metaClassForProperty(prop);
      return metaProp.getGetterType(prop.getChildren());
    }
    // 无子表达式则返回该属性的get方法的返回类型
    // issue #506. Resolve the type inside a Collection Object
    return getGetterType(prop);
  }



  private Class<?> getGetterType(PropertyTokenizer prop) {
    // 获取第一个分词的name对应的get方法的返回类型
    Class<?> type = reflector.getGetterType(prop.getName());
    // 当分词index不为空时，即"items[index]"，且type为集合类型
    if (prop.getIndex() != null && Collection.class.isAssignableFrom(type)) {
      // 获取返回类型的Type实例s
      Type returnType = getGenericGetterType(prop.getName());
      // 返回类型是泛型
      if (returnType instanceof ParameterizedType) {
        // 获取实际的参数类型
        Type[] actualTypeArguments = ((ParameterizedType) returnType).getActualTypeArguments();
        // 不为空且仅包含一个元素
        if (actualTypeArguments != null && actualTypeArguments.length == 1) {
          returnType = actualTypeArguments[0];
          if (returnType instanceof Class) {
            type = (Class<?>) returnType;
          } else if (returnType instanceof ParameterizedType) {
            type = (Class<?>) ((ParameterizedType) returnType).getRawType();
          }
        }
      }
    }
    return type;
  }

  private Type getGenericGetterType(String propertyName) {
    try {
      // 获取propertyName对应的get方法
      Invoker invoker = reflector.getGetInvoker(propertyName);
      // MethodInvoker类型
      if (invoker instanceof MethodInvoker) {
        // 获取Invoker的Method属性
        Field _method = MethodInvoker.class.getDeclaredField("method");
        _method.setAccessible(true);
        Method method = (Method) _method.get(invoker);
        return TypeParameterResolver.resolveReturnType(method, reflector.getType());
      } else if (invoker instanceof GetFieldInvoker) {
        Field _field = GetFieldInvoker.class.getDeclaredField("field");
        _field.setAccessible(true);
        Field field = (Field) _field.get(invoker);
        return TypeParameterResolver.resolveFieldType(field, reflector.getType());
      }
    } catch (NoSuchFieldException e) {
    } catch (IllegalAccessException e) {
    }
    return null;
  }

  public boolean hasSetter(String name) {
    PropertyTokenizer prop = new PropertyTokenizer(name);
    if (prop.hasNext()) {
      if (reflector.hasSetter(prop.getName())) {
        MetaClass metaProp = metaClassForProperty(prop.getName());
        return metaProp.hasSetter(prop.getChildren());
      } else {
        return false;
      }
    } else {
      return reflector.hasSetter(prop.getName());
    }
  }

  // 判断给定属性名是否有get方法
  public boolean hasGetter(String name) {
    // 获取分词实例
    PropertyTokenizer prop = new PropertyTokenizer(name);
    // 有子表达式
    if (prop.hasNext()) {
      if (reflector.hasGetter(prop.getName())) {
        MetaClass metaProp = metaClassForProperty(prop);
        // 递归判断子表达式是否有get方法
        return metaProp.hasGetter(prop.getChildren());
      } else {
        return false;
      }
    }
    // 无子表达式
    else {
      return reflector.hasGetter(prop.getName());
    }
  }

  public Invoker getGetInvoker(String name) {
    return reflector.getGetInvoker(name);
  }

  public Invoker getSetInvoker(String name) {
    return reflector.getSetInvoker(name);
  }

  // todo-frag:分析如何构建property
  private StringBuilder buildProperty(String name, StringBuilder builder) {
    // 创建分词实例
    PropertyTokenizer prop = new PropertyTokenizer(name);
    // 有剩余分词
    if (prop.hasNext()) {
      // 获取type类中实际的属性名
      String propertyName = reflector.findPropertyName(prop.getName());
      if (propertyName != null) {
        builder.append(propertyName);
        builder.append(".");
        // 递归调用buildProperty
        MetaClass metaProp = metaClassForProperty(propertyName);
        metaProp.buildProperty(prop.getChildren(), builder);
      }
    }
    // 最后一个分词
    else {
      // 获取type类中实际的属性名
      String propertyName = reflector.findPropertyName(name);
      if (propertyName != null) {
        builder.append(propertyName);
      }
    }
    return builder;
  }

  public boolean hasDefaultConstructor() {
    return reflector.hasDefaultConstructor();
  }

}
