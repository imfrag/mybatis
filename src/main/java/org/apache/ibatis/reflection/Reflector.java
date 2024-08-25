/**
 *    Copyright 2009-2018 the original author or authors.
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

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.ReflectPermission;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.ibatis.reflection.invoker.GetFieldInvoker;
import org.apache.ibatis.reflection.invoker.Invoker;
import org.apache.ibatis.reflection.invoker.MethodInvoker;
import org.apache.ibatis.reflection.invoker.SetFieldInvoker;
import org.apache.ibatis.reflection.property.PropertyNamer;

/**
 * This class represents a cached set of class definition information that
 * allows for easy mapping between property names and getter/setter methods.
 *
 * @author Clinton Begin
 */
public class Reflector {

  // 每个Reflector实例对应一个类
  private final Class<?> type;
  // 可读属性，存在getter方法
  private final String[] readablePropertyNames;
  // 可写属性，存在setter方法
  private final String[] writeablePropertyNames;
  // set方法
  private final Map<String, Invoker> setMethods = new HashMap<String, Invoker>();
  // get方法
  private final Map<String, Invoker> getMethods = new HashMap<String, Invoker>();
  // set方法的参数类型
  private final Map<String, Class<?>> setTypes = new HashMap<String, Class<?>>();
  // get方法的返回类型
  private final Map<String, Class<?>> getTypes = new HashMap<String, Class<?>>();
  // 默认构造方法
  private Constructor<?> defaultConstructor;
  // 不区分大小写的属性集合
  private Map<String, String> caseInsensitivePropertyMap = new HashMap<String, String>();

  // 创建clazz的Reflector实例
  public Reflector(Class<?> clazz) {
    type = clazz;
    addDefaultConstructor(clazz);
    addGetMethods(clazz);
    addSetMethods(clazz);
    // 处理没有getter/setter方法的字段
    addFields(clazz);

    readablePropertyNames = getMethods.keySet().toArray(new String[getMethods.keySet().size()]);
    writeablePropertyNames = setMethods.keySet().toArray(new String[setMethods.keySet().size()]);

    for (String propName : readablePropertyNames) {
      caseInsensitivePropertyMap.put(propName.toUpperCase(Locale.ENGLISH), propName);
    }
    for (String propName : writeablePropertyNames) {
      caseInsensitivePropertyMap.put(propName.toUpperCase(Locale.ENGLISH), propName);
    }
  }

  private void addDefaultConstructor(Class<?> clazz) {
    // 获取全部构造方法
    Constructor<?>[] consts = clazz.getDeclaredConstructors();
    for (Constructor<?> constructor : consts) {
      // 构造方法参数数量为0
      if (constructor.getParameterTypes().length == 0) {
        // 判断是否可以修改方法可访问性，即setAccessible
        if (canAccessPrivateMethods()) {
          try {
            // 设置可访问性
            constructor.setAccessible(true);
          } catch (Exception e) {
            // Ignored. This is only a final precaution, nothing we can do.
          }
        }
        if (constructor.isAccessible()) {
          this.defaultConstructor = constructor;
        }
      }
    }
  }

  private void addGetMethods(Class<?> cls) {
    // Map的value是List类型是因为，子类和父类可能实现相同的get方法
    Map<String, List<Method>> conflictingGetters = new HashMap<String, List<Method>>();
    Method[] methods = getClassMethods(cls);

    /**
     * 遍历每个Method，判断是否为get方法
     * 1. 方法参数为0
     * 2. 方法名为get*或is*格式
     */
    for (Method method : methods) {
      // get方法参数数量为0
      if (method.getParameterTypes().length > 0) {
        continue;
      }
      String name = method.getName();
      // get方法通常以get或is开头
      if ((name.startsWith("get") && name.length() > 3)
          || (name.startsWith("is") && name.length() > 2)) {
        // 获取属性，即将方法名转换为属性名，getName -> name
        name = PropertyNamer.methodToProperty(name);
        // 添加进冲突集合中
        addMethodConflict(conflictingGetters, name, method);
      }
    }

    // 解决冲突方法，如子类重写父类方法并修改了合法返回类型
    resolveGetterConflicts(conflictingGetters);
  }

  private void resolveGetterConflicts(Map<String, List<Method>> conflictingGetters) {
    // 遍历每个属性，查找其最匹配的方法。因为子类可以覆写父类的方法，所以一个属性，可能对应多个 getting 方法
    for (Entry<String, List<Method>> entry : conflictingGetters.entrySet()) {
      // 最匹配的方法
      Method winner = null;
      // 方法名
      String propName = entry.getKey();
      for (Method candidate : entry.getValue()) {
        // 最匹配方法默认为结合中第一个
        if (winner == null) {
          winner = candidate;
          continue;
        }

        // 获取最匹配和当前方法的返回类型
        Class<?> winnerType = winner.getReturnType();
        Class<?> candidateType = candidate.getReturnType();

        // 返回类型相同
        if (candidateType.equals(winnerType)) {
          // 返回类型不是boolean，两个方法一样有歧义
          if (!boolean.class.equals(candidateType)) {
            throw new ReflectionException(
                "Illegal overloaded getter method with ambiguous type for property "
                    + propName + " in class " + winner.getDeclaringClass()
                    + ". This breaks the JavaBeans specification and can cause unpredictable results.");
          }
          // 如果返回类型是boolean，则判断方法名是否以"is"开头
          else if (candidate.getName().startsWith("is")) {
            winner = candidate;
          }
        }
        // candidateType是winnerType的父类，即winnerType更具体
        else if (candidateType.isAssignableFrom(winnerType)) {
          // OK getter type is descendant
        }
        // winnerType是candidateType的父类
        else if (winnerType.isAssignableFrom(candidateType)) {
          winner = candidate;
        }
        // 返回类型冲突，即不相同，也非继承关系
        else {
          throw new ReflectionException(
              "Illegal overloaded getter method with ambiguous type for property "
                  + propName + " in class " + winner.getDeclaringClass()
                  + ". This breaks the JavaBeans specification and can cause unpredictable results.");
        }
      }
      // 添加进getMethods中
      addGetMethod(propName, winner);
    }
  }

  private void addGetMethod(String name, Method method) {
    // 验证方法名的合法性
    if (isValidPropertyName(name)) {
      // 将Method封装为MethodInvoker
      getMethods.put(name, new MethodInvoker(method));
      Type returnType = TypeParameterResolver.resolveReturnType(method, type);
      // 添加该方法的返回类型（将Type转换为Class）
      getTypes.put(name, typeToClass(returnType));
    }
  }

  private void addSetMethods(Class<?> cls) {
    Map<String, List<Method>> conflictingSetters = new HashMap<String, List<Method>>();
    Method[] methods = getClassMethods(cls);
    for (Method method : methods) {
      String name = method.getName();
      // 1.与addGetMethods差异点，判断方法名是否以set开头
      if (name.startsWith("set") && name.length() > 3) {
        // 且参数数量为1
        if (method.getParameterTypes().length == 1) {
          name = PropertyNamer.methodToProperty(name);
          addMethodConflict(conflictingSetters, name, method);
        }
      }
    }
    // 2.与addGetMethods差异点
    resolveSetterConflicts(conflictingSetters);
  }

  private void addMethodConflict(Map<String, List<Method>> conflictingMethods, String name, Method method) {
    List<Method> list = conflictingMethods.get(name);
    if (list == null) {
      list = new ArrayList<Method>();
      conflictingMethods.put(name, list);
    }
    list.add(method);
  }

  private void resolveSetterConflicts(Map<String, List<Method>> conflictingSetters) {
    for (String propName : conflictingSetters.keySet()) {
      List<Method> setters = conflictingSetters.get(propName);
      // 获取属性名对应getMethod的返回类型
      Class<?> getterType = getTypes.get(propName);
      // 匹配的set方法
      Method match = null;
      ReflectionException exception = null;
      for (Method setter : setters) {
        // 获取第一个方法参数
        Class<?> paramType = setter.getParameterTypes()[0];
        // 对应的get方法返回类型与set方法的参数类型一致
        if (paramType.equals(getterType)) {
          // should be the best match
          // 此时为最匹配
          match = setter;
          break;
        }

        // 无异常
        if (exception == null) {
          try {
            match = pickBetterSetter(match, setter, propName);
          } catch (ReflectionException e) {
            // there could still be the 'best match'
            match = null;
            exception = e;
          }
        }
      }

      // 没有匹配的set方法，抛出异常
      if (match == null) {
        throw exception;
      }
      // 将匹配的方法添加进setMethods中
      else {
        addSetMethod(propName, match);
      }
    }
  }

  /**
   *
   * @param setter1 已经匹配到的方法, 可能为null
   * @param setter2 循环遍历属性对应的set方法集合中一个方法
   * @param property 属性名
   * @return
   */
  private Method pickBetterSetter(Method setter1, Method setter2, String property) {
    // 还未有匹配的set方法
    if (setter1 == null) {
      return setter2;
    }

    // 获取两个方法的参数类型
    Class<?> paramType1 = setter1.getParameterTypes()[0];
    Class<?> paramType2 = setter2.getParameterTypes()[0];
    // paramType1是paramType2的父类，setter2方法的参数更具体
    if (paramType1.isAssignableFrom(paramType2)) {
      return setter2;
    } else if (paramType2.isAssignableFrom(paramType1)) {
      return setter1;
    }
    // 两个方法类型不相等且没有继承关系
    throw new ReflectionException("Ambiguous setters defined for property '" + property + "' in class '"
        + setter2.getDeclaringClass() + "' with types '" + paramType1.getName() + "' and '"
        + paramType2.getName() + "'.");
  }

  private void addSetMethod(String name, Method method) {
    if (isValidPropertyName(name)) {
      setMethods.put(name, new MethodInvoker(method));
      Type[] paramTypes = TypeParameterResolver.resolveParamTypes(method, type);
      setTypes.put(name, typeToClass(paramTypes[0]));
    }
  }

  private Class<?> typeToClass(Type src) {
    Class<?> result = null;
    // src为Class实例
    if (src instanceof Class) {
      result = (Class<?>) src;
    }
    // src为泛型类型
    else if (src instanceof ParameterizedType) {
      result = (Class<?>) ((ParameterizedType) src).getRawType();
    }
    // src为泛型数组
    else if (src instanceof GenericArrayType) {
      // 获取数组元素类型
      Type componentType = ((GenericArrayType) src).getGenericComponentType();
      if (componentType instanceof Class) {
        result = Array.newInstance((Class<?>) componentType, 0).getClass();
      } else {
        // 递归调用
        Class<?> componentClass = typeToClass(componentType);
        result = Array.newInstance((Class<?>) componentClass, 0).getClass();
      }
    }
    // 默认为Object类型
    if (result == null) {
      result = Object.class;
    }
    return result;
  }

  private void addFields(Class<?> clazz) {
    // 获取当前类声明的属性
    Field[] fields = clazz.getDeclaredFields();
    for (Field field : fields) {
      // 判断并设置可访问性
      if (canAccessPrivateMethods()) {
        try {
          field.setAccessible(true);
        } catch (Exception e) {
          // Ignored. This is only a final precaution, nothing we can do.
        }
      }

      // 属性可访问
      if (field.isAccessible()) {
        // 字段名不在setMethods中，即该字段没有set方法
        if (!setMethods.containsKey(field.getName())) {
          // issue #379 - removed the check for final because JDK 1.5 allows
          // modification of final fields through reflection (JSR-133). (JGB)
          // pr #16 - final static can only be set by the classloader
          // 属性修饰符
          int modifiers = field.getModifiers();
          // 当前属性非final且非static，将当前属性封装为FieldInvoker并添加进setFields中
          if (!(Modifier.isFinal(modifiers) && Modifier.isStatic(modifiers))) {
            addSetField(field);
          }
        }

        // 该属性没有get方法，将Field封装为FieldInvoker添加进getMethods中
        if (!getMethods.containsKey(field.getName())) {
          addGetField(field);
        }
      }
    }

    // 递归遍历父类
    if (clazz.getSuperclass() != null) {
      addFields(clazz.getSuperclass());
    }
  }

  private void addSetField(Field field) {
    if (isValidPropertyName(field.getName())) {
      setMethods.put(field.getName(), new SetFieldInvoker(field));
      Type fieldType = TypeParameterResolver.resolveFieldType(field, type);
      setTypes.put(field.getName(), typeToClass(fieldType));
    }
  }

  private void addGetField(Field field) {
    if (isValidPropertyName(field.getName())) {
      getMethods.put(field.getName(), new GetFieldInvoker(field));
      Type fieldType = TypeParameterResolver.resolveFieldType(field, type);
      getTypes.put(field.getName(), typeToClass(fieldType));
    }
  }

  private boolean isValidPropertyName(String name) {
    return !(name.startsWith("$") || "serialVersionUID".equals(name) || "class".equals(name));
  }

  /*
   * This method returns an array containing all methods
   * declared in this class and any superclass.
   * We use this method, instead of the simpler Class.getMethods(),
   * because we want to look for private methods as well.
   *
   * @param cls The class
   * @return An array containing all methods in this class
   */
  private Method[] getClassMethods(Class<?> cls) {
    Map<String, Method> uniqueMethods = new HashMap<String, Method>();
    Class<?> currentClass = cls;
    // 从cls向上遍历其父类
    while (currentClass != null && currentClass != Object.class) {
      // 添加类中声明的方法
      addUniqueMethods(uniqueMethods, currentClass.getDeclaredMethods());

      // we also need to look for interface methods -
      // because the class may be abstract
      // 将当前类实现的接口中声明的方法也添加仅uniqueMethods中
      Class<?>[] interfaces = currentClass.getInterfaces();
      for (Class<?> anInterface : interfaces) {
        addUniqueMethods(uniqueMethods, anInterface.getMethods());
      }

      // 获取父类
      currentClass = currentClass.getSuperclass();
    }

    Collection<Method> methods = uniqueMethods.values();
    return methods.toArray(new Method[methods.size()]);
  }

  private void addUniqueMethods(Map<String, Method> uniqueMethods, Method[] methods) {
    for (Method currentMethod : methods) {
      // 非桥接方法
      if (!currentMethod.isBridge()) {
        // 方法签名
        String signature = getSignature(currentMethod);
        // check to see if the method is already known
        // if it is known, then an extended class must have
        // overridden a method
        // 方法未添加（子类中未重写父类中的方法）
        if (!uniqueMethods.containsKey(signature)) {
          if (canAccessPrivateMethods()) {
            try {
              currentMethod.setAccessible(true);
            } catch (Exception e) {
              // Ignored. This is only a final precaution, nothing we can do.
            }
          }

          uniqueMethods.put(signature, currentMethod);
        }
      }
    }
  }

  private String getSignature(Method method) {
    // 签名结果，格式为"[返回类型#]方法名:参数1,参数2,..."
    StringBuilder sb = new StringBuilder();
    // 返回类型
    Class<?> returnType = method.getReturnType();
    // 返回类型非空时，结果拼接，"类名#"
    if (returnType != null) {
      sb.append(returnType.getName()).append('#');
    }
    // 拼接方法名，"类名#方法名"
    sb.append(method.getName());
    // 拼接方法参数类型，"类名#方法名:参数1,参数2"
    Class<?>[] parameters = method.getParameterTypes();
    for (int i = 0; i < parameters.length; i++) {
      if (i == 0) {
        sb.append(':');
      } else {
        sb.append(',');
      }
      sb.append(parameters[i].getName());
    }
    return sb.toString();
  }

  private static boolean canAccessPrivateMethods() {
    try {
      SecurityManager securityManager = System.getSecurityManager();
      if (null != securityManager) {
        securityManager.checkPermission(new ReflectPermission("suppressAccessChecks"));
      }
    } catch (SecurityException e) {
      return false;
    }
    return true;
  }

  /*
   * Gets the name of the class the instance provides information for
   *
   * @return The class name
   */
  public Class<?> getType() {
    return type;
  }

  public Constructor<?> getDefaultConstructor() {
    if (defaultConstructor != null) {
      return defaultConstructor;
    } else {
      throw new ReflectionException("There is no default constructor for " + type);
    }
  }

  public boolean hasDefaultConstructor() {
    return defaultConstructor != null;
  }

  public Invoker getSetInvoker(String propertyName) {
    Invoker method = setMethods.get(propertyName);
    if (method == null) {
      throw new ReflectionException("There is no setter for property named '" + propertyName + "' in '" + type + "'");
    }
    return method;
  }

  public Invoker getGetInvoker(String propertyName) {
    Invoker method = getMethods.get(propertyName);
    if (method == null) {
      throw new ReflectionException("There is no getter for property named '" + propertyName + "' in '" + type + "'");
    }
    return method;
  }

  /*
   * Gets the type for a property setter
   *
   * @param propertyName - the name of the property
   * @return The Class of the propery setter
   */
  public Class<?> getSetterType(String propertyName) {
    Class<?> clazz = setTypes.get(propertyName);
    if (clazz == null) {
      throw new ReflectionException("There is no setter for property named '" + propertyName + "' in '" + type + "'");
    }
    return clazz;
  }

  /*
   * Gets the type for a property getter
   *
   * @param propertyName - the name of the property
   * @return The Class of the propery getter
   */
  public Class<?> getGetterType(String propertyName) {
    Class<?> clazz = getTypes.get(propertyName);
    if (clazz == null) {
      throw new ReflectionException("There is no getter for property named '" + propertyName + "' in '" + type + "'");
    }
    return clazz;
  }

  /*
   * Gets an array of the readable properties for an object
   *
   * @return The array
   */
  public String[] getGetablePropertyNames() {
    return readablePropertyNames;
  }

  /*
   * Gets an array of the writeable properties for an object
   *
   * @return The array
   */
  public String[] getSetablePropertyNames() {
    return writeablePropertyNames;
  }

  /*
   * Check to see if a class has a writeable property by name
   *
   * @param propertyName - the name of the property to check
   * @return True if the object has a writeable property by the name
   */
  public boolean hasSetter(String propertyName) {
    return setMethods.keySet().contains(propertyName);
  }

  /*
   * Check to see if a class has a readable property by name
   *
   * @param propertyName - the name of the property to check
   * @return True if the object has a readable property by the name
   */
  public boolean hasGetter(String propertyName) {
    return getMethods.keySet().contains(propertyName);
  }

  public String findPropertyName(String name) {
    return caseInsensitivePropertyMap.get(name.toUpperCase(Locale.ENGLISH));
  }
}
