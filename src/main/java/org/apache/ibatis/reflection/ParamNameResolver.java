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

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;

// 参数名解析类
public class ParamNameResolver {

  private static final String GENERIC_NAME_PREFIX = "param";

  /**
   * <p>
   * The key is the index and the value is the name of the parameter.<br />
   * The name is obtained from {@link Param} if specified. When {@link Param} is not specified,
   * the parameter index is used. Note that this index could be different from the actual index
   * when the method has special parameters (i.e. {@link RowBounds} or {@link ResultHandler}).
   * </p>
   * <ul>
   * <li>aMethod(@Param("M") int a, @Param("N") int b) -&gt; {{0, "M"}, {1, "N"}}</li>
   * <li>aMethod(int a, int b) -&gt; {{0, "0"}, {1, "1"}}</li>
   * <li>aMethod(int a, RowBounds rb, int b) -&gt; {{0, "0"}, {2, "1"}}</li>
   * </ul>
   *
   * 参数映射
   * key：参数顺序
   * value：参数名称
   */
  private final SortedMap<Integer, String> names;
  /**
   * 是否有{@link Param}注解
   */
  private boolean hasParamAnnotation;

  public ParamNameResolver(Configuration config, Method method) {
    // 获取参数类型
    final Class<?>[] paramTypes = method.getParameterTypes();
    // 获取参数注解（每个参数可能有多个注解）
    final Annotation[][] paramAnnotations = method.getParameterAnnotations();
    final SortedMap<Integer, String> map = new TreeMap<Integer, String>();
    int paramCount = paramAnnotations.length;
    // get names from @Param annotations
    for (int paramIndex = 0; paramIndex < paramCount; paramIndex++) {
      // 参数类型为RowBounds或ResultHandler的子类时跳过
      if (isSpecialParameter(paramTypes[paramIndex])) {
        // skip special parameters
        continue;
      }

      String name = null;
      // 获取参数的注解
      for (Annotation annotation : paramAnnotations[paramIndex]) {
        // 注解为@Param类型，参数名则为注解中的value
        if (annotation instanceof Param) {
          hasParamAnnotation = true;
          name = ((Param) annotation).value();
          break;
        }
      }

      // 无@Param注解修饰当前参数
      if (name == null) {
        // @Param was not specified.
        if (config.isUseActualParamName()) {  // true
          // 委托给ParamNameUtil类，寻找method方法的第paramIndex的参数名
          name = getActualParamName(method, paramIndex);
        }

        // 兜底，使用map的顺序
        if (name == null) {
          // use the parameter index as the name ("0", "1", ...)
          // gcode issue #71
          name = String.valueOf(map.size());
        }
      }
      map.put(paramIndex, name);
    }

    // 构建不可变集合
    names = Collections.unmodifiableSortedMap(map);
  }

  private String getActualParamName(Method method, int paramIndex) {
    if (Jdk.parameterExists) {
      return ParamNameUtil.getParamNames(method).get(paramIndex);
    }
    return null;
  }

  private static boolean isSpecialParameter(Class<?> clazz) {
    return RowBounds.class.isAssignableFrom(clazz) || ResultHandler.class.isAssignableFrom(clazz);
  }

  /**
   * Returns parameter names referenced by SQL providers.
   */
  public String[] getNames() {
    return names.values().toArray(new String[0]);
  }

  /**
   * <p>
   * A single non-special parameter is returned without a name.<br />
   * Multiple parameters are named using the naming rule.<br />
   * In addition to the default names, this method also adds the generic names (param1, param2,
   * ...).
   * </p>
   */
  public Object getNamedParams(Object[] args) {
    final int paramCount = names.size();
    // args为空或该方法参数为空
    if (args == null || paramCount == 0) {
      return null;
    }
    // 该方法参数无@Param注解，且仅有一个参数
    else if (!hasParamAnnotation && paramCount == 1) {
      // 获取映射关系中第一个参数对应的值
      return args[names.firstKey()];
    }
    else {
      final Map<String, Object> param = new ParamMap<Object>();
      int i = 0;
      // 遍历参数映射集合
      for (Map.Entry<Integer, String> entry : names.entrySet()) {
        param.put(entry.getValue(), args[entry.getKey()]);
        // add generic param names (param1, param2, ...)
        final String genericParamName = GENERIC_NAME_PREFIX + String.valueOf(i + 1);
        // ensure not to overwrite parameter named with @Param
        if (!names.containsValue(genericParamName)) {
          param.put(genericParamName, args[entry.getKey()]);
        }
        i++;
      }
      return param;
    }
  }
}
