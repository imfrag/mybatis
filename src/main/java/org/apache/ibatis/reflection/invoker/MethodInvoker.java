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
package org.apache.ibatis.reflection.invoker;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * @author Clinton Begin
 */
public class MethodInvoker implements Invoker {

  private final Class<?> type;
  private final Method method;

  // 构造方法
  public MethodInvoker(Method method) {
    this.method = method;

    // 方法参数数量为1，表明该方法为对象的set方法，type设置为参数类型
    if (method.getParameterTypes().length == 1) {
      type = method.getParameterTypes()[0];
    }
    // 否则为get方法，type设置为返回类型
    else {
      type = method.getReturnType();
    }
  }

  @Override
  public Object invoke(Object target, Object[] args) throws IllegalAccessException, InvocationTargetException {
    // 调用target对象的method方法
    return method.invoke(target, args);
  }

  @Override
  public Class<?> getType() {
    return type;
  }
}
