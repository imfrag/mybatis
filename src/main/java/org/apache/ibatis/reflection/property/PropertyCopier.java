/**
 *    Copyright 2009-2015 the original author or authors.
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
package org.apache.ibatis.reflection.property;

import java.lang.reflect.Field;

/**
 * @author Clinton Begin
 */
public final class PropertyCopier {

  private PropertyCopier() {
    // Prevent Instantiation of Static Class
  }

  // 指定类型type，将sourceBean属性复制到destinationBean中
  public static void copyBeanProperties(Class<?> type, Object sourceBean, Object destinationBean) {
    Class<?> parent = type;
    while (parent != null) {
      // 获取当前类型声明的属性
      final Field[] fields = parent.getDeclaredFields();
      for(Field field : fields) {
        try {
          field.setAccessible(true);
          // 复制属性
          field.set(destinationBean, field.get(sourceBean));
        } catch (Exception e) {
          // Nothing useful to do, will only fail on final fields, which will be ignored.
        }
      }
      // 遍历父类中声明的属性
      parent = parent.getSuperclass();
    }
  }

}
