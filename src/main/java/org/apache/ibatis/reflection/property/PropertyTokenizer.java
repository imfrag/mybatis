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
package org.apache.ibatis.reflection.property;

import java.util.Iterator;

/**
 * @author Clinton Begin
 * 属性分词器，实现Iterator接口支持迭代访问
 * 如order[0].items[0].name -> {order[0], items[0], name}
 */
public class PropertyTokenizer implements Iterator<PropertyTokenizer> {
  // 当前字符串
  private String name;
  // 索引的name
  private final String indexedName;
  // 索引编号
  // 对于数组 arr[0]，index = 0
  // 对于Map map.get(key)，index = key
  private String index;
  // 剩余字符串
  private final String children;

  /**
   * fullname = "order[0].items[0].name"
   * 1. name = "order[0]", children = "items[0].name" 拆分
   * 2. indexedName = name = "order[0]" 设置分词
   * 3. index = "0", name = "order"
   *
   * fullname = "item.name"
   * 1. name = "item", children = "name"
   * 2. indexedName = name = "item"
   * 3. index = null, name = "item"
   */
  public PropertyTokenizer(String fullname) {
    // 以"."为分隔符
    int delim = fullname.indexOf('.');
    if (delim > -1) {
      // 将字符串拆分
      name = fullname.substring(0, delim);
      children = fullname.substring(delim + 1);
    }
    // 无分隔符，则fullname为整个词，无需拆分
    else {
      name = fullname;
      children = null;
    }

    // 记录当前识别的分词
    indexedName = name;
    delim = name.indexOf('[');
    if (delim > -1) {
      index = name.substring(delim + 1, name.length() - 1);
      name = name.substring(0, delim);
    }
  }

  public String getName() {
    return name;
  }

  public String getIndex() {
    return index;
  }

  public String getIndexedName() {
    return indexedName;
  }

  public String getChildren() {
    return children;
  }

  @Override
  public boolean hasNext() {
    return children != null;
  }

  // 迭代获取剩余字符串的分词结果
  @Override
  public PropertyTokenizer next() {
    return new PropertyTokenizer(children);
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("Remove is not supported, as it has no meaning in the context of properties.");
  }
}
