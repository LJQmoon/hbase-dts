/*
 * Copyright 中国电信甘肃万维公司 All rights reserved.
 * 中国电信甘肃万维公司 专有/保密源代码,未经许可禁止任何人通过任何* 渠道使用、修改源代码.
 *
 * Author: yangpy
 * Created: 18-11-3 上午11:55
 */

package com.gsww.healthplatform.hbasedts.hbase.write;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface RowKeyBuilder {
    String getRowKey(ResultSet rs) throws SQLException;
}
