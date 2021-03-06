/*
 * Copyright 中国电信甘肃万维公司 All rights reserved.
 * 中国电信甘肃万维公司 专有/保密源代码,未经许可禁止任何人通过任何* 渠道使用、修改源代码.
 *
 * Author: yangpy
 * Created: 18-11-3 上午11:55
 */

package com.gsww.healthplatform.hbasedts.hbase.write;

import java.util.HashSet;
import java.util.Set;

public class HFamilyDesc {
    private String familyName;
    private int blockSize = 64 * 1024;
    private Set<String> columns = new HashSet<>();

    public HFamilyDesc(String familyName) {
        this.familyName = familyName;
    }

    public HFamilyDesc(String familyName, int blockSize) {
        this.familyName = familyName;
        this.blockSize = blockSize;
    }

    public HFamilyDesc addColumn(String columnName) {
        columns.add(columnName);
        return this;
    }

    public HFamilyDesc addColumn(String... columnsName) {
        for (String s : columnsName) {
            columns.add(s);
        }
        return this;
    }

    public String getFamilyName() {
        return familyName;
    }

    public void setFamilyName(String familyName) {
        this.familyName = familyName;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public HFamilyDesc setBlockSize(int blockSize) {
        this.blockSize = blockSize;
        return this;
    }

    public Set<String> getColumns() {
        return columns;
    }

    public void setColumns(Set<String> columns) {
        this.columns = columns;
    }
}
