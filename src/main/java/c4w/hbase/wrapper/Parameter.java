package c4w.hbase.wrapper;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;

/*
 * Copyright (C) Cloud4Wi - All Rights Reserved 2017
 *
 * Created by jay on 9/22/17.
 */
public class Parameter {

    private static final String LONG = "LONG";
    private static final String BYTE = "BYTE";

    // Proper parameter data type mapping.
    // this solves multiple constructor overloading.
    // Instead, we have a one contructor and this class
    // will determine param conversion.
    //
    static final Map<String, String> PARAMS = ImmutableMap.<String, String>builder()
            .put("TENANT_ID", LONG)
            .put("VENUE_ID", LONG)
            .put("ACCESS_POINT_ID", LONG)
            .put("TIMESTAMP", LONG)
            .put("DIM_BR", BYTE)
            .put("TIME_GR", BYTE)
            .build();

    private byte[] family = null;
    private byte[] qualifier = null;
    private CompareFilter.CompareOp compareOp1 = null;
    private byte[] v1 = null;
    private CompareFilter.CompareOp compareOp2 = null;
    private byte[] v2 = null;

    /**
     * Accepts string value comparison.
     * @param family
     * @param qualifier
     * @param compareOp1
     * @param v1
     * @param compareOp2
     * @param v2
     */
    public Parameter(String family, String qualifier, CompareFilter.CompareOp compareOp1, String v1, CompareFilter.CompareOp compareOp2, String v2) {


        switch(PARAMS.get(qualifier)) {
            case LONG:
                this.v1 = Bytes.toBytes(Long.valueOf(v1));
                this.v2 = Bytes.toBytes(Long.valueOf(v2));
                break;
            case BYTE:
                this.v1 = Bytes.toBytes(Byte.valueOf(v1));
                this.v2 = Bytes.toBytes(Byte.valueOf(v2));
                break;
        }

        this.family = Bytes.toBytes(family);
        this.qualifier = Bytes.toBytes(qualifier);
        this.compareOp1 = compareOp1;
        this.compareOp2 = compareOp2;
    }


    public byte[] getFamily() {
        return family;
    }

    public byte[] getQualifier() {
        return qualifier;
    }

    public CompareFilter.CompareOp getCompareOp1() {
        return compareOp1;
    }

    public byte[] getV1() {
        return v1;
    }

    public CompareFilter.CompareOp getCompareOp2() {
        return compareOp2;
    }

    public byte[] getV2() {
        return v2;
    }
}