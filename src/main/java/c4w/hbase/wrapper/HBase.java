package c4w.hbase.wrapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.hbase.util.Pair;

/*
 * Copyright (C) Cloud4Wi - All Rights Reserved 2017
 *
 * Created by jay on 9/22/17.
 */
public class HBase implements IStorageable {

    private final static Logger logger = LoggerFactory.getLogger(HBase.class);

    private Configuration configuration = null;
    private Connection connection = null;
    private Table table = null;
    private Get get = null;
    private Put put = null;
    private Scan scan = null;
    private Filter filter = null;
    private FilterList filterList = null;
    private ResultScanner resultScanner = null;

    /**
     * Constructor (default)
     */
    public HBase() {
        try {
            if(configuration == null) {
                configuration = HBaseConfiguration.create();
                configuration.setInt("timeout", 120000);
                //zookeeper quorum, basic info needed to proceed, add host as needed.
                configuration.set("hbase.zookeeper.quorum","localhost");
                configuration.set("hbase.zookeeper.property.clientPort", "2181");
            }
            if(connection == null) {
                connection = ConnectionFactory.createConnection(configuration);
            }
        } catch (IOException e) {
            configuration = null;
            connection = null;

            logger.error(e.getMessage());
        }
    }

    /**
     * Constructor (overload). Accepts zookeeper details.
     * @param zookeeperQuorum
     * @param zookeeperClientPort
     */
    public HBase(String zookeeperQuorum, String zookeeperClientPort) {
        try {
            if(configuration == null) {
                configuration = HBaseConfiguration.create();
                configuration.setInt("timeout", 120000);
                //zookeeper quorum, basic info needed to proceed, add host as needed.
                configuration.set("hbase.zookeeper.quorum", zookeeperQuorum);
                configuration.set("hbase.zookeeper.property.clientPort", zookeeperClientPort);
            }
            if(connection == null) {
                connection = ConnectionFactory.createConnection(configuration);
            }
        } catch (IOException e) {
            configuration = null;
            connection = null;

            logger.error(e.getMessage());
        }
    }

    /**
     * Table getter.
     * @return Table.
     */
    public Table getTable() {
        return table;
    }

    /**
     * Single-record saving.
     * @param tableName
     * @param row
     * @param family
     * @param qualifier
     * @param value
     */
    public void put(String tableName, String row, String family, String qualifier, String value) {

        byte[] bytesRow = Bytes.toBytes(row);
        byte[] bytesFamily = Bytes.toBytes(family);
        byte[] bytesQualifier = Bytes.toBytes(qualifier);
        byte[] bytesValue = Bytes.toBytes(value);

        try {
            if(put == null) {
                put = new Put(bytesRow);
            }

            if(table == null) {
                table = connection.getTable(TableName.valueOf(tableName));
            }

            put.addColumn(bytesFamily, bytesQualifier, bytesValue);

            table.put(put);
        } catch (IOException e) {
            logger.error(e.getMessage());
        } finally {
            if(table != null) {
                table = null;
            }

            if(put != null) {
                put = null;
            }
        }
    }

    /**
     * Single-record saving.
     * @param tableName
     * @param row
     * @param family
     * @param qualifier
     * @param value
     */
    public void put(String tableName, String row, String family, String qualifier, int value) {

        byte[] bytesRow = Bytes.toBytes(row);
        byte[] bytesFamily = Bytes.toBytes(family);
        byte[] bytesQualifier = Bytes.toBytes(qualifier);
        byte[] bytesValue = Bytes.toBytes(value);

        try {
            if(put == null) {
                put = new Put(bytesRow);
            }

            if(table == null) {
                table = connection.getTable(TableName.valueOf(tableName));
            }

            put.addColumn(bytesFamily, bytesQualifier, bytesValue);

            table.put(put);
        } catch (IOException e) {
            logger.error(e.getMessage());
        } finally {
            if(table != null) {
                table = null;
            }

            if(put != null) {
                put = null;
            }
        }
    }

    /**
     * Single-record saving.
     * @param tableName
     * @param row
     * @param family
     * @param qualifier
     * @param value
     */
    public void put(String tableName, String row, String family, String qualifier, long value) {

        byte[] bytesRow = Bytes.toBytes(row);
        byte[] bytesFamily = Bytes.toBytes(family);
        byte[] bytesQualifier = Bytes.toBytes(qualifier);
        byte[] bytesValue = Bytes.toBytes(value);

        try {
            if(put == null) {
                put = new Put(bytesRow);
            }

            if(table == null) {
                table = connection.getTable(TableName.valueOf(tableName));
            }

            put.addColumn(bytesFamily, bytesQualifier, bytesValue);

            table.put(put);
        } catch (IOException e) {
            logger.error(e.getMessage());
        } finally {
            if(table != null) {
                table = null;
            }

            if(put != null) {
                put = null;
            }
        }
    }

    /**
     * Multiple-record saving.
     * @param tableName
     * @param row
     * @param family
     * @param KV
     */
    public void put(String tableName, String row, String family, List<Pair<String, String>> KV) {
        byte[] bytesRow = Bytes.toBytes(row);
        byte[] bytesFamily = Bytes.toBytes(family);

        try {
            if(put == null) {
                put = new Put(bytesRow);
            }

            if(table == null) {
                table = connection.getTable(TableName.valueOf(tableName));
            }

            for (Object p : KV) {
                Pair<String, String> pair = (Pair<String, String>) p;

                put.addColumn(bytesFamily, Bytes.toBytes(pair.getFirst()), Bytes.toBytes(pair.getSecond()));

                table.put(put);
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
        } finally {
            if(table != null) {
                table = null;
            }

            if(put != null) {
                put = null;
            }
        }
    }

    /**
     * Metrics-integrated filter.
     * @param tableName
     * @param family
     * @param rowStart
     * @param rowEnd
     * @param parameterList
     * @param prefix
     * @return ResultScanner instance.
     */
    public ResultScanner filter(
            String tableName,
            String family,
            String rowStart,
            String rowEnd,
            List<Parameter> parameterList,
            String prefix) {

        List<Pair<byte[], byte[]>> res = new ArrayList<>();

        try {
            if(table == null) {
                table = connection.getTable(TableName.valueOf(tableName));
            }

            if(scan == null) {
                if(rowStart != null && rowEnd != null) {
                    scan = new Scan(Bytes.toBytes(rowStart), Bytes.toBytes(rowEnd));
                } else {
                    // Is this expensive?
                    scan = new Scan();
                }
            }

            // Check for any prefixes.
            if(prefix != null) {
                filter = new ColumnPrefixFilter(Bytes.toBytes(prefix));
                scan.setBatch(10); // set this if there could be many columns returned
            }

            // Check for any value comparisons.
            if(parameterList != null) {
                filterList = handleParameters(parameterList);
            }

            // Set filter if any.
            if(filterList != null) {
                scan.setFilter(filterList);
            }

            // Invoke get scanner.
            if(resultScanner == null) {
                resultScanner = table.getScanner(scan);
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
        }

        return resultScanner;
    }

    /**
     * This handles multiple parameters dynamically, acts as the query condition mapper in SQL format (WHERE a = ? AND ..)
     * @param parameterList
     * @return FilterList instance.
     */
    private FilterList handleParameters(List<Parameter> parameterList) {

        if(parameterList != null) {

            if(filterList == null) {
                // MUST_PASS_ALL ~ WHERE columnA = ? AND columnB = ?
                // MUST_PASS_ONE ~ WHERE columnA = ? OR columnB = ?
                filterList=new FilterList(FilterList.Operator.MUST_PASS_ALL);
            }

            for(Parameter parameter : parameterList) {

                scan.addColumn(parameter.getFamily(), parameter.getQualifier());

                SingleColumnValueFilter singleColumnValueFilter1 = new SingleColumnValueFilter(
                        parameter.getFamily(),
                        parameter.getQualifier(),
                        parameter.getCompareOp1(),
                        parameter.getV1()
                );
                SingleColumnValueFilter singleColumnValueFilter2 = new SingleColumnValueFilter(
                        parameter.getFamily(),
                        parameter.getQualifier(),
                        parameter.getCompareOp2(),
                        parameter.getV2()
                );

                FilterList andFilter1 =  new FilterList(FilterList.Operator.MUST_PASS_ALL);
                andFilter1.addFilter(singleColumnValueFilter1);
                andFilter1.addFilter(singleColumnValueFilter2);

                filterList.addFilter(andFilter1);
            }
        }

        return filterList;
    }

    /**
     * De-allocates all resources.
     */
    public void deallocate() {
        if(table != null) {
            table = null;
        }

        if(scan != null) {
            scan = null;
        }

        if(resultScanner != null) {
            resultScanner.close();
            resultScanner = null;
        }

        if(filter != null) {
            filter = null;
        }

        if(filterList != null) {
            filterList = null;
        }

        if(get != null) {
            get = null;
        }

        if(put != null) {
            put = null;
        }
    }
}
