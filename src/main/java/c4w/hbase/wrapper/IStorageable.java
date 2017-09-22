package c4w.hbase.wrapper;

import org.apache.hadoop.hbase.client.ResultScanner;
import java.util.List;

/*
 * Copyright (C) Cloud4Wi - All Rights Reserved 2017
 *
 * Created by jay on 9/22/17.
 */
public interface IStorageable {

    ResultScanner filter(
            String tableName,
            String family,
            String rowStart,
            String rowEnd,
            List<Parameter> parameterList,
            String prefix
    );
}
