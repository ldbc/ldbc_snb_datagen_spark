/**
 * 
 */
package com.openlinksw.bibm.tpchAcid;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.openlinksw.bibm.Exceptions.BadSetupException;

class ConsistencyTest1 extends AcidTest {
    long[] oKeys;

    ConsistencyTest1(TestDriver driver) {
        super(driver);
        // TODO Auto-generated constructor stub
    }

    /**
     * checks that O_TOTALPRICE = SUM(trunc(trunc(L_EXTENDEDPRICE *(1 -
     * L_DISCOUNT),2) * (1+L_TAX),2))
     * 
     * @param oKey
     * @return
     * @throws SQLException
     */
    boolean consistencyCheck(long oKey) throws SQLException {
        String queryString = "select O_TOTALPRICE from ORDERS where O_ORDERKEY = " + oKey;
        ResultSet resultset = statement.executeQuery(queryString);
        if (!resultset.next()) {
            throw new BadSetupException("no order found with key=" + oKey);
        }
        BigDecimal ototal = getBigDecimal(resultset, 1);
        // System.out.println(ototal.toPlainString());
        queryString = "select L_EXTENDEDPRICE, L_DISCOUNT, L_TAX from LINEITEM where  L_ORDERKEY = " + oKey;
        resultset = statement.executeQuery(queryString);
        BigDecimal ltotal = big0_2;
        int itemCount = 0;
        while (resultset.next()) {
            BigDecimal extprice = getBigDecimal(resultset, 1);
            BigDecimal disc = getBigDecimal(resultset, 2);
            BigDecimal tax = getBigDecimal(resultset, 3);
            // System.out.print(" extprice="+extprice.toPlainString());
            BigDecimal mult1 = big1_2.subtract(disc);
            BigDecimal mult2 = big1_2.add(tax);
            BigDecimal price1 = trunc(extprice.multiply(mult1), 2);
            BigDecimal price = trunc(price1.multiply(mult2), 2);
            ltotal = ltotal.add(price);
            itemCount++;
        }
        resultset.close();
        if (itemCount == 0) {
            throw new BadSetupException("no lineitem found with order key=" + oKey);
        }
        // System.out.println(ltotal.toPlainString());
        return ototal.equals(ltotal);
    }

    @Override
    public String runTest() throws SQLException {
        // Verify that the ORDERS, and LINEITEM tables are initially
        // consistent as defined in Clause 3.3.2.1,
        // based on a random sample of at least 10 distinct values of
        // O_ORDERKEY.
        int count = 10;
        oKeys = new long[count];
        for (int i = 0; i < count; i++) {
            long o_key = selectRandomOrderKey();
            oKeys[i] = o_key;
            boolean ok = consistencyCheck(o_key);
            if (!ok) {
                return "first consistency check failed for orderkey=" + o_key;
            }
        }

        // Submit at least 100 ACID Transactions from each of at least the
        // number of execution streams
        // ( # query streams + 1 refresh stream) used in the reported
        // throughput test (see Clause 5.3.4).
        int count2 = 100;
        AcidTransaction ts = new AcidTransaction(driver);
        for (int i = 0; i < count2; i++) {
            long o_key;
            // Ensure that all the values of O_ORDERKEY chosen in Step 1 are
            // used by some transaction in Step 2.
            if (i < oKeys.length) {
                o_key = oKeys[i];
            } else {
                o_key = selectRandomOrderKey();
            }
            // Each transaction must use values of (O_KEY, L_KEY, DELTA)
            // randomly generated within the ranges defined in Clause
            // 3.1.6.2.
            TransactionParams params = new TransactionParams(o_key);
            ts.runAcidTransaction(params);
        }
        
        // Re-verify the consistency of the ORDERS, and LINEITEM tables as
        // defined in Clause 3.3.2.1 based on the same sample values
        // of O_ORDERKEY selected in Step 1.
        for (int i = 0; i < count; i++) {
            long o_key = oKeys[i];
            boolean ok = consistencyCheck(o_key);
            if (!ok) {
                return "second consistency check failed";
            }
        }
        return null;
    }

}