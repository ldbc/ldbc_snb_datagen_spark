/**
 * 
 */
package com.openlinksw.bibm.tpchAcid;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.openlinksw.bibm.Exceptions.BadSetupException;

class AcidTransaction extends AcidTest implements Runnable {
    TransactionParams params;
    public int step = 0;
    BigDecimal l_quantity;
    BigDecimal o_total;
    BigDecimal l_extendedPrice;
    BigDecimal new_extprice;
    SQLException exception;

    public AcidTransaction(TestDriver driver) {
        super(driver);
    }

    AcidTransaction(TestDriver driver, TransactionParams params) {
        super(driver);
        this.params=params;
    }

    void startAcidTransaction() throws SQLException {
        // BEGIN TRANSACTION
        conn.setAutoCommit(false);
        // Read O_TOTALPRICE from ORDERS into [ototal] where O_ORDERKEY =
        // [o_key]
        String queryString = "select O_TOTALPRICE from ORDERS where O_ORDERKEY = " + params.o_key;
        ResultSet resultset = statement.executeQuery(queryString);
        if (!resultset.next()) {
            throw new BadSetupException("no order found with key=" + params.o_key);
        }
        o_total = resultset.getBigDecimal(1);
        step = 1;

//System.out.println("L_ORDERKEY = " + params.o_key + " and L_LINENUMBER = " + params.l_key);
        // Read L_QUANTITY, L_EXTENDEDPRICE, L_PARTKEY, L_SUPPKEY, L_TAX,
        // L_DISCOUNT
        // into [quantity], [extprice], [pkey], [skey], [tax], [disc]
        // where L_ORDERKEY = [o_key] and L_LINENUMBER = [l_key]
        queryString = "select  L_QUANTITY, L_EXTENDEDPRICE, L_PARTKEY, L_SUPPKEY, L_TAX, L_DISCOUNT "
            + " from LINEITEM where L_ORDERKEY = " + params.o_key + " and L_LINENUMBER = " + params.l_key;
        resultset = statement.executeQuery(queryString);
        if (!resultset.next()) {
            throw new BadSetupException("no LINEITEM found with o_key=" + params.o_key + " linenumber=" + params.l_key);
        }
        BigDecimal quantity = getBigDecimal(resultset, 1);
        l_extendedPrice = getBigDecimal(resultset, 2);
        int pkey = resultset.getInt(3);
        int skey = resultset.getInt(4);
        BigDecimal tax = getBigDecimal(resultset, 5);
        BigDecimal disc = getBigDecimal(resultset, 6);
        resultset.close();
        step = 2;
//System.out.println("l_extendedPrice read:"+l_extendedPrice.toPlainString());
        // Set [ototal] = [ototal] - trunc( trunc([extprice] * (1 - [disc]),
        // 2) * (1 + [tax]), 2)
        o_total = o_total.subtract(trunc(trunc(l_extendedPrice.multiply(big1_2.subtract(disc)), 2).multiply(big1_2.add(tax)), 2));
        // Set [rprice] = trunc([extprice]/[quantity], 2)
        BigDecimal rprice = toBigDecimal(l_extendedPrice.doubleValue() / quantity.doubleValue(), 2);
        // Set [cost] = trunc([rprice] * [delta], 2)
        BigDecimal cost = trunc(rprice.multiply(params.delta), 2);
        // Set [new_extprice] = [extprice] + [cost]
        new_extprice = l_extendedPrice.add(cost);
        // Set [new_ototal] = trunc([new_extprice] * (1.0 - [disc]), 2)
        BigDecimal new_ototal = trunc(new_extprice.multiply(big1_2.subtract(disc)), 2);
        // Set [new_ototal] = trunc([new_ototal] * (1.0 + [tax]), 2)
        new_ototal = trunc(new_ototal.multiply(big1_2.add(tax)), 2);
        // Set [new_ototal] = [ototal] + [new_ototal]
        new_ototal = o_total.add(new_ototal);
        // Update LINEITEM where L_ORDERKEY = [o_key] and L_LINENUMBER =
        // [l_key]
        // Set L_EXTENDEDPRICE = [new_extprice]
        // Set L_QUANTITY = [quantity] + [delta]
        // Write L_EXTENDEDPRICE, L_QUANTITY to LINEITEM
        l_quantity = quantity.add(params.delta);
        queryString = "update  LINEITEM " + " set L_EXTENDEDPRICE = " + new_extprice + " , L_QUANTITY = " + l_quantity + " where L_ORDERKEY = "
                + params.o_key + " and L_LINENUMBER = " + params.l_key;
        statement.executeUpdate(queryString);
        step = 3;
// System.out.println("l_extendedPrice set:"+new_extprice.toPlainString());

        // Update ORDERS where O_ORDERKEY = [o_key]
        // Set O_TOTALPRICE = [new_ototal]
        // Write O_TOTALPRICE to ORDERS
        queryString = "update  ORDERS " + " set O_TOTALPRICE = " + new_ototal + " where O_ORDERKEY = " + params.o_key;
        statement.executeUpdate(queryString);
        step = 4;

        // Insert Into HISTORY Values ([pkey], [skey], [o_key], [l_key],
        // [delta], [current_date_time])
    }

    String verifyUpdated() throws SQLException {
        // Read O_TOTALPRICE
        String queryString = "select O_TOTALPRICE from ORDERS where O_ORDERKEY = " + params.o_key;
        ResultSet resultset = statement.executeQuery(queryString);
        if (!resultset.next()) {
            throw new BadSetupException("no order found with key=" + params.o_key);
        }
        BigDecimal o_total2 = resultset.getBigDecimal(1);
        resultset.close();
        // Read L_EXTENDEDPRICE
        queryString = "select  L_EXTENDEDPRICE from LINEITEM where L_ORDERKEY = " + params.o_key
                + " and L_LINENUMBER = " + params.l_key;
        resultset = statement.executeQuery(queryString);
        if (!resultset.next()) {
            throw new BadSetupException("no LINEITEM found with o_key=" + params.o_key + " linenumber=" + params.l_key);
        }
        BigDecimal l_extendedPrice2 = getBigDecimal(resultset, 1);
        resultset.close();
        if (l_extendedPrice.equals(l_extendedPrice2)) {
            return "update failed: l_extendedPrice=" + l_extendedPrice.toPlainString() + " not changed";
        }
        if (o_total.equals(o_total2)) {
            return "update failed: o_total=" + o_total.toPlainString() + " not changed";
        }
        return null;
    }

    void commitTransaction(boolean doCommit) throws SQLException {
        if (doCommit) {
            conn.commit();
        } else {
            conn.rollback();
        }
        step = 5;
        conn.setAutoCommit(true);
    }

    void runAcidTransaction() throws SQLException {
        startAcidTransaction();
        // COMMIT TRANSACTION
        commitTransaction(true);
        // Return [rprice], [quantity], [tax], [disc], [extprice], [ototal]
    }

    public void runAcidTransaction(TransactionParams params) throws SQLException {
        this.params=params;
        runAcidTransaction();
    }

    @Override
    public void run() {
        try {
//            System.err.println("AcidTransaction.run started");
            runAcidTransaction();
//            System.err.println("AcidTransaction.run finished");
        } catch (SQLException e) {
            exception=e;
        }
    }

}