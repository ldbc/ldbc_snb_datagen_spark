/**
 * 
 */
package com.openlinksw.bibm.tpchAcid;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import com.openlinksw.bibm.Exceptions.BadSetupException;
import com.openlinksw.bibm.Exceptions.ExceptionException;

abstract class AcidTest {
    TestDriver driver;
    protected Statement statement;
    protected Connection conn;

    AcidTest(TestDriver driver) {
        this.driver = driver;
        int timeoutInSeconds = driver.timeout.getValue() / 1000;
        try {
            conn = DriverManager.getConnection(driver.endPoint);
            statement = conn.createStatement();
            statement.setQueryTimeout(timeoutInSeconds);
            // statement.setFetchSize(fetchSize);
        } catch (SQLException e0) {
            SQLException e = e0;
            while (e != null) {
                e.printStackTrace();
                e = e.getNextException();
            }
            throw new ExceptionException("SQLConnection()", e0);
        }

    }

    protected static BigDecimal getBigDecimal(ResultSet resultset, int col) throws SQLException {
        return resultset.getBigDecimal(col).setScale(2, BigDecimal.ROUND_HALF_EVEN);
    }

    public static BigDecimal toBigDecimal(double d, int precision) {
        for (int k = 0; k < precision; k++) {
            d = d * 10;
        }
        return BigDecimal.valueOf((long) d, precision);
    }

    protected static BigDecimal trunc(BigDecimal multiply, int precision) {
        return multiply.setScale(precision, BigDecimal.ROUND_DOWN);
    }

    /**
     * O_KEY selected at random from the same distribution as that used to
     * populate L_ORDERKEY in the qualification database (see Clause 4.2.3)
     * 4.2.3: O_ORDERKEY unique within [SF * 1,500,000 * 4]. The ORDERS and
     * LINEITEM tables are sparsely populated by generating a key value that
     * causes the first 8 keys of each 32 to be populated, yielding a 25%
     * use of the key range.
     * 
     * @return
     */
    protected long selectRandomOrderKey() {
        long sz = Math.round(driver.scaleFactor * 1500000 / 8);
        if (sz > Integer.MAX_VALUE) {
            throw new BadSetupException("scale factor too big:" + driver.scaleFactor); // FIXME
        }
        for (;;) {
            long hiBits = driver.rand.nextInt((int) sz);
            long lowBits = driver.rand.nextInt(8);
            long oKey = ((hiBits << 5) | lowBits);
            if (oKey != 0) { // experimental fact
                return oKey;
            }
        }
    }

    /**
     * @param num
     *            test number
     * @return error string, or null if the test passed
     * @throws SQLException
     * @throws InterruptedException
     */
    public String runTest() throws SQLException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    static final BigDecimal big0_2 = BigDecimal.valueOf(0, 2);
    static final BigDecimal big1_2 = BigDecimal.valueOf(100, 2);

    class TransactionParams {
        long o_key;
        int l_key;
        BigDecimal delta;

        TransactionParams(long o_key, int l_key) throws SQLException {
            this.o_key = o_key;
            this.l_key = l_key;
            Random rand = driver.rand;
            // System.out.println(" o_key="+ o_key+"; m="+m);
            // [delta] selected at random within [1 .. 100]:
            delta = new BigDecimal(rand.nextInt(100) + 1);
        }

        TransactionParams(long o_key) throws SQLException {
            this.o_key = o_key;
            // L_KEY selected at random from [1 .. M] where
            // M = SELECT MAX(L_LINENUMBER) from LINEITEM where L_ORDERKEY =
            // O_KEY
            String queryString = "select MAX(L_LINENUMBER) from LINEITEM where L_ORDERKEY = " + o_key;
            ResultSet resultset = statement.executeQuery(queryString);
            resultset.next();
            int m = resultset.getInt(1);
            resultset.close();
            if (m < 1) {
                throw new BadSetupException("no LINEITEM found for L_ORDERKEY=" + o_key);
            }
            Random rand = driver.rand;
            l_key = rand.nextInt(m) + 1;
            // System.out.println(" o_key="+ o_key+"; m="+m);
            // [delta] selected at random within [1 .. 100]:
            delta = new BigDecimal(rand.nextInt(100) + 1);
        }

        TransactionParams() throws SQLException {
            this(selectRandomOrderKey());
        }
    }

    class CheckFields {
        BigDecimal ototal;
        BigDecimal extprice;
        BigDecimal quantity;

        CheckFields(TransactionParams params) throws SQLException {
            String queryString = "select O_TOTALPRICE from ORDERS where O_ORDERKEY = " + params.o_key;
            ResultSet resultset = statement.executeQuery(queryString);
            if (!resultset.next()) {
                throw new BadSetupException("no order found with key=" + params.o_key);
            }
            ototal = resultset.getBigDecimal(1);
            queryString = "select L_EXTENDEDPRICE,L_QUANTITY from LINEITEM where  L_ORDERKEY = " + params.o_key + " and L_LINENUMBER = " + params.l_key;
            resultset = statement.executeQuery(queryString);
            if (!resultset.next()) {
                throw new BadSetupException("no lineitem found with orderkey=" + params.o_key + " linenumber=" + params.l_key);
            }
            extprice = getBigDecimal(resultset, 1);
            quantity = getBigDecimal(resultset, 2);
            resultset.close();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof CheckFields)) {
                return false;
            }
            CheckFields other = (CheckFields) obj;
            return ototal.equals(other.ototal) && extprice.equals(other.extprice) && quantity.equals(other.quantity);
        }

    }
}