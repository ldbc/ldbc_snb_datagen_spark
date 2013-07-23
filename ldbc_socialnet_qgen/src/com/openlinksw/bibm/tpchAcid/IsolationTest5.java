/**
 * 
 */
package com.openlinksw.bibm.tpchAcid;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;

import com.openlinksw.bibm.Exceptions.BadSetupException;

class IsolationTest5 extends AcidTest implements Runnable {
        SQLException txn2Exception;

        IsolationTest5(TestDriver driver) {
            super(driver);
        }

        @Override
        public void run() {
            Random rand = driver.rand;
            // 4. Select random values of PS_PARTKEY and PS_SUPPKEY.
            // Return all columns of the PARTSUPP table for which PS_PARTKEY and
            // PS_SUPPKEY are equal to the selected values.
            try {
                String queryString = "select MAX(P_PARTKEY) from PART";
                ResultSet resultset = statement.executeQuery(queryString);
                resultset.next();
                int m = resultset.getInt(1);
                resultset.close();
                if (m < 1) {
                    throw new BadSetupException("no PART record found");
                }
                int p_key = rand.nextInt(m) + 1;
                queryString = "select MAX(S_SUPPKEY) from SUPPLIER";
                resultset = statement.executeQuery(queryString);
                resultset.next();
                m = resultset.getInt(1);
                resultset.close();
                if (m < 1) {
                    throw new BadSetupException("no PART record found");
                }
                int s_key = rand.nextInt(m) + 1;
                queryString = "select * from PARTSUPP where PS_PARTKEY="+p_key+" AND PS_SUPPKEY="+s_key;
                resultset = statement.executeQuery(queryString);
                while (resultset.next()) {}
                resultset.close();
            } catch (SQLException e) {
                txn2Exception = e;
            }
        }

        /**
         * This test demonstrates the ability of read and write transactions
         * affecting different database tables to make progress concurrently.
         * 
         * @return error string, or null if the test passed
         * @throws SQLException
         * @throws InterruptedException
         */
        @Override
        public String runTest() throws SQLException, InterruptedException {
            // Perform the following steps:
            // 1. Start an ACID Transaction Txn1 for a randomly selected O_KEY,
            // L_KEY, and DELTA.
            // 2. Suspend Txn1 immediately prior to COMMIT.
            TransactionParams params1 = new TransactionParams();
            AcidTransaction txn1 = new AcidTransaction(driver, params1);
            txn1.startAcidTransaction();
            // 3. Start a transaction Txn2 that does the following:
            // 4. Select random values of PS_PARTKEY and PS_SUPPKEY.
            // Return all columns of the PARTSUPP table for which PS_PARTKEY and
            // PS_SUPPKEY are equal to the selected values.
            Thread txn2Thr = new Thread(this);
            txn2Thr.start();
            // 5. Verify that Txn2 completes.
            txn2Thr.join(3000);
            if (txn2Thr.isAlive()) {
                txn2Thr.interrupt();
                txn2Thr.join();
                return "Txn2 hangs.";
            }
            if (txn2Exception != null) {
                return "Txn2 threw " + txn2Exception;
            }
            // 6. Allow Txn1 to complete.
            txn1.commitTransaction(true);
            // Verify that the appropriate rows in the ORDERS, LINEITEM and
            // HISTORY tables have been changed.
            return txn1.verifyUpdated();
        }

}