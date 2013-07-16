/**
 * 
 */
package com.openlinksw.bibm.tpchAcid;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.File;
import com.openlinksw.util.FileUtil;

class IsolationTest6 extends AcidTest implements Runnable {
    String queryString;
    volatile boolean txn1Active;
    SQLException txn1Exception;

    IsolationTest6(TestDriver driver) {
        super(driver);
    }

    @Override
    public void run() {
//        System.err.println("IsolationTest6.run started");
        // Txn1 executes Q1 (from Clause 2.4)
        // against the qualification database
        // where the sub-stitution parameter [delta] is chosen from the
        // interval [0 .. 2159] so that the query runs for a sufficient
        // length of time.
        // Comment: Choosing [delta] = 0 will maximize the run time of Txn1.
        try {
            ResultSet resultset = statement.executeQuery(queryString);
            resultset.close();
        } catch (SQLException e) {
            txn1Exception=e;
        }
    }

    /**
     * This test demonstrates that the continuous submission of arbitrary
     * (read-only) queries against one or more tables of the database does
     * not indefinitely delay update transactions affecting those tables
     * from making progress.
     * 
     * @return error message string
     * @throws SQLException
     * @throws InterruptedException 
     */
    @Override
    public String runTest() throws SQLException, InterruptedException {
        Thread txn1Thr=null, txn2Thr=null, txn3Thr=null;
        String q1=FileUtil.file2string(new File(driver.querymixDir, "Q1_1.sql"));
        String q2=FileUtil.file2string(new File(driver.querymixDir, "Q1_2.sql"));
        try {
            TransactionParams params = new TransactionParams();
            AcidTransaction txn2 = new AcidTransaction(driver, params);
            // 1. Start a transaction Txn1.
            this.queryString=q1;
            txn1Thr = new Thread(this);
            txn1Thr.start();
            Thread.sleep(500); // let txn1 to start

            // 2. Before Txn1 completes, submit an ACID Transaction Txn2 with
            // randomly selected values of O_KEY, L_KEY and DELTA.
            txn2Thr = new Thread(txn2);
            txn2Thr.start();

            // If Txn2 completes before Txn1 completes, verify that the
            // appropriate rows in the ORDERS, LINEITEM and HIS-TORY tables have
            // been changed.
            // In this case, the test is complete with only Steps 1 and 2. If
            // Txn2 will not complete before Txn1 completes, perform Steps 3 and
            txn2Thr.join(500);
            if (!txn1Thr.isAlive()) {
                return "Txn1 completed too early";
            }
            if (txn2Thr.isAlive()) {
                // 3. Ensure that Txn1 is still active. Submit a third transaction
                // Txn3, which executes Q1 against the qualification database
                // with a test-sponsor selected value of the substitution parameter
                // [delta] that is not equal to the one used in Step 1.
                IsolationTest6 txn3=new IsolationTest6(driver);
                txn3.queryString=q2;
                txn3Thr = new Thread(txn3);
                txn3Thr.start();
                // 4. Verify that Txn2 completes before Txn3, and that the
                // appropriate rows in the ORDERS, LINEITEM and HIS-TORY tables have
                // been changed.
                // Comment: In some implementations Txn2 will not queue behind Txn1.
                txn2Thr.join();
                if (txn2.exception!=null) {
                    return "Txn2 failed: "+txn2.exception.toString();
                }
                if (!txn3Thr.isAlive()) {
                    return "Txn3 completed too early";
                }
            }
            return txn2.verifyUpdated();
        } finally {
            Thread[] threads = new Thread[]{txn1Thr, txn2Thr, txn3Thr};
            for (Thread txnThr: threads) {
                if (txnThr != null && txnThr.isAlive()) {
                    txnThr.interrupt();
                }
            }
            for (Thread txnThr: threads) {
                if (txnThr != null && txnThr.isAlive()) {
                    txnThr.join();
                }
            }
        }
    }

}