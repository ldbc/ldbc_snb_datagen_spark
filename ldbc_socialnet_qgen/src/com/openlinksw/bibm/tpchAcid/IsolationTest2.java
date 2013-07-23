/**
 * 
 */
package com.openlinksw.bibm.tpchAcid;

import java.sql.SQLException;

class IsolationTest2 extends AcidTest implements Runnable {
        TransactionParams params1;
        AcidTransaction txn2;
        SQLException txn2Exception;

        IsolationTest2(TestDriver driver) {
            super(driver);
        }

        @Override
        public void run() {
            try {
                TransactionParams params2 = new TransactionParams(params1.o_key);
                txn2 = new AcidTransaction(driver, params2);
                txn2.startAcidTransaction();
                txn2.commitTransaction(true);
            } catch (SQLException e) {
                txn2Exception = e;
            }
        }

        /**
         * This test demonstrates isolation for the read-write conflict of a
         * read-write transaction and a read-only transaction when the
         * read-write transaction is rolled back.
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
            params1 = new TransactionParams();
            AcidTransaction txn1 = new AcidTransaction(driver, params1);
            txn1.startAcidTransaction();
            // 3. Start an ACID Query Txn2 for the same O_KEY as in Step 1.
            // (Txn2 attempts to read the data that has just been updated by
            // Txn1.)
            Thread txn2Thr = new Thread(this);
            txn2Thr.start();
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                txn1.commitTransaction(false);
                txn2Thr.interrupt();
                throw e;
            }
            // 4. Verify that Txn2 does not see Txn1's updates.
            int step = txn2.step;
            if (step != 0) {
                txn1.commitTransaction(false);
                txn2Thr.interrupt();
                txn2Thr.join(3000);
                return "Txn2 advanced to step " + step;
            }
            // 5. Force Txn1 to rollback.
            txn1.commitTransaction(false);
            // 6. Txn2 should now have completed.
            txn2Thr.join(3000);
            if (txn2Thr.isAlive()) {
                step = txn2.step;
                txn2Thr.interrupt();
                txn2Thr.join();
                return "Txn2 hangs on step " + step;
            }
            if (txn2Exception != null) {
                return "Txn2 threw " + txn2Exception;
            }

            return null;
        }
    }