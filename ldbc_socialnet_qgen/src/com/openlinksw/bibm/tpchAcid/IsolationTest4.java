/**
 * 
 */
package com.openlinksw.bibm.tpchAcid;

import java.sql.SQLException;

class IsolationTest4 extends AcidTest implements Runnable {
        TransactionParams params1;
        AcidTransaction txn2;
        SQLException txn2Exception;

        IsolationTest4(TestDriver driver) {
            super(driver);
        }

        @Override
        public void run() {
            txn2 = new AcidTransaction(driver);
            try {
                TransactionParams params2 = new TransactionParams(params1.o_key, params1.l_key);
                txn2.runAcidTransaction(params2);
            } catch (SQLException e) {
                txn2Exception = e;
            }
        }

        /**
         * This test demonstrates isolation for the write-write conflict of two
         * update transactions when the first transaction is rolled back. FIXME:
         * The test specification is wrong: AcidTransaction starts with reading,
         * so read write conflict occurs, not write-write one. However, the test
         * follows the spec.
         * 
         * @return error string, or null if the test passed
         * @throws SQLException
         * @throws InterruptedException
         */
        @Override
        public String runTest() throws SQLException, InterruptedException {
            // Perform the following steps:
            // 1. Start an ACID Transaction Txn1 for a randomly selected O_KEY,
            // L_KEY, and DELTA1.
            // 2. Suspend Txn1 immediately prior to COMMIT.
            params1 = new TransactionParams();
            AcidTransaction txn1 = new AcidTransaction(driver, params1);
            txn1.startAcidTransaction();
            // 3. Start another ACID Transaction Txn2 for the same O_KEY, L_KEY
            // and for a randomly selected DELTA2.
            // (Txn2 attempts to read and update the data that has just been
            // updated by Txn1.)
            Thread txn2Thr = new Thread(this);
            txn2Thr.start();
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                txn1.commitTransaction(false);
                txn2Thr.interrupt();
                throw e;
            }
            // 4. Verify that Txn2 waits.
            int step = txn2.step;
            if (step != 0) {
                txn1.commitTransaction(true);
                txn2Thr.interrupt();
                txn2Thr.join(3000);
                return "Txn2 advanced to step " + step;
            }
            // 5. Force Txn1 to rollback.
            txn1.commitTransaction(false);
            // Txn2 should now have completed.
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
            // 6. Verify that Txn2.L_EXTENDEDPRICE = Txn1.L_EXTENDEDPRICE
            if (!txn2.l_extendedPrice.equals(txn1.l_extendedPrice)) {
                return "comparison failed: " + txn2.l_extendedPrice.toPlainString() + " != " + txn1.l_extendedPrice;
            }

            return null;
        }
 }