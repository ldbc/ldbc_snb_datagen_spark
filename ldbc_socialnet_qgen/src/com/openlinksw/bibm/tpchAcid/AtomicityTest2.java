/**
 * 
 */
package com.openlinksw.bibm.tpchAcid;

import java.sql.SQLException;

class AtomicityTest2 extends AcidTest {

    AtomicityTest2(TestDriver driver) {
        super(driver);
        // TODO Auto-generated constructor stub
    }
    @Override
    public String runTest() throws SQLException {
        // Perform the ACID Transaction for a randomly selected set of
        // input data
        TransactionParams params = new TransactionParams();
        AcidTransaction ts = new AcidTransaction(driver, params);
        CheckFields before = new CheckFields(params);
        // substituting a ROLLBACK of the transaction for the COMMIT of
        // the transaction.
        ts.startAcidTransaction();
        ts.commitTransaction(false);
        // verify that the appropriate rows have not been changed in the
        // ORDERS, LINEITEM, and HISTORY tables.
        CheckFields after = new CheckFields(params);
        if (after.equals(before)) {
            return null;
        } else {
            return "database records changed";
        }
    }

}