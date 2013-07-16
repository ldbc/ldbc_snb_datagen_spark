/**
 * 
 */
package com.openlinksw.bibm.tpchAcid;

import java.sql.SQLException;

class AtomicityTest1 extends AcidTest {

    AtomicityTest1(TestDriver driver) {
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
        ts.runAcidTransaction();
        // verify that the appropriate rows have been changed in the
        // ORDERS, LINEITEM, and HISTORY tables.
        CheckFields after = new CheckFields(params);
        if (after.equals(before)) {
            return "database records not changed";
        } else {
            return null;
        }
    }

}