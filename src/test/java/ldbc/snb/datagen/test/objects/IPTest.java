package ldbc.snb.datagen.test.objects;

import ldbc.snb.datagen.objects.IP;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

/**
 * Created by aprat on 26/07/17.
 */
public class IPTest {

    @Test
    public void testIPLogic() {
        IP ip1 = new IP(192,168,1,1,24);
        IP ip2 = new IP(192,168,1,100,24);
        int network = 0xC0A80100;
        assertTrue(ip1.getNetwork() == network);
        assertTrue(ip2.getNetwork() == network);
    }
}
