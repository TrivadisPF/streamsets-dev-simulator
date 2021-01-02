import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.time.DateUtil;
import org.junit.Test;

public class TestDateUtil {

    @Test
    public void testWithTimeOnly() {
        long epoc = DateUtil.parseCustomFormat("HH:mm:ss", "20:00:00");
        System.out.println(epoc);
    }

    @Test
    public void testWithDateAndTime() {
        long epoc = DateUtil.parseCustomFormat("yyyy-MM-dd HH:mm:ss", "2021-01-02 20:00:00");
        System.out.println(epoc);
    }

}
