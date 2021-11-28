import com.trivadis.streamsets.devtest.simulator.stage.origin.sample.config.time.DateUtil;
import org.junit.Test;

public class TestDateUtil {

    @Test
    public void testWithTimeOnly() {
        long epoc = DateUtil.parseCustomFormatToEpocMs("HH:mm:ss", "20:00:00");
        System.out.println(epoc);
    }

    @Test
    public void testWithDateAndTime() {
        long epoc = DateUtil.parseCustomFormatToEpocMs("yyyy-MM-dd HH:mm:ss", "2021-01-02 20:00:00");
        System.out.println(epoc);
    }

    @Test
    public void testWithDateAndTimeWithZ() {
        long epoc = DateUtil.parseCustomFormatToEpocMs("yyyy-MM-dd'T'HH:mm:ssZ", "2021-01-02T20:00:00+0100");
        System.out.println(epoc);
    }

    @Test
    public void testWithDateAndTimeMsWithZ() {
        long epoc = DateUtil.parseCustomFormatToEpocMs("yyyy-MM-dd'T'HH:mm:ss.SSSZ", "2021-01-02T20:01:01.010+0100");
        System.out.println(epoc);
    }

    @Test
    public void testWithDateAndTimeWithZ2() {
        long epoc = DateUtil.parseCustomFormatToEpocMs("yyyy-MM-dd'T'HH:mm:ssZ", "2021-11-16T09:00:00+0100");
        System.out.println(epoc);
    }
}
