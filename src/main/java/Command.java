import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Created by dj_di_000 on 11/11/2017.
 */
public interface Command {
    void execute() throws InterruptedException, IOException, TimeoutException;
}
