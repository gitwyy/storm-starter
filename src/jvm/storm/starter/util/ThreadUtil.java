package storm.starter.util;

public class ThreadUtil {
	public static void sleep(Long sleepTime){
		try {
			Thread.sleep(sleepTime);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
