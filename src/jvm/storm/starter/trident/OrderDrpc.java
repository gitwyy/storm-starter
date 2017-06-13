package storm.starter.trident;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import storm.starter.util.OrderUtility;
import storm.starter.util.ThreadUtil;
import storm.starter.util.TridentUtility;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.FilterNull;

public class OrderDrpc {

	private static StormTopology buildStormTopologyII(LocalDRPC localDrpc) {

		TridentTopology topo = new TridentTopology();

		Stream cdlStreamPan = topo.newDRPCStream("cdl_pan", localDrpc).shuffle().parallelismHint(4);
		Stream cdlStreamCus = topo.newDRPCStream("cdl_cus", localDrpc).shuffle().parallelismHint(4);
		Stream cdlStreamPos = topo.newDRPCStream("cdl_pos", localDrpc).shuffle().parallelismHint(4);

		cdlStreamPan.each(new Fields("args"), new FilterNull())
				.each(new Fields("args"), new TridentUtility.PrintFun(), new Fields("line")).shuffle().parallelismHint(4)
				.each(new Fields("line"), new TridentUtility.SplitTrans(), new Fields("id", "orderStatus", "amount", "customerNo", "issuer", "cardType", "transType", "createTime", "externalId"))
				.each(new Fields("customerNo", "amount"), new OrderUtility.cusNoAmountFunction(), new Fields("customer_amount"));

		cdlStreamCus.each(new Fields("args"), new FilterNull())
				.each(new Fields("args"), new TridentUtility.PrintFun(), new Fields("line")).shuffle().parallelismHint(4)
				.each(new Fields("line"), new TridentUtility.SplitTrans(), new Fields("id", "orderStatus", "amount", "customerNo", "issuer", "cardType", "transType", "createTime", "externalId"))
				.each(new Fields("customerNo", "issuer"), new OrderUtility.cusNoIssuerFunction(), new Fields("customer_issuer"));

		cdlStreamPos.each(new Fields("args"), new FilterNull())
				.each(new Fields("args"), new TridentUtility.PrintFun(), new Fields("line")).shuffle().parallelismHint(4)
				.each(new Fields("line"), new TridentUtility.SplitTrans(), new Fields("id", "orderStatus", "amount", "customerNo", "issuer", "cardType", "transType", "createTime", "externalId"))
				.each(new Fields("customerNo", "transType"), new OrderUtility.cusNoTransTypeFunction(), new Fields("customer_transType"));

		return topo.build();

	}

	public static void main(String[] args) {
		LocalCluster local = new LocalCluster();
		LocalDRPC localDrpc = new LocalDRPC();

		Config conf = new Config();

		local.submitTopology("drpc", conf, buildStormTopologyII(localDrpc));
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(new File("G:/Order.txt")));

			for (int i = 0; i < 100; i++) {

				String line = reader.readLine();
				funcI(localDrpc, null);
				//funcII(localDrpc, null);
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (reader != null)
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
		}
	}

	public static void funcI(final LocalDRPC localDrpc, final String line) {

		ExecutorService executorPool = Executors.newFixedThreadPool(100);
		try {
			Long start = System.currentTimeMillis();

			Future<String> futurePan = executorPool.submit(new Callable<String>() {
				@Override
				public String call() throws Exception {
					long s = System.currentTimeMillis();
					String result = localDrpc.execute("cdl_pan", line);
					// System.out.println("result cdl_pan:> > > " +result);
					System.out.println("pan cost " + (System.currentTimeMillis() - s));
					return result;
				}
			});

			Future<String> futurePos = executorPool.submit(new Callable<String>() {
				@Override
				public String call() throws Exception {
					long s = System.currentTimeMillis();
					String result = localDrpc.execute("cdl_pos", line);
					// System.out.println("result cdl_pan:> > > " +result);
					System.out.println("pos cost " + (System.currentTimeMillis() - s));
					return result;
				}
			});

			Future<String> futureCus = executorPool.submit(new Callable<String>() {
				@Override
				public String call() throws Exception {
					long s = System.currentTimeMillis();
					String result = localDrpc.execute("cdl_cus", line);
					// System.out.println("result cdl_pan:> > > " +result);
					System.out.println("cus cost " + (System.currentTimeMillis() - s));
					return result;
				}
			});

			try {
				String resultPan = futurePan.get();
				String resultCus = futureCus.get();
				String resultPos = futurePos.get();
				System.out.println("resultPan >>>>>>>> pan " + resultPan);
				System.out.println("resultCus :::::::: cus " + resultCus);
				System.out.println("resultPos -------- pos " + resultPos);
			} catch (Exception e) {
				e.printStackTrace();
			}
			System.out.println("cost ---------> : " + (System.currentTimeMillis() - start));

			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void funcII(final LocalDRPC localDrpc, final String line) {
		String result = localDrpc.execute("cdl_pan", line);
		System.out.println("result cdl_pan:> > > " + result);
		ThreadUtil.sleep(3000L);
	}
}
