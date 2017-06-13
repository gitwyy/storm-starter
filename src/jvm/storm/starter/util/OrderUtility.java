package storm.starter.util;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class OrderUtility {
	
	public static class cusNoAmountFunction extends BaseFunction{

		private static final long serialVersionUID = 1L;

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			String customerNo = tuple.getStringByField("customerNo");
			String amount = tuple.getStringByField("amount");
			String cnAmount = customerNo + "_" + amount;
			collector.emit(new Values(cnAmount));
		}
		
	}
	
	
	public static class cusNoIssuerFunction extends BaseFunction{

		private static final long serialVersionUID = 1L;

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			String customerNo = tuple.getStringByField("customerNo");
			String issuer = tuple.getStringByField("issuer");
			String cnIssuer = customerNo + "_" + issuer;
			collector.emit(new Values(cnIssuer));
		}
		
	}
	
	
	public static class cusNoTransTypeFunction extends BaseFunction{

		private static final long serialVersionUID = 1L;

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			String customerNo = tuple.getStringByField("customerNo");
			String transType = tuple.getStringByField("transType");
			String cnTransType = customerNo + "_" + transType;
			collector.emit(new Values(cnTransType));
		}
		
	}
}
