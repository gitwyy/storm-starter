package storm.starter.util;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class TridentUtility {
	/*
	 * Get the comma separated value as input, split the field by comma, and
	 * then emits multiple tuple as output.
	 */
	public static class Split extends BaseFunction {
		private static final long serialVersionUID = 2L;

		public void execute(TridentTuple tuple, TridentCollector collector) {
			String countries = tuple.getString(0);
			for (String word : countries.split(",")) {
				collector.emit(new Values(word));
			}
		}
	}

	/*
	 * This class extends BaseFilter and contain isKeep method which emits only
	 * those tuple which has #FIFA in text field.
	 */
	public static class TweetFilter extends BaseFilter {
		private static final long serialVersionUID = 1L;

		@Override
		public boolean isKeep(TridentTuple tuple) {
			if (tuple.getString(0).contains("#FIFA")) {
				return true;

			} else {
				return false;
			}
		}
	}

	/*
	 * This class extends BaseFilter and contain isKeep method which will print
	 * the input tuple.
	 */
	public static class Print extends BaseFilter {
		private static final long serialVersionUID = 1L;

		public boolean isKeep(TridentTuple tuple) {
			System.out.println(tuple);
			return true;
		}
	}

	public static class Nothing extends BaseFilter {

		private static final long serialVersionUID = 1L;

		@Override
		public boolean isKeep(TridentTuple tuple) {
			return true;
		}

	}

	public static class PrintFun extends BaseFunction {
		private static final long serialVersionUID = 1L;

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			System.out.println("PrintFun :::"+tuple);
			collector.emit(tuple);
		}
	}
	
	
	public static class SplitTrans extends BaseFunction {

		private static final long serialVersionUID = 1L;

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			String line = tuple.getStringByField("line");
			String[] split = line.split("\t");
			if (split.length >= 9) {
				String id = split[0];
				String orderStatus = split[1].trim();
				String amount = split[2].trim();
				String customerNo = split[3].trim();
				String issuer = split[4].trim();
				String cardType = split[5].trim();
				String transType = split[6];
				String createTime = split[7];
				String externalId = split[8];
				collector.emit(new Values(id, orderStatus, amount, customerNo, issuer, cardType, transType, createTime,
						externalId));
			}
		}
	}

}