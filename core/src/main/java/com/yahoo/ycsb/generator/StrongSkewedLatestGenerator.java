/**
 * Copyright (c) 2016 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.generator;

import com.yahoo.ycsb.Utils;

import org.apache.commons.math3.distribution.ZipfDistribution;

/**
 * Generate a popularity distribution of items, skewed to favor recent items significantly more than older items.
 */
public class StrongSkewedLatestGenerator extends NumberGenerator
{
	private double skew;
	private CounterGenerator basis;
	private ZipfDistribution zipf;

	public StrongSkewedLatestGenerator(CounterGenerator basis)
	{
		this(basis, 1.0d);
	}

	public StrongSkewedLatestGenerator(CounterGenerator basis, double skew)
	{
		this.skew = skew;
		this.basis = basis;
		this.zipf = new ZipfDistribution(basis.lastValue() + 1, this.skew);
	}

	/**
	 * Generate the next string in the distribution, skewed Zipfian favoring the items most recently returned by the basis generator.
	 */
	@Override
  public Long nextValue()
	{
    double u = Utils.random().nextDouble();
    double st = 0.0d;
    int next = 1;

    // System.out.println("u: " + u);
    while(u >= this.zipf.cumulativeProbability(next)) {
      next+=15;
    }
		while(u < this.zipf.cumulativeProbability(next - 1)) {
			next--;
		}
		
		long ret = basis.lastValue() + 1L - next;
		setLastValue(ret);
		return ret;
	}

	public static void main(String[] args)
	{
		if((args.length < 1) && (args.length > 3)) {
			System.out.println("usage: ./StrongSkewedLatestGenerator size [skew:1.0] [count:1000]");
			System.exit(-1);
    }

		StrongSkewedLatestGenerator gen;
		if (args.length == 1) { 
			gen = new StrongSkewedLatestGenerator(new CounterGenerator(Integer.valueOf(args[0])));
		}
		else {
			gen = new StrongSkewedLatestGenerator(new CounterGenerator(Integer.valueOf(args[0])), Double.valueOf(args[1]));
		}
    // gen.printlog();

    int count = 1000;
    if (args.length == 3) {
      count = Integer.valueOf(args[2]);
    }

    for(int i = 0; i < count; i++) {
      System.out.println(gen.nextValue());
    }
	}

	@Override
	public double mean() {
		throw new UnsupportedOperationException("Can't compute mean of non-stationary distribution!");
	}

}
