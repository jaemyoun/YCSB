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

import java.util.HashMap;
import java.util.Map;

/**
 * Generate a popularity distribution of items, skewed to favor recent items significantly more than older items.
 */
public class StrongSkewedLatestGenerator extends NumberGenerator
{
	private double skew;
	private CounterGenerator basis;
  private double bottom = 0;
	private Map<Integer, Double> zipfReadMap;

	public StrongSkewedLatestGenerator(CounterGenerator basis)
	{
		this(basis, 1.0d);
	}

	public StrongSkewedLatestGenerator(CounterGenerator basis, double skew)
	{
		this.skew = skew;
		this.basis = basis;
		this.zipfReadMap = new HashMap<Integer, Double>();
		
		long size = basis.lastValue() + 1L;
    for(int i=1;i < size; i++) {
      this.bottom += (1/Math.pow(i, this.skew));
    }
	} 

	/**
	 * Generate the next string in the distribution, skewed Zipfian favoring the items most recently returned by the basis generator.
	 */
	@Override
  public Long nextValue()
	{
    double u = Utils.random().nextDouble();
		int next = 1;
		int loop = 0;

    loop = 1;
    while(u >= getProbability(next)) {
      next+=loop * 2;
      loop++;
    }
    loop = 1;
    while(u < getProbability(next - 1)) {
      next-=loop * 2;
      loop++;
    }
    while(u >= getProbability(next)) {
      next++;
    }
		
		long ret = basis.lastValue() + 1L - next;
		setLastValue(ret);
		return ret;
	}

	public double getProbability(int index) {
    Double ret;
    double cumProbability = 0.0;

    ret = zipfReadMap.get(index);
    if (ret == null) {
      for(int i = index; i >= 1; i--) {
        cumProbability += (1.0d / Math.pow(i, this.skew)) / this.bottom;

        if (i != 1) {
          ret = zipfReadMap.get(i-1);
          if (ret != null) {
            cumProbability += ret.doubleValue();
            break;
          }
        }
      }
      zipfReadMap.put(index, cumProbability);
      ret = new Double(cumProbability);
    }

    return ret.doubleValue();
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
