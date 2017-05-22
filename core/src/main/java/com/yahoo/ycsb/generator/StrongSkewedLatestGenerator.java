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

/**
 * Generate a popularity distribution of items, skewed to favor recent items significantly more than older items.
 */
public class StrongSkewedLatestGenerator extends NumberGenerator {
  public static final double ZIPFIAN_ALPHA_ADJUSTMENTS_CONSTANT = 1.9;

  private CounterGenerator basis;
  private final ZipfianGenerator zipfian;

  public StrongSkewedLatestGenerator(CounterGenerator basis) {
    this(basis, ZIPFIAN_ALPHA_ADJUSTMENTS_CONSTANT);
  }

  public StrongSkewedLatestGenerator(CounterGenerator basis, double skew) {
    this.basis=basis;
    this.zipfian=new ZipfianGenerator(0, this.basis.lastValue()-1, ZipfianGenerator.ZIPFIAN_CONSTANT, skew);
    nextValue();
  } 

  /**
   * Generate the next string in the distribution, skewed Zipfian favoring the items most recently returned 
   * by the basis generator.
   */
  @Override
  public Long nextValue() {
    long max=this.basis.lastValue();
    long next=max-this.zipfian.nextLong(max);
    setLastValue(next);
    return next;
  }

  public static void main(String[] args) {
    if((args.length < 1) && (args.length > 3)) {
      System.out.println("usage: ./StrongSkewedLatestGenerator size [skew:1.0] [count:1000]");
      System.exit(-1);
    }

    StrongSkewedLatestGenerator gen;
    if (args.length == 1) { 
      gen = new StrongSkewedLatestGenerator(new CounterGenerator(Integer.valueOf(args[0])));
    } else {
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
