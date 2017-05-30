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

package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.NoRouteToHostException;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.Map.Entry;

import org.json.JSONObject;

import bingoJavaApi.Bingo;

/**
 * YCSB binding for Bingo.
 *
 * See {@code bingo/README.md} for details.
 */
public class BingoClient extends DB {
  private Bingo bingo;
  
  public static final String BINGO_HOST_PROPERTY = "bingo.host";
  public static final String BINGO_HOST_DEFAULT = "localhost";
  public static final String BINGO_PORT_PROPERTY = "bingo.port";
  public static final String BINGO_PORT_DEFAULT = "51000";
  
  public void init() throws DBException {
    Properties props = getProperties();
    
    String host = props.getProperty(BINGO_HOST_PROPERTY);
    if (host == null) {
      host = BINGO_HOST_DEFAULT;
    }
    String port = props.getProperty(BINGO_PORT_PROPERTY);
    if (port == null) {
      port = BINGO_PORT_DEFAULT;
    }
    bingo = new Bingo(host, Integer.parseInt(port));
  }

  public void cleanup() throws DBException {
    try {
      bingo.shutdown();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    ByteArrayOutputStream oStream = new ByteArrayOutputStream();
    Status ret = Status.ERROR;
    int retryChance = 10;
    // Try read.
    while (retryChance > 0) {
      try {
        bingo.download(oStream, key);
        ret = Status.OK;
        break;
      } catch (NoRouteToHostException ne) {
        // If meet NoRouteToHostException: Cannot assign requested address
        // cause by too many dangling TIME_WAIT status socket in kernel, 
        // fall back to 10 seconds and retry it.
        try {
          Thread.sleep(1 * 1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
          break;
        }
      } catch (Throwable e1) {
        try {
          Thread.sleep(100 / retryChance);
        } catch (InterruptedException e) {
          e.printStackTrace();
          break;
        }
        retryChance--;
        if (retryChance == 0) {
          e1.printStackTrace();
        }        
      }
    }
    // Clean up.
    try {
      oStream.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return ret;
  }

  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    JSONObject json = new JSONObject();
    for (final Entry<String, ByteIterator> e : values.entrySet()) {
      json.put(e.getKey(), e.getValue().toString());
    }
    String str = json.toString();
    InputStream iStream = new ByteArrayInputStream(str.getBytes());
    
    Status ret = Status.ERROR;
    // Try write.
    while (true) {
      try {
        bingo.upload(iStream, key, str.getBytes().length);
        ret = Status.OK;
        break;
      } catch (NoRouteToHostException ne) {
        // If meet NoRouteToHostException: Cannot assign requested address
        // cause by too many dangling TIME_WAIT status socket in kernel, 
        // fall back to 10 seconds and retry it.
        try {
          Thread.sleep(10 * 1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
          break;
        }
      } catch (Throwable e1) {
        e1.printStackTrace();
        break;
      }
    }
    // Clean up.
    try {
      iStream.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return ret;
  }

  @Override
  public Status delete(String table, String key) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }
}
