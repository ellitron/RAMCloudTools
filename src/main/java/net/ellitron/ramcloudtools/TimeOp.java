package net.ellitron.ramcloudtools;

import edu.stanford.ramcloud.*;
import edu.stanford.ramcloud.ClientException.*;

import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Map;

import org.docopt.Docopt;

/**
 * TimeOp is a utility for timing various RAMCloud operations. This version is
 * implemented in Java, and tests the performance of the RAMCloud Java API
 * implementation.
 *
 */
public class TimeOp 
{
    private static final String doc =
    "TimeOp.\n"
    + "\n"
    + "Usage:\n"
    + "  TimeOp [options] read\n"
    + "  TimeOp [options] write\n"
    + "  TimeOp (-h | --help)\n"
    + "  TimeOp --version\n"
    + "\n"
    + "Options:\n"
    + "  -C=<coord>       RAMCloud coordinator's locator string [default: tcp:host=127.0.0.1,port=12246].\n"
    + "  --keySize=<b>    Size of object keys to use (in bytes) [default: 30].\n"
    + "  --valueSize=<b>  Size of object values to use (in bytes) [default: 100].\n"
    + "  --nonexist       Perform op on nonexistant objects.\n"
    + "  --tx             Perform the op in a transaction context.\n"
    + "  --objsPerTx=<n>  If using transactions, number of objects per TX [default: 1].\n"
    + "  --txRollback     If using transactions, whether or not to rollback the transaction in the end. Otherwise commit the transaction.\n"
    + "  --count=<n>      Number of times to perform the op. If using transactions, then this is the number of transactions to perform. [default: 3].\n"
    + "  -h --help        Show this screen.\n"
    + "  --version        Show version.\n"
    + "\n";
    
    private static final Logger logger = Logger.getLogger(TimeOp.class);
    
    public static void main( String[] args ) {
        Map<String, Object> opts =
        new Docopt(doc).withVersion("TimeOp 0.1").parse(args);
        System.out.println(opts);
       
        int keySize = Integer.decode((String)opts.get("--keySize"));
        int valueSize = Integer.decode((String)opts.get("--valueSize"));
        Boolean doNonexist = (Boolean) opts.get("--nonexist");
        Boolean doTx = (Boolean) opts.get("--tx");
        int objsPerTx = Integer.decode((String)opts.get("--objsPerTx"));
        Boolean doTxRollback = (Boolean) opts.get("--txRollback");
        int count = Integer.decode((String)opts.get("--count"));

        // Setup client and test table for performing operations.
        RAMCloud client = new RAMCloud((String)opts.get("-C"));
        long tableId = client.createTable("test");

        // If we're performing these operations on existant objects, first write
        // the objects into a RAMCloud test table. Otherwise don't write any
        // objects into RAMCloud.
        if (!doNonexist) {
          byte[] key = new byte[keySize];
          byte[] value = new byte[valueSize];

          if (doTx) {
            // In the case where we want to perform the operation inside of a
            // transaction context, we may also want to perform the op several
            // times on different objects in one transaction to see the
            // differences in performance. Prepare each of these objects in
            // advance.
            for (int i = 0; i < objsPerTx; i++) {
              String keyStr = String.format("%d", i);
              System.arraycopy(keyStr.getBytes(), 0, key, 0, keyStr.getBytes().length);
              client.write(tableId, key, value, null);
            }
          } else {
            // We are not performing the op in a transaction context. In this
            // case, we are just going to perform the op over and over again
            // on a single object.
            Arrays.fill(key, (byte) 0);
            client.write(tableId, key, value, null);
          }
        }

        if ((Boolean) opts.get("read")) {
          byte[] key = new byte[keySize];

          if (doTx) {
            for (int i = 0; i < count; i++) {
              RAMCloudTransaction tx = new RAMCloudTransaction(client);

              for (int j = 0; j < objsPerTx; j++) {
                String keyStr = String.format("%d", j);
                System.arraycopy(keyStr.getBytes(), 0, key, 0, keyStr.getBytes().length);

                long startTime = System.nanoTime();
                tx.read(tableId, key);
                long endTime = System.nanoTime();

                System.out.println(String.format("Time[%d]: %dus", j, (endTime - startTime)/1000l));
              }

              if (objsPerTx > 1) {
                System.out.println();
              }
              
              if (doTxRollback) {
                tx.close();
              } else {
                tx.commitAndSync();
                tx.close();
              }
            }
          } else {
            Arrays.fill(key, (byte) 0);

            for (int i = 0; i < count; i++) {
              long startTime = System.nanoTime();
              client.read(tableId, key);
              long endTime = System.nanoTime();

              System.out.println(String.format("Time: %dus", (endTime - startTime)/1000l));
            }
          }

          client.dropTable("test");
          client.disconnect();
        } else if ((Boolean) opts.get("write")) {
          System.out.println("Not implemented!");
        }
    }
}
