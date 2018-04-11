package net.ellitron.ramcloudtools;

import edu.stanford.ramcloud.*;
import edu.stanford.ramcloud.ClientException.*;

import org.apache.log4j.Logger;

import java.util.Map;

import org.docopt.Docopt;

/**
 * Hello world!
 *
 */
public class TimeOp 
{
    private static final String doc =
    "TimeOp.\n"
    + "\n"
    + "Usage:\n"
    + "  TimeOp -C COORDINATORLOCATOR [--count=<ops>] [--tx] [--noexist] read\n"
    + "  TimeOp (-h | --help)\n"
    + "  TimeOp --version\n"
    + "\n"
    + "Options:\n"
    + "  -C             RAMCloud coordinator's locator string.\n"
    + "  --count=<ops>  Number of times to perform the op [default: 3].\n"
    + "  --tx           Perform the operation in a transaction context.\n"
    + "  --noexist      Perform the operation on a nonexistant object.\n"
    + "  -h --help      Show this screen.\n"
    + "  --version      Show version.\n"
    + "\n";
    
    private static final Logger logger = Logger.getLogger(TimeOp.class);
    
    public static void main( String[] args ) {
        Map<String, Object> opts =
        new Docopt(doc).withVersion("TimeOp 0.1").parse(args);
        System.out.println(opts);
        
        if ((Boolean)opts.get("read")) {
            RAMCloud client = new RAMCloud((String)opts.get("COORDINATORLOCATOR"));
            
            long tableId = client.createTable("test");

            byte[] key = new byte[32];
            byte[] value = new byte[100];

            if (!(Boolean)opts.get("--noexist")) {
              client.write(tableId, key, value, null);
            }

            int exceptionCount = 0;
            for (int i = 0; i < Integer.decode((String)opts.get("--count")); i++) {
              if ((Boolean)opts.get("--tx")) {              
                RAMCloudTransaction tx = new RAMCloudTransaction(client);

                long startTime = System.nanoTime();
                try {
                  tx.read(tableId, key);
                } catch (ClientException ex) {
                  exceptionCount++;
                }
                long endTime = System.nanoTime();
            
                System.out.println(String.format("Time: %dus (%d exceptions caught)", (endTime - startTime)/1000l, exceptionCount));
                tx.commitAndSync();
              } else {
                long startTime = System.nanoTime();
                try { 
                  client.read(tableId, key);
                } catch (ObjectDoesntExistException ex) {
                  exceptionCount++;
                }
                long endTime = System.nanoTime();                

                System.out.println(String.format("Time: %dus (%d exceptions caught)", (endTime - startTime)/1000l, exceptionCount));
              }
            }

            client.dropTable("test");
        } 
    }
}
