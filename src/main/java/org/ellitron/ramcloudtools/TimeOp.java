package org.ellitron.ramcloudtools;

import edu.stanford.ramcloud.RAMCloud;
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
    + "  TimeOp -C <coordinatorLocator> read\n"
    + "  TimeOp -C <coordinatorLocator> write\n"
    + "  TimeOp (-h | --help)\n"
    + "  TimeOp --version\n"
    + "\n"
    + "Options:\n"
    + "  -C            RAMCloud coordinator's locator string.\n"
    + "  -h --help     Show this screen.\n"
    + "  --version     Show version.\n"
    + "\n";
    
//    private static final String navalfate =
//    "Naval Fate.\n"
//    + "\n"
//    + "Usage:\n"
//    + "  naval_fate ship new <name>...\n"
//    + "  naval_fate ship <name> move <x> <y> [--speed=<kn>]\n"
//    + "  naval_fate ship shoot <x> <y>\n"
//    + "  naval_fate mine (set|remove) <x> <y> [--moored | --drifting]\n"
//    + "  naval_fate (-h | --help)\n"
//    + "  naval_fate --version\n"
//    + "\n"
//    + "Options:\n"
//    + "  -h --help     Show this screen.\n"
//    + "  --version     Show version.\n"
//    + "  --speed=<kn>  Speed in knots [default: 10].\n"
//    + "  --moored      Moored (anchored) mine.\n"
//    + "  --drifting    Drifting mine.\n"
//    + "\n";
    private static final Logger logger = Logger.getLogger(TimeOp.class);
    
    public static void main( String[] args ) {
        Map<String, Object> opts =
        new Docopt(doc).withVersion("TimeOp 0.1").parse(args);
        System.out.println(opts);
        
        if ((Boolean)opts.get("read")) {
            RAMCloud client = new RAMCloud((String)opts.get("-C"));
        } else if ((Boolean)opts.get("write")) {
            System.out.println("command is write");
        }
    }
}
