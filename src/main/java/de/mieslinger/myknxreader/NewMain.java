/*
 * The MIT License
 *
 * Copyright 2019 mieslingert.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package de.mieslinger.myknxreader;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.sampullara.cli.Args;
import com.sampullara.cli.Argument;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tuwien.auto.calimero.GroupAddress;
import tuwien.auto.calimero.KNXFormatException;
import tuwien.auto.calimero.datapoint.DatapointMap;
import tuwien.auto.calimero.datapoint.DatapointModel;
import tuwien.auto.calimero.datapoint.StateDP;

/**
 *
 * @author mieslingert
 */
public class NewMain {

    @Argument(alias = "g", description = "KNX tunnel device")
    private static String KNXIPGateway = "10.2.215.62";

    @Argument(alias = "j", description = "jdbcurl")
    private static String jdbcUrl = "jdbc:mysql://localhost:3306/knx_test_db";

    @Argument(alias = "c", description = "jdbc class")
    private static String jdbcClass = "com.mysql.jdbc.Driver";

    @Argument(alias = "u", description = "user to connect to db")
    private static String dbUser = "root";

    @Argument(alias = "p", description = "db password")
    private static String dbPassword = "";

    @Argument(alias = "f", description = "file to read group addresses (csv format)")
    private static String gaFile = "ga.csv";

    @Argument(alias = "cs", description = "Characterset of group addresses file (UTF-8)")
    private static String characterSetGaFile = "UTF-8";

    // FIXME: implement me!
    @Argument(alias = "d", description = "enable debug")
    private static boolean debug = false;

    private static final Logger logger = LoggerFactory.getLogger(NewMain.class);
    private static final ConcurrentLinkedQueue<KNXEvent> queue = new ConcurrentLinkedQueue<>();
    private static final DatapointModel<StateDP> datapoints = new DatapointMap<>();

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

        List<String> unparsed = Args.parseOrExit(NewMain.class, args);
        try {
            /* Example content
             * "EG"; ; ;"1/-/-";"";"";"";"";"Auto"
             *  ;"Licht"; ;"1/0/-";"";"";"";"";"Auto"
             *  ; ;"Flur oben - Schalten";"1/0/9";"";"";"Flur Licht Treppe";"DPST-1-1";"Auto"
             */

            // relevant fields 0,1,2,3,6,7
            // 0 HG
            // 1 MG
            // 2 GA desc1
            // 3 GA
            // 6 desc2
            // 7 dpt
            String lastHgSeen = "";
            String lastMgSeen = "";
            String[] nextLine;

            FileInputStream fis = new FileInputStream(gaFile);
            InputStreamReader isr = new InputStreamReader(fis, Charset.forName(characterSetGaFile));

            CSVParser parser = new CSVParserBuilder()
                    .withSeparator(';')
                    .withIgnoreQuotations(false)
                    .build();

            CSVReader csvReader = new CSVReaderBuilder(isr)
                    .withSkipLines(0)
                    .withCSVParser(parser)
                    .build();

            while ((nextLine = csvReader.readNext()) != null) {

                int numElements = nextLine.length;

                if (numElements > 0 && nextLine[0].length() > 1) {
                    lastHgSeen = nextLine[0];
                }
                if (numElements > 1 && nextLine[1].length() > 1) {
                    lastMgSeen = nextLine[1];
                }
                if (numElements > 7 && nextLine[7].length() > 1) {
                    //addDP("5/0/0", "Eltern Bad", 9, "9.001");
                    addDP(nextLine[3],
                            lastHgSeen + "-" + lastMgSeen + "-" + nextLine[2] + "-" + nextLine[6],
                            nextLine[7]);
                    logger.info("added DP: " + nextLine[3] + " ", lastHgSeen + "-" + lastMgSeen + "-" + nextLine[2] + "-" + nextLine[6]);
                }
            }

        } catch (Exception e) {
            logger.error("unable to load datapoint information", e);
            System.exit(1);
        }

        // setup communication ConcurrentLinkedQueue
        // setup thread for groupMonitor and run it
        Thread tGroupMonitor = new Thread(new GroupMonitor(KNXIPGateway, queue, datapoints));
        tGroupMonitor.setDaemon(true);
        tGroupMonitor.setName("GroupMonitor");
        tGroupMonitor.start();
        logger.debug("GroupMonitor Thread started");

        // setup DB wirter and run it
        Thread tDBWriter = new Thread(new DbWriter(jdbcClass, jdbcUrl, dbUser, dbPassword, queue, datapoints));
        tDBWriter.setDaemon(true);
        tDBWriter.setName("DBWriter");
        tDBWriter.run();
        logger.debug("DBWriter started");

    }

    private static void addDP(String ga, String desc, String dpst) throws KNXFormatException {
        StateDP dp;
        String dptID;
        // change DPST-1-1 to 1.001
        String[] parts = dpst.split("-");

        switch (parts[0]) {
            case "DPT":
                // FIXME: Hack: just use first subtype to get things going
                dptID = String.format("%d.%03d", Integer.parseInt(parts[1]), 1);
                dp = new StateDP(new GroupAddress(ga), desc, Integer.parseInt(parts[1]), dptID);
                datapoints.add(dp);
                logger.warn("applying default subtype for ga: {} desc: {} dpst: {}", ga, desc, dpst);
                break;
            case "DPST":
                dptID = String.format("%d.%03d", Integer.parseInt(parts[1]), Integer.parseInt(parts[2]));
                dp = new StateDP(new GroupAddress(ga), desc, Integer.parseInt(parts[1]), dptID);
                datapoints.add(dp);
                break;
            default:
                logger.warn("ignoring ga: {} desc: {} dpst: {}", ga, desc, dpst);
        }
    }
}
