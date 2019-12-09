/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.mieslinger.myknxreader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tuwien.auto.calimero.GroupAddress;
import tuwien.auto.calimero.IndividualAddress;
import tuwien.auto.calimero.datapoint.DatapointModel;
import tuwien.auto.calimero.datapoint.StateDP;

/**
 *
 * @author mieslingert
 */
public class DbWriter implements Runnable {

    private boolean keepOnRunning = true;
    private String jdbcClass;
    private String jdbcUrl;
    private String user;
    private String password;
    private Connection conn;
    private ConcurrentLinkedQueue<KNXEvent> queue;
    private DatapointModel<StateDP> datapoints;
    private PreparedStatement checkTableExists;
    private PreparedStatement createTable;
    private PreparedStatement insertData;

    private final static Logger logger = LoggerFactory.getLogger(DbWriter.class);

    private DbWriter() {
    }

    public DbWriter(String jdbcClass, String jdbcUrl, String user, String password, ConcurrentLinkedQueue<KNXEvent> queue, DatapointModel<StateDP> datapoints) {
        this.jdbcClass = jdbcClass;
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
        this.queue = queue;
        this.datapoints = datapoints;
        try {
            Class.forName(jdbcClass);
            conn = DriverManager.getConnection(jdbcUrl, user, password);
            if (conn.isValid(5)) {
                logger.info("Successfully connected to DB");
            }
        } catch (Exception e) {
            logger.error("DB Connection failed on initial connection, exiting", e);
            System.exit(1);
        }
        logger.info("DbWriter instantiated");
    }

    @Override
    public void run() {
        while (keepOnRunning) {
            try {
                KNXEvent kev = queue.poll();
                if (kev != null) {
                    persist(kev);
                } else {
                    Thread.sleep(5000);
                }
            } catch (Exception e) {
                logger.warn("DbWriter Exception: ", e);
            }
        }
    }

    private void persist(KNXEvent kev) {
        // DEBUG:
        // check that we get the event and can decode it
        try {
            IndividualAddress sAddr = kev.getEv().getSourceAddr();
            GroupAddress dAddr = kev.getEv().getDestination();
            String val = kev.asString();
            String desc = datapoints.get(dAddr).getName();
            String dpt = datapoints.get(dAddr).getDPT();
            logger.debug("{} -> {} ({}): {}, DPT: {}", sAddr, dAddr, desc, val, dpt);

        } catch (Exception e) {
            logger.error("Exception decoding event from " + kev.getEv().getSourceAddr().toString() + " to GA " + kev.getEv().getDestination().toString() + " :" + e.toString());
            e.printStackTrace();
        }

        tryAndReconnect();

        checkAndCreateTable(kev);

    }

    private void checkAndCreateTable(KNXEvent e) {
        // generate table name

        // GA DPT
        // 5/0/2 9.001
        // data_5_0_2_9_001
        GroupAddress ga = e.getEv().getDestination();
        StateDP dp = datapoints.get(ga);
        String dpt = dp.getDPT();
        String tableName = "data_" + ga.toString().replace('/', '_') + "_" + dpt.replace('.', '_');

        // select 1 from table name
        try {
            checkTableExists = conn.prepareStatement("select 1 from " + tableName);
            checkTableExists.execute();
        } catch (Exception ex) {
            // -> Exception -> create table
            logger.info("Table {} does not exist, creating", tableName);
            try {
                createTable = conn.prepareStatement("create table " + tableName + " ("
                        + "ts timestamp NOT NULL DEFAULT current_timestamp(),"
                        + "value double not null,"
                        + "primary key (ts)"
                        + ")");
                createTable.executeUpdate();
                logger.info("created table {}", tableName);
            } catch (Exception exc) {
                logger.warn("unexpected exception during create table: {}", exc.getMessage());
                exc.printStackTrace();
            }
        }

        // -> do insert
        try {
            insertData = conn.prepareStatement("insert into " + tableName + " (ts,value) values (?,?)");
            Timestamp sqlTs = new Timestamp(e.getTs().toEpochSecond(ZoneOffset.UTC) * 1000);
            insertData.setTimestamp(1, sqlTs);
            insertData.setDouble(2, e.getNumericValue());
            insertData.execute();
        } catch (Exception ex) {
            logger.warn("unexpected exception during insert data: {}", ex.getMessage());
            ex.printStackTrace();
        }
    }

    private void tryAndReconnect() {
        // sleep 30s after every connection fail
        boolean connectionOK;
        try {
            connectionOK = conn.isValid(5);
        } catch (Exception e) {
            connectionOK = false;
            logger.warn("Connection to DB broken", e);
        }

        if (connectionOK) {
            return;
        }

        while (!connectionOK) {
            try {
                conn = DriverManager.getConnection(jdbcUrl, user, password);
                connectionOK = true;
                return;
            } catch (Exception e) {

            }
            if (!connectionOK) {
                logger.warn("Connection to DB still broken, sleeping");
                try {
                    Thread.sleep(600000); // use min 600s in production
                } catch (Exception e) {

                }
            }
        }
    }
}
