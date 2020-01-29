/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.mieslinger.myknxreader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
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
    private PreparedStatement insertLog;
    private PreparedStatement cleanupLog;
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
        checkAndCreateLogTable(kev);
        checkAndCreateDataTable(kev);
    }

    private void checkAndCreateLogTable(KNXEvent e) {
        // select 1 from table name
        try {
            checkTableExists = conn.prepareStatement("select 1 from knx_log");
            checkTableExists.execute();
        } catch (Exception ex) {
            // -> Exception -> create table
            logger.info("Table knx_log does not exist, creating");
            try {
                createTable = conn.prepareStatement("create table knx_log ("
                        + "ts timestamp NOT NULL DEFAULT current_timestamp(),"
                        + "src_addr varchar(16) not null,"
                        + "dst_addr varchar(16) not null,"
                        + "dst_desc varchar(4000),"
                        + "dpt varchar(10) not null,"
                        + "value double not null,"
                        + "key (ts),"
                        + "key (src_addr),"
                        + "key (dst_addr)"
                        + ")");
                createTable.executeUpdate();
                createTable.close();
                logger.info("created table knx_log");
            } catch (Exception exc) {
                logger.warn("unexpected exception during create table knx_log: {}", exc.getMessage());
                exc.printStackTrace();
            }
        }

        // -> do insert
        try {
            insertLog = conn.prepareStatement("insert into knx_log (ts, src_addr, dst_addr, dst_desc, dpt, value) values (?,?,?,?,?,?)");
            insertLog.setTimestamp(1, e.getSqlTs());
            insertLog.setString(2, e.getEv().getSourceAddr().toString());
            insertLog.setString(3, e.getEv().getDestination().toString());
            insertLog.setString(4, datapoints.get(e.getEv().getDestination()).getName());
            insertLog.setString(5, datapoints.get(e.getEv().getDestination()).getDPT());
            insertLog.setDouble(6, e.getNumericValue());
            insertLog.execute();
            insertLog.close();
            cleanupLog = conn.prepareStatement("delete from knx_log where ts < date_sub(now(), interval  3 month)");
            cleanupLog.executeUpdate();
            cleanupLog.close();
        } catch (Exception ex) {
            logger.warn("unexpected exception during insert data: {}", ex.getMessage());
            ex.printStackTrace();
        }

    }

    private void checkAndCreateDataTable(KNXEvent e) {
        // generate table name

        // GA DPT
        // 5/0/2 9.001
        // data_5_0_2_9_001
        GroupAddress ga = e.getEv().getDestination();
        StateDP dp = datapoints.get(ga);
        String dpt = dp.getDPT();

        // filter DPT. Do not record typical on/off events (lights/rollershutter/...)
        if (dpt.startsWith("2")) {
            logger.info("ignoring event from {} to GA {} ({}), DPT {}", e.getEv().getSourceAddr().toString(),
                    e.getEv().getDestination().toString(),
                    datapoints.get(e.getEv().getDestination()).getName(),
                    datapoints.get(e.getEv().getDestination()).getDPT());
            return;
        }

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
                createTable.close();
                logger.info("created table {}", tableName);
            } catch (Exception exc) {
                logger.warn("unexpected exception during create table {}: {}", tableName, exc.getMessage());
                exc.printStackTrace();
            }
        }

        // -> do insert
        try {
            insertData = conn.prepareStatement("insert into " + tableName + " (ts,value) values (?,?)");
            insertData.setTimestamp(1, e.getSqlTs());
            insertData.setDouble(2, e.getNumericValue());
            insertData.execute();
            insertData.close();
        } catch (Exception ex) {
            logger.warn("unexpected exception during insert data into {}: {}", tableName, ex.getMessage());
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
