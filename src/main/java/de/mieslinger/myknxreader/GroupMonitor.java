package de.mieslinger.myknxreader;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tuwien.auto.calimero.DetachEvent;
import tuwien.auto.calimero.KNXException;
import tuwien.auto.calimero.datapoint.DatapointModel;
import tuwien.auto.calimero.datapoint.StateDP;
import tuwien.auto.calimero.link.KNXNetworkLink;
import tuwien.auto.calimero.link.KNXNetworkLinkIP;
import tuwien.auto.calimero.link.medium.TPSettings;
import tuwien.auto.calimero.process.ProcessCommunicator;
import tuwien.auto.calimero.process.ProcessCommunicatorImpl;
import tuwien.auto.calimero.process.ProcessEvent;
import tuwien.auto.calimero.process.ProcessListener;

/*
    Calimero 2 - A library for KNX network access
    Copyright (c) 2015, 2018 B. Malinowsky
    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; either version 2 of the License, or
    (at your option) any later version.
    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
    GNU General Public License for more details.
    You should have received a copy of the GNU General Public License
    along with this program; if not, write to the Free Software
    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
/**
 * Example code showing how to use KNX process communication for group
 * monitoring on a KNX Twisted Pair 1 (TP1) network. On receiving group
 * notifications, the KNX source and destination address are printed to
 * System.out, as well as any data part of the application service data unit
 * (ASDU) in hexadecimal format.
 * <p>
 * Note that this example does not exit, i.e., it monitors forever or until the
 * KNX network link connection got closed. Hence, with KNX servers that have a
 * limit on active tunneling connections (usually 1 or 4), if the group monitor
 * in connected state is terminated by the client (you), the pending state of
 * the open tunnel on the KNX server might temporarily cause an error on
 * subsequent connection attempts.
 *
 * @author B. Malinowsky
 */
public class GroupMonitor implements ProcessListener, Runnable {

    /**
     * Address of your KNXnet/IP server. Replace the host or IP address as
     * necessary.
     */
    private String remoteHost;
    private ConcurrentLinkedQueue<KNXEvent> queue;
    private DatapointModel<StateDP> datapoints;
    private final static Logger logger = LoggerFactory.getLogger(GroupMonitor.class);

    private GroupMonitor() {

    }

    public GroupMonitor(String remoteHost, ConcurrentLinkedQueue<KNXEvent> queue, DatapointModel<StateDP> datapoints) {
        this.remoteHost = remoteHost;
        this.queue = queue;
        this.datapoints = datapoints;
        logger.info("Monitoring KNX network using KNXnet/IP server " + remoteHost + " instantiated");
    }

    public void run() {
        final InetSocketAddress remote = new InetSocketAddress(remoteHost, 3671);
        try (KNXNetworkLink knxLink = KNXNetworkLinkIP.newTunnelingLink(null, remote, false, TPSettings.TP1);
                ProcessCommunicator pc = new ProcessCommunicatorImpl(knxLink)) {

            // start listening to group notifications using a process listener
            pc.addProcessListener(this);
            logger.info("Monitoring KNX network using KNXnet/IP server " + remoteHost + " running");
            while (knxLink.isOpen()) {
                Thread.sleep(1000);
            }
            knxLink.close();
        } catch (final KNXException | InterruptedException | RuntimeException e) {
            logger.warn("KNX Monitor Exception: ", e);
        }
    }

    @Override
    public void groupWrite(final ProcessEvent e) {
        trace(KNXEventEnum.groupWrite, e);
    }

    @Override
    public void groupReadRequest(final ProcessEvent e) {
        trace(KNXEventEnum.groupReadRequest, e);
    }

    @Override
    public void groupReadResponse(final ProcessEvent e) {
        trace(KNXEventEnum.groupReadResponse, e);
    }

    @Override
    public void detached(final DetachEvent e) {
    }

    // Called on every group notification issued by a datapoint on the KNX network. It prints the service primitive,
    // KNX source and destination address, and Application Service Data Unit (ASDU) to System.out.
    private void trace(final KNXEventEnum evType, final ProcessEvent e) {
        try {

            queue.add(new KNXEvent(evType, e, datapoints));
        } catch (final RuntimeException ex) {
            logger.warn("KNX Monitor: ", ex);
        }
    }
}
