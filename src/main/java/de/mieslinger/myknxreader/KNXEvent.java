/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.mieslinger.myknxreader;

import java.time.LocalDateTime;
import tuwien.auto.calimero.GroupAddress;
import tuwien.auto.calimero.KNXException;
import tuwien.auto.calimero.KNXFormatException;
import tuwien.auto.calimero.datapoint.Datapoint;
import tuwien.auto.calimero.datapoint.DatapointModel;
import tuwien.auto.calimero.datapoint.StateDP;
import tuwien.auto.calimero.dptxlator.DPTXlator;
import tuwien.auto.calimero.dptxlator.TranslatorTypes;
import tuwien.auto.calimero.process.ProcessEvent;

/**
 *
 * @author mieslingert
 */
public class KNXEvent {

    private KNXEventEnum evType;
    private ProcessEvent ev;
    private LocalDateTime ts;
    private GroupAddress d;
    private Datapoint dp;
    private byte[] asdu;
    private DatapointModel<StateDP> datapoints;
    private boolean isBoolean = false;
    private boolean isInteger = false;
    private boolean isFloat = false;

    private KNXEvent() {
    }

    public KNXEvent(KNXEventEnum evType, ProcessEvent e, DatapointModel<StateDP> datapoints) {
        this.evType = evType;
        this.ev = e;
        this.datapoints = datapoints;
        this.ts = LocalDateTime.now();
        this.d = e.getDestination();
        this.dp = datapoints.get(e.getDestination());
        this.asdu = e.getASDU();
    }

    public KNXEventEnum getEvType() {
        return evType;
    }

    public ProcessEvent getEv() {
        return ev;
    }

    public LocalDateTime getTs() {
        return ts;
    }

    /**
     * Returns a string translation of the datapoint data for the specified
     * datapoint type, using the process event ASDU.
     * <p>
     *
     * @param asdu the process event ASDU with the datapoint data
     * @param dptMainNumber DPT main number &ge; 0, can be 0 if the
     * <code>dptID</code> is unique
     * @param dptID datapoint type ID to lookup the translator
     * @return the datapoint value
     * @throws KNXException on failed creation of translator, or translator not
     * available
     */
    public String asString() throws KNXException {
        final DPTXlator t = TranslatorTypes.createTranslator(0, dp.getDPT());
        t.setData(asdu);
        return t.getValue();
    }

    /**
     * Returns the numeric representation of the first item stored by this
     * translator, if the DPT value can be represented numerically.
     *
     * @return the numeric representation of the value
     * @throws KNXFormatException if the value cannot be represented numerically
     */
    public double getNumericValue() throws KNXFormatException, KNXException {
        final DPTXlator xlt = TranslatorTypes.createTranslator(0, dp.getDPT());
        xlt.setData(asdu);
        return xlt.getNumericValue();
    }

    public boolean isBoolean() throws KNXFormatException, KNXException, Exception {
        return isBoolean;
    }

    public boolean isInteger() throws KNXFormatException, KNXException, Exception {
        return isInteger;
    }

    public boolean isFloat() throws KNXFormatException, KNXException, Exception {
        return isFloat;
    }

    public boolean getBooleanValue() throws KNXFormatException, KNXException, Exception {
        throw new Exception("Not yet implemented");
    }

    public Integer getIntegerValue() throws KNXFormatException, KNXException, Exception {
        throw new Exception("Not yet implemented");
    }

    public Double getFloatValue() throws KNXFormatException, KNXException, Exception {
        final DPTXlator t = TranslatorTypes.createTranslator(0, dp.getDPT());
        t.setData(asdu);
        return t.getNumericValue();
    }

}
