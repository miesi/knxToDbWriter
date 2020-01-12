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

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
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

    public Timestamp getSqlTs() {
        ZoneId zone = ZoneId.of("Europe/Berlin");
        ZoneOffset zoneOffSet = zone.getRules().getOffset(ts);
        return new Timestamp(ts.toEpochSecond(zoneOffSet) * 1000);
    }
}
