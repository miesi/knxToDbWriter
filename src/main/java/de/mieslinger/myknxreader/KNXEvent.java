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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tuwien.auto.calimero.GroupAddress;
import tuwien.auto.calimero.IndividualAddress;
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
    private static final Logger logger = LoggerFactory.getLogger(KNXEvent.class);

    private KNXEvent() {
    }

    public KNXEvent(KNXEventEnum evType, ProcessEvent e, DatapointModel<StateDP> datapoints) {
        this.evType = evType;
        this.ev = e;
        this.ts = LocalDateTime.now();
        this.d = e.getDestination();
        this.asdu = e.getASDU();

        this.datapoints = datapoints;
        // TODO: add checking for NULL
        this.dp = datapoints.get(e.getDestination());

        /*    
     * 1.yyy = boolesch, wie Schalten, Bewegen nach oben/unten, Schritt
     * 2.yyy = 2 x boolesch, z. B. Schalten + Prioritätssteuerung
     * 3.yyy = boolesch + vorzeichenloser 3-Bit-Wert, z. B. Auf-/Abdimmen
     * 4.yyy = Zeichen (8-Bit)
     * 5.yyy = vorzeichenloser 8-Bit-Wert, wie Dimm-Wert (0..100 %), Jalousienposition (0..100 %)
     * 6.yyy = 8-Bit-2-Komplement, z. B. %
     * 7.yyy = 2 x vorzeichenloser 8-Bit-Wert, z. B. Impulszähler
     * 8.yyy = 2 x 8-Bit-2-Komplement, z. B. %
     * 9.yyy = 16-Bit-Gleitkommazahl, z. B. Temperatur
     * 10.yyy = Uhrzeit
     * 11.yyy = Datum
     * 12.yyy = 4 x vorzeichenloser 8-Bit-Wert,z. B. Impulszähler
     * 13.yyy = 4 x 8-Bit-2-Komplement, z. B. Impulszähler
     * 14.yyy = 32-Bit-Gleitkommazahl, z. B. Temperatur
     * 15.yyy = Zugangskontrolle
     * 16.yyy = String -> 14 Zeichen (14 x 8-Bit)
     * 17.yyy = Szenennummer
     * 18.yyy = Szenensteuerung
     * 19.yyy = Uhrzeit + Datum
     * 20.yyy = 8-Bit-Nummerierung,z. B. HLK-Modus („Automatik“, „Komfort“, „Stand-by“, „Sparen“, „Schutz“)
         */
        if (dp.getMainNumber() == 1) {
            isBoolean = true;
        }
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
        if (isBoolean) {
            return asdu.toString();
        } else {
            final DPTXlator t = TranslatorTypes.createTranslator(0, dp.getDPT());
            t.setData(asdu);
            return t.getValue();
        }
    }

    /**
     * Returns the numeric representation of the first item stored by this
     * translator, if the DPT value can be represented numerically.
     *
     * @return the numeric representation of the value
     * @throws KNXFormatException if the value cannot be represented numerically
     */
    public double getNumericValue() throws KNXFormatException, KNXException {
        double rv = 0;
        final DPTXlator xlt = TranslatorTypes.createTranslator(0, dp.getDPT());
        xlt.setData(asdu);
        try {
            rv = xlt.getNumericValue();
        } catch (Exception e) {
            IndividualAddress sAddr = ev.getSourceAddr();
            GroupAddress dAddr = ev.getDestination();
            String val = toHex(asdu, "");
            String desc = datapoints.get(dAddr).getName();
            String dpt = datapoints.get(dAddr).getDPT();
            logger.warn("{} -> {} ({}): [0x{}], DPT: {}", sAddr, dAddr, desc, val, dpt);
        }
        return rv;
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
        return new Timestamp(ts.toInstant(zoneOffSet).toEpochMilli());
    }

    /**
     * Returns the content of <code>data</code> as unsigned bytes in hexadecimal
     * string representation.
     * <p>
     * This method does not add hexadecimal prefixes (like 0x).
     *
     * @param data data array to format
     * @param sep separator to insert between 2 formatted data bytes,
     * <code>null</code> or "" for no gap between byte tokens
     * @return an unsigned hexadecimal string of data
     */
    public static String toHex(final byte[] data, final String sep) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < data.length; ++i) {
            final int no = data[i] & 0xff;
            if (no < 0x10) {
                sb.append('0');
            }
            sb.append(Integer.toHexString(no));
            if (sep != null && i < data.length - 1) {
                sb.append(sep);
            }
        }
        return sb.toString();
    }
}
