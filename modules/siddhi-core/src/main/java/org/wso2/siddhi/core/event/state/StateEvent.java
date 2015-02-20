/*
 * Copyright (c) 2005 - 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.wso2.siddhi.core.event.state;

import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;

import java.util.Arrays;

import static org.wso2.siddhi.core.util.SiddhiConstants.*;

/**
 * Implementation of StateEvent to be used when executing join/pattern queries
 */
public class StateEvent implements ComplexEvent {

    private long id;
    protected StreamEvent[] streamEvents;
    protected StateEvent next;

    protected long timestamp = -1;
    protected Type type = Type.CURRENT;
    protected Object[] outputData;      //Attributes to sent as output


    public StateEvent(int streamEventsSize, int outputSize) {
        streamEvents = new StreamEvent[streamEventsSize];
        outputData = new Object[outputSize];
    }

    public StreamEvent getStreamEvent(int position) {
        return streamEvents[position];
    }

    public StreamEvent[] getStreamEvents() {
        return streamEvents;
    }

    public void setNext(ComplexEvent stateEvent) {
        next = (StateEvent) stateEvent;
    }

    public StateEvent getNext() {
        return next;
    }

    @Override
    public void setOutputData(Object object, int index) {
        outputData[index] = object;
    }

    /**
     * @param position int array of 3 or 4 elements
     *                 int array of 3 : position[0]-which element of the streamEvents array, position[1]-BeforeWindowData or OutputData or AfterWindowData,
     *                 position[2]- which attribute
     *                 int array of 4 : position[0]-which element of the streamEvents array, position[1]-which event of the event chain,
     *                 position[3]- BeforeWindowData or OutputData or AfterWindowData, position[4]- which attribute
     * @return
     */
    @Override
    public Object getAttribute(int[] position) {
        if (position[STREAM_ATTRIBUTE_TYPE_INDEX] == STATE_OUTPUT_DATA_INDEX) {
            return outputData[position[STREAM_ATTRIBUTE_INDEX]];
        } else {
            StreamEvent streamEvent = streamEvents[position[STREAM_EVENT_CHAIN_INDEX]];
            if (streamEvent == null) {
                return null;
            }
            int streamEventIndex = position[STREAM_EVENT_INDEX];

            switch (streamEventIndex) {
                case LAST:
                    while (streamEvent.getNext() != null) {
                        streamEvent = streamEvent.getNext();
                    }
                    break;
                default:
                    for (int i = 1; i <= position[STREAM_EVENT_INDEX]; i++) {
                        streamEvent = streamEvent.getNext();
                        if (streamEvent == null) {
                            return null;
                        }
                    }
                    break;
            }

            switch (position[STREAM_ATTRIBUTE_TYPE_INDEX]) {
                case BEFORE_WINDOW_DATA_INDEX:
                    return streamEvent.getBeforeWindowData()[position[STREAM_ATTRIBUTE_INDEX]];
                case OUTPUT_DATA_INDEX:
                    return streamEvent.getOutputData()[position[STREAM_ATTRIBUTE_INDEX]];
                case ON_AFTER_WINDOW_DATA_INDEX:
                    return streamEvent.getOnAfterWindowData()[position[STREAM_ATTRIBUTE_INDEX]];
                default:
                    throw new IllegalStateException("STREAM_ATTRIBUTE_TYPE_INDEX cannot be " + position[STREAM_ATTRIBUTE_TYPE_INDEX]);
            }
        }
    }

    public Object[] getOutputData() {
        return outputData;
    }
//    public Object[] getPreOutputData() {
//        return preOutputData;
//    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setType(Type type) {
        this.type = type;
    }

    @Override
    public Type getType() {
        return type;
    }

    public void setEvent(int position, StreamEvent streamEvent) {
        streamEvents[position] = streamEvent;
    }

    public void addEvent(int position, StreamEvent streamEvent) {
        StreamEvent aStreamEvent = streamEvents[position];
        if (aStreamEvent == null) {
            setEvent(position, streamEvent);
        } else {
            while (aStreamEvent.getNext() != null) {
                aStreamEvent = aStreamEvent.getNext();
            }
            aStreamEvent.setNext(streamEvent);
        }
    }

    public void removeLastEvent(int position) {
        StreamEvent aStreamEvent = streamEvents[position];
        if (aStreamEvent != null) {
            while (aStreamEvent.getNext() != null) {
                if (aStreamEvent.getNext().getNext() == null) {
                    aStreamEvent.setNext(null);
                    return;
                }
                aStreamEvent = aStreamEvent.getNext();
            }
            streamEvents[position] = null;
        }
    }

    @Override
    public String toString() {
        return "StateEvent{" +
                "streamEvents=" + Arrays.toString(streamEvents) +
                ", timestamp=" + timestamp +
                ", type=" + type +
                ", outputData=" + Arrays.toString(outputData) +
                ", next=" + next +
                '}';
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }


}
