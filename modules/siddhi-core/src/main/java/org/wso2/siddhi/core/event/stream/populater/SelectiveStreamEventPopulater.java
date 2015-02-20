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

package org.wso2.siddhi.core.event.stream.populater;

import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;

import java.util.List;

import static org.wso2.siddhi.core.util.SiddhiConstants.*;

/**
 * The populater class that populates StateEvents
 */
public class SelectiveStreamEventPopulater implements StreamEventPopulater {

    private List<StreamMappingElement> streamMappingElements;       //List to hold information needed for population

    public SelectiveStreamEventPopulater(List<StreamMappingElement> streamMappingElements) {
        this.streamMappingElements = streamMappingElements;
    }

    @Override
    public void populateStreamEvent(ComplexEvent complexEvent, Object data) {
        populateStreamEvent(complexEvent, data, streamMappingElements.get(0).getToPosition());
    }

    public void populateStreamEvent(ComplexEvent complexEvent, Object[] data) {
        for (StreamMappingElement mappingElement : streamMappingElements) {
            populateStreamEvent(complexEvent, data[mappingElement.getFromPosition()], mappingElement.getToPosition());
        }
    }

    private void populateStreamEvent(ComplexEvent complexEvent, Object data, int[] toPosition) {
        StreamEvent streamEvent;
        if (complexEvent instanceof StreamEvent) {
            streamEvent = (StreamEvent) complexEvent;
        } else {
            streamEvent = ((StateEvent) complexEvent).getStreamEvent(toPosition[STREAM_EVENT_CHAIN_INDEX]);
            for (int i = 0; i <= toPosition[STREAM_EVENT_INDEX]; i++) {
                streamEvent = streamEvent.getNext();
            }
        }
        switch (toPosition[STREAM_ATTRIBUTE_TYPE_INDEX]) {
            case 0:
                streamEvent.setBeforeWindowData(data, toPosition[STREAM_ATTRIBUTE_INDEX]);
                break;
            case 1:
                streamEvent.setOnAfterWindowData(data, toPosition[STREAM_ATTRIBUTE_INDEX]);
                break;
            case 2:
                complexEvent.setOutputData(data, toPosition[STREAM_ATTRIBUTE_INDEX]);
                break;
            default:
                //will not happen
                throw new IllegalStateException("To Position cannot be :" + toPosition);
        }
    }

}
