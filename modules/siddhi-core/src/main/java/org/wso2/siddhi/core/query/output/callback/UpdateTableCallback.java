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

package org.wso2.siddhi.core.query.output.callback;

import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.util.finder.Finder;
import org.wso2.siddhi.core.table.EventTable;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.List;

public class UpdateTableCallback implements OutputCallback {
    private EventTable eventTable;
    private Finder finder;
    private int[] mappingPosition;

    public UpdateTableCallback(EventTable eventTable, Finder finder, AbstractDefinition updatingStreamDefinition) {
        this.eventTable = eventTable;
        this.finder = finder;
        validateUpdateTable(eventTable.getTableDefinition(), updatingStreamDefinition.getAttributeList());
    }

    private void validateUpdateTable(AbstractDefinition tableDefinition, List<Attribute> updatingStreamDefinition) {
        mappingPosition = new int[updatingStreamDefinition.size()];
        for (int i = 0; i < updatingStreamDefinition.size(); i++) {
            Attribute streamAttribute = updatingStreamDefinition.get(i);
            mappingPosition[i] = tableDefinition.getAttributePosition(streamAttribute.getName());
        }
    }


    @Override
    public void send(ComplexEventChunk<StreamEvent> complexEventChunk) {
        eventTable.update(complexEventChunk, finder, mappingPosition);
    }

}
