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
package org.wso2.siddhi.core.query.selector.attribute.processor.executor;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.selector.attribute.aggergator.AttributeAggregator;
import org.wso2.siddhi.query.api.definition.Attribute;

public abstract class AbstractAggregationAttributeExecutor implements ExpressionExecutor {
    protected AttributeAggregator attributeAggregator;
    protected ExpressionExecutor[] attributeExpressionExecutors;
    protected ExecutionPlanContext executionPlanContext;
    protected int size;

    public AbstractAggregationAttributeExecutor(AttributeAggregator attributeAggregator,
                                                ExpressionExecutor[] attributeExpressionExecutors,
                                                ExecutionPlanContext executionPlanContext) {
        this.executionPlanContext = executionPlanContext;
        this.attributeExpressionExecutors = attributeExpressionExecutors;
        this.attributeAggregator = attributeAggregator;
        this.size = attributeExpressionExecutors.length;
    }

    @Override
    public Attribute.Type getReturnType() {
        return attributeAggregator.getReturnType();
    }

}

