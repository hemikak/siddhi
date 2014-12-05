/*
 * Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 * 
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.siddhi.extension.objectdetection;

import nu.pattern.OpenCV;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;
import org.opencv.core.Mat;
import org.opencv.core.MatOfByte;
import org.opencv.core.MatOfRect;
import org.opencv.highgui.Highgui;
import org.opencv.imgproc.Imgproc;
import org.opencv.objdetect.CascadeClassifier;
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.Attribute.Type;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;

/**
 * The Class for siddhi extension to count detected objects.
 */
@SiddhiExtension(namespace = "imageprocessorobjectdetection", function = "count")
public class ObjectDetectionExtension extends FunctionExecutor {

	/** The logger. */
	Logger log = Logger.getLogger(ObjectDetectionExtension.class);

	/** The return type for the extension. */
	Attribute.Type returnType;

	// loading native libraries for opencv
	static {
		OpenCV.loadLibrary();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.wso2.siddhi.core.extension.EternalReferencedHolder#destroy()
	 */
	public void destroy() {
	}

	/**
	 * Detect objects using OpenCV.
	 *
	 * @param imageHex
	 *            the image hex string
	 * @param cascadePath
	 *            the cascade path
	 * @return the detected object count
	 */
	private long detectObjects(String imageHex, String cascadePath) {
		long objectCount = 0;
		try {
			// conversion to Mat
			byte[] imageByteArr = (byte[]) new Hex().decode(imageHex);
			Mat image = Highgui.imdecode(new MatOfByte(imageByteArr), Highgui.IMREAD_UNCHANGED);

			// initializing classifier
			CascadeClassifier cClassifier = new CascadeClassifier();
			cClassifier.load(cascadePath);

			// pre-processing
			Imgproc.cvtColor(image, image, Imgproc.COLOR_RGB2GRAY);
			Imgproc.equalizeHist(image, image);

			// detecting objects
			MatOfRect imageRect = new MatOfRect();
			cClassifier.detectMultiScale(image, imageRect);

			// image count
			objectCount = ((Integer) imageRect.toList().size()).longValue();
		} catch (DecoderException e) {
			e.printStackTrace();
		}

		return objectCount;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.wso2.siddhi.core.executor.expression.ExpressionExecutor#getReturnType
	 * ()
	 */
	public Type getReturnType() {
		return returnType;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.wso2.siddhi.core.executor.function.FunctionExecutor#init(org.wso2
	 * .siddhi.query.api.definition.Attribute.Type[],
	 * org.wso2.siddhi.core.config.SiddhiContext)
	 */
	@Override
	public void init(Type[] types, SiddhiContext arg1) {
		returnType = Attribute.Type.LONG;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.wso2.siddhi.core.executor.function.FunctionExecutor#process(java.
	 * lang.Object)
	 */
	@Override
	protected Object process(Object obj) {
		long detectedObjectCount = 0;
		if (obj instanceof Object[]) {
			Object[] arguments = (Object[]) obj;
			if (arguments.length == 2) {
				if (arguments[0] instanceof String && arguments[1] instanceof String) {
					String imageHex = (String) arguments[0];
					String cascadePath = (String) arguments[1];
					detectedObjectCount = this.detectObjects(imageHex, cascadePath);
				}
				else{
					throw new IllegalArgumentException(
							"2 String arguments of the hex string of the image and the cascade path is expected.");
				}
			}else{
				throw new IllegalArgumentException(
						"2 String arguments of the hex string of the image and the cascade path is expected.");
			}
		} else {
			throw new IllegalArgumentException(
					"2 String arguments of the hex string of the image and the cascade path is expected.");
		}

		return detectedObjectCount;
	}
}
