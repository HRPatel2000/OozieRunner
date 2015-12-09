/*
 * Copyright 2013 Klarna AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.oozierunner.core;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class OozieProperties {

    private static OozieProperties oozieProperties = null;
    private static Properties prop = null;

    protected OozieProperties() {
    }

    public static OozieProperties getInstance(String ooziePropertiesFile) {

        if (oozieProperties == null) {
            oozieProperties = new OozieProperties();
        }

        prop = new Properties();
        InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(ooziePropertiesFile);

        try {
            prop.load(stream);
            System.out.println("Oozie Properties Loaded...");
        } catch (IOException e) {
            e.printStackTrace();
        }

        return oozieProperties;
    }

    public String getProperty(String key) {

        System.out.println("Property : Key :-> " + key + " : Value :-> " + prop.getProperty(key));
        return prop.getProperty(key);
    }

    public void setProperty(String key, String value) {

        System.out.println("Adding Property : Key :-> " + key + " : Value :-> " + value);
        prop.setProperty(key, value);
    }
}

