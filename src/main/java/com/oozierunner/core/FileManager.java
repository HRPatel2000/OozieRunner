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

import org.apache.commons.io.FileUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class FileManager {

    public static boolean contentEquals(String actualDataFileName, String expectedDataFileName) {

        File actualDataFile = new File(actualDataFileName);
        File expectedDataFile = new File(expectedDataFileName);

        if (!actualDataFile.exists()) {

            System.out.println("ActualDataFile :-> " + actualDataFileName + " : doesn't Exist...");
            return false;

        } else if (!expectedDataFile.exists()) {

            System.out.println("ExpectedDataFile :-> " + expectedDataFileName + " : doesn't Exist...");
            return false;

        }

        boolean contentMatched = false;
        try {
            contentMatched = FileUtils.contentEquals(actualDataFile, expectedDataFile);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        return contentMatched;
    }


    public static BufferedWriter getFileWriter(String fileName) throws IOException {

        System.out.println("File to write in :-> " + fileName);

        File file = new File(fileName);

        // if file doesnt exists, then create it
        if (!file.exists()) {
            file.createNewFile();
        }

        FileWriter fileWriter = new FileWriter(file.getAbsoluteFile());
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

        return bufferedWriter;
    }

    public static void write(BufferedWriter bufferedWriter, boolean newline, String dataline) throws IOException {

        bufferedWriter.write(dataline);

        if(newline) {

            bufferedWriter.newLine();
        }
    }

    public static void close(BufferedWriter bufferedWriter) throws IOException {

        bufferedWriter.close();
    }
}

