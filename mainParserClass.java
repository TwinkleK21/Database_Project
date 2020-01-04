/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author twinkle
 */
package com.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class main_parserclass {
    static final ObjectMapper mapper = new ObjectMapper();

    public static void readJsonFile(String filepath) throws IOException {



        BufferedReader reader;
        reader = new BufferedReader(new FileReader(filepath));
        String line = reader.readLine();
        while (line != null) {
            JsonNode actualObj = mapper.readTree(line);
            System.out.println(actualObj.toString());
            //Start a JDBC connection and insert each json object as a row into database
            line = reader.readLine();
        }
        reader.close();
    }
    public static void main(String[] args) throws IOException {
        readJsonFile(args[0]);
        readJsonFile(args[1]);
        readJsonFile(args[2]);
        readJsonFile(args[3]);
    }
}

