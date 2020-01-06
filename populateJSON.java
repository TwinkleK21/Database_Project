/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 *
 * @author khush
 */
public class Populate1 {

    static final ObjectMapper mapper = new ObjectMapper();
    static final String url = "jdbc:oracle:thin:@localhost:1521/xe";
    static final String user = "system";
    static final String pass = "oracle";
    static final String INSERT_TEMPLATE = "INSERT INTO %s VALUES(%S);";
    static final ArrayList<String> categoryList = new ArrayList<>(Arrays.asList("Active Life", "Arts & Entertainment", "Automotive", "Car Rental",
            "Cafes", "Beauty & Spas", "Convenience Stores", "Dentists", "Doctors", "Drugstores", "Department Stores", "Education", "Event Planning & Services",
            "Flowers & Gifts", "Food", "Health & Medical", "Home Services", "Home & Garden", "Hospitals", "Hotels & Travel", "Hardware Stores",
            "Grocery", "Medical Centers", "Nurseries & Gardening", "Nightlife", "Restaurants", "Shopping", "Transportation"));

//CTREATE CONNECTION OBJECT     
    public static Connection getConnectionObj() {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            return DriverManager.getConnection(url, user, pass);
        } catch (ClassNotFoundException | SQLException ex) {
            System.out.println("Error loading driver: " + ex);
        }
        return null;
    }

    //READING JSON FILE
    public static List<JsonNode> readJsonFile(String filepath) throws IOException {
        List<JsonNode> nodes = new ArrayList<JsonNode>();
        BufferedReader reader;
        reader = new BufferedReader(new FileReader(filepath));
        String line = reader.readLine();
        while (line != null) {
            JsonNode actualObj = mapper.readTree(line);
            nodes.add(actualObj);
            line = reader.readLine();
        }
        reader.close();
        return nodes;
    }

    //Main Class
    public static void main(String[] args) throws IOException, SQLException {

        Connection oracleConn = getConnectionObj();
        //Inserting into business table

        String query_business = "INSERT INTO yelp_business (business_id, name, city, state, star)" + "VALUES (?,?,?,?,?)";
        PreparedStatement s = oracleConn.prepareStatement(query_business);
        List<JsonNode> nodes = readJsonFile(args[0]);
        Statement deleteStatement = oracleConn.createStatement();
        deleteStatement.executeQuery("DELETE FROM yelp_business WHERE 1 = 1");
        for (JsonNode json : nodes) {
            s.setString(1, json.get("business_id").toString());
            s.setString(2, json.get("name").toString());
            s.setString(3, json.get("city").toString());
            s.setString(4, json.get("state").toString());
            s.setString(5, json.get("stars").toString());
            s.execute();
        }

        // Inserting into hours
        String query_hour = "INSERT INTO hours (business_id, day, open, close)" + "VALUES (?,?,?,?)";
        PreparedStatement s1 = oracleConn.prepareStatement(query_hour);
        for (JsonNode json : nodes) {
            final JsonNode objNode1 = json.get("hours");
            Iterator<Map.Entry<String, JsonNode>> iter = objNode1.fields();
            while (iter.hasNext()) {
                Map.Entry<String, JsonNode> entry = iter.next();
                Iterator<Map.Entry<String, JsonNode>> nestedIt = entry.getValue().fields();
                String open = "";
                String close = "";
                while (nestedIt.hasNext()) {
                    Map.Entry<String, JsonNode> nestedEntry = nestedIt.next();
                    if (nestedEntry.getKey().equals("close")) {
                        close = nestedEntry.getValue().asText();
                    } else {
                        open = nestedEntry.getValue().asText();
                    }
                }

                s1.setString(1, json.get("business_id").toString());
                s1.setString(2, entry.getKey().toString());
                s1.setString(3, open);
                s1.setString(4, close);
                s1.execute();
            }
        }

        //Inserting into categories, sub_categories
        String query_category = "INSERT INTO categories (business_id, main_category)" + "VALUES (?,?)";
        String query_subcategory = "INSERT INTO sub_categories (business_id, main_category, sub_category)" + "VALUES (?,?,?)";
        PreparedStatement s2 = oracleConn.prepareStatement(query_category);
        PreparedStatement s3 = oracleConn.prepareStatement(query_subcategory);
        for (JsonNode json : nodes) {
            final JsonNode arrNode = json.get("categories");
            if (arrNode.isArray()) {
                String mainCategory = "";
                String subCategory = "";
                for (final JsonNode textNode : arrNode) {
                    if (categoryList.contains(textNode.asText())) {
                        mainCategory = textNode.asText();
                        s2.setString(1, json.get("business_id").toString());
                        s2.setString(2, mainCategory);
                        s2.execute();
                    }

                }
                for (final JsonNode textNode : arrNode) {
                    if (!categoryList.contains(textNode.asText()) && !mainCategory.equals("")) {
                        subCategory = textNode.asText();
                        s3.setString(1, json.get("business_id").toString());
                        s3.setString(2, mainCategory);
                        s3.setString(3, subCategory);
                        s3.execute();
                    }
                }
            }
        }

        //Inserting into attributes
        String query_attribute = "INSERT INTO attributes (business_id, attribute)" + "VALUES (?,?)";
        PreparedStatement s4 = oracleConn.prepareStatement(query_attribute);
        deleteStatement.executeQuery("DELETE FROM attributes WHERE 1 = 1");
        for (JsonNode json : nodes) {
            final JsonNode attrNode = json.get("attributes");
            Iterator<Map.Entry<String, JsonNode>> attributeIter = attrNode.fields();
            while (attributeIter.hasNext()) {
                Map.Entry<String, JsonNode> attrEntry = attributeIter.next();
                String attrValue = attrEntry.getKey();
                if (!attrEntry.getValue().equals("true")) {
                    if (attrEntry.getValue().getNodeType() == JsonNodeType.OBJECT) {
                        Iterator<Map.Entry<String, JsonNode>> nestedAttributeIter = attrEntry.getValue().fields();
                        while (nestedAttributeIter.hasNext()) {
                            Map.Entry<String, JsonNode> nestedAttrEntry = nestedAttributeIter.next();
                            if (!nestedAttrEntry.getValue().equals("true")) {
                                String attribute = attrValue + "_" + nestedAttrEntry.getKey();
                                attribute = attribute.replace("\"", "").replaceAll(" ", "_");
                                s4.setString(1, json.get("business_id").toString());
                                s4.setString(2, attribute);
                                s4.execute();
                            } else {
                                String attribute = attrValue + "_" + nestedAttrEntry.getKey() + "_" + nestedAttrEntry.getValue();
                                attribute = attribute.replace("\"", "").replaceAll(" ", "_");
                                s4.setString(1, json.get("business_id").toString());
                                s4.setString(2, attribute);
                                s4.execute();
                            }
                        }
                    } else {
                        String attribute = attrValue + "_" + attrEntry.getValue();
                        attribute = attribute.replace("\"", "").replaceAll(" ", "_");
                        s4.setString(1, json.get("business_id").toString());
                        s4.setString(2, attribute);
                        s4.execute();
                    }
                } else {
                    String attribute = attrValue.replaceAll(" ", "_");
                    attribute = attribute.replace("\"", "");
                    s4.setString(1, json.get("business_id").toString());
                    s4.setString(2, attribute);
                    s4.execute();
                }
            }
        }

        String query_checkin = "INSERT INTO yelp_checkins (business_id, hour, count, day)" + "VALUES (?,?,?,?)";
        PreparedStatement s_checkin = oracleConn.prepareStatement(query_checkin);
        nodes = readJsonFile(args[2]);
        deleteStatement.executeQuery("DELETE FROM yelp_checkins WHERE 1 = 1");
        for (JsonNode json : nodes) {
            final JsonNode objNode = json.get("checkin_info");
            Iterator<Map.Entry<String, JsonNode>> it = objNode.fields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> entry = it.next();
                String[] arrOfStr = entry.getKey().split("-");
                s_checkin.setString(1, json.get("business_id").toString());
                s_checkin.setString(2, arrOfStr[1]);
                s_checkin.setInt(3, entry.getValue().asInt());
                s_checkin.setInt(4, Integer.valueOf(arrOfStr[0]));
                s_checkin.execute();
            }
        }

        String query_users = "INSERT INTO yelp_user (user_id, name, average_stars)" + "VALUES (?,?,?)";
        PreparedStatement s_user = oracleConn.prepareStatement(query_users);
        nodes = readJsonFile(args[3]);
        deleteStatement.executeQuery("DELETE FROM yelp_user WHERE 1 = 1");
        for (JsonNode json : nodes) {
            s_user.setString(1, json.get("user_id").toString());
            s_user.setString(2, json.get("name").toString());
            s_user.setString(3, json.get("average_stars").toString());
            s_user.execute();
        }

        String query_review = "INSERT INTO yelp_review (review_id, user_id, business_id, review_date, stars, review_text, funny_votes, useful_votes, cool_votes)" + "VALUES (?,?,?,?,?,?,?,?,?)";
        PreparedStatement s_review = oracleConn.prepareStatement(query_review);
        nodes = readJsonFile(args[1]);
        deleteStatement.executeQuery("DELETE FROM yelp_review WHERE 1 = 1");
        for (JsonNode json : nodes) {

            s_review.setString(1, json.get("review_id").toString());
            s_review.setString(2, json.get("user_id").toString());
            s_review.setString(3, json.get("business_id").toString());
            s_review.setString(4, json.get("date").toString());
            s_review.setDouble(5, json.get("stars").asDouble());
            s_review.setString(6, json.get("text").toString());
            final JsonNode objNode = json.get("votes");
            Iterator<Map.Entry<String, JsonNode>> it = objNode.fields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> entry1 = it.next();
                s_review.setInt(7, entry1.getValue().asInt());
                Map.Entry<String, JsonNode> entry2 = it.next();
                s_review.setInt(8, entry2.getValue().asInt());
                Map.Entry<String, JsonNode> entry3 = it.next();
                s_review.setInt(9, entry3.getValue().asInt());
            }

            s_review.execute();
        }

        oracleConn.close();
    }
}
