package com.lab5.resteventhub.service;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;
import org.json.JSONArray;
import org.json.JSONObject;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class SendDataEventHubImpl implements SendDataService {

    private static final boolean USE_SSL = true;

    private static final String CACHE_HOSTNAME = "levonovyy.redis.cache.windows.net";
    private static final String CACHE_KEY = "0QcREIP3asbAKVD2viAp6v4VbMo6kUeeGtRGDsysncY=";
    private static final String MAP_NAME = "EventLog";
    private static final String FILE_NAME = "Events";

    public void sendAndLog(String url) throws IOException, EventHubException {
        JedisShardInfo info = new JedisShardInfo(CACHE_HOSTNAME, 6380, USE_SSL);
        info.setPassword(CACHE_KEY);
        Jedis jedis = new Jedis(info);

        final ConnectionStringBuilder connStr = new ConnectionStringBuilder()
                .setNamespaceName("eventlabs")//namespace
                .setEventHubName("lab4")//hub name
                /*Connection string–primary key*/
                .setSasKeyName("Endpoint=sb://eventlabs.servicebus.windows.net/;SharedAccessKeyName=DataPolicy;SharedAccessKey=fuAbIgs8kT8N62RRJGB/X+Nh2oYt5A6sV1xVMiZnxV4=;EntityPath=lab4")
                /*Primary key*/
                .setSasKey("fuAbIgs8kT8N62RRJGB/X+Nh2oYt5A6sV1xVMiZnxV4=");

        final Gson gson = new GsonBuilder().create();
        final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(4);
        final EventHubClient ehClient = EventHubClient.createSync(connStr.toString(), executorService);

        try {
            URL data = new URL(url);
            HttpURLConnection con = (HttpURLConnection) data.openConnection();
            int responseCode = con.getResponseCode();
            BufferedReader br = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuilder response = new StringBuilder();
            while ((inputLine = br.readLine()) != null) {
                response.append(inputLine);
            }
            br.close();


            JSONArray jsonArray = new JSONArray(response.toString());
            int startRaw = 1;
            int endRaw = 0;
            int limit = 100;

            jedis.hset(MAP_NAME, "File", "None");
            Map<String, String> redisData = jedis.hgetAll("MAPNAME");
            if (checkIfFileExist(jedis)) {
                showData(startRaw, endRaw, limit, jsonArray, jedis, redisData, gson, ehClient);
            }
            System.out.println(Instant.now() + ": Send Complete...");
            System.out.println("Press Enter to stop.");
            System.in.read();
        } finally {
            ehClient.closeSync();
            executorService.shutdown();
        }
    }

    public void showData(int startRaw, int endRaw, int limit,
                         JSONArray jsonArray, Jedis jedis, Map<String,
            String> map, Gson gson, EventHubClient ehClient) throws EventHubException {
        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject jsonObject = (JSONObject) jsonArray.get(i);
            System.out.println("Document: " + i);
            byte[] payloadBytes = gson.toJson(jsonObject).getBytes(Charset.defaultCharset());
            EventData sendEvent = EventData.create(payloadBytes);

            ehClient.sendSync(sendEvent);
            if (i == limit) {
                endRaw = i;
                jedis.set("Raws", startRaw + ":" + endRaw);
                System.out.println(jedis.get("Raws"));
                jedis.hset(MAP_NAME, "Status", "NotFinished");
                limit = limit + 100;
            }
            if (i == 999) {
                jedis.hset(MAP_NAME, "Raws", "" + 1000);
                jedis.hset(MAP_NAME, "Status", "Completed");
                jedis.hset(MAP_NAME, "Info", "First attempt to input this file");
                System.out.println(map.get("Status"));
                jedis.close();

            }
            startRaw = endRaw + 1;
        }
    }

    public boolean checkIfFileExist(Jedis jedis) {
        Map<String, String> map = jedis.hgetAll(MAP_NAME);
        String fileName = map.get("File");
        String status = map.get("Status");

        if (!fileName.equals(FILE_NAME)) {
            jedis.hset(MAP_NAME, "File", FILE_NAME);
        } else {
            if (status.equals("Completed")) {
                jedis.hset(MAP_NAME, "Info", "Retry to input this file");
                System.out.println("Such file: " + "'" + FILE_NAME + "'" + " already exists. Application stop");
                jedis.close();
                return false;
            }
        }
        return true;
    }
}
