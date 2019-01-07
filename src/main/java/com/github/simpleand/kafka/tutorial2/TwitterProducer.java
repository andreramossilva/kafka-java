package com.github.simpleand.kafka.tutorial2;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;


import com.google.common.collect.Lists;

public class TwitterProducer {

    private String consumerKey = "ZtQRylD37aBfPpyto28IxhcxL";
    private String consumerSecret = "jv5rUYUWur3rQ0hUUtNvhtPxlX3YyjHUUhAIZkk87N0zazNUSk";
    private String token = "284488001-kRwoShps9C9mEYQWb7Uk6IOFi7nAIoA2iqxp8Io5";
    private String secret = "sLBJ3HM9EB0E5KyZ1Gg96EVooso5ukA5alCeCk17GaDAA";

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run(){
        BlockingDeque<String> msgQueue = new LinkedBlockingDeque<>(1000);
         Client client = createTwitterClient(msgQueue);
         client.connect();
    }

    public Client createTwitterClient(BlockingDeque<String> msgQueue){
        Hosts hoseBirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hoseBirdEndpoint = new StatusesFilterEndpoint();

        List<String> terms = Lists.newArrayList("kafka");
        hoseBirdEndpoint.trackTerms(terms);

        Authentication hoseBirdAuth = new OAuth1(consumerKey,consumerSecret,token,secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hoseBirdHosts)
                .authentication(hoseBirdAuth)
                .endpoint(hoseBirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hoseBirdClient = builder.build();
        return hoseBirdClient;
    }
}
