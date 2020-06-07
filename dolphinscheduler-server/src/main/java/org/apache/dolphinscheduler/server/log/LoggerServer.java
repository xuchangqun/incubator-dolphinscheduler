/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.server.log;

import org.apache.dolphinscheduler.remote.NettyRemotingServer;
import org.apache.dolphinscheduler.remote.command.CommandType;
import org.apache.dolphinscheduler.remote.config.NettyServerConfig;
import org.apache.dolphinscheduler.server.worker.config.WorkerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.ComponentScan;

import javax.annotation.PostConstruct;

/**
 *  logger server
 */
@ComponentScan("org.apache.dolphinscheduler")
public class LoggerServer {

    private static  final Logger logger = LoggerFactory.getLogger(LoggerServer.class);

    /**
     *  netty server
     */
    private NettyRemotingServer server;

    @Autowired
    private WorkerConfig workerConfig;

    /**
     * main launches the server from the command line.
     * @param args arguments
     */
    public static void main(String[] args)  {
        new SpringApplicationBuilder(LoggerServer.class).web(WebApplicationType.NONE).run(args);
    }

    @PostConstruct
    public void run(){
        logger.info("logger server started, listening on port : {}" , workerConfig.getLoggerPort());

        //init remoting server
        NettyServerConfig serverConfig = new NettyServerConfig();
        serverConfig.setListenPort(workerConfig.getLoggerPort());
        this.server = new NettyRemotingServer(serverConfig);
        LoggerRequestProcessor requestProcessor = new LoggerRequestProcessor();

        this.server.registerProcessor(CommandType.GET_LOG_BYTES_REQUEST, requestProcessor, requestProcessor.getExecutor());
        this.server.registerProcessor(CommandType.ROLL_VIEW_LOG_REQUEST, requestProcessor, requestProcessor.getExecutor());
        this.server.registerProcessor(CommandType.VIEW_WHOLE_LOG_REQUEST, requestProcessor, requestProcessor.getExecutor());

        this.server.start();

        /**
         * register hooks, which are called before the process exits
         */
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                stop();
            }
        }));
    }

    /**
     * stop
     */
    public void stop() {
        this.server.close();
        logger.info("logger server shut down");
    }

}
