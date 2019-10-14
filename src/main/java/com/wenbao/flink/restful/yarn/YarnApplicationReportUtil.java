/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wenbao.flink.restful.yarn;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class YarnApplicationReportUtil {


    public static ApplicationReport getYarnApplicationReport(String yarnConfDir, String applicationName) {
        assert applicationName != null;
        if (StringUtils.isNotBlank(yarnConfDir)) {

            try {
                YarnConfiguration yarnConf = YarnConfLoader.getYarnConf(yarnConfDir);
                YarnClient yarnClient = YarnClient.createYarnClient();
                yarnClient.init(yarnConf);
                yarnClient.start();

                Set<String> set = new HashSet<>();
                set.add("Apache Flink");
                EnumSet<YarnApplicationState> enumSet = EnumSet.noneOf(YarnApplicationState.class);
                enumSet.add(YarnApplicationState.RUNNING);
                List<ApplicationReport> reportList = yarnClient.getApplications(set, enumSet);
                int maxMemory = -1;
                int maxCores = -1;
                for (ApplicationReport report : reportList) {

                    if (!applicationName.startsWith(report.getName())) {
                        continue;
                    }

                    if (!report.getYarnApplicationState().equals(YarnApplicationState.RUNNING)) {
                        continue;
                    }
                    int thisMemory = report.getApplicationResourceUsageReport().getNeededResources().getMemory();
                    int thisCores = report.getApplicationResourceUsageReport().getNeededResources().getVirtualCores();
                    if (thisMemory > maxMemory || thisMemory == maxMemory && thisCores > maxCores) {
                        return report;
                    }
                }
                throw new RuntimeException("No flink session found on yarn cluster.");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        throw new UnsupportedOperationException("Haven't been developed yet!");
    }


}
