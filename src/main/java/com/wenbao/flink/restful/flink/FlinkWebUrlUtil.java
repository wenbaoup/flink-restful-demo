package com.wenbao.flink.restful.flink;

import com.wenbao.flink.restful.yarn.YarnApplicationReportUtil;
import org.apache.hadoop.yarn.api.records.ApplicationReport;

public class FlinkWebUrlUtil {
    /**
     * Flink restfulAPI查询 取消使用  不能upload jar包 不能run任务
     *
     * @param applicationName yarn任务名称
     * @return yarn代理的Flink web 地址
     * @throws Exception
     */
    public static String getProxyFlinkUrl(String applicationName) throws Exception {
        ApplicationReport report = YarnApplicationReportUtil.getYarnApplicationReport("yarn-site.xml", applicationName);
        return report.getTrackingUrl();
    }

    /**
     * Flink restfulAPI查询 通用
     *
     * @param applicationName yarn任务名称
     * @return 真实的Flink web 地址
     * @throws Exception
     */
    public static String getRealFlinkUrl(String applicationName) throws Exception {
        ApplicationReport report = YarnApplicationReportUtil.getYarnApplicationReport("yarn-site.xml", applicationName);
        return report.getOriginalTrackingUrl();
    }
}
