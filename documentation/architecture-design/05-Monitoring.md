# Monitoring

Effective monitoring is critical to ensure the smooth operation of Microsoft Fabric workloads. This section outlines the available tools and methods for monitoring activities, managing resources, and diagnosing issues across various layers and components of the Fabric ecosystem.

## Monitoring Hub

The [Monitoring Hub](https://learn.microsoft.com/en-us/fabric/admin/monitoring-hub) is the primary tool for tracking activities across Microsoft Fabric workloads, including:

- Semantic Model refreshes
- Spark Notebook executions
- Pipeline runs
- SQL query operations

### Features

- Filter activities using the drop-down filters in the upper right corner.
- Select an activity to view detailed information about its execution.
- Provides an overview of job run times, aiding in debugging and optimization of notebooks, pipelines, and SQL queries.

### API Access

The [List Item Job Instances](https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/list-item-job-instances) API enables programmatic access to job instance details for specific items, such as notebooks or pipelines. This allows for deeper integrations and automated monitoring solutions.

## Tenant Monitoring

The Fabric [Admin Monitoring Workspace](https://learn.microsoft.com/en-us/fabric/admin/monitoring-workspace) provides a centralized location for monitoring and managing workloads, usage, and governance within a tenant.

### Features

- Security audits
- Performance monitoring
- Governance insights

### Custom Solutions

Admins can create tailored solutions to extract tenant settings, item metadata, and activity/audit events for advanced monitoring and reporting requirements.

## Capacity Monitoring

The [Capacity Metrics App](https://learn.microsoft.com/en-us/fabric/enterprise/metrics-app) offers detailed insights into capacity usage across all Fabric workloads. Key capabilities include:

- Tenant-wide visibility into capacity usage.
- Resource usage trends across artifacts and operations.
- Monitoring throttling impact to optimize scaling decisions.
- Detailed analysis of workload operations down to 30-second granularity.
- Real-time tracking of in-progress operations.
- Assessing the impact of long-running jobs on capacity limits.

### Recommendations

- Configure [Capacity Notifications](https://learn.microsoft.com/en-us/fabric/admin/service-admin-premium-capacity-notifications) to receive alerts about capacity issues.

## Workspace Monitoring

[Workspace Monitoring](https://learn.microsoft.com/en-us/fabric/get-started/workspace-monitoring-overview) is a built-in solution for troubleshooting and diagnostics at the workspace level. It leverages the Eventhouse (KQL database) for efficient and self-service insights.

### Features

- Root-cause analysis, historical log analysis, and anomaly detection.
- Simplifies access to diagnostic logs and metrics.
- Correlates events across services for comprehensive analysis.

### Workspace-Level Enablement

- Send diagnostics to an Eventhouse for analysis.
- Access pre-built dashboards and Querysets for quick insights.
- Utilize raw data for custom dashboards, alerts, and insights.

### Current Scope

While the Workspace Monitoring feature is expanding, current capabilities include:

- Semantic Model queries and refreshes
- Eventhouse queries and metrics
- GraphQL operations

## Custom Monitoring

AquaVilla does not currently provide custom logging or monitoring solutions. However, if significant demand arises, custom monitoring capabilities may be introduced in future updates.
