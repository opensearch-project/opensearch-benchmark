---
layout: default
title: Create and manage pipelines
parent: Benchmark Use Cases
nav_order: 20
---

///// EXAMPLE TEMPLATE 

Pipelines allow you to perform X task. Enter some additional introductory information about pipelines here.

NOTE: This page is an example of how pipelines can be used in the context of OpenSearch Benchmark. It uses content from a different user guide as a template. Use the following example from the Observability plugin to form of the basis of an OpenSearch Benchmark User Guide.

# Notebooks

An OpenSearch Dashboards notebook is an interface that lets you easily combine code snippets, live visualizations, and narrative text in a single notebook interface.

Notebooks let you interactively explore data by running different visualizations that you can share with team members to collaborate on a project.

A notebook is a document composed of two elements: code blocks (Markdown/SQL/PPL) and visualizations. Choose multiple timelines to compare and contrast visualizations.

You can also generate [reports]({{site.url}}{{site.baseurl}}/dashboards/reporting/) directly from your notebooks.

Common use cases include creating postmortem reports, designing runbooks, building live infrastructure reports, and writing documentation.

Tenants in OpenSearch Dashboards are spaces for saving notebooks and other OpenSearch Dashboards objects. For more information, see [OpenSearch Dashboards multi-tenancy]({{site.url}}{{site.baseurl}}/security-plugin/access-control/multi-tenancy/).
{: .note }


## Get started with notebooks

To get started, choose **Notebooks** within OpenSearch Dashboards.


### Step 1: Create a notebook

A notebook is an interface for creating reports.

1. Choose **Create notebook** and enter a descriptive name.
1. Choose **Create**.

Choose **Actions** to rename, duplicate, or delete a notebook.

![Create notebook]({{site.url}}{{site.baseurl}}/images/create_notebook.gif)

### Step 2: Add a paragraph

Paragraphs combine code blocks and visualizations for describing data.

#### Add a code block

Code blocks support markdown, SQL, and PPL languages.

Specify the input language on the first line using `%[language type]` syntax.
For example, type `%md` for markdown, `%sql` for SQL, and `%ppl` for PPL.

##### Sample markdown block

```
%md
Add in text formatted in markdown.
```

![Markdown paragraph]({{site.url}}{{site.baseurl}}/images/markdown_notebooks.gif)

##### Sample SQL block

```sql
%sql
Select * from opensearch_dashboards_sample_data_flights limit 20;
```

![SQL paragraph]({{site.url}}{{site.baseurl}}/images/sql_notebooks.gif)

##### Sample PPL block

```
%ppl
source=opensearch_dashboards_sample_data_logs | head 20
```

![PPL paragraph]({{site.url}}{{site.baseurl}}/images/ppl_notebooks.gif)


#### Add a visualization

1. To add a visualization, choose **Add paragraph** and select **Visualization**.
1. In **Title**, select your visualization and choose a date range. You can choose multiple timelines to compare and contrast visualizations.
1. To run and save a paragraph, choose **Run**.

![Visualization paragraph]({{site.url}}{{site.baseurl}}/images/visualization_notebooks.gif)

## Paragraph actions

You can perform the following actions on paragraphs:

- Add a new paragraph to the top of a report.
- Add a new paragraph to the bottom of a report.
- Run all the paragraphs at the same time.
- Clear the outputs of all paragraphs.
- Delete all the paragraphs.

![Sample notebooks]({{site.url}}{{site.baseurl}}/images/paragraphs_notebooks.gif)

## Sample notebooks

We prepared the following sample notebooks that showcase a variety of use cases:

- Using SQL to query the OpenSearch Dashboards sample flight data.
- Using PPL to query the OpenSearch Dashboards sample web logs data.
- Using PPL and visualizations to perform sample root cause event analysis on the OpenSearch Dashboards sample web logs data.

To add a sample notebook, choose **Actions** and select **Add sample notebooks**.

![Sample notebooks]({{site.url}}{{site.baseurl}}/images/sample_notebooks.gif)

## Create a report

You can use notebooks to create PNG and PDF reports:

1. From the top menu bar, choose **Reporting actions**.
1. You can choose to **Download PDF** or **Download PNG**.

   Reports generate asynchronously in the background and might take a few minutes, depending on the size of the report. A notification appears when your report is ready to download.

1. To create a schedule-based report, choose **Create report definition**. For steps to create a report definition, see [Create reports using a definition]({{site.url}}{{site.baseurl}}/dashboards/reporting#create-reports-using-a-definition).
1. To see all your reports, choose **View all reports**.

![Report notebooks]({{site.url}}{{site.baseurl}}/images/report_notebooks.gif)
