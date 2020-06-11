# Omnitracking ETL

The Omnitracking analytical model process omnitracking data effortlessly, continuously and rapidly flowing into an easy-to-use analytical tool at event level and session-level that is available for all with the tight security permissions.

The Omnitracking ETL is a sub project of the Omnitracking product that performs all the major ETL transformations as a spark application.
This is second part in the entire project.

The other steps in the process are:
-   [ODS](https://gitlab.fftech.info/BusinessIntelligence/farfetchbi-omnitracking-ods)
-   [BQ](https://gitlab.fftech.info/BusinessIntelligence/farfetchbi-omnitracking-bq)


For more information about this project check the [documentation page](https://farfetch.atlassian.net/wiki/spaces/DATA/pages/305139339/Aware+-+Initiatives+-+Omnitracking+Data+Model).

## Getting started

### Prerequisites

-   Python
-   [Airflow](https://airflow.apache.org/)
-   IntelliJ Idea + SBT
-   Scala 2.11.8
-   Spark 2.3.0

### Installing

- Clone the repository
```bash
git clone git@gitlab.fftech.info:BusinessIntelligence/farfetchbi-omnitracking-etl.git
```
- Install your Airflow personal space follow these [steps](https://farfetch.atlassian.net/wiki/spaces/DATA/pages/51264478/KB+-+Getting+started+with+Airflow).

#### Details
-   Spark source code is located in /source folder
-   Package (.jar) generated in sources/.../target folder must be copied to /orchestration/airflow/resources folder
-   DAGs are located in /orchestration/airflow/dags folder
-   General deploy files are located in /Hadoop folder, separated by version
-   If you need to deploy anything in the cluster, please update the VERSION file

## Data Modeling

### Sources

The table sources of this process are the table produced by the previous Omnitracking step (ODS)
These tables are on the parquet file format in hdfs.

### Final Tables

The tables produced by this step are on the parquet file format in hdfs.

## Automatic/Regression Tests

N/A

## Deployment

All deploys are managed automatically by [Jenkings](https://jenkins.fftech.info/) with a new tag on the repository. ([list of tags](https://gitlab.fftech.info/BusinessIntelligence/farfetchbi-omnitracking-etl/tags))

-   Dev deploys:
    -   All DEV deploys need to have a tag ending with _-beta##_ (i.e. 1.02.00-beta01), allowing to do multiple deploys per version.
-   LIVE deploys:
    -   All LIVE deplys can't have the tag ending with _-beta##_ and so can only happen once per version.
    -   All LIVE deploys must originate from the master branch too.

## Built With

-   [Airflow](https://airflow.apache.org/) - Orchestration

## Versioning

We use [Semmantic Versioning(SemVer)](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://gitlab.fftech.info/BusinessIntelligence/farfetchbi-omnitracking-etl/tags).

Check the change log on this [page](https://gitlab.fftech.info/BusinessIntelligence/farfetchbi-omnitracking-etl/blob/master/CHANGELOG.md).

## Authors

-   **[AWARE Team](https://farfetch.atlassian.net/wiki/spaces/DATA/pages/121176598/BI+-+Team+-+Aware)**

See also the list of [contributors](https://gitlab.fftech.info/BusinessIntelligence/farfetchbi-omnitracking-etl/graphs/master) who participated in this project.

### Contacts

#### Slack Channels

- [ask-data](https://farfetch.slack.com/messages/C518HCANT/)
- [data-plataform_status](https://farfetch.slack.com/messages/C8WHMG65V/)

## Usefull links

- [Confluence Page](https://farfetch.atlassian.net/wiki/spaces/DATA/pages/305139339/Aware+-+Initiatives+-+Omnitracking+Data+Model)
- [Jenkings](https://jenkins.fftech.info/view/Continuous%20Integration/view/BI-Omnitracking/)
- [Octopus QA](https://octopus-qa.fftech.info/app#/projects/bi-omnitracking)
- [Ocpopus LIVE](https://octopus-live.farfetch.net/app#/projects/bi-omnitracking)

## Acknowledgments

-   Hat tip to anyone whose code was used
-   Inspiration
-   etc
