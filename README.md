# ReplaceRedshiftDataLambda

This repository contains the code used by the [ReplaceRedshiftData](https://us-east-1.console.aws.amazon.com/lambda/home?region=us-east-1#/functions/ReplaceRedshiftData-qa?newFunction=true&tab=code) AWS lambda function. It copies data into a table while ensuring some key remains unique. For now, that key is hardcoded to be the patron_id, but it would be easy to generalize if necessary. Given a staging table and a main table, this function does the following:
1) Deletes all rows in the main table that match rows in the staging table for a specified column
2) Copies over all the rows from the staging table into the main table
3) Deletes all rows in the staging table

## Git workflow
This repo uses the [Main-QA-Production](https://github.com/NYPL/engineering-general/blob/main/standards/git-workflow.md#main-qa-production) git workflow.

`main` has the latest and greatest commits, `qa` has what's in our QA environment, and `production` has what's in our production environment.

## Deployment
CI/CD is not enabled. To deploy a new version of this function, first modify the code in the git repo and open a pull request to the appropriate environment branch. Then run `source deployment_script.sh` and upload the resulting zip. Note that if any files are added or deleted, this script must be modified. For more information, see the directions [here](https://docs.aws.amazon.com/lambda/latest/dg/python-package.html).
