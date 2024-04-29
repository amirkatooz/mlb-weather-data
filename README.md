# mlb-weather-data

This application runs an ETL that:

1. Pulls future MLB games data
2. Pulls hourly weather data for the game locations
3. Adds weather data for the hour that a game starts to the MLB games data
4. Adds a few randomly generated columns to the MLB games data
5. Drops 3 versions of the MLB table into S3 (3 versions are .csv, .feather, and .parquet)
6. Writes the MLB games table to Postgres

### Access/Permission
In order to send the data to external endpoints (S3 and Postgres), a file named ".env" should be included in the folder (will not be included in the repo since hidden files are ignored by git). The content should look like the contents of "env_example.txt" file with the correct credentials.

## Documention/Write-Up Questions

### Automation
Using a scheduling tool such as cron, we can set up a job that runs on a schedule. Typically this is done in AWS using Lambda and CloudWatch services. Lambda is used to write/upload the code that needs to be run automatically, and on CloudWatch we can define rules with a cron schedule expression.

### Alerts/Notifications
I would add a snippet of code to the ETL that runs after writing the data to S3 and Postgres. This snippet pulls the data from the destination sources (S3 and Postgres) and compares it with the transformed dataframe. This comparison can happen at row level since the dataset is small. For larger datasets (which is typically the case), adding an “updated_at” field and comparing that with the current date can help us make sure the data is updated successfully. In this case we should also compare some aggregate metrics between the transformed dataframe and the tables pulled from destination sources (eg. number of rows, average/sum of numeric columns, etc.)

### Ennhancements
1. Faster:
Data pull from 3 different APIs can happen at the same time. We ca turn this one ETL job into 3 ETLs and create a DAG that runs the 3 ETLs parallel and then combines them. This requires storing the lat/lng data for venues so the weather ETL does not depend on the MLB games ETL.
We can also use PySpark which is designed for distributed computing.

2. More secure:
 * First we need to make sure we’re not hard-coding secrets and we’re using AWS Secrets Manager or something similar to store any credentials and only grant the Lambda function to use the secrets.
 * Sensitive data should be encrypted both for S3 and Postgres destinations. For S3 we can use AWS Key Management Service and for Postgres it should be done through connection configurations.
 * For access we can configure S3 bucket policies to restrict access based on IP addresses, IAM roles, or VPC endpoints. Also use security groups and network ACLs to control inbound and outbound traffic to the Postgres database.

3. More useful:
Add data dictionary that has descriptions for the table and for every field.
Add updated_at field so the user knows when the data was refreshed.

4. Easy to diagnose errors/More fail safe:
All functions in the ETL file need unit testing to ensure their behavior.
Add moore "try-except" commands along with more clear error logging.