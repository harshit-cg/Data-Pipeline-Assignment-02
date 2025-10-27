create storage integration aws_integration_data_pipeline
type=external_stage
enabled=true
storage_provider=s3
storage_allowed_locations=('s3://harshit-data-pipeline-bucket/')
storage_aws_role_arn='arn:aws:iam::680498180231:role/lambda-s3-role-harshit';

desc storage integration aws_integration_data_pipeline;

use database SNOWFLAKE_LEARNING_DB;

create stage aws_stage_data_pipeline
url = 's3://harshit-data-pipeline-bucket/'
storage_integration = aws_integration_data_pipeline;

ls @aws_stage_data_pipeline;

CREATE OR REPLACE FILE FORMAT json_format TYPE = 'JSON';


SELECT $1
FROM @aws_stage_data_pipeline/fda_drug_event.json
(FILE_FORMAT => json_format)
LIMIT 5;


SELECT $1
FROM @aws_stage_data_pipeline/nyc_data.json
(FILE_FORMAT => json_format)
LIMIT 5;


CREATE OR REPLACE TABLE raw_fda (data VARIANT);
COPY INTO raw_fda
FROM @aws_stage_data_pipeline/fda_drug_event.json
FILE_FORMAT = (FORMAT_NAME = json_format);


SELECT
    data:"results"[0]::STRING AS serious_flag,
    f.value:"medicinalproduct"::STRING AS drug_name,
    f.value:"drugcharacterization"::STRING AS drug_characterization
FROM raw_fda,
LATERAL FLATTEN(input => data:"results"[0]) f;


CREATE OR REPLACE TABLE raw_nyc (data VARIANT);
COPY INTO raw_nyc
FROM @aws_stage_data_pipeline/nyc_data.json
FILE_FORMAT = (FORMAT_NAME = json_format);

CREATE OR REPLACE VIEW view_transformed_fda AS
SELECT
    r.value['serious']::STRING AS serious_flag,
    d.value['medicinalproduct']::STRING AS drug_name,
    d.value['drugcharacterization']::STRING AS drug_characterization
FROM raw_fda,
LATERAL FLATTEN(input => data:"results") r,
LATERAL FLATTEN(input => r.value['patient']['drug']) d;

CREATE OR REPLACE VIEW view_transformed_nyc AS
SELECT
    data:"borough"::STRING AS borough,
    data:"number_of_persons_injured"::INT AS persons_injured,
    data:"number_of_persons_killed"::INT AS persons_killed,
    data:"collision_id"::STRING AS collision_id
FROM raw_nyc;



SELECT * FROM view_transformed_fda LIMIT 10;
SELECT * FROM view_transformed_nyc LIMIT 10;
