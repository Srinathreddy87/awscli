import datetime
import logging
import boto3
import yaml
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

# Set up a logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

class TableCloner:
    def __init__(self, jira_story, databricks_path):
        self.jira_story = jira_story
        self.config_file = f"sparta/story-plan-{jira_story}.yml"
        self.s3_bucket = databricks_path
        self.config = self.load_config_from_s3()

        # Initialize Spark session
        self.spark = SparkSession.builder \
            .appName("ReadYAMLFromS3") \
            .getOrCreate()

    def load_config_from_s3(self):
        """
        Loads the YAML configuration file from S3.
        """
        s3_client = boto3.client("s3")
        try:
            yaml_df = self.spark.read.text(f"{self.s3_bucket}{self.config_file}")
            config_content = "\n".join([row.value for row in yaml_df.collect()])
            return yaml.safe_load(config_content)
        except s3_client.exceptions.NoSuchKey:
            logger.error(f"Error retrieving S3 file: {self.config_file}")
            return None
        except yaml.YAMLError as exc:
            logger.error(f"Error parsing YAML file: {exc}")
            return None
        except Exception as e:
            logger.error(f"Error retrieving file from S3: {e}")
            return None

    def clone_table(self, table, options):
        """
        Clones a source table to a target table using the specified method.
        """
        table_name = table["table"]
        copy_method = next((opt["value"] for opt in options if opt["key"] == "copy_method"), None)
        source_table = f"{self.jira_story}.{table_name}"
        target_table = f"{self.jira_story}_{table_name}"

        try:
            if copy_method == "Deep clone":
                clone_command = f"CREATE OR REPLACE TABLE {target_table} DEEP CLONE {source_table}"
                logger.info(f"Clone command for Deep Clone is: {clone_command}")
            elif copy_method == "select_into":
                filter_condition = next((opt["value"] for opt in options if opt["key"] == "filter_condition"), "1=1")
                clone_command = f"CREATE OR REPLACE TABLE {target_table} AS SELECT * FROM {source_table} WHERE {filter_condition}"
                logger.info(f"Clone command for Select Into is: {clone_command}")
            else:
                raise ValueError(f"Unsupported copy method: {copy_method}")

            self.spark.sql(clone_command)
            logger.info(f"Table {source_table} cloned to {target_table} using {copy_method}")
        except Exception as e:
            logger.error(f"Error cloning table {table_name}: {e}")

    def clone_tables(self):
        """
        Clones all tables listed in the YAML configuration.
        """
        if not self.config:
            return

        for table in self.config.get("tables", []):
            options = table["options"]
            self.clone_table(table, options)

if __name__ == "__main__":
    jira_story = "sparta-4"
    s3_bucket = "s3://hs-bdes-dev-landzone/"
    cloner = TableCloner(jira_story, s3_bucket)
    cloner.clone_tables()
