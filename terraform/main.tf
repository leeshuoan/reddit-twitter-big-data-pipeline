provider "aws" {
  region = "us-east-1"
}

########################
### LAMBDA FUNCTIONS ###
########################
data "archive_file" "twitter_zip" {
  type        = "zip"
  source_file = "../twitter/lambda_function.py"
  output_path = "../twitter/lambda_function.zip"
}

data "archive_file" "reddit_zip" {
  type        = "zip"
  source_file = "../reddit/lambda_function_initial.py"
  output_path = "../reddit/lambda_function_initial.zip"
}

data "archive_file" "reddit_aggregate_zip" {
  type        = "zip"
  source_file = "../reddit/lambda_function_aggregate.py"
  output_path = "../reddit/lambda_function_aggregate.zip"
}


########################
### S3 Buckets ###
########################
// Data Bucket
resource "aws_s3_bucket" "tf-is459-project" {
  bucket = var.data_bucket
}

// Glue Asset bucket
resource "aws_s3_bucket" "tf_glue_assets" {
  bucket = var.glue_assets_bucket
}

##################
### S3 Objects ###
##################
resource "aws_s3_object" "twitter_glue_script" {
  bucket = aws_s3_bucket.tf_glue_assets.id
  key    = "scripts/twitter_glue.py"
  source = "../glue/twitter_glue.py"
  etag   = filemd5("../glue/twitter_glue.py")
}

resource "aws_s3_object" "reddit_glue_script" {
  bucket = aws_s3_bucket.tf_glue_assets.id
  key    = "scripts/reddit_glue.py"
  source = "../glue/reddit_glue.py"
  etag   = filemd5("../glue/reddit_glue.py")
}

resource "aws_s3_object" "topic_object" {
  bucket = aws_s3_bucket.tf-is459-project.id
  key    = "topics.txt"
  source = "topics.txt"
  etag   = filemd5("topics.txt")
}



####################
### Scraper Role ###
####################
data "aws_iam_policy_document" "scraper_policy" {
  statement {
    sid    = ""
    effect = "Allow"

    principals {
      identifiers = ["lambda.amazonaws.com"]
      type        = "Service"
    }
    actions = ["sts:AssumeRole"]
  }
}


resource "aws_iam_role" "scraper_role" {
  name               = "scraper_role"
  assume_role_policy = data.aws_iam_policy_document.scraper_policy.json
}


resource "aws_iam_role_policy_attachment" "scraper_role_lambda_basic_policy_attachment" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole" # Attach the basic Lambda execution role
  role       = aws_iam_role.scraper_role.name
}


resource "aws_iam_role_policy_attachment" "scraper_role_s3_full_access_policy_attachment" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess" # Attach the Amazon S3 full access policy
  role       = aws_iam_role.scraper_role.name
}

resource "aws_iam_role_policy_attachment" "scraper_role_glue_service_policy_attachment" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole" # Attach the AWS Glue service role policy
  role       = aws_iam_role.scraper_role.name
}


#####################
### Lambda Layers ###
#####################
resource "aws_lambda_layer_version" "twitter_scraper_layer" {
  layer_name = "twitter-scraper"
  compatible_runtimes = [
    "python3.7",
    "python3.8",
    "python3.9"
  ]
  filename    = "../twitter/layer.zip"
  description = "Layer that includes boto3 and snscrape"
}


resource "aws_lambda_layer_version" "reddit_scraper_layer" {
  layer_name = "reddit-scraper"
  compatible_runtimes = [
    "python3.7",
    "python3.8",
    "python3.9"
  ]
  filename    = "../reddit/layer.zip"
  description = "Layer that includes boto3 and praw"
}





########################
### Lambda Functions ###
########################
resource "aws_lambda_function" "twitter_scraper" {
  description      = "Twitter scraper that scrapes tweets based on topics.txt"
  runtime          = var.lambda_runtime
  handler          = "lambda_function.lambda_handler"
  function_name    = "twitter_scraper"
  filename         = data.archive_file.twitter_zip.output_path
  source_code_hash = data.archive_file.twitter_zip.output_base64sha256

  role    = aws_iam_role.scraper_role.arn
  layers  = [aws_lambda_layer_version.twitter_scraper_layer.arn]
  timeout = 300
}

resource "aws_lambda_function" "reddit_scraper" {
  description      = "Reddit scraper that scrapes tweets based on topics.txt"
  runtime          = var.lambda_runtime
  handler          = "lambda_function_initial.lambda_handler"
  function_name    = "reddit_scaper"
  filename         = data.archive_file.reddit_zip.output_path
  source_code_hash = data.archive_file.reddit_zip.output_base64sha256

  role    = aws_iam_role.scraper_role.arn
  layers  = [aws_lambda_layer_version.reddit_scraper_layer.arn]
  timeout = 300
  environment {
    variables = {
      REDDIT_CLIENT_ID     = var.REDDIT_CLIENT_ID
      REDDIT_CLIENT_SECRET = var.REDDIT_CLIENT_SECRET
      REDDIT_PASSWORD      = var.REDDIT_PASSWORD
      REDDIT_USERNAME      = var.REDDIT_USERNAME
      REDDIT_USER_AGENT    = var.REDDIT_USER_AGENT
    }
  }
}

resource "aws_lambda_function" "reddit_scraper_aggregate" {
  description      = "Aggregates the scraped reddit jsons"
  runtime          = var.lambda_runtime
  handler          = "lambda_function_aggregate.lambda_handler"
  function_name    = "reddit_scaper_aggregate"
  filename         = data.archive_file.reddit_aggregate_zip.output_path
  source_code_hash = data.archive_file.reddit_aggregate_zip.output_base64sha256

  role    = aws_iam_role.scraper_role.arn
  layers  = [aws_lambda_layer_version.reddit_scraper_layer.arn]
  timeout = 900
  environment {
    variables = {
      REDDIT_CLIENT_ID     = var.REDDIT_CLIENT_ID
      REDDIT_CLIENT_SECRET = var.REDDIT_CLIENT_SECRET
      REDDIT_PASSWORD      = var.REDDIT_PASSWORD
      REDDIT_USERNAME      = var.REDDIT_USERNAME
      REDDIT_USER_AGENT    = var.REDDIT_USER_AGENT
    }
  }
}


###########################
### Trigger Definitions ###
###########################
resource "aws_cloudwatch_event_rule" "scraper_trigger_15_minutes" {
  name        = "scraper_trigger_15_minutes"
  description = "Triggers every 15 minutes"

  schedule_expression = "rate(15 minutes)"
}

resource "aws_cloudwatch_event_rule" "scraper_trigger_daily_midnight" {
  name                = "scraper_trigger_daily_midnight"
  description         = "Triggers every midnight"
  schedule_expression = "cron(0 0 * * ? *)"
}

resource "aws_cloudwatch_event_target" "cloudwatch_lambda_target_twitter" {
  rule      = aws_cloudwatch_event_rule.scraper_trigger_15_minutes.name
  arn       = aws_lambda_function.twitter_scraper.arn
  target_id = "twitter_scraper"
}


resource "aws_lambda_permission" "cloudwatch_lambda_trigger_twitter" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.twitter_scraper.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.scraper_trigger_15_minutes.arn
}

resource "aws_cloudwatch_event_target" "cloudwatch_lambda_target_reddit" {
  rule      = aws_cloudwatch_event_rule.scraper_trigger_15_minutes.name
  arn       = aws_lambda_function.reddit_scraper.arn
  target_id = "reddit_scraper"
}


resource "aws_lambda_permission" "cloudwatch_lambda_trigger_reddit" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.reddit_scraper.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.scraper_trigger_15_minutes.arn
}

resource "aws_cloudwatch_event_target" "cloudwatch_lambda_target_reddit_aggregate" {
  rule      = aws_cloudwatch_event_rule.scraper_trigger_daily_midnight.name
  arn       = aws_lambda_function.reddit_scraper_aggregate.arn
  target_id = "reddit_scraper_aggregate"
}


resource "aws_lambda_permission" "cloudwatch_lambda_trigger_reddit_aggregate" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.reddit_scraper_aggregate.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.scraper_trigger_daily_midnight.arn
}

#################
### Glue Role ###
#################
resource "aws_iam_role" "glue_role" {
  name = "glue_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_role_lambda_basic_policy_attachment" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole" # Attach the basic Lambda execution role
  role       = aws_iam_role.glue_role.name
}

resource "aws_iam_role_policy_attachment" "glue_role_s3_full_access_policy_attachment" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess" # Attach the Amazon S3 full access policy
  role       = aws_iam_role.glue_role.name
}
resource "aws_iam_role_policy_attachment" "glue_role_glue_service_policy_attachment" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole" # Attach the AWS Glue service role policy
  role       = aws_iam_role.glue_role.name
}

####################
### Glue Catalog ###
####################
resource "aws_glue_catalog_database" "project_catalog_database" {
  name = "project-database"
}
##########################
### Crawler Classifier ###
##########################
resource "aws_glue_classifier" "json_array_classifier" {
  name = "json_array_classifier"
  json_classifier {
    json_path = "$[*]"
  }
}
####################
### Glue Crawler ###
####################
resource "aws_glue_crawler" "project_crawler" {
  name          = "project-crawler"
  schedule      = "cron(0 0 * * ? *)"
  role          = aws_iam_role.glue_role.arn
  database_name = aws_glue_catalog_database.project_catalog_database.name
  s3_target {
    path = "s3://${var.data_bucket}/project/"
  }
  classifiers = [aws_glue_classifier.json_array_classifier.name]
}

#####################
### Glue Job Role ###
#####################
resource "aws_iam_role" "glue_job_role" {
  name = "glue_job_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_job_role_s3_full_access" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
  role       = aws_iam_role.glue_job_role.name
}

resource "aws_iam_role_policy_attachment" "glue_job_role_glue_service" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
  role       = aws_iam_role.glue_job_role.name
}

resource "aws_iam_role_policy_attachment" "glue_job_role_comprehend_full_access" {
  policy_arn = "arn:aws:iam::aws:policy/ComprehendFullAccess"
  role       = aws_iam_role.glue_job_role.name
}



#################
### Glue Jobs ###
#################
resource "aws_glue_job" "twitter_glue" {
  name         = "tf-twitter-glue-job"
  timeout      = 2880
  role_arn     = aws_iam_role.glue_job_role.arn
  glue_version = "3.0"

  default_arguments = {
    "--additional-python-modules"        = "neo4j==5.6.0,deep-translator==1.10.1"
    "--job-language"                     = "python"
    "--NEO_URI"                          = var.NEO_URI
    "--NEO_USER"                         = var.NEO_USER
    "--NEO_PASSWORD"                     = var.NEO_PASSWORD
    "--CLAIMBUSTER_API_KEY"              = var.CLAIMBUSTER_API_KEY
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = ""
  }

  worker_type       = "G.1X"
  number_of_workers = 10
  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.tf_glue_assets.bucket}/${aws_s3_object.twitter_glue_script.key}"
  }

}

resource "aws_glue_job" "reddit_glue" {
  name         = "tf-reddit-glue-job"
  timeout      = 2880
  role_arn     = aws_iam_role.glue_job_role.arn
  glue_version = "3.0"

  default_arguments = {
    "--additional-python-modules"        = "neo4j==5.6.0,deep-translator==1.10.1"
    "--job-language"                     = "python"
    "--NEO_URI"                          = var.NEO_URI
    "--NEO_USER"                         = var.NEO_USER
    "--NEO_PASSWORD"                     = var.NEO_PASSWORD
    "--CLAIMBUSTER_API_KEY"              = var.CLAIMBUSTER_API_KEY
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = ""
  }

  worker_type       = "G.1X"
  number_of_workers = 10
  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.tf_glue_assets.bucket}/${aws_s3_object.reddit_glue_script.key}"
  }
}

#########################
### Glue Job Triggers ###
#########################
resource "aws_glue_trigger" "reddit_glue_trigger" {
  name              = "reddit_glue_trigger"
  type              = "SCHEDULED"
  start_on_creation = true
  schedule          = "cron(0 1 * * ? *)"
  actions {
    job_name = aws_glue_job.reddit_glue.name
  }
}


resource "aws_glue_trigger" "twitter_glue_trigger" {
  name              = "twitter_glue_trigger"
  type              = "SCHEDULED"
  schedule          = "cron(0 1 * * ? *)"
  start_on_creation = true

  actions {
    job_name = aws_glue_job.twitter_glue.name
  }
}
