SELECT
    TIMESTAMP_SECONDS(`author`.`time_sec`) AS timestamp,
    `author`.`email` AS author_id,
    `author`.`name` AS author_name,
    REGEXP_EXTRACT(`author`.`email`, r'@(.+\..+)$') AS author_domain,
    LENGTH(subject) AS subject_length,
    LENGTH(message) AS message_length
FROM `bigquery-public-data.github_repos.commits`
WHERE TIMESTAMP_SECONDS(`author`.`time_sec`) < CURRENT_TIMESTAMP()