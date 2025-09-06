-- Create Production Database
CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}
COMMENT = 'Production database deployed via CI/CD';

USE DATABASE {DATABASE_NAME};
