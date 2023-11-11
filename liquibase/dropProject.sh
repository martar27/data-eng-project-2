docker run --rm \
  --network data-eng-project_dwh \
  -v "$(pwd)"/liquibase/changelog:/liquibase/changelog \
  liquibase/liquibase:4.24-alpine \
  --defaults-file=/liquibase/changelog/liquibase.docker.properties \
  dropAll
