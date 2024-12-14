rm -rf ${PGDATA}/*
PGPASSWORD=replicator_password pg_basebackup -h primary -D ${PGDATA} -U replicator -R
