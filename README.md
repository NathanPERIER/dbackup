
# DBackup

This is a simple script designed to backup databases.

## Use with Docker (recommended)

### Build

```bash
docker build . -t dbackup
```

### Run

#### Command-line

```bash
docker run --rm \
    -v './dbackup.yaml:/etc/dbackup/dbackup.yaml:ro' \
    -v './output:/output' \
    -v '/path/to/postgresql_socket:/sockets/postgres/sql.sock' \
    -v '/path/to/mariadb_socket:/sockets/maria/sql.sock' \
    dbackup
```

#### Docker compose

```yaml
version: '3.9'
services:
  dbackup:
    image: dbackup:latest
    container_name: dbackup
    volumes:
      - /path/to/dbackup.yaml:/etc/dbackup/dbackup.yaml:ro
      - /path/to/output:/output
      - /path/to/postgresql_socket:/sockets/postgres/sql.sock
      - /path/to/mariadb_socket:/sockets/maria/sql.sock
```

> [!WARNING]
> If the script in the container is executed by a non-root user, make sure that the directory mapped to `/output` is already created and writable for this user.


## Configuration

```yaml
backup1:
    type: postgresql
    socket: /sockets/postgres/sql.sock
    user: backup
    password: pass1

backup2:
    type: maria
    socket: /sockets/maria/sql.sock
    user: backup
    password: pass2
```

> [!NOTE]
> Only UNIX socket connection is currently supported.
