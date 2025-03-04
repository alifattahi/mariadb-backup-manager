# MariaDB Backup Manager

## Overview

This document provides comprehensive documentation for the MariaDB Backup Manager script, a powerful Bash utility designed to manage backups, restoration, and point-in-time recovery for MariaDB database servers.

**Version:** 1.1.0

## Features

- **Multiple Backup Types:**
  - Full backups
  - Incremental backups
  - Binary log backups

- **Advanced Recovery Options:**
  - Full backup restoration
  - Point-in-time recovery **(PITR)**
  - Binary log application

- **Security:**
  - Encryption support (AES-256)
  - Secure handling of credentials

- **Performance:**
  - Parallel processing
  - Compression support
  - I/O throttling

- **Automation:**
  - Cronjob setup
  - Configurable retention policies

- **Monitoring:**
  - Email notifications
  - Webhook alerts
  - Slack integration
  - Backup reports

- **Validation:**
  - Backup verification
  - Test recovery

## Prerequisites

Before using this script, ensure you have the following:

- MariaDB server installed
- `mariabackup` utility installed
- Proper database credentials with backup permissions
- Sufficient disk space for backups
- (Optional) Mail utility for email notifications

### Installing mariabackup

#### Debian/Ubuntu
```bash
sudo apt-get update
sudo apt-get install mariadb-backup
```

#### CentOS/RHEL
```bash
# For MariaDB from the official repositories
sudo yum install MariaDB-backup

# Or if using MariaDB from MariaDB repositories
sudo yum install mariadb-backup
```

#### Fedora
```bash
sudo dnf install mariadb-backup
```

#### SUSE/openSUSE
```bash
sudo zypper install mariadb-backup
```

#### Verify Installation
```bash
mariabackup --version
```

## Installation

1. Copy the script to a suitable location:
   ```bash
   cp mariadb-backup.sh /usr/local/bin/
   ```

2. Make it executable:
   ```bash
   chmod +x /usr/local/bin/mariadb-backup.sh
   ```

3. (Optional) Create a configuration file (see Configuration section)

## Basic Usage

### Performing a Full Backup

```bash
# Basic full backup
./mariadb-backup.sh --type full --mysql-user root --backup-dir /var/backup/mariadb

# Full backup with password
./mariadb-backup.sh --type full --mysql-user root --mysql-password "your_password" --backup-dir /var/backup/mariadb

# Full backup using MySQL defaults file
./mariadb-backup.sh --type full --defaults-file /root/.my.cnf --backup-dir /var/backup/mariadb
```

### Performing an Incremental Backup

```bash
# Basic incremental backup
./mariadb-backup.sh --type incremental --mysql-user root --backup-dir /var/backup/mariadb

# Incremental backup with password
./mariadb-backup.sh --type incremental --mysql-user root --mysql-password "your_password" --backup-dir /var/backup/mariadb

# Incremental backup using MySQL defaults file
./mariadb-backup.sh --type incremental --defaults-file /root/.my.cnf --backup-dir /var/backup/mariadb
```

### Backing Up Binary Logs

```bash
# Basic binary log backup
./mariadb-backup.sh --type binlog --mysql-user root --backup-dir /var/backup/mariadb

# Binary log backup with password
./mariadb-backup.sh --type binlog --mysql-user root --mysql-password "your_password" --backup-dir /var/backup/mariadb

# Binary log backup using MySQL defaults file
./mariadb-backup.sh --type binlog --defaults-file /root/.my.cnf --backup-dir /var/backup/mariadb
```

### Restoring a Backup

```bash
# Restore from a full backup
./mariadb-backup.sh --restore /var/backup/mariadb/full_20250304120000

# Restore from an incremental backup (automatically applies the full backup and all necessary incremental backups)
./mariadb-backup.sh --restore /var/backup/mariadb/incr_20250305120000

# Restore using MySQL defaults file
./mariadb-backup.sh --restore /var/backup/mariadb/full_20250304120000 --defaults-file /root/.my.cnf
```

### Performing Point-in-Time Recovery

```bash
# PITR from a full backup
./mariadb-backup.sh --restore /var/backup/mariadb/full_20250304120000 --pitr "2025-03-04 15:30:00"

# PITR from an incremental backup
./mariadb-backup.sh --restore /var/backup/mariadb/incr_20250305120000 --pitr "2025-03-05 15:30:00"
```

## Command Line Options

### Basic Options

| Option | Description | Default |
|--------|-------------|---------|
| `--backup-dir DIR` | Backup directory | `/var/backup/mariadb` |
| `--type TYPE` | Backup type: full, incremental, binlog | `full` |
| `--retention DAYS` | Overall retention period in days | `7` |
| `--retention-full N` | Number of full backups to keep | `4` |
| `--retention-incr N` | Number of incremental backups to keep | `14` |
| `--compress` | Compress backup using mbstream | No |
| `--compress-threads N` | Number of compression threads | `4` |
| `--encrypt` | Encrypt backup with AES-256 | No |
| `--encrypt-key-file FILE` | Path to encryption key file | |
| `--parallel N` | Number of parallel threads | `4` |
| `--dry-run` | Show what would be done without making changes | |
| `--temp-dir DIR` | Temporary directory for operations | `/tmp` |
| `--throttle-io N` | Limit I/O with ionice/nice settings (1-10) | |

### Connection Options

| Option | Description | Default |
|--------|-------------|---------|
| `--mysql-user USER` | MySQL user | `root` |
| `--mysql-password PASS` | MySQL password | empty |
| `--mysql-host HOST` | MySQL host | `127.0.0.1` |
| `--mysql-port PORT` | MySQL port | `3306` |
| `--mysql-datadir DIR` | MySQL data directory | `/var/lib/mysql` |
| `--defaults-file FILE` | Use MySQL defaults file instead of command line credentials | |

### Restore Options

| Option | Description |
|--------|-------------|
| `--restore PATH` | Restore from a full or incremental backup path |
| `--pitr DATETIME` | Point-in-time recovery to specified datetime (YYYY-MM-DD HH:MM:SS) |
| `--pitr-only DATETIME` | PITR without restoring backup sequences |
| `--test-recovery` | Test restore to validate backup (requires additional disk space) |
| `--force` | Force operations without confirmation |
| `--ignore-errors` | Continue even if non-critical errors occur |

### Notification Options

| Option | Description |
|--------|-------------|
| `--webhook URL` | Webhook URL for notifications |
| `--send-webhook` | Enable webhook notifications |
| `--slack-webhook URL` | Slack webhook URL for notifications |
| `--email EMAIL` | Email address for notifications |

### Configuration Options

| Option | Description | Default |
|--------|-------------|---------|
| `--config FILE` | Load settings from configuration file | |
| `--setup-cron` | Setup cronjob for regular backups | |
| `--log-file FILE` | Log file path | stdout |
| `--log-level LEVEL` | Log level: DEBUG, INFO, WARN, ERROR | `INFO` |

### Miscellaneous Options

| Option | Description |
|--------|-------------|
| `--version` | Display version information |
| `--help` | Display help message |

## Configuration File

Instead of specifying all options on the command line, you can create a configuration file:

```bash
# MariaDB Backup configuration
BACKUP_DIR="/var/backup/mariadb"
MYSQL_USER="backup"
MYSQL_PASSWORD="securepassword"
MYSQL_HOST="127.0.0.1"
MYSQL_PORT="3306"
MYSQL_DATADIR="/var/lib/mysql"
RETENTION_DAYS=7
RETENTION_FULL=4
RETENTION_INCR=14
COMPRESS=1
COMPRESS_THREADS=4
PARALLEL_THREADS=8
EMAIL_RECIPIENT="admin@example.com"
LOG_FILE="/var/log/mariadb-backup.log"
LOG_LEVEL="INFO"
```

Use the configuration file:

```bash
./mariadb-backup.sh --config /etc/mariadb-backup.conf --type full
```

### Using MySQL Defaults File

As an alternative to providing credentials on the command line or in the backup script's configuration, you can use a MySQL defaults file. Create a file `/root/.my.cnf` with the following content:

```
[client]
user=root
password=your_password
host=localhost
port=3306
```

Secure the file:

```bash
chmod 600 /root/.my.cnf
```

Then use it with the backup script:

```bash
./mariadb-backup.sh --defaults-file /root/.my.cnf --type full
```

This is more secure than passing the password on the command line, as the password won't appear in the process list or command history.

## Backup Types Explained

### Full Backup

A full backup contains a complete copy of all database data at a specific point in time. It serves as the foundation for incremental backups and is required for any restoration process.

### Incremental Backup

An incremental backup only contains the changes made since the last backup (either full or incremental). This significantly reduces backup size and time. However, restoration requires having both the full backup and all subsequent incremental backups in sequence.

### Binary Log Backup

Binary logs record all changes to the database. Backing up binary logs enables point-in-time recovery, allowing restoration to any moment between backups.

## Backup Verification

The script automatically verifies backups after creation. You can also use the `--test-recovery` option for more thorough verification, which tests the backup by performing a recovery simulation.

## Encryption

To use encryption:

1. Generate an encryption key:
   ```bash
   openssl rand -base64 24 > /root/backup_encryption.key
   chmod 400 /root/backup_encryption.key
   ```

2. Enable encryption in your backups:
   ```bash
   ./mariadb-backup.sh --type full --encrypt --encrypt-key-file /root/backup_encryption.key
   ```

## Automated Backups

Set up automated backups using the built-in cronjob setup:

```bash
./mariadb-backup.sh --setup-cron --config /etc/mariadb-backup.conf
```

This creates the following schedule:
- Weekly full backup (Sunday 1 AM)
- Daily incremental backup (weekdays 1 AM)
- Hourly binary log backup

## Backup Restoration Process

### Basic Restoration

Restoring a full backup:

```bash
# Basic restoration
./mariadb-backup.sh --restore /var/backup/mariadb/full_20250304120000

# Restoration with credentials
./mariadb-backup.sh --restore /var/backup/mariadb/full_20250304120000 --mysql-user root --mysql-password "your_password"

# Restoration using defaults file
./mariadb-backup.sh --restore /var/backup/mariadb/full_20250304120000 --defaults-file /root/.my.cnf
```

Restoring an incremental backup (this automatically applies all necessary incremental backups on top of the full backup):

```bash
# Basic incremental restoration
./mariadb-backup.sh --restore /var/backup/mariadb/incr_20250305120000

# Incremental restoration with credentials
./mariadb-backup.sh --restore /var/backup/mariadb/incr_20250305120000 --mysql-user root --mysql-password "your_password"

# Incremental restoration using defaults file
./mariadb-backup.sh --restore /var/backup/mariadb/incr_20250305120000 --defaults-file /root/.my.cnf

# Force restoration (without asking for confirmation)
./mariadb-backup.sh --restore /var/backup/mariadb/incr_20250305120000 --force
```

### Point-in-Time Recovery

Restore to a specific point in time:

```bash
./mariadb-backup.sh --restore /var/backup/mariadb/full_20250304120000 --pitr "2025-03-04 15:30:00"

# Incremental restoration with pitr
./mariadb-backup.sh --restore /var/backup/mariadb/incr_20250304130000 --pitr "2025-03-04 15:30:00"
```

Apply binary logs without restoring a backup:

```bash
./mariadb-backup.sh --pitr-only "2025-03-04 15:30:00"
```

## Notifications

The script can send notifications through multiple channels:

### Email Notifications

```bash
./mariadb-backup.sh --type full --email admin@example.com
```

### Webhook Notifications

```bash
./mariadb-backup.sh --type full --send-webhook --webhook https://example.com/webhook
```

### Slack Notifications

```bash
./mariadb-backup.sh --type full --slack-webhook https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX
```

## Performance Tuning

### Parallel Processing

```bash
./mariadb-backup.sh --type full --parallel 8
```

### Compression [unstable]

```bash
./mariadb-backup.sh --type full --compress --compress-threads 4
```

### I/O Throttling

```bash
./mariadb-backup.sh --type full --throttle-io 5
```

## Logs and Reporting

The script generates logs and can create backup reports:

- Logs can be directed to a file using `--log-file`
- Log verbosity can be adjusted with `--log-level`
- A backup report is automatically generated in the backup directory

## Best Practices

1. **Regular Testing**: Periodically test backup restoration to ensure reliability
2. **Diversify Storage**: Store backups on different physical servers or cloud storage
3. **Monitor Backup Jobs**: Set up alerts for backup failures
4. **Secure Encryption Keys**: Store encryption keys securely and separately from backups
5. **Maintain Backup Logs**: Keep backup logs for auditing and troubleshooting
6. **Adjust Retention**: Configure retention based on your recovery point objective (RPO)
7. **Check Disk Space**: Ensure sufficient disk space for backups and temporary operations

## Troubleshooting

### Common Issues

1. **Backup Fails with "Permission Denied"**
   - Ensure the user running the script has appropriate permissions
   - Check directory permissions for backup location

2. **"mariabackup: command not found"**
   - Install the mariabackup package: `apt-get install mariadb-backup` or `yum install MariaDB-backup`

3. **Connection Errors**
   - Verify database credentials
   - Check if the MariaDB server is running
   - Ensure network connectivity to the database server

4. **Out of Disk Space**
   - Increase available disk space
   - Adjust retention policies
   - Enable compression to reduce backup size

5. **Recovery Fails**
   - Check MariaDB error logs for detailed information
   - Ensure all required incremental backups are available
   - Verify permissions on the data directory

## Example Workflow

### Daily Backup Routine

```bash
# Morning full backup
./mariadb-backup.sh --type full --backup-dir /var/backup/mariadb --mysql-user backup --compress

# Hourly incremental backups throughout the day
./mariadb-backup.sh --type incremental --backup-dir /var/backup/mariadb --mysql-user backup --compress

# Binary log backups every 15 minutes
./mariadb-backup.sh --type binlog --backup-dir /var/backup/mariadb --mysql-user backup
```

### Disaster Recovery

```bash
# Restore the most recent full backup
./mariadb-backup.sh --restore /var/backup/mariadb/full_20250304120000

# Apply all changes up to the point of failure
./mariadb-backup.sh --pitr-only "2025-03-04 15:28:30"
```

## Security Considerations

- Store MySQL passwords in a defaults file with restricted permissions
- Encrypt backups containing sensitive data
- Secure encryption keys with proper permissions (chmod 400)
- Use a dedicated backup user with minimal privileges
- Protect backup storage with appropriate access controls

## Conclusion

The MariaDB Backup Manager script provides a comprehensive solution for database backup and recovery. By following the guidelines in this documentation, you can implement a robust backup strategy for your MariaDB databases.