#!/bin/bash
# MariaDB Backup Script with Full, Incremental and PITR capabilities
# Ali Fattahi

# Exit on error, undefined variables, and propagate pipe failures
#set -euo pipefail

# Script version
VERSION="1.1.0"

# Default parameter values
BACKUP_DIR="/var/backup/mariadb"
MYSQL_USER="root"
MYSQL_PASSWORD=""
MYSQL_HOST="127.0.0.1"
MYSQL_PORT="3306"
MYSQL_DATADIR="/var/lib/mysql"
MYSQL_DEFAULTS_FILE=""
WEBHOOK_URL=""
SEND_WEBHOOK=0
SLACK_WEBHOOK=""
EMAIL_RECIPIENT=""
COMPRESS=0
COMPRESS_THREADS=4
ENCRYPT=0
ENCRYPTION_KEY_FILE=""
RETENTION_DAYS=7
RETENTION_FULL=4     # Number of full backups to keep
RETENTION_INCR=14    # Number of incremental backups to keep
BACKUP_TYPE="full"
BINLOG_ONLY=0
MAX_BINLOG_DAYS_TO_BACKUP="-2"
MIN_DISK_SPACE=5     # Minimum disk space percentage required
PITR_ONLY=0
LOG_FILE=""
LOG_LEVEL="INFO"     # DEBUG, INFO, WARN, ERROR
TEST_RECOVERY=0
PARALLEL_THREADS=4
THROTTLE_IO=""
TEMP_DIR="/tmp"
DRY_RUN=0
CONFIG_FILE=""
IGNORE_ERRORS=0
LOCK_WAIT_TIMEOUT=60 # Seconds to wait for locks before aborting
DATE_FORMAT="%Y-%m-%d %H:%M:%S"

# Color codes for terminal output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to display usage information
usage() {
    cat <<EOF
$(basename "$0") v${VERSION} - MariaDB Backup Manager

Usage: $(basename "$0") [options]

Basic Options:
  --backup-dir DIR       Backup directory (default: $BACKUP_DIR)
  --type TYPE            Backup type: full, incremental, binlog (default: $BACKUP_TYPE)
  --retention DAYS       Overall retention period in days (default: $RETENTION_DAYS)
  --retention-full N     Number of full backups to keep (default: $RETENTION_FULL)
  --retention-incr N     Number of incremental backups to keep (default: $RETENTION_INCR)
  --compress             Compress backup using mbstream (default: no) [unstable]
  --compress-threads N   Number of compression threads (default: $COMPRESS_THREADS) [unstable]
  --encrypt              Encrypt backup with AES-256 (default: no)
  --encrypt-key-file     Path to encryption key file
  --parallel N           Number of parallel threads (default: $PARALLEL_THREADS)
  --dry-run              Show what would be done without making changes
  --temp-dir DIR         Temporary directory for operations (default: $TEMP_DIR)
  --throttle-io N        Limit I/O with ionice/nice settings (1-10)

Connection Options:
  --mysql-user USER      MySQL user (default: $MYSQL_USER)
  --mysql-password PASS  MySQL password (default: empty)
  --mysql-host HOST      MySQL host (default: $MYSQL_HOST)
  --mysql-port PORT      MySQL port (default: $MYSQL_PORT)
  --mysql-datadir DIR    MySQL data directory (default: $MYSQL_DATADIR)
  --defaults-file FILE   Use MySQL defaults file instead of command line credentials

Restore Options:
  --restore PATH         Restore from backup path
  --pitr DATETIME        Point-in-time recovery to specified datetime (YYYY-MM-DD HH:MM:SS)
  --pitr-only DATETIME   PITR without restoring backup sequences
  --test-recovery        Test restore to validate backup (requires additional disk space)
  --force                Force operations without confirmation
  --ignore-errors        Continue even if non-critical errors occur

Notification Options:
  --webhook URL          Webhook URL for notifications
  --send-webhook         Enable webhook notifications
  --slack-webhook URL    Slack webhook URL for notifications
  --email EMAIL          Email address for notifications

Configuration Options:
  --config FILE          Load settings from configuration file
  --setup-cron           Setup cronjob for regular backups
  --log-file FILE        Log file path (default: stdout)
  --log-level LEVEL      Log level: DEBUG, INFO, WARN, ERROR (default: INFO)

Miscellaneous Options:
  --version              Display version information
  --help                 Display this help message

Examples:
  # Perform a full backup
  $(basename "$0") --type full --mysql-user root --compress

  # Perform incremental backup with credentials from defaults file
  $(basename "$0") --type incremental --defaults-file /root/.my.cnf

  # Restore a backup with PITR
  $(basename "$0") --restore /var/backup/mariadb/full_20250304120000 --pitr "2025-03-04 15:30:00"

  # Setup automated backup jobs
  $(basename "$0") --setup-cron --config /etc/mariadb-backup.conf
EOF
    exit 1
}

# Function to load configuration from file
load_config() {
    local config_file="$1"
    
    if [[ ! -f "$config_file" ]]; then
        log ERROR "Configuration file not found: $config_file"
        exit 1
    fi
    
    log INFO "Loading configuration from: $config_file"
    
    # Source the config file
    # shellcheck source=/dev/null
    source "$config_file"
    
    # Validate required settings after loading config
    validate_settings
}

# Function to validate all settings
validate_settings() {
    # Check backup directory
    if [[ ! -d "$BACKUP_DIR" ]] && [[ "$DRY_RUN" -eq 0 ]]; then
        log WARN "Backup directory does not exist: $BACKUP_DIR"
        if ! mkdir -p "$BACKUP_DIR"; then
            log ERROR "Failed to create backup directory: $BACKUP_DIR"
            exit 1
        fi
        log INFO "Created backup directory: $BACKUP_DIR"
    fi
    
    # Check MySQL credentials
    if [[ -z "$MYSQL_DEFAULTS_FILE" ]]; then
        if [[ -z "$MYSQL_USER" ]]; then
            log ERROR "MySQL user is required"
            exit 1
        fi
        # Password can be empty if local socket auth is used
    else
        if [[ ! -f "$MYSQL_DEFAULTS_FILE" ]]; then
            log ERROR "MySQL defaults file not found: $MYSQL_DEFAULTS_FILE"
            exit 1
        fi
        # Check if the file is readable only by the owner
        local file_perms
        file_perms=$(stat -c "%a" "$MYSQL_DEFAULTS_FILE")
        if [[ "$file_perms" != "600" ]]; then
            log WARN "MySQL defaults file has insecure permissions: $file_perms, should be 600"
        fi
    fi
    
    # Check encryption settings
    if [[ "$ENCRYPT" -eq 1 ]]; then
        if [[ -z "$ENCRYPTION_KEY_FILE" ]]; then
            log ERROR "Encryption key file is required when encryption is enabled"
            exit 1
        fi
        if [[ ! -f "$ENCRYPTION_KEY_FILE" ]]; then
            log ERROR "Encryption key file not found: $ENCRYPTION_KEY_FILE"
            exit 1
        fi
        # Check if the file is readable only by the owner
        local key_perms
        key_perms=$(stat -c "%a" "$ENCRYPTION_KEY_FILE")
        if [[ "$key_perms" != "400" ]] && [[ "$key_perms" != "600" ]]; then
            log WARN "Encryption key file has insecure permissions: $key_perms, should be 400 or 600"
        fi
    fi
    
    # Validate numeric parameters
    if ! [[ "$RETENTION_DAYS" =~ ^[0-9]+$ ]]; then
        log ERROR "Retention days must be a positive integer: $RETENTION_DAYS"
        exit 1
    fi
    
    if ! [[ "$PARALLEL_THREADS" =~ ^[0-9]+$ ]]; then
        log ERROR "Parallel threads must be a positive integer: $PARALLEL_THREADS"
        exit 1
    fi
    
    if ! [[ "$COMPRESS_THREADS" =~ ^[0-9]+$ ]]; then
        log ERROR "Compression threads must be a positive integer: $COMPRESS_THREADS"
        exit 1
    fi
    
    if [[ -n "$THROTTLE_IO" ]] && ! [[ "$THROTTLE_IO" =~ ^[1-9]|10$ ]]; then
        log ERROR "Throttle IO must be between 1-10: $THROTTLE_IO"
        exit 1
    fi
    
    # Validate backup type
    case "$BACKUP_TYPE" in
        full|incremental|binlog)
            ;;
        *)
            log ERROR "Invalid backup type: $BACKUP_TYPE. Must be one of: full, incremental, binlog"
            exit 1
            ;;
    esac
}

# Function to log messages with timestamps and log levels
log() {
    local level="$1"
    local message="$2"
    local timestamp
    timestamp=$(date +"$DATE_FORMAT")
    
    # Filter logs based on log level
    case "$level" in
        DEBUG)
            [[ "$LOG_LEVEL" == "DEBUG" ]] || return 0
            color=$BLUE
            ;;
        INFO)
            [[ "$LOG_LEVEL" == "DEBUG" || "$LOG_LEVEL" == "INFO" ]] || return 0
            color=$GREEN
            ;;
        WARN)
            [[ "$LOG_LEVEL" != "ERROR" ]] || return 0
            color=$YELLOW
            ;;
        ERROR)
            color=$RED
            ;;
        *)
            color=$NC
            ;;
    esac
    
    # Format the message
    local formatted_message="[$timestamp] [$level] $message"
    
    # Send to log file if specified, otherwise to stdout
    if [[ -n "$LOG_FILE" ]]; then
        echo "$formatted_message" >> "$LOG_FILE"
    else
        echo -e "${color}$formatted_message${NC}"
    fi
    
    # For ERROR level, also send notifications
    if [[ "$level" == "ERROR" ]]; then
        send_notification "ERROR: $message"
    fi
}

# Function to check if mariabackup is installed
check_mariabackup() {
    if ! command -v mariabackup &> /dev/null; then
        log ERROR "mariabackup is not installed. Please install it first."
        exit 1
    fi
    
    # Get mariabackup version
    local mariabackup_version
    mariabackup_version=$(mariabackup --version 2>&1 | head -n 1)
    log INFO "Using $mariabackup_version"
}

# Function to check if server is running
check_server_running() {
    log DEBUG "Checking if MariaDB server is running"
    
    local cmd
    if [[ -n "$MYSQL_DEFAULTS_FILE" ]]; then
        cmd="mysql --defaults-file=$MYSQL_DEFAULTS_FILE -h $MYSQL_HOST -P $MYSQL_PORT -e 'SELECT 1' &>/dev/null"
    else
        cmd="mysql -u $MYSQL_USER ${MYSQL_PASSWORD:+-p$MYSQL_PASSWORD} -h $MYSQL_HOST -P $MYSQL_PORT -e 'SELECT 1' &>/dev/null"
    fi
    
    if eval "$cmd"; then
        log DEBUG "MariaDB server is running"
        return 0
    else
        log ERROR "MariaDB server is not running or connection failed"
        return 1
    fi
}

# Function to check available disk space
check_disk_space() {
    local backup_dir="$1"
    local min_required="$2"
    
    log DEBUG "Checking disk space in $backup_dir"
    
    # Create directory if it doesn't exist
    if [[ ! -d "$backup_dir" ]] && [[ "$DRY_RUN" -eq 0 ]]; then
        mkdir -p "$backup_dir" || {
            log ERROR "Failed to create directory: $backup_dir"
            return 1
        }
    fi
    
    # Get available disk space percentage
    local disk_space_used
    disk_space_used=$(df -h "$backup_dir" | awk 'NR==2 {print $5}' | sed 's/%//')
    local disk_space_available=$((100 - disk_space_used))
    
    log INFO "Disk space check: Available: $disk_space_available%, Required: $min_required%"
    
    if [[ "$disk_space_available" -lt "$min_required" ]]; then
        log ERROR "Not enough disk space. Available: $disk_space_available%, Required: $min_required%"
        send_notification "Backup failed - Not enough disk space. Available: $disk_space_available%, Required: $min_required%"
        return 1
    fi
    
    return 0
}

# Function to estimate backup size
estimate_backup_size() {
    log INFO "Estimating required backup size"
    
    local datadir_size
    datadir_size=$(du -sm "$MYSQL_DATADIR" | awk '{print $1}')
    
    local estimated_size
    if [[ "$BACKUP_TYPE" == "full" ]]; then
        # Full backup will be roughly the size of the data directory
        estimated_size=$datadir_size
    elif [[ "$BACKUP_TYPE" == "incremental" ]]; then
        # Incremental backup size depends on changes, estimate 10-30% of full
        estimated_size=$((datadir_size / 5))
    else
        # Binary log backup size is usually small
        estimated_size=100  # Default 100MB estimate for binlogs
    fi
    
    if [[ "$COMPRESS" -eq 1 ]]; then
        # Compression typically reduces size by 60-80%
        estimated_size=$((estimated_size * 3 / 10))
    fi
    
    log INFO "Estimated backup size: $estimated_size MB"
    return "$estimated_size"
}

# Function to build mariabackup command with all options
build_mariabackup_cmd() {
    local operation="$1"  # backup, prepare, copy-back
    local target_dir="$2"
    local extra_opts="$3"  # Additional options like incremental-basedir
    
    local cmd="mariabackup --$operation --target-dir=$target_dir"
    
    # Add connection parameters
    if [[ -n "$MYSQL_DEFAULTS_FILE" ]]; then
        cmd="$cmd --defaults-file=$MYSQL_DEFAULTS_FILE"
    else
        cmd="$cmd --user=$MYSQL_USER"
        if [[ -n "$MYSQL_PASSWORD" ]]; then
            cmd="$cmd --password=$MYSQL_PASSWORD"
        fi
        cmd="$cmd --host=$MYSQL_HOST --port=$MYSQL_PORT"
    fi
    
    # Add extra options
    if [[ -n "$extra_opts" ]]; then
        cmd="$cmd $extra_opts"
    fi
    
    # Add parallelization
    if [[ "$PARALLEL_THREADS" -gt 1 ]]; then
        cmd="$cmd --parallel=$PARALLEL_THREADS"
    fi
    
    # Add throttling if specified
    if [[ -n "$THROTTLE_IO" ]]; then
        local nice_level=$((20 - THROTTLE_IO * 2))
        cmd="nice -n $nice_level ionice -c2 -n$THROTTLE_IO $cmd"
    fi
    
    # Return command
    echo "$cmd"
}

# Function to send notification through multiple channels
send_notification() {
    local message="$1"
    local timestamp
    timestamp=$(date +"$DATE_FORMAT")
    local hostname
    hostname=$(hostname)
    
    # Format the message with hostname
    local formatted_message="[$timestamp] [$hostname] $message"
    
    log DEBUG "Sending notification: $message"
    
    # Send webhook notification
    if [[ "$SEND_WEBHOOK" -eq 1 ]] && [[ -n "$WEBHOOK_URL" ]]; then
        log DEBUG "Sending webhook notification"
        
        if ! curl -s -S -X POST -H "Content-Type: application/json" \
             -d "{\"message\":\"$formatted_message\", \"timestamp\":\"$timestamp\", \"hostname\":\"$hostname\"}" \
             "$WEBHOOK_URL" &>/dev/null; then
            log WARN "Failed to send webhook notification"
        fi
    fi
    
    # Send Slack notification
    if [[ -n "$SLACK_WEBHOOK" ]]; then
        log DEBUG "Sending Slack notification"
        
        if ! curl -s -S -X POST -H "Content-Type: application/json" \
             -d "{\"text\":\"$formatted_message\"}" \
             "$SLACK_WEBHOOK" &>/dev/null; then
            log WARN "Failed to send Slack notification"
        fi
    fi
    
    # Send email notification
    if [[ -n "$EMAIL_RECIPIENT" ]]; then
        log DEBUG "Sending email notification"
        
        if command -v mail &>/dev/null; then
            echo "$formatted_message" | mail -s "MariaDB Backup: $message" "$EMAIL_RECIPIENT" || \
                log WARN "Failed to send email notification"
        else
            log WARN "mail command not found, cannot send email notification"
        fi
    fi
}

# Function to acquire database lock
acquire_lock() {
    log INFO "Acquiring database lock for backup"
    
    local lock_timeout=${1:-$LOCK_WAIT_TIMEOUT}
    local lock_file="/tmp/mariadb_backup_lock_$(date +%s).lock"
    
    if [[ "$DRY_RUN" -eq 1 ]]; then
        log INFO "[DRY RUN] Would acquire database lock"
        return 0
    fi
    
    local cmd
    if [[ -n "$MYSQL_DEFAULTS_FILE" ]]; then
        cmd="mysql --defaults-file=$MYSQL_DEFAULTS_FILE -h $MYSQL_HOST -P $MYSQL_PORT"
    else
        cmd="mysql -u $MYSQL_USER ${MYSQL_PASSWORD:+-p$MYSQL_PASSWORD} -h $MYSQL_HOST -P $MYSQL_PORT"
    fi
    
    # Set a timeout for lock acquisition
    $cmd -e "SET SESSION lock_wait_timeout=$lock_timeout; FLUSH TABLES WITH READ LOCK;" &
    local lock_pid=$!
    
    # Wait for the lock to be acquired
    sleep 2
    if ! kill -0 $lock_pid 2>/dev/null; then
        log ERROR "Failed to acquire database lock"
        return 1
    fi
    
    # Record the lock PID for later release
    echo "$lock_pid" > "$lock_file"
    log INFO "Database lock acquired (PID: $lock_pid)"
    
    return 0
}

# Function to release database lock
release_lock() {
    local lock_file="/tmp/mariadb_backup_lock_*.lock"
    
    if [[ "$DRY_RUN" -eq 1 ]]; then
        log INFO "[DRY RUN] Would release database lock"
        return 0
    fi
    
    for file in $lock_file; do
        if [[ -f "$file" ]]; then
            local lock_pid
            lock_pid=$(cat "$file")
            
            log INFO "Releasing database lock (PID: $lock_pid)"
            
            # Kill the mysql process that's holding the lock
            if kill -TERM "$lock_pid" 2>/dev/null; then
                log INFO "Database lock released"
            else
                log WARN "Failed to release database lock, process may not exist"
            fi
            
            rm -f "$file"
        fi
    done
}

# Function to verify backup with better compressed backup handling
verify_backup() {
    local backup_path="$1"
    
    log INFO "Verifying backup: $backup_path"
    
    if [[ ! -d "$backup_path" ]]; then
        log ERROR "Backup directory does not exist: $backup_path"
        return 1
    fi
    
    # Determine if this is a compressed backup
    # Check both from command line options and from metadata if available
    local is_compressed=0
    
    # First check from current script state
    if [[ "$COMPRESS" -eq 1 ]]; then
        is_compressed=1
        log INFO "Detected compressed backup from script parameters"
    fi
    
    # Also check metadata file if it exists
    if [[ -f "$backup_path/backup_metadata.txt" ]]; then
        if grep -q "Compression: 1" "$backup_path/backup_metadata.txt"; then
            is_compressed=1
            log INFO "Detected compressed backup from metadata"
        fi
    fi
    
    # Also check for qp files which indicate compression
    if [[ $(find "$backup_path" -name "*.qp" | wc -l) -gt 0 ]]; then
        is_compressed=1
        log INFO "Detected compressed backup files (*.qp)"
    fi
    
    # Verification logic based on compression status
    if [[ "$is_compressed" -eq 1 ]]; then
        # For compressed backups, only check for xtrabackup_checkpoints 
        # which is not compressed by mariabackup
        if [[ ! -f "$backup_path/xtrabackup_checkpoints" ]]; then
            log ERROR "Backup verification failed - missing xtrabackup_checkpoints"
            return 1
        fi
        
        log INFO "Compressed backup verified with basic checks only"
        
        # Skip test recovery for compressed backups
        if [[ "$TEST_RECOVERY" -eq 1 ]]; then
            log INFO "Skipping test recovery for compressed backup"
        fi
    else
        # For non-compressed backups, check for all required files
        local required_files=(
            "xtrabackup_checkpoints"
            "xtrabackup_info"
        )
        
        for file in "${required_files[@]}"; do
            if [[ ! -f "$backup_path/$file" ]]; then
                log ERROR "Backup verification failed - missing $file"
                return 1
            fi
        done
        
        # Perform test recovery if requested
        if [[ "$TEST_RECOVERY" -eq 1 ]]; then
            log INFO "Performing test recovery to verify backup"
            
            # Create temporary directory for test restore
            local test_dir="$TEMP_DIR/mariadb_test_restore_$(date +%s)"
            mkdir -p "$test_dir"
            
            # Check if there's enough space for test recovery
            local backup_size
            backup_size=$(du -sm "$backup_path" | awk '{print $1}')
            local temp_space_available
            temp_space_available=$(df -m "$TEMP_DIR" | awk 'NR==2 {print $4}')
            
            if [[ "$temp_space_available" -lt "$backup_size" ]]; then
                log ERROR "Not enough space for test recovery. Required: $backup_size MB, Available: $temp_space_available MB"
                return 1
            fi
            
            # Prepare the backup to test its integrity
            log INFO "Preparing backup for test recovery"
            
            local prepare_cmd
            prepare_cmd=$(build_mariabackup_cmd "prepare" "$backup_path" "--export")
            
            if ! eval "$prepare_cmd"; then
                log ERROR "Test recovery preparation failed"
                rm -rf "$test_dir"
                return 1
            fi
            
            log INFO "Backup verification with test recovery passed"
            rm -rf "$test_dir"
        fi
    fi
    
    log INFO "Backup verification passed: $backup_path"
    return 0
}

# Function to perform full backup
perform_full_backup() {
    local timestamp
    timestamp=$(date +%Y%m%d%H%M%S)
    local backup_path="$BACKUP_DIR/full_$timestamp"
    
    log INFO "Starting full backup to $backup_path"
    
    if [[ "$DRY_RUN" -eq 1 ]]; then
        log INFO "[DRY RUN] Would perform full backup to $backup_path"
        return 0
    fi
    
    # Check disk space first
    if ! check_disk_space "$BACKUP_DIR" "$MIN_DISK_SPACE"; then
        return 1
    fi
    
    # Create backup directory
    mkdir -p "$backup_path"
    
    # Build backup command
    local backup_options=""
    
    if [[ "$COMPRESS" -eq 1 ]]; then
        backup_options="$backup_options --compress --compress-threads=$COMPRESS_THREADS"
    fi
    
    # Add encryption if enabled
    if [[ "$ENCRYPT" -eq 1 ]]; then
        backup_options="$backup_options --encrypt=AES256 --encrypt-key-file=$ENCRYPTION_KEY_FILE"
    fi
    
    local backup_cmd
    backup_cmd=$(build_mariabackup_cmd "backup" "$backup_path" "$backup_options")
    
    log DEBUG "Executing command: $backup_cmd"
    
    # Execute backup command
    local backup_start
    backup_start=$(date +%s)
    
    if eval "$backup_cmd"; then
        local backup_end
        backup_end=$(date +%s)
        local backup_duration=$((backup_end - backup_start))
        
        log INFO "Full backup completed successfully in $backup_duration seconds"
        
        # Verify the backup
        if verify_backup "$backup_path"; then
            # Update last backup pointers
            echo "$backup_path" > "$BACKUP_DIR/last_full_backup"
            echo "$backup_path" > "$BACKUP_DIR/last_incr_backup"
            
            # Add backup metadata
            echo "Backup Type: full" > "$backup_path/backup_metadata.txt"
            echo "Backup Date: $(date +"$DATE_FORMAT")" >> "$backup_path/backup_metadata.txt"
            echo "Backup Size: $(du -sh "$backup_path" | awk '{print $1}')" >> "$backup_path/backup_metadata.txt"
            echo "Compression: $COMPRESS" >> "$backup_path/backup_metadata.txt"
            echo "Encryption: $ENCRYPT" >> "$backup_path/backup_metadata.txt"
            
            send_notification "Full backup completed successfully: $backup_path ($(du -sh "$backup_path" | awk '{print $1}'))"
            return 0
        else
            log ERROR "Full backup verification failed"
            send_notification "Full backup verification failed: $backup_path"
            return 1
        fi
    else
        log ERROR "Full backup failed"
        send_notification "Full backup failed"
        return 1
    fi
}

# Function to perform incremental backup
perform_incremental_backup() {
    # Check if full backup exists
    if [[ ! -f "$BACKUP_DIR/last_full_backup" ]]; then
        log WARN "No full backup found. Performing full backup first."
        perform_full_backup
        return $?
    fi
    
    # Find the most recent backup (either full or incremental)
    local last_full_backup
    last_full_backup=$(cat "$BACKUP_DIR/last_full_backup")
    local last_incr_backup=""
    local last_backup=""
    
    # Look for the most recent incremental backup
    if [[ -f "$BACKUP_DIR/last_incr_backup" ]]; then
        last_incr_backup=$(cat "$BACKUP_DIR/last_incr_backup")
        
        # Verify that the incremental backup exists and is valid
        if [[ -d "$last_incr_backup" ]] && [[ -f "$last_incr_backup/xtrabackup_checkpoints" ]]; then
            log INFO "Found previous incremental backup: $last_incr_backup"
            last_backup="$last_incr_backup"
        else
            log WARN "Previous incremental backup is invalid or missing. Using full backup as base."
            last_backup="$last_full_backup"
        fi
    else
        log INFO "No previous incremental backup found. Using full backup as base."
        last_backup="$last_full_backup"
    fi
    
    local timestamp
    timestamp=$(date +%Y%m%d%H%M%S)
    local backup_path="$BACKUP_DIR/incr_$timestamp"
    
    log INFO "Starting incremental backup to $backup_path (based on $last_backup)"
    
    if [[ "$DRY_RUN" -eq 1 ]]; then
        log INFO "[DRY RUN] Would perform incremental backup to $backup_path based on $last_backup"
        return 0
    fi
    
    # Check disk space first
    if ! check_disk_space "$BACKUP_DIR" "$MIN_DISK_SPACE"; then
        return 1
    fi
    
    # Create backup directory
    mkdir -p "$backup_path"
    
    # Build backup command
    local backup_options="--incremental-basedir=$last_backup"
    
    if [[ "$COMPRESS" -eq 1 ]]; then
        backup_options="$backup_options --compress --compress-threads=$COMPRESS_THREADS"
    fi
    
    # Add encryption if enabled
    if [[ "$ENCRYPT" -eq 1 ]]; then
        backup_options="$backup_options --encrypt=AES256 --encrypt-key-file=$ENCRYPTION_KEY_FILE"
    fi
    
    local backup_cmd
    backup_cmd=$(build_mariabackup_cmd "backup" "$backup_path" "$backup_options")
    
    log DEBUG "Executing command: $backup_cmd"
    
    # Execute backup command
    local backup_start
    backup_start=$(date +%s)
    
    if eval "$backup_cmd"; then
        local backup_end
        backup_end=$(date +%s)
        local backup_duration=$((backup_end - backup_start))
        
        log INFO "Incremental backup completed successfully in $backup_duration seconds"
        
        # Verify the backup
        if verify_backup "$backup_path"; then
            # Update last incremental backup pointer
            echo "$backup_path" > "$BACKUP_DIR/last_incr_backup"
            
            # Add backup metadata
            echo "Backup Type: incremental" > "$backup_path/backup_metadata.txt"
            echo "Base Backup: $last_backup" >> "$backup_path/backup_metadata.txt"
            echo "Backup Date: $(date +"$DATE_FORMAT")" >> "$backup_path/backup_metadata.txt"
            echo "Backup Size: $(du -sh "$backup_path" | awk '{print $1}')" >> "$backup_path/backup_metadata.txt"
            echo "Compression: $COMPRESS" >> "$backup_path/backup_metadata.txt"
            echo "Encryption: $ENCRYPT" >> "$backup_path/backup_metadata.txt"
            
            send_notification "Incremental backup completed successfully: $backup_path ($(du -sh "$backup_path" | awk '{print $1}'))"
            return 0
        else
            log ERROR "Incremental backup verification failed"
            send_notification "Incremental backup verification failed: $backup_path"
            return 1
        fi
    else
        log ERROR "Incremental backup failed"
        send_notification "Incremental backup failed"
        return 1
    fi
}

# Function to backup binary logs
backup_binlogs() {
    local timestamp
    timestamp=$(date +%Y%m%d%H%M%S)
    local binlog_backup_dir="$BACKUP_DIR/binlogs_$timestamp"
    
    log INFO "Starting binary log backup to $binlog_backup_dir"
    
    if [[ "$DRY_RUN" -eq 1 ]]; then
        log INFO "[DRY RUN] Would perform binary log backup to $binlog_backup_dir"
        return 0
    fi
    
    # Check disk space first
    if ! check_disk_space "$BACKUP_DIR" "$MIN_DISK_SPACE"; then
        return 1
    fi
    
    # Create backup directory
    mkdir -p "$binlog_backup_dir"
    
    # Get current binary log information
    local mysql_cmd
    if [[ -n "$MYSQL_DEFAULTS_FILE" ]]; then
        mysql_cmd="mysql --defaults-file=$MYSQL_DEFAULTS_FILE -h $MYSQL_HOST -P $MYSQL_PORT"
    else
        mysql_cmd="mysql -u $MYSQL_USER ${MYSQL_PASSWORD:+-p$MYSQL_PASSWORD} -h $MYSQL_HOST -P $MYSQL_PORT"
    fi
    
    local binlog_info
    binlog_info=$($mysql_cmd -e "SHOW MASTER STATUS\G")
    local current_binlog
    current_binlog=$(echo "$binlog_info" | grep "File:" | awk '{print $2}')
    
    if [[ -z "$current_binlog" ]]; then
        log ERROR "Could not determine current binary log file"
        send_notification "Binary log backup failed - could not determine current binary log file"
        return 1
    fi
    
    log INFO "Current binary log file: $current_binlog"
    
    # Get binary log directory
    local binlog_dir
    binlog_dir=$($mysql_cmd -e "SHOW VARIABLES LIKE 'log_bin_basename'\G" | \
                 grep "Value" | awk '{print $2}' | sed 's/\/[^\/]*$//')
    
    # If we couldn't get the directory, assume it's in the datadir
    if [[ -z "$binlog_dir" ]]; then
        binlog_dir="$MYSQL_DATADIR"
    fi
    
    log INFO "Binary log directory: $binlog_dir"
    
    # Extract binary log prefix (e.g., "mysql-bin" from "mysql-bin.000123")
    local binlog_prefix
    binlog_prefix=$(echo "$current_binlog" | sed 's/\.[0-9]*$//')
    
    # Find all binary logs in the binary log directory
    log DEBUG "Looking for binary logs with pattern: ${binlog_prefix}.*"
    local binlog_files
    binlog_files=$(find "$binlog_dir" -name "${binlog_prefix}.*" -type f -mtime $MAX_BINLOG_DAYS_TO_BACKUP)
    
    if [[ -z "$binlog_files" ]]; then
        log WARN "No binary log files found in $binlog_dir"
        send_notification "Binary log backup warning - no binary log files found"
        return 1
    fi
    
    # Count binary logs
    local binlog_count
    binlog_count=$(echo "$binlog_files" | wc -l)
    log INFO "Found $binlog_count binary log files"
    
    # Copy binary logs to backup directory
    local copied_count=0
    for binlog in $binlog_files; do
        log DEBUG "Copying binary log: $(basename "$binlog")"
        if cp "$binlog" "$binlog_backup_dir/"; then
            ((copied_count++))
        else
            log WARN "Failed to copy binary log: $binlog"
        fi
    done
    
    # Save binary log info for PITR
    echo "$binlog_info" > "$binlog_backup_dir/binlog_info.txt"
    
    # Add backup metadata
    echo "Backup Type: binlog" > "$binlog_backup_dir/backup_metadata.txt"
    echo "Backup Date: $(date +"$DATE_FORMAT")" >> "$binlog_backup_dir/backup_metadata.txt"
    echo "Binary Logs: $copied_count of $binlog_count" >> "$binlog_backup_dir/backup_metadata.txt"
    echo "Current Log: $current_binlog" >> "$binlog_backup_dir/backup_metadata.txt"
    
    if [[ "$copied_count" -eq "$binlog_count" ]]; then
        log INFO "Binary log backup completed successfully ($copied_count logs)"
        send_notification "Binary log backup completed successfully: $binlog_backup_dir ($copied_count logs)"
        return 0
    else
        log WARN "Binary log backup completed with warnings - copied $copied_count of $binlog_count logs"
        send_notification "Binary log backup completed with warnings: $binlog_backup_dir ($copied_count of $binlog_count logs)"
        
        if [[ "$IGNORE_ERRORS" -eq 1 ]]; then
            return 0
        else
            return 1
        fi
    fi
}

# Function to prepare backup for restoration
prepare_backup() {
    local backup_path="$1"
    local is_full="${2:-0}"
    
    if [[ "$DRY_RUN" -eq 1 ]]; then
        if [[ "$is_full" -eq 1 ]]; then
            log INFO "[DRY RUN] Would prepare full backup: $backup_path"
        else
            log INFO "[DRY RUN] Would prepare incremental backup chain ending with: $backup_path"
        fi
        return 0
    fi
    
    # Check if this is a compressed backup by looking at metadata
    local is_compressed=0
    if grep -q "compressed = 1" "$backup_path/xtrabackup_info" || \
       [[ -f "$backup_path/backup_metadata.txt" && $(grep -c "Compression: 1" "$backup_path/backup_metadata.txt") -gt 0 ]]; then
        log INFO "Detected compressed backup, will decompress during preparation"
        is_compressed=1
    fi
    
    if [[ "$is_full" -eq 1 ]]; then
        log INFO "Preparing full backup: $backup_path"
        
        local prepare_options=""
        # Add decompression option if needed
        if [[ "$is_compressed" -eq 1 ]]; then
            prepare_options="--decompress"
            if [[ "$PARALLEL_THREADS" -gt 1 ]]; then
                prepare_options="$prepare_options --decompress-threads=$PARALLEL_THREADS"
            fi
        fi
        
        local prepare_cmd
        prepare_cmd=$(build_mariabackup_cmd "prepare" "$backup_path" "$prepare_options")
        
        log DEBUG "Executing command: $prepare_cmd"
        
        if ! eval "$prepare_cmd"; then
            log ERROR "Failed to prepare full backup: $backup_path"
            return 1
        fi
    else
        # For incremental backups, we need to apply all incremental backups in sequence
        local full_backup_path
        full_backup_path=$(cat "$BACKUP_DIR/last_full_backup")
        
        log INFO "Preparing incremental backup chain starting with full backup: $full_backup_path"
        
        # Check if full backup is compressed
        local full_is_compressed=0
        if grep -q "compressed = 1" "$full_backup_path/xtrabackup_info" || \
           [[ -f "$full_backup_path/backup_metadata.txt" && $(grep -c "Compression: 1" "$full_backup_path/backup_metadata.txt") -gt 0 ]]; then
            log INFO "Detected compressed full backup, will decompress during preparation"
            full_is_compressed=1
        fi
        
        # First prepare the full backup with --apply-log-only
        local full_prepare_options=""
        if [[ "$full_is_compressed" -eq 1 ]]; then
            full_prepare_options="$full_prepare_options --decompress"
            if [[ "$PARALLEL_THREADS" -gt 1 ]]; then
                full_prepare_options="$full_prepare_options"
            fi
        fi
        
        local prepare_full_cmd
        prepare_full_cmd=$(build_mariabackup_cmd "prepare" "$full_backup_path" "$full_prepare_options")
        echo "CMD $prepare_full_cmd"
        log DEBUG "Executing command: $prepare_full_cmd"
        
        if ! eval "$prepare_full_cmd"; then
            log ERROR "Failed to prepare full backup: $full_backup_path"
            return 1
        fi
        
        # Find all incremental backups between the full backup and the target backup
        local timestamp_pattern="[0-9]\+"
        local full_timestamp
        full_timestamp=$(echo "$full_backup_path" | grep -o "$timestamp_pattern")
        local target_timestamp
        target_timestamp=$(echo "$backup_path" | grep -o "$timestamp_pattern")
        
        # Get list of all incremental backups
        local all_incrementals
        mapfile -t all_incrementals < <(find "$BACKUP_DIR" -name "incr_*" -type d | sort)
        local apply_incrementals=()
        
        # Filter incrementals between full and target using numeric comparison
        for incr in "${all_incrementals[@]}"; do
            local incr_timestamp
            incr_timestamp=$(echo "$incr" | grep -o "$timestamp_pattern")
            
            # Convert timestamps to integers for numeric comparison
            if [[ "$incr_timestamp" -gt "$full_timestamp" ]] && [[ "$incr_timestamp" -le "$target_timestamp" ]]; then
                apply_incrementals+=("$incr")
                log DEBUG "Will apply incremental: $incr"
            fi
        done
        
        local incr_count=${#apply_incrementals[@]}
        log INFO "Found $incr_count incremental backups to apply"
        
        # Apply each incremental backup in sequence except the last one
        for ((i=0; i<incr_count-1; i++)); do
            log INFO "Applying incremental backup [$((i+1))/$incr_count]: ${apply_incrementals[$i]}"
            
            # Check if this incremental backup is compressed
            local incr_is_compressed=0
            if grep -q "compressed = 1" "${apply_incrementals[$i]}/xtrabackup_info" || \
               [[ -f "${apply_incrementals[$i]}/backup_metadata.txt" && $(grep -c "Compression: 1" "${apply_incrementals[$i]}/backup_metadata.txt") -gt 0 ]]; then
                log INFO "Detected compressed incremental backup, will decompress during application"
                incr_is_compressed=1
            fi
            
            local incr_options="--incremental-dir=${apply_incrementals[$i]}"
            if [[ "$incr_is_compressed" -eq 1 ]]; then
                incr_options="$incr_options --decompress"
                if [[ "$PARALLEL_THREADS" -gt 1 ]]; then
                    incr_options="$incr_options --decompress-threads=$PARALLEL_THREADS"
                fi
            fi
            
            local prepare_incr_cmd
            prepare_incr_cmd=$(build_mariabackup_cmd "prepare" "$full_backup_path" "$incr_options")
            
            log DEBUG "Executing command: $prepare_incr_cmd"
            
            if ! eval "$prepare_incr_cmd"; then
                log ERROR "Failed to apply incremental backup: ${apply_incrementals[$i]}"
                return 1
            fi
        done
        
        # Apply the final incremental backup without --apply-log-only
        if [[ "$incr_count" -gt 0 ]]; then
            local last_idx=$((incr_count-1))
            log INFO "Applying final incremental backup [$incr_count/$incr_count]: ${apply_incrementals[$last_idx]}"
            
            # Check if the final incremental backup is compressed
            local final_incr_is_compressed=0
            if grep -q "compressed = 1" "${apply_incrementals[$last_idx]}/xtrabackup_info" || \
               [[ -f "${apply_incrementals[$last_idx]}/backup_metadata.txt" && $(grep -c "Compression: 1" "${apply_incrementals[$last_idx]}/backup_metadata.txt") -gt 0 ]]; then
                log INFO "Detected compressed final incremental backup, will decompress during application"
                final_incr_is_compressed=1
            fi
            
            local final_options="--incremental-dir=${apply_incrementals[$last_idx]}"
            if [[ "$final_incr_is_compressed" -eq 1 ]]; then
                final_options="$final_options --decompress"
                if [[ "$PARALLEL_THREADS" -gt 1 ]]; then
                    final_options="$final_options --decompress-threads=$PARALLEL_THREADS"
                fi
            fi
            
            local prepare_final_cmd
            prepare_final_cmd=$(build_mariabackup_cmd "prepare" "$full_backup_path" "$final_options")
            
            log DEBUG "Executing command: $prepare_final_cmd"
            
            if ! eval "$prepare_final_cmd"; then
                log ERROR "Failed to apply final incremental backup: ${apply_incrementals[$last_idx]}"
                return 1
            fi
        fi
    fi
    
    log INFO "Backup preparation completed successfully"
    return 0
}
# Function to restore backup
restore_backup() {
    local backup_path="$1"
    local is_incremental=0
    local full_backup_path=""
    
    # Determine if this is an incremental backup
    if [[ "$backup_path" == *"incr_"* ]]; then
        is_incremental=1
        # For incremental backups, we need to use the full backup path
        # because all incremental changes are applied to the full backup during preparation
        if [[ -f "$BACKUP_DIR/last_full_backup" ]]; then
            full_backup_path=$(cat "$BACKUP_DIR/last_full_backup")
            log INFO "This is an incremental backup, will restore from prepared full backup: $full_backup_path"
        else
            log ERROR "Cannot find full backup path for incremental restore"
            return 1
        fi
    else
        # For full backups, use the provided path
        full_backup_path="$backup_path"
    fi
    
    if [[ "$DRY_RUN" -eq 1 ]]; then
        log INFO "[DRY RUN] Would restore backup from: $full_backup_path"
        return 0
    fi
    
    log INFO "Starting restore from: $full_backup_path"
    
    # Check if MariaDB data directory is empty
    if [[ -d "$MYSQL_DATADIR" ]] && [[ "$(ls -A "$MYSQL_DATADIR" 2>/dev/null)" ]]; then
        log WARN "MySQL data directory is not empty: $MYSQL_DATADIR"
        
        # Check if we need to ask for confirmation
        if [[ "$FORCE" -ne 1 ]]; then
            read -p "MySQL data directory is not empty. Do you want to empty it? (yes/no): " confirm
            
            if [[ "$confirm" != "yes" ]]; then
                log INFO "Restoration aborted by user"
                return 1
            fi
        fi
        
        # Stop MariaDB service
        log INFO "Stopping MariaDB service"
        if ! systemctl stop mariadb; then
            log ERROR "Failed to stop MariaDB service"
            return 1
        fi
        
        # Empty data directory
        log INFO "Emptying MySQL data directory"
        rm -rf "${MYSQL_DATADIR:?}"/* || {
            log ERROR "Failed to empty MySQL data directory"
            return 1
        }
    fi
    
    # Ensure MariaDB is stopped
    log INFO "Ensuring MariaDB is stopped"
    systemctl stop mariadb || {
        log WARN "Could not stop MariaDB service, it may already be stopped"
    }
    
    # Restore backup
    log INFO "Restoring backup from: $full_backup_path"
    
    local restore_cmd
    restore_cmd=$(build_mariabackup_cmd "copy-back" "$full_backup_path" "")
    
    log DEBUG "Executing command: $restore_cmd"
    
    if ! eval "$restore_cmd"; then
        log ERROR "Backup restoration failed"
        send_notification "Backup restoration failed from: $full_backup_path"
        return 1
    fi
    
    # Fix permissions
    log INFO "Setting correct permissions for MySQL data directory"
    chown -R mysql:mysql "$MYSQL_DATADIR" || {
        log ERROR "Failed to set correct permissions for MySQL data directory"
        return 1
    }
    
    # Start MariaDB
    log INFO "Starting MariaDB service"
    if ! systemctl start mariadb; then
        log ERROR "Failed to start MariaDB after restoration"
        send_notification "Backup restoration failed - could not start MariaDB"
        return 1
    fi
    
    # Verify that MariaDB is actually running
    sleep 5
    if systemctl is-active --quiet mariadb; then
        log INFO "MariaDB service is running"
    else
        log ERROR "MariaDB service failed to start properly"
        log ERROR "Check the MariaDB error log for details"
        send_notification "Backup restoration failed - MariaDB service failed to start properly"
        return 1
    fi
    
    log INFO "Backup restoration completed successfully"
    send_notification "Backup restoration completed successfully from: $full_backup_path"
    return 0
}

# Function to perform point-in-time recovery
perform_pitr() {
    local target_time="$1"
    local backup_path="$2"
    local pitr_only="${3:-0}"
    
    if [[ "$DRY_RUN" -eq 1 ]]; then
        if [[ "$pitr_only" -eq 1 ]]; then
            log INFO "[DRY RUN] Would perform PITR only (without backup restoration) to: $target_time"
        else
            log INFO "[DRY RUN] Would perform PITR with backup restoration from $backup_path to: $target_time"
        fi
        return 0
    fi
    
    log INFO "Starting Point-In-Time Recovery to: $target_time"
    
    # First restore the backup if not pitr only
    if [[ "$pitr_only" -ne 1 ]]; then
        log INFO "Preparing backup before restoration"
        # Determine if it's a full or incremental backup
        if [[ "$backup_path" == *"full_"* ]]; then
            prepare_backup "$backup_path" 1
        else
            prepare_backup "$backup_path" 0
        fi
        
        if [[ $? -ne 0 ]]; then
            log ERROR "PITR failed - could not prepare backup"
            return 1
        fi
        
        log INFO "Restoring backup before applying binary logs"
        restore_backup "$backup_path"
        
        if [[ $? -ne 0 ]]; then
            log ERROR "PITR failed - could not restore backup"
            return 1
        fi
    else
        log INFO "Skipping backup restoration, applying binary logs only"
    fi
    
    # Find the most recent binary log backup
    local latest_binlog_backup
    latest_binlog_backup=$(find "$BACKUP_DIR" -name "binlogs_*" -type d | sort | tail -1)
    
    if [[ -z "$latest_binlog_backup" ]]; then
        log ERROR "PITR failed - no binary log backups found"
        return 1
    fi
    
    log INFO "Using binary logs from: $latest_binlog_backup"
    
    # Apply binary logs up to the target time
    log INFO "Applying binary logs up to: $target_time"
    
    # Find all binary logs in the backup directory
    local binlog_files
    mapfile -t binlog_files < <(find "$latest_binlog_backup" -name "*.0*" | sort)
    
    if [[ ${#binlog_files[@]} -eq 0 ]]; then
        log ERROR "No binary log files found in: $latest_binlog_backup"
        return 1
    fi
    
    log INFO "Found ${#binlog_files[@]} binary log files"
    
    # Create a recovery SQL file
    local recovery_sql
    recovery_sql="/tmp/recovery_$(date +%Y%m%d%H%M%S).sql"
    
    # Sets the sql_log_bin system variable, which disables binary logging for the current connection
    echo "SET SQL_LOG_BIN=0;" > "$recovery_sql"
    
    # Add mysqlbinlog commands for each binary log
    for binlog in "${binlog_files[@]}"; do
        log INFO "Processing binary log: $(basename "$binlog") up to $target_time"
        
        # Use mysqlbinlog to extract statements up to the target time
        mysqlbinlog --stop-datetime="$target_time" "$binlog" >> "$recovery_sql" || {
            log ERROR "Failed to process binary log: $binlog"
            return 1
        }
    done
    log INFO "Analyzing recovery file content"

    # Count statements in recovery SQL
    local statement_pattern="INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|TRUNCATE|RENAME|REPLACE|BEGIN|COMMIT"
    local statement_count=$(grep -i -E "$statement_pattern" "$recovery_sql" | wc -l)

    # Alternative count method focusing on semicolons that terminate SQL statements
    local semicolon_count=$(grep -c ";" "$recovery_sql")

    log INFO "*** \e[1m Recovery file contains approximately $statement_count SQL statements ($semicolon_count semicolons) *** \e[0m"
    
    # Apply the recovery SQL
    log INFO "Applying recovery SQL to the database"
    
    local mysql_cmd
    if [[ -n "$MYSQL_DEFAULTS_FILE" ]]; then
        mysql_cmd="mysql --defaults-file=$MYSQL_DEFAULTS_FILE -h $MYSQL_HOST -P $MYSQL_PORT"
    else
        mysql_cmd="mysql -u $MYSQL_USER ${MYSQL_PASSWORD:+-p$MYSQL_PASSWORD} -h $MYSQL_HOST -P $MYSQL_PORT"
    fi
    
    if ! $mysql_cmd < "$recovery_sql"; then
        log ERROR "PITR failed during binary log application"
        send_notification "PITR failed during binary log application"
        return 1
    fi
    
    log INFO "PITR completed successfully to: $target_time"
    send_notification "PITR completed successfully to: $target_time"
    
    # Clean up temporary file
    #rm -f "$recovery_sql"
    
    return 0
}
# Function to clean up old backups
cleanup_old_backups() {
    log INFO "Cleaning up old backups"
    
    if [[ "$DRY_RUN" -eq 1 ]]; then
        log INFO "[DRY RUN] Would clean up backups older than $RETENTION_DAYS days"
        log INFO "[DRY RUN] Would keep at most $RETENTION_FULL full backups"
        log INFO "[DRY RUN] Would keep at most $RETENTION_INCR incremental backups"
        return 0
    fi
    
    # Clean by age
    if [[ "$RETENTION_DAYS" -gt 0 ]]; then
        log INFO "Removing backups older than $RETENTION_DAYS days"
        
        # Find and remove old full backups
        find "$BACKUP_DIR" -name "full_*" -type d -mtime +"$RETENTION_DAYS" -exec rm -rf {} \; &>/dev/null
        
        # Find and remove old incremental backups
        find "$BACKUP_DIR" -name "incr_*" -type d -mtime +"$RETENTION_DAYS" -exec rm -rf {} \; &>/dev/null
        
        # Find and remove old binary log backups
        find "$BACKUP_DIR" -name "binlogs_*" -type d -mtime +"$RETENTION_DAYS" -exec rm -rf {} \; &>/dev/null
    fi
    
    # Clean by count - keep only N most recent full backups
    if [[ "$RETENTION_FULL" -gt 0 ]]; then
        log INFO "Keeping at most $RETENTION_FULL full backups"
        
        # Get list of full backups sorted by date (oldest first)
        local full_backups
        mapfile -t full_backups < <(find "$BACKUP_DIR" -name "full_*" -type d | sort)
        
        # Calculate how many to remove
        local full_count=${#full_backups[@]}
        local remove_count=$((full_count - RETENTION_FULL))
        
        if [[ "$remove_count" -gt 0 ]]; then
            log INFO "Removing $remove_count old full backups"
            
            # Remove oldest backups
            for ((i=0; i<remove_count; i++)); do
                log DEBUG "Removing old full backup: ${full_backups[$i]}"
                rm -rf "${full_backups[$i]}"
            done
        fi
    fi
    
    # Clean by count - keep only N most recent incremental backups
    if [[ "$RETENTION_INCR" -gt 0 ]]; then
        log INFO "Keeping at most $RETENTION_INCR incremental backups"
        
        # Get list of incremental backups sorted by date (oldest first)
        local incr_backups
        mapfile -t incr_backups < <(find "$BACKUP_DIR" -name "incr_*" -type d | sort)
        
        # Calculate how many to remove
        local incr_count=${#incr_backups[@]}
        local remove_count=$((incr_count - RETENTION_INCR))
        
        if [[ "$remove_count" -gt 0 ]]; then
            log INFO "Removing $remove_count old incremental backups"
            
            # Remove oldest backups
            for ((i=0; i<remove_count; i++)); do
                log DEBUG "Removing old incremental backup: ${incr_backups[$i]}"
                rm -rf "${incr_backups[$i]}"
            done
        fi
    fi
    
    log INFO "Cleanup completed"
}

# Function to setup cronjob
setup_cron() {
    local script_path
    script_path=$(realpath "$0")
    
    if [[ "$DRY_RUN" -eq 1 ]]; then
        log INFO "[DRY RUN] Would setup cronjobs for regular backups"
        return 0
    fi
    
    log INFO "Setting up cronjobs for regular backups"
    
    # Create cron entries
    (crontab -l 2>/dev/null || echo "") | grep -v "$script_path" > /tmp/crontab.tmp
    
    # Build common parameters
    local common_params=""
    
    # Add configuration file if specified
    if [[ -n "$CONFIG_FILE" ]]; then
        common_params="$common_params --config $CONFIG_FILE"
    else
        # Otherwise, add individual parameters
        common_params="$common_params --backup-dir $BACKUP_DIR"
        
        if [[ -n "$MYSQL_DEFAULTS_FILE" ]]; then
            common_params="$common_params --defaults-file $MYSQL_DEFAULTS_FILE"
        else
            common_params="$common_params --mysql-user $MYSQL_USER"
            if [[ -n "$MYSQL_PASSWORD" ]]; then
                common_params="$common_params --mysql-password '$MYSQL_PASSWORD'"
            fi
            common_params="$common_params --mysql-host $MYSQL_HOST --mysql-port $MYSQL_PORT"
        fi
        
        if [[ "$COMPRESS" -eq 1 ]]; then
            common_params="$common_params --compress"
        fi
        
        if [[ "$ENCRYPT" -eq 1 ]]; then
            common_params="$common_params --encrypt --encrypt-key-file $ENCRYPTION_KEY_FILE"
        fi
        
        if [[ -n "$LOG_FILE" ]]; then
            common_params="$common_params --log-file $LOG_FILE"
        fi
        
        if [[ -n "$LOG_LEVEL" ]]; then
            common_params="$common_params --log-level $LOG_LEVEL"
        fi
        
        # Add notification parameters
        if [[ "$SEND_WEBHOOK" -eq 1 ]] && [[ -n "$WEBHOOK_URL" ]]; then
            common_params="$common_params --send-webhook --webhook $WEBHOOK_URL"
        fi
        
        if [[ -n "$SLACK_WEBHOOK" ]]; then
            common_params="$common_params --slack-webhook $SLACK_WEBHOOK"
        fi
        
        if [[ -n "$EMAIL_RECIPIENT" ]]; then
            common_params="$common_params --email $EMAIL_RECIPIENT"
        fi
    fi
    
    # Add weekly full backup at 1 AM on Sunday
    echo "0 1 * * 0 $script_path --type full $common_params" >> /tmp/crontab.tmp
    
    # Add daily incremental backup at 1 AM on weekdays
    echo "0 1 * * 1-6 $script_path --type incremental $common_params" >> /tmp/crontab.tmp
    
    # Add hourly binary log backup
    echo "0 * * * * $script_path --type binlog $common_params" >> /tmp/crontab.tmp
    
    # Install new crontab
    crontab /tmp/crontab.tmp
    rm -f /tmp/crontab.tmp
    
    log INFO "Cronjobs have been set up successfully"
    
    # Show current crontab
    log INFO "Current crontab:"
    crontab -l
}

# Function to generate backup report
generate_backup_report() {
    log INFO "Generating backup report"
    
    local report_file="$BACKUP_DIR/backup_report_$(date +%Y%m%d).txt"
    
    echo "MariaDB Backup Report - $(date +"$DATE_FORMAT")" > "$report_file"
    echo "==============================================" >> "$report_file"
    echo "" >> "$report_file"
    
    # Get full backups
    local full_backups
    mapfile -t full_backups < <(find "$BACKUP_DIR" -name "full_*" -type d | sort)
    
    echo "Full Backups (${#full_backups[@]}):" >> "$report_file"
    echo "------------------------" >> "$report_file"
    
    for backup in "${full_backups[@]}"; do
        local backup_date
        backup_date=$(stat -c "%y" "$backup" | cut -d'.' -f1)
        local backup_size
        backup_size=$(du -sh "$backup" | awk '{print $1}')
        echo "- $backup ($backup_date, $backup_size)" >> "$report_file"
    done
    
    echo "" >> "$report_file"
    
    # Get incremental backups
    local incr_backups
    mapfile -t incr_backups < <(find "$BACKUP_DIR" -name "incr_*" -type d | sort)
    
    echo "Incremental Backups (${#incr_backups[@]}):" >> "$report_file"
    echo "----------------------------" >> "$report_file"
    
    for backup in "${incr_backups[@]}"; do
        local backup_date
        backup_date=$(stat -c "%y" "$backup" | cut -d'.' -f1)
        local backup_size
        backup_size=$(du -sh "$backup" | awk '{print $1}')
        echo "- $backup ($backup_date, $backup_size)" >> "$report_file"
    done
    
    echo "" >> "$report_file"
    
    # Get binary log backups
    local binlog_backups
    mapfile -t binlog_backups < <(find "$BACKUP_DIR" -name "binlogs_*" -type d | sort)
    
    echo "Binary Log Backups (${#binlog_backups[@]}):" >> "$report_file"
    echo "----------------------------" >> "$report_file"
    
    for backup in "${binlog_backups[@]}"; do
        local backup_date
        backup_date=$(stat -c "%y" "$backup" | cut -d'.' -f1)
        local backup_size
        backup_size=$(du -sh "$backup" | awk '{print $1}')
        local binlog_count
        binlog_count=$(find "$backup" -name "*.0*" | wc -l)
        echo "- $backup ($backup_date, $backup_size, $binlog_count logs)" >> "$report_file"
    done
    
    echo "" >> "$report_file"
    
    # Storage summary
    local total_size
    total_size=$(du -sh "$BACKUP_DIR" | awk '{print $1}')
    local available_space
    available_space=$(df -h "$BACKUP_DIR" | awk 'NR==2 {print $4}')
    
    echo "Storage Summary:" >> "$report_file"
    echo "----------------" >> "$report_file"
    echo "Total backup size: $total_size" >> "$report_file"
    echo "Available space: $available_space" >> "$report_file"
    
    log INFO "Backup report generated: $report_file"
    
    # Send report via email if configured
    if [[ -n "$EMAIL_RECIPIENT" ]]; then
        if command -v mail &>/dev/null; then
            log INFO "Sending backup report via email to $EMAIL_RECIPIENT"
            mail -s "MariaDB Backup Report - $(date +%Y-%m-%d)" "$EMAIL_RECIPIENT" < "$report_file" || \
                log WARN "Failed to send backup report via email"
        else
            log WARN "mail command not found, cannot send backup report via email"
        fi
    fi
    
    return 0
}

# Main execution flow

# Show version if requested
if [[ "$1" == "--version" ]]; then
    echo "$(basename "$0") v$VERSION"
    exit 0
fi

# Show help if requested or no arguments provided
if [[ "$1" == "--help" || "$1" == "-h" || $# -eq 0 ]]; then
    usage
fi

# Parse command-line arguments
while [ "$#" -gt 0 ]; do
    case "$1" in
        --backup-dir)
            BACKUP_DIR="$2"
            shift 2
            ;;
        --mysql-user)
            MYSQL_USER="$2"
            shift 2
            ;;
        --mysql-password)
            MYSQL_PASSWORD="$2"
            shift 2
            ;;
        --mysql-host)
            MYSQL_HOST="$2"
            shift 2
            ;;
        --mysql-port)
            MYSQL_PORT="$2"
            shift 2
            ;;
        --mysql-datadir)
            MYSQL_DATADIR="$2"
            shift 2
            ;;
        --defaults-file)
            MYSQL_DEFAULTS_FILE="$2"
            shift 2
            ;;
        --webhook)
            WEBHOOK_URL="$2"
            shift 2
            ;;
        --send-webhook)
            SEND_WEBHOOK=1
            shift
            ;;
        --slack-webhook)
            SLACK_WEBHOOK="$2"
            shift 2
            ;;
        --email)
            EMAIL_RECIPIENT="$2"
            shift 2
            ;;
        --compress)
            COMPRESS=1
            shift
            ;;
        --compress-threads)
            COMPRESS_THREADS="$2"
            shift 2
            ;;
        --encrypt)
            ENCRYPT=1
            shift
            ;;
        --encrypt-key-file)
            ENCRYPTION_KEY_FILE="$2"
            shift 2
            ;;
        --retention)
            RETENTION_DAYS="$2"
            shift 2
            ;;
        --retention-full)
            RETENTION_FULL="$2"
            shift 2
            ;;
        --retention-incr)
            RETENTION_INCR="$2"
            shift 2
            ;;
        --type)
            BACKUP_TYPE="$2"
            shift 2
            ;;
        --restore)
            RESTORE_PATH="$2"
            shift 2
            ;;
        --pitr)
            PITR_TIME="$2"
            shift 2
            ;;
        --pitr-only)
            PITR_TIME="$2"
            PITR_ONLY=1
            shift 2
            ;;
        --test-recovery)
            TEST_RECOVERY=1
            shift
            ;;
        --parallel)
            PARALLEL_THREADS="$2"
            shift 2
            ;;
        --throttle-io)
            THROTTLE_IO="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=1
            shift
            ;;
        --force)
            FORCE=1
            shift
            ;;
        --temp-dir)
            TEMP_DIR="$2"
            shift 2
            ;;
        --ignore-errors)
            IGNORE_ERRORS=1
            shift
            ;;
        --log-file)
            LOG_FILE="$2"
            shift 2
            ;;
        --log-level)
            LOG_LEVEL="$2"
            shift 2
            ;;
        --config)
            CONFIG_FILE="$2"
            shift 2
            ;;
        --setup-cron)
            SETUP_CRON=1
            shift
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# Load configuration from file if specified
if [[ -n "$CONFIG_FILE" ]]; then
    load_config "$CONFIG_FILE"
fi

# Validate settings
validate_settings

# Print script banner
log INFO "MariaDB Backup Script v$VERSION starting"
log INFO "Backup type: $BACKUP_TYPE"
log INFO "Backup directory: $BACKUP_DIR"

# Check mariabackup installation
check_mariabackup

# Handle different operations
if [[ -n "$SETUP_CRON" ]]; then
    setup_cron
elif [[ -n "$RESTORE_PATH" ]]; then
    if [[ -n "$PITR_TIME" ]]; then
        perform_pitr "$PITR_TIME" "$RESTORE_PATH" "$PITR_ONLY"
    else
        # Determine if it's a full or incremental backup
        if [[ "$RESTORE_PATH" == *"full_"* ]]; then
            prepare_backup "$RESTORE_PATH" 1
        else
            prepare_backup "$RESTORE_PATH" 0
        fi
        restore_backup "$RESTORE_PATH"
    fi
else
    # Check if MariaDB server is running
    check_server_running
    
    # Check disk space
    check_disk_space "$BACKUP_DIR" "$MIN_DISK_SPACE"
    
    # Perform backup based on type
    case "$BACKUP_TYPE" in
        full)
            perform_full_backup
            ;;
        incremental)
            perform_incremental_backup
            ;;
        binlog)
            backup_binlogs
            ;;
        *)
            log ERROR "Unknown backup type: $BACKUP_TYPE"
            usage
            ;;
    esac
    
    # Clean up old backups
    cleanup_old_backups
    
    # Generate backup report
    generate_backup_report
fi

log INFO "MariaDB Backup Script v$VERSION completed"
exit 0
