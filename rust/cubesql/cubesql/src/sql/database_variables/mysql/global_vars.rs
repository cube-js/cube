pub const DEFAULT_VARS: &str = r#"[
  {
    "VARIABLE_NAME": "activate_all_roles_on_login",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "admin_address",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "admin_port",
    "VARIABLE_VALUE": "33062"
  },
  {
    "VARIABLE_NAME": "admin_ssl_ca",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "admin_ssl_capath",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "admin_ssl_cert",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "admin_ssl_cipher",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "admin_ssl_crl",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "admin_ssl_crlpath",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "admin_ssl_key",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "admin_tls_ciphersuites",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "admin_tls_version",
    "VARIABLE_VALUE": "TLSv1.2,TLSv1.3"
  },
  {
    "VARIABLE_NAME": "authentication_policy",
    "VARIABLE_VALUE": "*,,"
  },
  {
    "VARIABLE_NAME": "auto_generate_certs",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "auto_increment_increment",
    "VARIABLE_VALUE": "1"
  },
  {
    "VARIABLE_NAME": "auto_increment_offset",
    "VARIABLE_VALUE": "1"
  },
  {
    "VARIABLE_NAME": "autocommit",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "automatic_sp_privileges",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "avoid_temporal_upgrade",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "back_log",
    "VARIABLE_VALUE": "151"
  },
  {
    "VARIABLE_NAME": "basedir",
    "VARIABLE_VALUE": "/usr/"
  },
  {
    "VARIABLE_NAME": "big_tables",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "bind_address",
    "VARIABLE_VALUE": "*"
  },
  {
    "VARIABLE_NAME": "binlog_cache_size",
    "VARIABLE_VALUE": "32768"
  },
  {
    "VARIABLE_NAME": "binlog_checksum",
    "VARIABLE_VALUE": "CRC32"
  },
  {
    "VARIABLE_NAME": "binlog_direct_non_transactional_updates",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "binlog_encryption",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "binlog_error_action",
    "VARIABLE_VALUE": "ABORT_SERVER"
  },
  {
    "VARIABLE_NAME": "binlog_expire_logs_seconds",
    "VARIABLE_VALUE": "2592000"
  },
  {
    "VARIABLE_NAME": "binlog_format",
    "VARIABLE_VALUE": "ROW"
  },
  {
    "VARIABLE_NAME": "binlog_group_commit_sync_delay",
    "VARIABLE_VALUE": "0"
  },
  {
    "VARIABLE_NAME": "binlog_group_commit_sync_no_delay_count",
    "VARIABLE_VALUE": "0"
  },
  {
    "VARIABLE_NAME": "binlog_gtid_simple_recovery",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "binlog_max_flush_queue_time",
    "VARIABLE_VALUE": "0"
  },
  {
    "VARIABLE_NAME": "binlog_order_commits",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "binlog_rotate_encryption_master_key_at_startup",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "binlog_row_event_max_size",
    "VARIABLE_VALUE": "8192"
  },
  {
    "VARIABLE_NAME": "binlog_row_image",
    "VARIABLE_VALUE": "FULL"
  },
  {
    "VARIABLE_NAME": "binlog_row_metadata",
    "VARIABLE_VALUE": "MINIMAL"
  },
  {
    "VARIABLE_NAME": "binlog_row_value_options",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "binlog_rows_query_log_events",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "binlog_stmt_cache_size",
    "VARIABLE_VALUE": "32768"
  },
  {
    "VARIABLE_NAME": "binlog_transaction_compression",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "binlog_transaction_compression_level_zstd",
    "VARIABLE_VALUE": "3"
  },
  {
    "VARIABLE_NAME": "binlog_transaction_dependency_history_size",
    "VARIABLE_VALUE": "25000"
  },
  {
    "VARIABLE_NAME": "binlog_transaction_dependency_tracking",
    "VARIABLE_VALUE": "COMMIT_ORDER"
  },
  {
    "VARIABLE_NAME": "block_encryption_mode",
    "VARIABLE_VALUE": "aes-128-ecb"
  },
  {
    "VARIABLE_NAME": "bulk_insert_buffer_size",
    "VARIABLE_VALUE": "8388608"
  },
  {
    "VARIABLE_NAME": "caching_sha2_password_auto_generate_rsa_keys",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "caching_sha2_password_digest_rounds",
    "VARIABLE_VALUE": "5000"
  },
  {
    "VARIABLE_NAME": "caching_sha2_password_private_key_path",
    "VARIABLE_VALUE": "private_key.pem"
  },
  {
    "VARIABLE_NAME": "caching_sha2_password_public_key_path",
    "VARIABLE_VALUE": "public_key.pem"
  },
  {
    "VARIABLE_NAME": "character_set_client",
    "VARIABLE_VALUE": "utf8mb4"
  },
  {
    "VARIABLE_NAME": "sessionauto_increment_increment",
    "VARIABLE_VALUE": "1"
  },
  {
    "VARIABLE_NAME": "sessiontransaction_isolation",
    "VARIABLE_VALUE": "REPEATABLE-READ"
  },
  {
    "VARIABLE_NAME": "character_set_connection",
    "VARIABLE_VALUE": "utf8mb4"
  },
  {
    "VARIABLE_NAME": "character_set_database",
    "VARIABLE_VALUE": "utf8mb4"
  },
  {
    "VARIABLE_NAME": "character_set_filesystem",
    "VARIABLE_VALUE": "binary"
  },
  {
    "VARIABLE_NAME": "character_set_results",
    "VARIABLE_VALUE": "utf8mb4"
  },
  {
    "VARIABLE_NAME": "character_set_server",
    "VARIABLE_VALUE": "utf8mb4"
  },
  {
    "VARIABLE_NAME": "character_set_system",
    "VARIABLE_VALUE": "utf8mb3"
  },
  {
    "VARIABLE_NAME": "character_sets_dir",
    "VARIABLE_VALUE": "/usr/share/mysql-8.0/charsets/"
  },
  {
    "VARIABLE_NAME": "check_proxy_users",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "collation_connection",
    "VARIABLE_VALUE": "utf8mb4_general_ci"
  },
  {
    "VARIABLE_NAME": "collation_database",
    "VARIABLE_VALUE": "utf8mb4_0900_ai_ci"
  },
  {
    "VARIABLE_NAME": "collation_server",
    "VARIABLE_VALUE": "utf8mb4_0900_ai_ci"
  },
  {
    "VARIABLE_NAME": "completion_type",
    "VARIABLE_VALUE": "NO_CHAIN"
  },
  {
    "VARIABLE_NAME": "concurrent_insert",
    "VARIABLE_VALUE": "AUTO"
  },
  {
    "VARIABLE_NAME": "connect_timeout",
    "VARIABLE_VALUE": "10"
  },
  {
    "VARIABLE_NAME": "connection_memory_chunk_size",
    "VARIABLE_VALUE": "8912"
  },
  {
    "VARIABLE_NAME": "connection_memory_limit",
    "VARIABLE_VALUE": "18446744073709551615"
  },
  {
    "VARIABLE_NAME": "core_file",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "create_admin_listener_thread",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "cte_max_recursion_depth",
    "VARIABLE_VALUE": "1000"
  },
  {
    "VARIABLE_NAME": "datadir",
    "VARIABLE_VALUE": "/var/lib/mysql/"
  },
  {
    "VARIABLE_NAME": "default_authentication_plugin",
    "VARIABLE_VALUE": "caching_sha2_password"
  },
  {
    "VARIABLE_NAME": "default_collation_for_utf8mb4",
    "VARIABLE_VALUE": "utf8mb4_0900_ai_ci"
  },
  {
    "VARIABLE_NAME": "default_password_lifetime",
    "VARIABLE_VALUE": "0"
  },
  {
    "VARIABLE_NAME": "default_storage_engine",
    "VARIABLE_VALUE": "InnoDB"
  },
  {
    "VARIABLE_NAME": "default_table_encryption",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "default_tmp_storage_engine",
    "VARIABLE_VALUE": "InnoDB"
  },
  {
    "VARIABLE_NAME": "default_week_format",
    "VARIABLE_VALUE": "0"
  },
  {
    "VARIABLE_NAME": "delay_key_write",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "delayed_insert_limit",
    "VARIABLE_VALUE": "100"
  },
  {
    "VARIABLE_NAME": "delayed_insert_timeout",
    "VARIABLE_VALUE": "300"
  },
  {
    "VARIABLE_NAME": "delayed_queue_size",
    "VARIABLE_VALUE": "1000"
  },
  {
    "VARIABLE_NAME": "disabled_storage_engines",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "disconnect_on_expired_password",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "div_precision_increment",
    "VARIABLE_VALUE": "4"
  },
  {
    "VARIABLE_NAME": "end_markers_in_json",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "enforce_gtid_consistency",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "eq_range_index_dive_limit",
    "VARIABLE_VALUE": "200"
  },
  {
    "VARIABLE_NAME": "event_scheduler",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "expire_logs_days",
    "VARIABLE_VALUE": "0"
  },
  {
    "VARIABLE_NAME": "explicit_defaults_for_timestamp",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "flush",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "flush_time",
    "VARIABLE_VALUE": "0"
  },
  {
    "VARIABLE_NAME": "foreign_key_checks",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "ft_boolean_syntax",
    "VARIABLE_VALUE": "+ -><()~*:\"\"&|"
  },
  {
    "VARIABLE_NAME": "ft_max_word_len",
    "VARIABLE_VALUE": "84"
  },
  {
    "VARIABLE_NAME": "ft_min_word_len",
    "VARIABLE_VALUE": "4"
  },
  {
    "VARIABLE_NAME": "ft_query_expansion_limit",
    "VARIABLE_VALUE": "20"
  },
  {
    "VARIABLE_NAME": "ft_stopword_file",
    "VARIABLE_VALUE": "(built-in)"
  },
  {
    "VARIABLE_NAME": "general_log",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "general_log_file",
    "VARIABLE_VALUE": "/var/lib/mysql/5c7d972c6d46.log"
  },
  {
    "VARIABLE_NAME": "generated_random_password_length",
    "VARIABLE_VALUE": "20"
  },
  {
    "VARIABLE_NAME": "global_connection_memory_limit",
    "VARIABLE_VALUE": "18446744073709551615"
  },
  {
    "VARIABLE_NAME": "global_connection_memory_tracking",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "group_concat_max_len",
    "VARIABLE_VALUE": "1024"
  },
  {
    "VARIABLE_NAME": "group_replication_consistency",
    "VARIABLE_VALUE": "EVENTUAL"
  },
  {
    "VARIABLE_NAME": "gtid_executed",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "gtid_executed_compression_period",
    "VARIABLE_VALUE": "0"
  },
  {
    "VARIABLE_NAME": "gtid_mode",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "gtid_owned",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "gtid_purged",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "have_compress",
    "VARIABLE_VALUE": "YES"
  },
  {
    "VARIABLE_NAME": "have_dynamic_loading",
    "VARIABLE_VALUE": "YES"
  },
  {
    "VARIABLE_NAME": "have_geometry",
    "VARIABLE_VALUE": "YES"
  },
  {
    "VARIABLE_NAME": "have_openssl",
    "VARIABLE_VALUE": "YES"
  },
  {
    "VARIABLE_NAME": "have_profiling",
    "VARIABLE_VALUE": "YES"
  },
  {
    "VARIABLE_NAME": "have_query_cache",
    "VARIABLE_VALUE": "NO"
  },
  {
    "VARIABLE_NAME": "have_rtree_keys",
    "VARIABLE_VALUE": "YES"
  },
  {
    "VARIABLE_NAME": "have_ssl",
    "VARIABLE_VALUE": "YES"
  },
  {
    "VARIABLE_NAME": "have_statement_timeout",
    "VARIABLE_VALUE": "YES"
  },
  {
    "VARIABLE_NAME": "have_symlink",
    "VARIABLE_VALUE": "DISABLED"
  },
  {
    "VARIABLE_NAME": "histogram_generation_max_mem_size",
    "VARIABLE_VALUE": "20000000"
  },
  {
    "VARIABLE_NAME": "host_cache_size",
    "VARIABLE_VALUE": "279"
  },
  {
    "VARIABLE_NAME": "hostname",
    "VARIABLE_VALUE": "5c7d972c6d46"
  },
  {
    "VARIABLE_NAME": "information_schema_stats_expiry",
    "VARIABLE_VALUE": "86400"
  },
  {
    "VARIABLE_NAME": "init_connect",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "init_file",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "init_replica",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "init_slave",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "innodb_adaptive_flushing",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "innodb_adaptive_flushing_lwm",
    "VARIABLE_VALUE": "10"
  },
  {
    "VARIABLE_NAME": "innodb_adaptive_hash_index",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "innodb_adaptive_hash_index_parts",
    "VARIABLE_VALUE": "8"
  },
  {
    "VARIABLE_NAME": "innodb_adaptive_max_sleep_delay",
    "VARIABLE_VALUE": "150000"
  },
  {
    "VARIABLE_NAME": "innodb_api_bk_commit_interval",
    "VARIABLE_VALUE": "5"
  },
  {
    "VARIABLE_NAME": "innodb_api_disable_rowlock",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "innodb_api_enable_binlog",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "innodb_api_enable_mdl",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "innodb_api_trx_level",
    "VARIABLE_VALUE": "0"
  },
  {
    "VARIABLE_NAME": "innodb_autoextend_increment",
    "VARIABLE_VALUE": "64"
  },
  {
    "VARIABLE_NAME": "innodb_autoinc_lock_mode",
    "VARIABLE_VALUE": "2"
  },
  {
    "VARIABLE_NAME": "innodb_buffer_pool_chunk_size",
    "VARIABLE_VALUE": "134217728"
  },
  {
    "VARIABLE_NAME": "innodb_buffer_pool_dump_at_shutdown",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "innodb_buffer_pool_dump_now",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "innodb_buffer_pool_dump_pct",
    "VARIABLE_VALUE": "25"
  },
  {
    "VARIABLE_NAME": "innodb_buffer_pool_filename",
    "VARIABLE_VALUE": "ib_buffer_pool"
  },
  {
    "VARIABLE_NAME": "innodb_buffer_pool_in_core_file",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "innodb_buffer_pool_instances",
    "VARIABLE_VALUE": "1"
  },
  {
    "VARIABLE_NAME": "innodb_buffer_pool_load_abort",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "innodb_buffer_pool_load_at_startup",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "innodb_buffer_pool_load_now",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "innodb_buffer_pool_size",
    "VARIABLE_VALUE": "134217728"
  },
  {
    "VARIABLE_NAME": "innodb_change_buffer_max_size",
    "VARIABLE_VALUE": "25"
  },
  {
    "VARIABLE_NAME": "innodb_change_buffering",
    "VARIABLE_VALUE": "all"
  },
  {
    "VARIABLE_NAME": "innodb_checksum_algorithm",
    "VARIABLE_VALUE": "crc32"
  },
  {
    "VARIABLE_NAME": "innodb_cmp_per_index_enabled",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "innodb_commit_concurrency",
    "VARIABLE_VALUE": "0"
  },
  {
    "VARIABLE_NAME": "innodb_compression_failure_threshold_pct",
    "VARIABLE_VALUE": "5"
  },
  {
    "VARIABLE_NAME": "innodb_compression_level",
    "VARIABLE_VALUE": "6"
  },
  {
    "VARIABLE_NAME": "innodb_compression_pad_pct_max",
    "VARIABLE_VALUE": "50"
  },
  {
    "VARIABLE_NAME": "innodb_concurrency_tickets",
    "VARIABLE_VALUE": "5000"
  },
  {
    "VARIABLE_NAME": "innodb_data_file_path",
    "VARIABLE_VALUE": "ibdata1:12M:autoextend"
  },
  {
    "VARIABLE_NAME": "innodb_data_home_dir",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "innodb_ddl_buffer_size",
    "VARIABLE_VALUE": "1048576"
  },
  {
    "VARIABLE_NAME": "innodb_ddl_threads",
    "VARIABLE_VALUE": "4"
  },
  {
    "VARIABLE_NAME": "innodb_deadlock_detect",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "innodb_dedicated_server",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "innodb_default_row_format",
    "VARIABLE_VALUE": "dynamic"
  },
  {
    "VARIABLE_NAME": "innodb_directories",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "innodb_disable_sort_file_cache",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "innodb_doublewrite",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "innodb_doublewrite_batch_size",
    "VARIABLE_VALUE": "0"
  },
  {
    "VARIABLE_NAME": "innodb_doublewrite_dir",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "innodb_doublewrite_files",
    "VARIABLE_VALUE": "2"
  },
  {
    "VARIABLE_NAME": "innodb_doublewrite_pages",
    "VARIABLE_VALUE": "4"
  },
  {
    "VARIABLE_NAME": "innodb_extend_and_initialize",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "innodb_fast_shutdown",
    "VARIABLE_VALUE": "1"
  },
  {
    "VARIABLE_NAME": "innodb_file_per_table",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "innodb_fill_factor",
    "VARIABLE_VALUE": "100"
  },
  {
    "VARIABLE_NAME": "innodb_flush_log_at_timeout",
    "VARIABLE_VALUE": "1"
  },
  {
    "VARIABLE_NAME": "innodb_flush_log_at_trx_commit",
    "VARIABLE_VALUE": "1"
  },
  {
    "VARIABLE_NAME": "innodb_flush_method",
    "VARIABLE_VALUE": "fsync"
  },
  {
    "VARIABLE_NAME": "innodb_flush_neighbors",
    "VARIABLE_VALUE": "0"
  },
  {
    "VARIABLE_NAME": "innodb_flush_sync",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "innodb_flushing_avg_loops",
    "VARIABLE_VALUE": "30"
  },
  {
    "VARIABLE_NAME": "innodb_force_load_corrupted",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "innodb_force_recovery",
    "VARIABLE_VALUE": "0"
  },
  {
    "VARIABLE_NAME": "innodb_fsync_threshold",
    "VARIABLE_VALUE": "0"
  },
  {
    "VARIABLE_NAME": "innodb_ft_aux_table",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "innodb_ft_cache_size",
    "VARIABLE_VALUE": "8000000"
  },
  {
    "VARIABLE_NAME": "innodb_ft_enable_diag_print",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "innodb_ft_enable_stopword",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "innodb_ft_max_token_size",
    "VARIABLE_VALUE": "84"
  },
  {
    "VARIABLE_NAME": "innodb_ft_min_token_size",
    "VARIABLE_VALUE": "3"
  },
  {
    "VARIABLE_NAME": "innodb_ft_num_word_optimize",
    "VARIABLE_VALUE": "2000"
  },
  {
    "VARIABLE_NAME": "innodb_ft_result_cache_limit",
    "VARIABLE_VALUE": "2000000000"
  },
  {
    "VARIABLE_NAME": "innodb_ft_server_stopword_table",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "innodb_ft_sort_pll_degree",
    "VARIABLE_VALUE": "2"
  },
  {
    "VARIABLE_NAME": "innodb_ft_total_cache_size",
    "VARIABLE_VALUE": "640000000"
  },
  {
    "VARIABLE_NAME": "innodb_ft_user_stopword_table",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "innodb_idle_flush_pct",
    "VARIABLE_VALUE": "100"
  },
  {
    "VARIABLE_NAME": "innodb_io_capacity",
    "VARIABLE_VALUE": "200"
  },
  {
    "VARIABLE_NAME": "innodb_io_capacity_max",
    "VARIABLE_VALUE": "2000"
  },
  {
    "VARIABLE_NAME": "innodb_lock_wait_timeout",
    "VARIABLE_VALUE": "50"
  },
  {
    "VARIABLE_NAME": "innodb_log_buffer_size",
    "VARIABLE_VALUE": "16777216"
  },
  {
    "VARIABLE_NAME": "innodb_log_checksums",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "innodb_log_compressed_pages",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "innodb_log_file_size",
    "VARIABLE_VALUE": "50331648"
  },
  {
    "VARIABLE_NAME": "innodb_log_files_in_group",
    "VARIABLE_VALUE": "2"
  },
  {
    "VARIABLE_NAME": "innodb_log_group_home_dir",
    "VARIABLE_VALUE": "./"
  },
  {
    "VARIABLE_NAME": "innodb_log_spin_cpu_abs_lwm",
    "VARIABLE_VALUE": "80"
  },
  {
    "VARIABLE_NAME": "innodb_log_spin_cpu_pct_hwm",
    "VARIABLE_VALUE": "50"
  },
  {
    "VARIABLE_NAME": "innodb_log_wait_for_flush_spin_hwm",
    "VARIABLE_VALUE": "400"
  },
  {
    "VARIABLE_NAME": "innodb_log_write_ahead_size",
    "VARIABLE_VALUE": "8192"
  },
  {
    "VARIABLE_NAME": "innodb_log_writer_threads",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "innodb_lru_scan_depth",
    "VARIABLE_VALUE": "1024"
  },
  {
    "VARIABLE_NAME": "innodb_max_dirty_pages_pct",
    "VARIABLE_VALUE": "90.000000"
  },
  {
    "VARIABLE_NAME": "innodb_max_dirty_pages_pct_lwm",
    "VARIABLE_VALUE": "10.000000"
  },
  {
    "VARIABLE_NAME": "innodb_max_purge_lag",
    "VARIABLE_VALUE": "0"
  },
  {
    "VARIABLE_NAME": "innodb_max_purge_lag_delay",
    "VARIABLE_VALUE": "0"
  },
  {
    "VARIABLE_NAME": "innodb_max_undo_log_size",
    "VARIABLE_VALUE": "1073741824"
  },
  {
    "VARIABLE_NAME": "innodb_monitor_disable",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "innodb_monitor_enable",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "innodb_monitor_reset",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "innodb_monitor_reset_all",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "innodb_numa_interleave",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "innodb_old_blocks_pct",
    "VARIABLE_VALUE": "37"
  },
  {
    "VARIABLE_NAME": "innodb_old_blocks_time",
    "VARIABLE_VALUE": "1000"
  },
  {
    "VARIABLE_NAME": "innodb_online_alter_log_max_size",
    "VARIABLE_VALUE": "134217728"
  },
  {
    "VARIABLE_NAME": "innodb_open_files",
    "VARIABLE_VALUE": "4000"
  },
  {
    "VARIABLE_NAME": "innodb_optimize_fulltext_only",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "innodb_page_cleaners",
    "VARIABLE_VALUE": "1"
  },
  {
    "VARIABLE_NAME": "innodb_page_size",
    "VARIABLE_VALUE": "16384"
  },
  {
    "VARIABLE_NAME": "innodb_parallel_read_threads",
    "VARIABLE_VALUE": "4"
  },
  {
    "VARIABLE_NAME": "innodb_print_all_deadlocks",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "innodb_print_ddl_logs",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "innodb_purge_batch_size",
    "VARIABLE_VALUE": "300"
  },
  {
    "VARIABLE_NAME": "innodb_purge_rseg_truncate_frequency",
    "VARIABLE_VALUE": "128"
  },
  {
    "VARIABLE_NAME": "innodb_purge_threads",
    "VARIABLE_VALUE": "4"
  },
  {
    "VARIABLE_NAME": "innodb_random_read_ahead",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "innodb_read_ahead_threshold",
    "VARIABLE_VALUE": "56"
  },
  {
    "VARIABLE_NAME": "innodb_read_io_threads",
    "VARIABLE_VALUE": "4"
  },
  {
    "VARIABLE_NAME": "innodb_read_only",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "innodb_redo_log_archive_dirs",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "innodb_redo_log_encrypt",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "innodb_replication_delay",
    "VARIABLE_VALUE": "0"
  },
  {
    "VARIABLE_NAME": "innodb_rollback_on_timeout",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "innodb_rollback_segments",
    "VARIABLE_VALUE": "128"
  },
  {
    "VARIABLE_NAME": "innodb_segment_reserve_factor",
    "VARIABLE_VALUE": "12.500000"
  },
  {
    "VARIABLE_NAME": "innodb_sort_buffer_size",
    "VARIABLE_VALUE": "1048576"
  },
  {
    "VARIABLE_NAME": "innodb_spin_wait_delay",
    "VARIABLE_VALUE": "6"
  },
  {
    "VARIABLE_NAME": "innodb_spin_wait_pause_multiplier",
    "VARIABLE_VALUE": "50"
  },
  {
    "VARIABLE_NAME": "innodb_stats_auto_recalc",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "innodb_stats_include_delete_marked",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "innodb_stats_method",
    "VARIABLE_VALUE": "nulls_equal"
  },
  {
    "VARIABLE_NAME": "innodb_stats_on_metadata",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "innodb_stats_persistent",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "innodb_stats_persistent_sample_pages",
    "VARIABLE_VALUE": "20"
  },
  {
    "VARIABLE_NAME": "innodb_stats_transient_sample_pages",
    "VARIABLE_VALUE": "8"
  },
  {
    "VARIABLE_NAME": "innodb_status_output",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "innodb_status_output_locks",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "innodb_strict_mode",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "innodb_sync_array_size",
    "VARIABLE_VALUE": "1"
  },
  {
    "VARIABLE_NAME": "innodb_sync_spin_loops",
    "VARIABLE_VALUE": "30"
  },
  {
    "VARIABLE_NAME": "innodb_table_locks",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "innodb_temp_data_file_path",
    "VARIABLE_VALUE": "ibtmp1:12M:autoextend"
  },
  {
    "VARIABLE_NAME": "innodb_temp_tablespaces_dir",
    "VARIABLE_VALUE": "./#innodb_temp/"
  },
  {
    "VARIABLE_NAME": "innodb_thread_concurrency",
    "VARIABLE_VALUE": "0"
  },
  {
    "VARIABLE_NAME": "innodb_thread_sleep_delay",
    "VARIABLE_VALUE": "10000"
  },
  {
    "VARIABLE_NAME": "innodb_tmpdir",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "innodb_undo_directory",
    "VARIABLE_VALUE": "./"
  },
  {
    "VARIABLE_NAME": "innodb_undo_log_encrypt",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "innodb_undo_log_truncate",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "innodb_undo_tablespaces",
    "VARIABLE_VALUE": "2"
  },
  {
    "VARIABLE_NAME": "innodb_use_fdatasync",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "innodb_use_native_aio",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "innodb_validate_tablespace_paths",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "innodb_version",
    "VARIABLE_VALUE": "8.0.28"
  },
  {
    "VARIABLE_NAME": "innodb_write_io_threads",
    "VARIABLE_VALUE": "4"
  },
  {
    "VARIABLE_NAME": "interactive_timeout",
    "VARIABLE_VALUE": "28800"
  },
  {
    "VARIABLE_NAME": "internal_tmp_mem_storage_engine",
    "VARIABLE_VALUE": "TempTable"
  },
  {
    "VARIABLE_NAME": "join_buffer_size",
    "VARIABLE_VALUE": "262144"
  },
  {
    "VARIABLE_NAME": "keep_files_on_create",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "key_buffer_size",
    "VARIABLE_VALUE": "8388608"
  },
  {
    "VARIABLE_NAME": "key_cache_age_threshold",
    "VARIABLE_VALUE": "300"
  },
  {
    "VARIABLE_NAME": "key_cache_block_size",
    "VARIABLE_VALUE": "1024"
  },
  {
    "VARIABLE_NAME": "key_cache_division_limit",
    "VARIABLE_VALUE": "100"
  },
  {
    "VARIABLE_NAME": "keyring_operations",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "large_files_support",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "large_page_size",
    "VARIABLE_VALUE": "0"
  },
  {
    "VARIABLE_NAME": "large_pages",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "lc_messages",
    "VARIABLE_VALUE": "en_US"
  },
  {
    "VARIABLE_NAME": "lc_messages_dir",
    "VARIABLE_VALUE": "/usr/share/mysql-8.0/"
  },
  {
    "VARIABLE_NAME": "lc_time_names",
    "VARIABLE_VALUE": "en_US"
  },
  {
    "VARIABLE_NAME": "license",
    "VARIABLE_VALUE": "Apache 2"
  },
  {
    "VARIABLE_NAME": "local_infile",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "lock_wait_timeout",
    "VARIABLE_VALUE": "31536000"
  },
  {
    "VARIABLE_NAME": "locked_in_memory",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "log_bin",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "log_bin_basename",
    "VARIABLE_VALUE": "/var/lib/mysql/binlog"
  },
  {
    "VARIABLE_NAME": "log_bin_index",
    "VARIABLE_VALUE": "/var/lib/mysql/binlog.index"
  },
  {
    "VARIABLE_NAME": "log_bin_trust_function_creators",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "log_bin_use_v1_row_events",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "log_error",
    "VARIABLE_VALUE": "stderr"
  },
  {
    "VARIABLE_NAME": "log_error_services",
    "VARIABLE_VALUE": "log_filter_internal; log_sink_internal"
  },
  {
    "VARIABLE_NAME": "log_error_suppression_list",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "log_error_verbosity",
    "VARIABLE_VALUE": "2"
  },
  {
    "VARIABLE_NAME": "log_output",
    "VARIABLE_VALUE": "FILE"
  },
  {
    "VARIABLE_NAME": "log_queries_not_using_indexes",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "log_raw",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "log_replica_updates",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "log_slave_updates",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "log_slow_admin_statements",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "log_slow_extra",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "log_slow_replica_statements",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "log_slow_slave_statements",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "log_statements_unsafe_for_binlog",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "log_throttle_queries_not_using_indexes",
    "VARIABLE_VALUE": "0"
  },
  {
    "VARIABLE_NAME": "log_timestamps",
    "VARIABLE_VALUE": "UTC"
  },
  {
    "VARIABLE_NAME": "long_query_time",
    "VARIABLE_VALUE": "10.000000"
  },
  {
    "VARIABLE_NAME": "low_priority_updates",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "lower_case_file_system",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "lower_case_table_names",
    "VARIABLE_VALUE": "0"
  },
  {
    "VARIABLE_NAME": "mandatory_roles",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "master_info_repository",
    "VARIABLE_VALUE": "TABLE"
  },
  {
    "VARIABLE_NAME": "tx_read_only",
    "VARIABLE_VALUE": "false"
  },
  {
    "VARIABLE_NAME": "master_verify_checksum",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "max_allowed_packet",
    "VARIABLE_VALUE": "67108864"
  },
  {
    "VARIABLE_NAME": "max_binlog_cache_size",
    "VARIABLE_VALUE": "18446744073709547520"
  },
  {
    "VARIABLE_NAME": "max_binlog_size",
    "VARIABLE_VALUE": "1073741824"
  },
  {
    "VARIABLE_NAME": "max_binlog_stmt_cache_size",
    "VARIABLE_VALUE": "18446744073709547520"
  },
  {
    "VARIABLE_NAME": "max_connect_errors",
    "VARIABLE_VALUE": "100"
  },
  {
    "VARIABLE_NAME": "max_connections",
    "VARIABLE_VALUE": "151"
  },
  {
    "VARIABLE_NAME": "max_delayed_threads",
    "VARIABLE_VALUE": "20"
  },
  {
    "VARIABLE_NAME": "max_digest_length",
    "VARIABLE_VALUE": "1024"
  },
  {
    "VARIABLE_NAME": "max_error_count",
    "VARIABLE_VALUE": "1024"
  },
  {
    "VARIABLE_NAME": "max_execution_time",
    "VARIABLE_VALUE": "0"
  },
  {
    "VARIABLE_NAME": "max_heap_table_size",
    "VARIABLE_VALUE": "16777216"
  },
  {
    "VARIABLE_NAME": "max_insert_delayed_threads",
    "VARIABLE_VALUE": "20"
  },
  {
    "VARIABLE_NAME": "max_join_size",
    "VARIABLE_VALUE": "18446744073709551615"
  },
  {
    "VARIABLE_NAME": "max_length_for_sort_data",
    "VARIABLE_VALUE": "4096"
  },
  {
    "VARIABLE_NAME": "max_points_in_geometry",
    "VARIABLE_VALUE": "65536"
  },
  {
    "VARIABLE_NAME": "max_prepared_stmt_count",
    "VARIABLE_VALUE": "16382"
  },
  {
    "VARIABLE_NAME": "max_relay_log_size",
    "VARIABLE_VALUE": "0"
  },
  {
    "VARIABLE_NAME": "max_seeks_for_key",
    "VARIABLE_VALUE": "18446744073709551615"
  },
  {
    "VARIABLE_NAME": "max_sort_length",
    "VARIABLE_VALUE": "1024"
  },
  {
    "VARIABLE_NAME": "max_sp_recursion_depth",
    "VARIABLE_VALUE": "0"
  },
  {
    "VARIABLE_NAME": "max_user_connections",
    "VARIABLE_VALUE": "0"
  },
  {
    "VARIABLE_NAME": "max_write_lock_count",
    "VARIABLE_VALUE": "18446744073709551615"
  },
  {
    "VARIABLE_NAME": "min_examined_row_limit",
    "VARIABLE_VALUE": "0"
  },
  {
    "VARIABLE_NAME": "myisam_data_pointer_size",
    "VARIABLE_VALUE": "6"
  },
  {
    "VARIABLE_NAME": "myisam_max_sort_file_size",
    "VARIABLE_VALUE": "9223372036853727232"
  },
  {
    "VARIABLE_NAME": "myisam_mmap_size",
    "VARIABLE_VALUE": "18446744073709551615"
  },
  {
    "VARIABLE_NAME": "myisam_recover_options",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "myisam_repair_threads",
    "VARIABLE_VALUE": "1"
  },
  {
    "VARIABLE_NAME": "myisam_sort_buffer_size",
    "VARIABLE_VALUE": "8388608"
  },
  {
    "VARIABLE_NAME": "myisam_stats_method",
    "VARIABLE_VALUE": "nulls_unequal"
  },
  {
    "VARIABLE_NAME": "myisam_use_mmap",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "mysql_native_password_proxy_users",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "mysqlx_bind_address",
    "VARIABLE_VALUE": "*"
  },
  {
    "VARIABLE_NAME": "mysqlx_compression_algorithms",
    "VARIABLE_VALUE": "DEFLATE_STREAM,LZ4_MESSAGE,ZSTD_STREAM"
  },
  {
    "VARIABLE_NAME": "mysqlx_connect_timeout",
    "VARIABLE_VALUE": "30"
  },
  {
    "VARIABLE_NAME": "mysqlx_deflate_default_compression_level",
    "VARIABLE_VALUE": "3"
  },
  {
    "VARIABLE_NAME": "mysqlx_deflate_max_client_compression_level",
    "VARIABLE_VALUE": "5"
  },
  {
    "VARIABLE_NAME": "mysqlx_document_id_unique_prefix",
    "VARIABLE_VALUE": "0"
  },
  {
    "VARIABLE_NAME": "mysqlx_enable_hello_notice",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "mysqlx_idle_worker_thread_timeout",
    "VARIABLE_VALUE": "60"
  },
  {
    "VARIABLE_NAME": "mysqlx_interactive_timeout",
    "VARIABLE_VALUE": "28800"
  },
  {
    "VARIABLE_NAME": "mysqlx_lz4_default_compression_level",
    "VARIABLE_VALUE": "2"
  },
  {
    "VARIABLE_NAME": "mysqlx_lz4_max_client_compression_level",
    "VARIABLE_VALUE": "8"
  },
  {
    "VARIABLE_NAME": "mysqlx_max_allowed_packet",
    "VARIABLE_VALUE": "67108864"
  },
  {
    "VARIABLE_NAME": "mysqlx_max_connections",
    "VARIABLE_VALUE": "100"
  },
  {
    "VARIABLE_NAME": "mysqlx_min_worker_threads",
    "VARIABLE_VALUE": "2"
  },
  {
    "VARIABLE_NAME": "mysqlx_port",
    "VARIABLE_VALUE": "33060"
  },
  {
    "VARIABLE_NAME": "mysqlx_port_open_timeout",
    "VARIABLE_VALUE": "0"
  },
  {
    "VARIABLE_NAME": "mysqlx_read_timeout",
    "VARIABLE_VALUE": "30"
  },
  {
    "VARIABLE_NAME": "mysqlx_socket",
    "VARIABLE_VALUE": "/var/run/mysqld/mysqlx.sock"
  },
  {
    "VARIABLE_NAME": "mysqlx_ssl_ca",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "mysqlx_ssl_capath",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "mysqlx_ssl_cert",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "mysqlx_ssl_cipher",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "mysqlx_ssl_crl",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "mysqlx_ssl_crlpath",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "mysqlx_ssl_key",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "mysqlx_wait_timeout",
    "VARIABLE_VALUE": "28800"
  },
  {
    "VARIABLE_NAME": "mysqlx_write_timeout",
    "VARIABLE_VALUE": "60"
  },
  {
    "VARIABLE_NAME": "mysqlx_zstd_default_compression_level",
    "VARIABLE_VALUE": "3"
  },
  {
    "VARIABLE_NAME": "mysqlx_zstd_max_client_compression_level",
    "VARIABLE_VALUE": "11"
  },
  {
    "VARIABLE_NAME": "net_buffer_length",
    "VARIABLE_VALUE": "16384"
  },
  {
    "VARIABLE_NAME": "net_read_timeout",
    "VARIABLE_VALUE": "30"
  },
  {
    "VARIABLE_NAME": "net_retry_count",
    "VARIABLE_VALUE": "10"
  },
  {
    "VARIABLE_NAME": "net_write_timeout",
    "VARIABLE_VALUE": "600"
  },
  {
    "VARIABLE_NAME": "new",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "ngram_token_size",
    "VARIABLE_VALUE": "2"
  },
  {
    "VARIABLE_NAME": "offline_mode",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "old",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "old_alter_table",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "open_files_limit",
    "VARIABLE_VALUE": "1048576"
  },
  {
    "VARIABLE_NAME": "optimizer_prune_level",
    "VARIABLE_VALUE": "1"
  },
  {
    "VARIABLE_NAME": "optimizer_search_depth",
    "VARIABLE_VALUE": "62"
  },
  {
    "VARIABLE_NAME": "optimizer_switch",
    "VARIABLE_VALUE": "index_merge=on,index_merge_union=on,index_merge_sort_union=on,index_merge_intersection=on,engine_condition_pushdown=on,index_condition_pushdown=on,mrr=on,mrr_cost_based=on,block_nested_loop=on,batched_key_access=off,materialization=on,semijoin=on,loosescan=on,firstmatch=on,duplicateweedout=on,subquery_materialization_cost_based=on,use_index_extensions=on,condition_fanout_filter=on,derived_merge=on,use_invisible_indexes=off,skip_scan=on,hash_join=on,subquery_to_derived=off,prefer_ordering_index=on,hypergraph_optimizer=off,derived_condition_pushdown=on"
  },
  {
    "VARIABLE_NAME": "optimizer_trace",
    "VARIABLE_VALUE": "enabled=off,one_line=off"
  },
  {
    "VARIABLE_NAME": "optimizer_trace_features",
    "VARIABLE_VALUE": "greedy_search=on,range_optimizer=on,dynamic_range=on,repeated_subselect=on"
  },
  {
    "VARIABLE_NAME": "optimizer_trace_limit",
    "VARIABLE_VALUE": "1"
  },
  {
    "VARIABLE_NAME": "optimizer_trace_max_mem_size",
    "VARIABLE_VALUE": "1048576"
  },
  {
    "VARIABLE_NAME": "optimizer_trace_offset",
    "VARIABLE_VALUE": "-1"
  },
  {
    "VARIABLE_NAME": "parser_max_mem_size",
    "VARIABLE_VALUE": "18446744073709551615"
  },
  {
    "VARIABLE_NAME": "partial_revokes",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "password_history",
    "VARIABLE_VALUE": "0"
  },
  {
    "VARIABLE_NAME": "password_require_current",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "password_reuse_interval",
    "VARIABLE_VALUE": "0"
  },
  {
    "VARIABLE_NAME": "performance_schema",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "performance_schema_accounts_size",
    "VARIABLE_VALUE": "-1"
  },
  {
    "VARIABLE_NAME": "performance_schema_digests_size",
    "VARIABLE_VALUE": "10000"
  },
  {
    "VARIABLE_NAME": "performance_schema_error_size",
    "VARIABLE_VALUE": "5035"
  },
  {
    "VARIABLE_NAME": "performance_schema_events_stages_history_long_size",
    "VARIABLE_VALUE": "10000"
  },
  {
    "VARIABLE_NAME": "performance_schema_events_stages_history_size",
    "VARIABLE_VALUE": "10"
  },
  {
    "VARIABLE_NAME": "performance_schema_events_statements_history_long_size",
    "VARIABLE_VALUE": "10000"
  },
  {
    "VARIABLE_NAME": "performance_schema_events_statements_history_size",
    "VARIABLE_VALUE": "10"
  },
  {
    "VARIABLE_NAME": "performance_schema_events_transactions_history_long_size",
    "VARIABLE_VALUE": "10000"
  },
  {
    "VARIABLE_NAME": "performance_schema_events_transactions_history_size",
    "VARIABLE_VALUE": "10"
  },
  {
    "VARIABLE_NAME": "performance_schema_events_waits_history_long_size",
    "VARIABLE_VALUE": "10000"
  },
  {
    "VARIABLE_NAME": "performance_schema_events_waits_history_size",
    "VARIABLE_VALUE": "10"
  },
  {
    "VARIABLE_NAME": "performance_schema_hosts_size",
    "VARIABLE_VALUE": "-1"
  },
  {
    "VARIABLE_NAME": "performance_schema_max_cond_classes",
    "VARIABLE_VALUE": "150"
  },
  {
    "VARIABLE_NAME": "performance_schema_max_cond_instances",
    "VARIABLE_VALUE": "-1"
  },
  {
    "VARIABLE_NAME": "performance_schema_max_digest_length",
    "VARIABLE_VALUE": "1024"
  },
  {
    "VARIABLE_NAME": "performance_schema_max_digest_sample_age",
    "VARIABLE_VALUE": "60"
  },
  {
    "VARIABLE_NAME": "performance_schema_max_file_classes",
    "VARIABLE_VALUE": "80"
  },
  {
    "VARIABLE_NAME": "performance_schema_max_file_handles",
    "VARIABLE_VALUE": "32768"
  },
  {
    "VARIABLE_NAME": "performance_schema_max_file_instances",
    "VARIABLE_VALUE": "-1"
  },
  {
    "VARIABLE_NAME": "performance_schema_max_index_stat",
    "VARIABLE_VALUE": "-1"
  },
  {
    "VARIABLE_NAME": "performance_schema_max_memory_classes",
    "VARIABLE_VALUE": "450"
  },
  {
    "VARIABLE_NAME": "performance_schema_max_metadata_locks",
    "VARIABLE_VALUE": "-1"
  },
  {
    "VARIABLE_NAME": "performance_schema_max_mutex_classes",
    "VARIABLE_VALUE": "350"
  },
  {
    "VARIABLE_NAME": "performance_schema_max_mutex_instances",
    "VARIABLE_VALUE": "-1"
  },
  {
    "VARIABLE_NAME": "performance_schema_max_prepared_statements_instances",
    "VARIABLE_VALUE": "-1"
  },
  {
    "VARIABLE_NAME": "performance_schema_max_program_instances",
    "VARIABLE_VALUE": "-1"
  },
  {
    "VARIABLE_NAME": "performance_schema_max_rwlock_classes",
    "VARIABLE_VALUE": "60"
  },
  {
    "VARIABLE_NAME": "performance_schema_max_rwlock_instances",
    "VARIABLE_VALUE": "-1"
  },
  {
    "VARIABLE_NAME": "performance_schema_max_socket_classes",
    "VARIABLE_VALUE": "10"
  },
  {
    "VARIABLE_NAME": "performance_schema_max_socket_instances",
    "VARIABLE_VALUE": "-1"
  },
  {
    "VARIABLE_NAME": "performance_schema_max_sql_text_length",
    "VARIABLE_VALUE": "1024"
  },
  {
    "VARIABLE_NAME": "performance_schema_max_stage_classes",
    "VARIABLE_VALUE": "175"
  },
  {
    "VARIABLE_NAME": "performance_schema_max_statement_classes",
    "VARIABLE_VALUE": "219"
  },
  {
    "VARIABLE_NAME": "performance_schema_max_statement_stack",
    "VARIABLE_VALUE": "10"
  },
  {
    "VARIABLE_NAME": "performance_schema_max_table_handles",
    "VARIABLE_VALUE": "-1"
  },
  {
    "VARIABLE_NAME": "performance_schema_max_table_instances",
    "VARIABLE_VALUE": "-1"
  },
  {
    "VARIABLE_NAME": "performance_schema_max_table_lock_stat",
    "VARIABLE_VALUE": "-1"
  },
  {
    "VARIABLE_NAME": "performance_schema_max_thread_classes",
    "VARIABLE_VALUE": "100"
  },
  {
    "VARIABLE_NAME": "performance_schema_max_thread_instances",
    "VARIABLE_VALUE": "-1"
  },
  {
    "VARIABLE_NAME": "performance_schema_session_connect_attrs_size",
    "VARIABLE_VALUE": "512"
  },
  {
    "VARIABLE_NAME": "performance_schema_setup_actors_size",
    "VARIABLE_VALUE": "-1"
  },
  {
    "VARIABLE_NAME": "performance_schema_setup_objects_size",
    "VARIABLE_VALUE": "-1"
  },
  {
    "VARIABLE_NAME": "performance_schema_show_processlist",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "performance_schema_users_size",
    "VARIABLE_VALUE": "-1"
  },
  {
    "VARIABLE_NAME": "persist_only_admin_x509_subject",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "persisted_globals_load",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "pid_file",
    "VARIABLE_VALUE": "/var/run/mysqld/mysqld.pid"
  },
  {
    "VARIABLE_NAME": "plugin_dir",
    "VARIABLE_VALUE": "/usr/lib/mysql/plugin/"
  },
  {
    "VARIABLE_NAME": "port",
    "VARIABLE_VALUE": "3306"
  },
  {
    "VARIABLE_NAME": "preload_buffer_size",
    "VARIABLE_VALUE": "32768"
  },
  {
    "VARIABLE_NAME": "print_identified_with_as_hex",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "profiling",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "profiling_history_size",
    "VARIABLE_VALUE": "15"
  },
  {
    "VARIABLE_NAME": "protocol_compression_algorithms",
    "VARIABLE_VALUE": "zlib,zstd,uncompressed"
  },
  {
    "VARIABLE_NAME": "protocol_version",
    "VARIABLE_VALUE": "10"
  },
  {
    "VARIABLE_NAME": "query_alloc_block_size",
    "VARIABLE_VALUE": "8192"
  },
  {
    "VARIABLE_NAME": "query_prealloc_size",
    "VARIABLE_VALUE": "8192"
  },
  {
    "VARIABLE_NAME": "range_alloc_block_size",
    "VARIABLE_VALUE": "4096"
  },
  {
    "VARIABLE_NAME": "range_optimizer_max_mem_size",
    "VARIABLE_VALUE": "8388608"
  },
  {
    "VARIABLE_NAME": "rbr_exec_mode",
    "VARIABLE_VALUE": "STRICT"
  },
  {
    "VARIABLE_NAME": "read_buffer_size",
    "VARIABLE_VALUE": "131072"
  },
  {
    "VARIABLE_NAME": "read_only",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "read_rnd_buffer_size",
    "VARIABLE_VALUE": "262144"
  },
  {
    "VARIABLE_NAME": "regexp_stack_limit",
    "VARIABLE_VALUE": "8000000"
  },
  {
    "VARIABLE_NAME": "regexp_time_limit",
    "VARIABLE_VALUE": "32"
  },
  {
    "VARIABLE_NAME": "relay_log",
    "VARIABLE_VALUE": "5c7d972c6d46-relay-bin"
  },
  {
    "VARIABLE_NAME": "relay_log_basename",
    "VARIABLE_VALUE": "/var/lib/mysql/5c7d972c6d46-relay-bin"
  },
  {
    "VARIABLE_NAME": "relay_log_index",
    "VARIABLE_VALUE": "/var/lib/mysql/5c7d972c6d46-relay-bin.index"
  },
  {
    "VARIABLE_NAME": "relay_log_info_file",
    "VARIABLE_VALUE": "relay-log.info"
  },
  {
    "VARIABLE_NAME": "relay_log_info_repository",
    "VARIABLE_VALUE": "TABLE"
  },
  {
    "VARIABLE_NAME": "relay_log_purge",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "relay_log_recovery",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "relay_log_space_limit",
    "VARIABLE_VALUE": "0"
  },
  {
    "VARIABLE_NAME": "replica_allow_batching",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "replica_checkpoint_group",
    "VARIABLE_VALUE": "512"
  },
  {
    "VARIABLE_NAME": "replica_checkpoint_period",
    "VARIABLE_VALUE": "300"
  },
  {
    "VARIABLE_NAME": "replica_compressed_protocol",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "replica_exec_mode",
    "VARIABLE_VALUE": "STRICT"
  },
  {
    "VARIABLE_NAME": "replica_load_tmpdir",
    "VARIABLE_VALUE": "/tmp"
  },
  {
    "VARIABLE_NAME": "replica_max_allowed_packet",
    "VARIABLE_VALUE": "1073741824"
  },
  {
    "VARIABLE_NAME": "replica_net_timeout",
    "VARIABLE_VALUE": "60"
  },
  {
    "VARIABLE_NAME": "replica_parallel_type",
    "VARIABLE_VALUE": "LOGICAL_CLOCK"
  },
  {
    "VARIABLE_NAME": "replica_parallel_workers",
    "VARIABLE_VALUE": "4"
  },
  {
    "VARIABLE_NAME": "replica_pending_jobs_size_max",
    "VARIABLE_VALUE": "134217728"
  },
  {
    "VARIABLE_NAME": "replica_preserve_commit_order",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "replica_skip_errors",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "replica_sql_verify_checksum",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "replica_transaction_retries",
    "VARIABLE_VALUE": "10"
  },
  {
    "VARIABLE_NAME": "replica_type_conversions",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "replication_optimize_for_static_plugin_config",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "replication_sender_observe_commit_only",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "report_host",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "report_password",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "report_port",
    "VARIABLE_VALUE": "3306"
  },
  {
    "VARIABLE_NAME": "report_user",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "require_secure_transport",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "rpl_read_size",
    "VARIABLE_VALUE": "8192"
  },
  {
    "VARIABLE_NAME": "rpl_stop_replica_timeout",
    "VARIABLE_VALUE": "31536000"
  },
  {
    "VARIABLE_NAME": "rpl_stop_slave_timeout",
    "VARIABLE_VALUE": "31536000"
  },
  {
    "VARIABLE_NAME": "schema_definition_cache",
    "VARIABLE_VALUE": "256"
  },
  {
    "VARIABLE_NAME": "secondary_engine_cost_threshold",
    "VARIABLE_VALUE": "100000.000000"
  },
  {
    "VARIABLE_NAME": "secure_file_priv",
    "VARIABLE_VALUE": "NULL"
  },
  {
    "VARIABLE_NAME": "select_into_buffer_size",
    "VARIABLE_VALUE": "131072"
  },
  {
    "VARIABLE_NAME": "select_into_disk_sync",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "select_into_disk_sync_delay",
    "VARIABLE_VALUE": "0"
  },
  {
    "VARIABLE_NAME": "server_id",
    "VARIABLE_VALUE": "1"
  },
  {
    "VARIABLE_NAME": "server_id_bits",
    "VARIABLE_VALUE": "32"
  },
  {
    "VARIABLE_NAME": "server_uuid",
    "VARIABLE_VALUE": "3f07a8ff-9f8c-11ec-9af1-0242ac110002"
  },
  {
    "VARIABLE_NAME": "session_track_gtids",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "session_track_schema",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "session_track_state_change",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "session_track_system_variables",
    "VARIABLE_VALUE": "time_zone,autocommit,character_set_client,character_set_results,character_set_connection"
  },
  {
    "VARIABLE_NAME": "session_track_transaction_info",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "sha256_password_auto_generate_rsa_keys",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "sha256_password_private_key_path",
    "VARIABLE_VALUE": "private_key.pem"
  },
  {
    "VARIABLE_NAME": "sha256_password_proxy_users",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "sha256_password_public_key_path",
    "VARIABLE_VALUE": "public_key.pem"
  },
  {
    "VARIABLE_NAME": "show_create_table_verbosity",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "show_old_temporals",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "skip_external_locking",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "skip_name_resolve",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "skip_networking",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "skip_replica_start",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "skip_show_database",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "skip_slave_start",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "slave_allow_batching",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "slave_checkpoint_group",
    "VARIABLE_VALUE": "512"
  },
  {
    "VARIABLE_NAME": "slave_checkpoint_period",
    "VARIABLE_VALUE": "300"
  },
  {
    "VARIABLE_NAME": "slave_compressed_protocol",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "slave_exec_mode",
    "VARIABLE_VALUE": "STRICT"
  },
  {
    "VARIABLE_NAME": "slave_load_tmpdir",
    "VARIABLE_VALUE": "/tmp"
  },
  {
    "VARIABLE_NAME": "slave_max_allowed_packet",
    "VARIABLE_VALUE": "1073741824"
  },
  {
    "VARIABLE_NAME": "slave_net_timeout",
    "VARIABLE_VALUE": "60"
  },
  {
    "VARIABLE_NAME": "slave_parallel_type",
    "VARIABLE_VALUE": "LOGICAL_CLOCK"
  },
  {
    "VARIABLE_NAME": "slave_parallel_workers",
    "VARIABLE_VALUE": "4"
  },
  {
    "VARIABLE_NAME": "slave_pending_jobs_size_max",
    "VARIABLE_VALUE": "134217728"
  },
  {
    "VARIABLE_NAME": "slave_preserve_commit_order",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "slave_rows_search_algorithms",
    "VARIABLE_VALUE": "INDEX_SCAN,HASH_SCAN"
  },
  {
    "VARIABLE_NAME": "slave_skip_errors",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "slave_sql_verify_checksum",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "slave_transaction_retries",
    "VARIABLE_VALUE": "10"
  },
  {
    "VARIABLE_NAME": "slave_type_conversions",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "slow_launch_time",
    "VARIABLE_VALUE": "2"
  },
  {
    "VARIABLE_NAME": "slow_query_log",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "slow_query_log_file",
    "VARIABLE_VALUE": "/var/lib/mysql/5c7d972c6d46-slow.log"
  },
  {
    "VARIABLE_NAME": "socket",
    "VARIABLE_VALUE": "/var/run/mysqld/mysqld.sock"
  },
  {
    "VARIABLE_NAME": "sort_buffer_size",
    "VARIABLE_VALUE": "262144"
  },
  {
    "VARIABLE_NAME": "source_verify_checksum",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "sql_auto_is_null",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "sql_big_selects",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "sql_buffer_result",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "sql_log_off",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "sql_mode",
    "VARIABLE_VALUE": "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"
  },
  {
    "VARIABLE_NAME": "sql_notes",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "sql_quote_show_create",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "sql_replica_skip_counter",
    "VARIABLE_VALUE": "0"
  },
  {
    "VARIABLE_NAME": "sql_require_primary_key",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "sql_safe_updates",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "sql_select_limit",
    "VARIABLE_VALUE": "18446744073709551615"
  },
  {
    "VARIABLE_NAME": "sql_slave_skip_counter",
    "VARIABLE_VALUE": "0"
  },
  {
    "VARIABLE_NAME": "sql_warnings",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "ssl_ca",
    "VARIABLE_VALUE": "ca.pem"
  },
  {
    "VARIABLE_NAME": "ssl_capath",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "ssl_cert",
    "VARIABLE_VALUE": "server-cert.pem"
  },
  {
    "VARIABLE_NAME": "ssl_cipher",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "ssl_crl",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "ssl_crlpath",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "ssl_fips_mode",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "ssl_key",
    "VARIABLE_VALUE": "server-key.pem"
  },
  {
    "VARIABLE_NAME": "stored_program_cache",
    "VARIABLE_VALUE": "256"
  },
  {
    "VARIABLE_NAME": "stored_program_definition_cache",
    "VARIABLE_VALUE": "256"
  },
  {
    "VARIABLE_NAME": "super_read_only",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "sync_binlog",
    "VARIABLE_VALUE": "1"
  },
  {
    "VARIABLE_NAME": "sync_master_info",
    "VARIABLE_VALUE": "10000"
  },
  {
    "VARIABLE_NAME": "sync_relay_log",
    "VARIABLE_VALUE": "10000"
  },
  {
    "VARIABLE_NAME": "sync_relay_log_info",
    "VARIABLE_VALUE": "10000"
  },
  {
    "VARIABLE_NAME": "sync_source_info",
    "VARIABLE_VALUE": "10000"
  },
  {
    "VARIABLE_NAME": "system_time_zone",
    "VARIABLE_VALUE": "UTC"
  },
  {
    "VARIABLE_NAME": "table_definition_cache",
    "VARIABLE_VALUE": "2000"
  },
  {
    "VARIABLE_NAME": "table_encryption_privilege_check",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "table_open_cache",
    "VARIABLE_VALUE": "4000"
  },
  {
    "VARIABLE_NAME": "table_open_cache_instances",
    "VARIABLE_VALUE": "16"
  },
  {
    "VARIABLE_NAME": "tablespace_definition_cache",
    "VARIABLE_VALUE": "256"
  },
  {
    "VARIABLE_NAME": "temptable_max_mmap",
    "VARIABLE_VALUE": "1073741824"
  },
  {
    "VARIABLE_NAME": "temptable_max_ram",
    "VARIABLE_VALUE": "1073741824"
  },
  {
    "VARIABLE_NAME": "temptable_use_mmap",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "terminology_use_previous",
    "VARIABLE_VALUE": "NONE"
  },
  {
    "VARIABLE_NAME": "thread_cache_size",
    "VARIABLE_VALUE": "9"
  },
  {
    "VARIABLE_NAME": "thread_handling",
    "VARIABLE_VALUE": "one-thread-per-connection"
  },
  {
    "VARIABLE_NAME": "thread_stack",
    "VARIABLE_VALUE": "1048576"
  },
  {
    "VARIABLE_NAME": "time_zone",
    "VARIABLE_VALUE": "SYSTEM"
  },
  {
    "VARIABLE_NAME": "tls_ciphersuites",
    "VARIABLE_VALUE": ""
  },
  {
    "VARIABLE_NAME": "tls_version",
    "VARIABLE_VALUE": "TLSv1.2,TLSv1.3"
  },
  {
    "VARIABLE_NAME": "tmp_table_size",
    "VARIABLE_VALUE": "16777216"
  },
  {
    "VARIABLE_NAME": "tmpdir",
    "VARIABLE_VALUE": "/tmp"
  },
  {
    "VARIABLE_NAME": "transaction_alloc_block_size",
    "VARIABLE_VALUE": "8192"
  },
  {
    "VARIABLE_NAME": "transaction_isolation",
    "VARIABLE_VALUE": "REPEATABLE-READ"
  },
  {
    "VARIABLE_NAME": "transaction_prealloc_size",
    "VARIABLE_VALUE": "4096"
  },
  {
    "VARIABLE_NAME": "transaction_read_only",
    "VARIABLE_VALUE": "OFF"
  },
  {
    "VARIABLE_NAME": "transaction_write_set_extraction",
    "VARIABLE_VALUE": "XXHASH64"
  },
  {
    "VARIABLE_NAME": "unique_checks",
    "VARIABLE_VALUE": "ON"
  },
  {
    "VARIABLE_NAME": "updatable_views_with_limit",
    "VARIABLE_VALUE": "YES"
  },
  {
    "VARIABLE_NAME": "version",
    "VARIABLE_VALUE": "8.0.28"
  },
  {
    "VARIABLE_NAME": "version_comment",
    "VARIABLE_VALUE": "mysql"
  },
  {
    "VARIABLE_NAME": "version_compile_machine",
    "VARIABLE_VALUE": "x86_64"
  },
  {
    "VARIABLE_NAME": "version_compile_os",
    "VARIABLE_VALUE": "Linux"
  },
  {
    "VARIABLE_NAME": "version_compile_zlib",
    "VARIABLE_VALUE": "1.2.11"
  },
  {
    "VARIABLE_NAME": "wait_timeout",
    "VARIABLE_VALUE": "28800"
  },
  {
    "VARIABLE_NAME": "windowing_use_high_precision",
    "VARIABLE_VALUE": "ON"
  }
]
"#;
