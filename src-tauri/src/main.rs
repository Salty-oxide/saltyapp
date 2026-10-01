#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod commands;
mod logging;
mod state;

use state::AppState;
use std::sync::Arc;
use tauri::Manager;

/// `--librdkafka-features [path]`: report which compression codecs *this
/// executable* can decode, then exit — 0 when every required one is compiled
/// in, 1 when any is missing.
///
/// Every earlier check of this was made against something other than the
/// artifact users install: a Cargo feature resolving, a vcpkg install
/// succeeding, a generated `config.h`, a `cargo test` binary built from the
/// same workspace. v0.37.0 shipped a Windows build with Snappy compiled out
/// while all of those were green. This one cannot be that kind of proxy — it
/// is the shipped executable answering about itself.
///
/// Release builds on Windows have no console (`windows_subsystem = "windows"`
/// above), so the answer also goes to `path` when one is given, and the exit
/// code carries the verdict regardless of where output can be seen.
fn report_librdkafka_features(path: Option<&str>) -> ! {
    let features = salty_kafka::build_info::builtin_features();
    let missing = salty_kafka::build_info::missing_required_features();

    let report = if missing.is_empty() {
        format!("ok\nbuiltin.features = {features}\n")
    } else {
        format!(
            "MISSING {}\nbuiltin.features = {features}\nTopics compressed with a missing \
             codec fail every poll with \"Local: Not Implemented\".\n",
            missing.join(", "),
        )
    };

    print!("{report}");
    if let Some(path) = path {
        let _ = std::fs::write(path, &report);
    }
    std::process::exit(if missing.is_empty() { 0 } else { 1 });
}

fn main() {
    let mut args = std::env::args().skip(1);
    if let Some(flag) = args.next() {
        if flag == "--librdkafka-features" {
            report_librdkafka_features(args.next().as_deref());
        }
    }

    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        // Only the KRaft explainer's "Learn more" button uses this, and the
        // capability below is scoped to kafka.apache.org — this is the app's
        // first outbound-link surface and is deliberately held to one host.
        .plugin(tauri_plugin_opener::init())
        .setup(|app| {
            let handle = app.handle().clone();

            // The window title is set here (not left as the static string in
            // tauri.conf.json) so it always shows the actual running
            // version — reading it from `package_info()` keeps it in sync
            // automatically with tauri.conf.json's `version` field with no
            // separate value to remember to update on every version bump.
            if let Some(window) = handle.get_webview_window("main") {
                let version = handle.package_info().version.to_string();
                let _ = window.set_title(&format!("Salty v{version}"));
            }

            // Before any client is built, so every connection this app opens
            // identifies itself as `salty/<version>` rather than
            // librdkafka's shared default of `rdkafka`. The version can only
            // come from here: the workspace crates all sit at 0.1.0 and are
            // not bumped per release, so `tauri.conf.json` (via
            // `package_info()`) is the only true version of the app.
            salty_kafka::set_app_version(&handle.package_info().version.to_string());

            tauri::async_runtime::block_on(async move {
                // What "Application started" below reports: opening the
                // database and running any outstanding migrations, which is
                // the one piece of startup work that can grow with use.
                let started = std::time::Instant::now();
                let data_dir = handle.path().app_data_dir().expect("app data dir");
                std::fs::create_dir_all(&data_dir).expect("create app data dir");

                // The app data directory is named after the bundle
                // identifier, and the identifier changed with the rename — so
                // an upgrading user's connections, tabs and saved schemas are
                // sitting one directory over, under the old name. Copy them
                // across before opening anything, or the first launch after
                // the upgrade looks like a fresh install.
                //
                // Deliberately not fatal: a failure here means the user has
                // an empty app, which is recoverable and worth a log line,
                // while panicking means they have no app at all. The
                // decision-making itself is in `salty_core`, where it is
                // tested — this crate cannot be compiled everywhere.
                let adopted = salty_core::adopt_legacy_app_data(&data_dir);

                let db_path = data_dir.join(salty_core::DB_FILE);
                let database_url = format!("sqlite://{}?mode=rwc", db_path.display());
                let pool = salty_db::init_pool(&database_url)
                    .await
                    .expect("failed to initialize database");

                handle.manage(AppState {
                    pool,
                    kafka: Arc::new(salty_kafka::RdKafkaClient::new()),
                    zookeeper: Arc::new(salty_kafka::TcpZookeeperClient),
                    connections: salty_core::ConnectionRegistry::default(),
                    fetch_cancellations: salty_core::FetchCancellations::default(),
                    schema_registry: salty_schema_registry::SchemaRegistryClients::default(),
                });

                logging::emit_log(
                    &handle,
                    "info",
                    format!("Application started in {} ms", started.elapsed().as_millis()),
                );

                // After the pool is up, so the line lands in a Logs panel the
                // user can actually open, and so a migration that went wrong
                // is visible rather than silent.
                match adopted {
                    Ok(Some(adopted)) => logging::emit_log(
                        &handle,
                        "info",
                        format!(
                            "Adopted data from the previous version ({}): {}",
                            adopted.from.display(),
                            adopted.files.join(", "),
                        ),
                    ),
                    Ok(None) => {}
                    Err(error) => logging::emit_log(
                        &handle,
                        "warn",
                        format!(
                            "Could not copy data from the previous version: {error}. Saved \
                             connections from before the rename to Salty will not appear.",
                        ),
                    ),
                }

                // Logged so the id is discoverable from the app itself: it is
                // what an operator filters broker metrics and quotas by, and
                // it is the line that makes a forgotten `set_app_version`
                // visible (it would read "salty" with no version).
                logging::emit_log(
                    &handle,
                    "info",
                    format!("Broker connections identify as client.id={}", salty_kafka::broker_client_id()),
                );

                // Enumerating the OS trust store is the one fixed cost every
                // TLS connection pays; doing it here means the user's first
                // click doesn't.
                let ca_started = std::time::Instant::now();
                salty_kafka::warm_native_ca_bundle();
                logging::emit_log(
                    &handle,
                    "info",
                    format!("Loaded the OS trust store in {} ms", ca_started.elapsed().as_millis()),
                );

                // Which compression codecs work is fixed at compile time
                // inside librdkafka, and a missing one stays invisible until a
                // user happens to open a topic that uses it — at which point
                // every poll fails with a bare "Local: Not Implemented". Six
                // releases went out before that could be diagnosed. Recording
                // it at startup means the Logs panel answers the question
                // directly, from the build the user is actually running.
                let features = salty_kafka::build_info::builtin_features();
                let missing = salty_kafka::build_info::missing_required_features();
                if missing.is_empty() {
                    logging::emit_log(&handle, "info", format!("librdkafka features: {features}"));
                } else {
                    logging::emit_log(
                        &handle,
                        "error",
                        format!(
                            "This build of librdkafka is missing {}. Topics compressed with \
                             those codecs will fail to fetch with \"Local: Not Implemented\". \
                             Compiled with: {features}",
                            missing.join(", "),
                        ),
                    );
                }
            });
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            commands::connections::connection_list,
            commands::connections::connection_create,
            commands::connections::connection_update,
            commands::connections::connection_delete,
            commands::connections::connections_export,
            commands::connections::connections_import,
            commands::connections::connection_check_status,
            commands::connections::connection_ping_bootstrap,
            commands::connections::connection_ping_zookeeper,
            commands::connections::connection_test,
            commands::connections::connection_detect_version,
            commands::connections::connection_connect,
            commands::connections::connection_disconnect,
            commands::connections::connection_is_connected,
            commands::connections::connection_auth_block_reason,
            commands::connections::connection_list_brokers,
            commands::connections::connection_list_topics,
            commands::connections::connection_list_consumer_groups,
            commands::connections::connection_count_topic_messages,
            commands::connections::connection_fetch_messages,
            commands::connections::connection_cancel_fetch,
            commands::connections::connection_list_partitions,
            commands::connections::connection_describe_topic_config,
            commands::connections::connection_fetch_consumer_group_lag,
            commands::acl::acl_list,
            commands::acl::acl_for_resource,
            commands::ksql::ksql_statement,
            commands::ksql::ksql_stream_for_topic,
            commands::ksql::ksql_query,
            commands::ksql::ksql_cancel,
            commands::publish::connection_publish_messages,
            commands::publish::connection_write_denied_reason,
            commands::schema::topic_schema_get,
            commands::schema::topic_schema_set,
            commands::schema::topic_schema_delete,
            commands::schema::connection_decode_avro,
            commands::schema::connection_decode_protobuf,
            commands::tabs::tab_list,
            commands::tabs::tab_create,
            commands::tabs::tab_rename,
            commands::tabs::tab_delete,
            commands::tabs::tab_reorder,
            commands::system::trim_process_memory,
            commands::system::payload_save,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
