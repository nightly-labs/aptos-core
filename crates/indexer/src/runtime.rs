// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

use crate::{
    api::TokenAPI,
    database::new_db_pool,
    indexer::{
        fetcher::TransactionFetcherOptions, tailer::Tailer,
        transaction_processor::TransactionProcessor,
    },
    processors::{
        coin_processor::CoinTransactionProcessor, default_processor::DefaultTransactionProcessor,
        marketplace_processor::MarketplaceProcessor, token_processor::TokenTransactionProcessor,
        Processor,
    },
};

use anyhow::Context as AnyhowContext;
use aptos_api::context::Context;
use aptos_api::{
    check_size::PostSizeLimit, error_converter::convert_error, log::middleware_log, set_failpoints,
};
use aptos_config::config::{IndexerConfig, NodeConfig};
use aptos_logger::{error, info};
use aptos_mempool::MempoolClientSender;
use aptos_types::chain_id::ChainId;
use diesel::{
    r2d2::{ConnectionManager, Pool},
    PgConnection,
};
use poem::{
    http::{header, Method},
    listener::{Listener, RustlsCertificate, RustlsConfig, TcpListener},
    middleware::Cors,
    EndpointExt, Route, Server,
};
use poem_openapi::{ContactObject, LicenseObject, OpenApiService};
use std::sync::Arc;
use std::{collections::VecDeque, net::SocketAddr};
use storage_interface::DbReader;
use tokio::runtime::{Builder, Handle, Runtime};

pub struct MovingAverage {
    window_millis: u64,
    // (timestamp_millis, value)
    values: VecDeque<(u64, u64)>,
    sum: u64,
}

impl MovingAverage {
    pub fn new(window_millis: u64) -> Self {
        Self {
            window_millis,
            values: VecDeque::new(),
            sum: 0,
        }
    }

    pub fn tick_now(&mut self, value: u64) {
        let now = chrono::Utc::now().naive_utc().timestamp_millis() as u64;
        self.tick(now, value);
    }

    pub fn tick(&mut self, timestamp_millis: u64, value: u64) -> f64 {
        self.values.push_back((timestamp_millis, value));
        self.sum += value;
        loop {
            match self.values.front() {
                None => break,
                Some((ts, val)) => {
                    if timestamp_millis - ts > self.window_millis {
                        self.sum -= val;
                        self.values.pop_front();
                    } else {
                        break;
                    }
                }
            }
        }
        self.avg()
    }

    pub fn avg(&self) -> f64 {
        if self.values.len() < 2 {
            0.0
        } else {
            let elapsed = self.values.back().unwrap().0 - self.values.front().unwrap().0;
            self.sum as f64 / elapsed as f64
        }
    }
}

/// Creates a runtime which creates a thread pool which reads from storage and writes to postgres
/// Returns corresponding Tokio runtime
pub fn bootstrap(
    config: &NodeConfig,
    chain_id: ChainId,
    db: Arc<dyn DbReader>,
    mp_sender: MempoolClientSender,
) -> Option<anyhow::Result<Runtime>> {
    if !config.indexer.enabled {
        return None;
    }

    let runtime = Builder::new_multi_thread()
        .thread_name("indexer")
        .disable_lifo_slot()
        .enable_all()
        .build()
        .expect("[indexer] failed to create runtime");

    let indexer_config = config.indexer.clone();
    let node_config = config.clone();
    let context = Context::new(chain_id, db, mp_sender, node_config);

    let db_uri = &indexer_config.postgres_uri.unwrap();
    info!(
        processor_name = indexer_config.processor.clone().unwrap(),
        "Creating connection pool..."
    );
    let conn_pool = new_db_pool(db_uri).expect("Failed to create connection pool");
    info!(
        processor_name = indexer_config.processor.clone().unwrap(),
        "Created the connection pool... "
    );

    attach_poem_to_runtime(
        runtime.handle(),
        context.clone(),
        config,
        conn_pool.get().unwrap(),
    )
    .context("Failed to attach poem to runtime")
    .ok()?;

    runtime.spawn(async move {
        run_forever(indexer_config, Arc::new(context), conn_pool).await;
    });

    Some(Ok(runtime))
}

pub async fn run_forever(
    config: IndexerConfig,
    context: Arc<Context>,
    conn_pool: Arc<Pool<ConnectionManager<PgConnection>>>,
) {
    // All of these options should be filled already with defaults
    let processor_name = config.processor.clone().unwrap();
    let check_chain_id = config.check_chain_id.unwrap();
    let skip_migrations = config.skip_migrations.unwrap();
    let fetch_tasks = config.fetch_tasks.unwrap();
    let processor_tasks = config.processor_tasks.unwrap();
    let emit_every = config.emit_every.unwrap();
    let batch_size = config.batch_size.unwrap();
    let lookback_versions = config.gap_lookback_versions.unwrap() as i64;

    info!(processor_name = processor_name, "Starting indexer...");

    info!(processor_name = processor_name, "Instantiating tailer... ");

    let processor_enum = Processor::from_string(&processor_name);
    let processor: Arc<dyn TransactionProcessor> = match processor_enum {
        Processor::DefaultProcessor => {
            Arc::new(DefaultTransactionProcessor::new(conn_pool.clone()))
        }
        Processor::TokenProcessor => Arc::new(TokenTransactionProcessor::new(
            conn_pool.clone(),
            config.ans_contract_address,
        )),
        Processor::CoinProcessor => Arc::new(CoinTransactionProcessor::new(conn_pool.clone())),
        Processor::MarketplaceProcessor => Arc::new(MarketplaceProcessor::new(conn_pool.clone())),
    };

    let options =
        TransactionFetcherOptions::new(None, None, Some(batch_size), None, fetch_tasks as usize);

    let tailer = Tailer::new(context, conn_pool.clone(), processor, options)
        .expect("Failed to instantiate tailer");

    if !skip_migrations {
        info!(processor_name = processor_name, "Running migrations...");
        tailer.run_migrations();
    }

    info!(
        processor_name = processor_name,
        lookback_versions = lookback_versions,
        "Fetching starting version from db..."
    );
    let start_version = match config.starting_version {
        None => tailer
            .get_start_version(&processor_name, lookback_versions)
            .unwrap_or_else(|| {
                info!(
                    processor_name = processor_name,
                    "Could not fetch version from db so starting from version 0"
                );
                0
            }) as u64,
        Some(version) => version,
    };

    info!(
        processor_name = processor_name,
        start_version = start_version,
        start_version_from_config = config.starting_version,
        "Setting starting version..."
    );
    tailer.set_fetcher_version(start_version as u64).await;

    info!(processor_name = processor_name, "Starting fetcher...");
    tailer.transaction_fetcher.lock().await.start().await;

    info!(
        processor_name = processor_name,
        start_version = start_version,
        "Indexing loop started!"
    );

    let mut versions_processed: u64 = 0;
    let mut base: u64 = 0;

    // Check once here to avoid a boolean check every iteration
    if check_chain_id {
        tailer
            .check_or_update_chain_id()
            .await
            .expect("Failed to get chain ID");
    }

    let (tx, mut receiver) = tokio::sync::mpsc::channel(100);
    let mut tasks = vec![];
    for _ in 0..processor_tasks {
        let other_tx = tx.clone();
        let other_tailer = tailer.clone();
        let task = tokio::task::spawn(async move {
            loop {
                let (num_res, res) = other_tailer.process_next_batch().await;
                other_tx.send((num_res, res)).await.unwrap();
            }
        });
        tasks.push(task);
    }

    let mut ma = MovingAverage::new(10_000);

    loop {
        let (num_res, result) = receiver
            .recv()
            .await
            .expect("Failed to receive batch results: got None!");

        let processing_result = match result {
            Ok(res) => res,
            Err(tpe) => {
                let (err, start_version, end_version, _) = tpe.inner();
                error!(
                    processor_name = processor_name,
                    start_version = start_version,
                    end_version = end_version,
                    error = format!("{:?}", err),
                    "Error processing batch!"
                );
                panic!(
                    "Error in '{}' while processing batch: {:?}",
                    processor_name, err
                );
            }
        };

        ma.tick_now(num_res);

        versions_processed += num_res;
        if emit_every != 0 {
            let new_base: u64 = versions_processed / (emit_every as u64);
            if base != new_base {
                base = new_base;
                info!(
                    processor_name = processor_name,
                    batch_start_version = processing_result.start_version,
                    batch_end_version = processing_result.end_version,
                    versions_processed = versions_processed,
                    tps = (ma.avg() * 1000.0) as u64,
                    "Processed batch version"
                );
            }
        }
    }
}

// Copied from ../../api/src/runtime.rs
fn attach_poem_to_runtime(
    runtime_handle: &Handle,
    context: Context,
    config: &NodeConfig,
    conn_pool: Pool<ConnectionManager<PgConnection>>,
) -> anyhow::Result<SocketAddr> {
    let context_arc = Arc::new(context);
    let size_limit = context.content_length_limit();
    let apis = TokenAPI {
        context: context_arc.clone(),
        conn: conn_pool.clone(),
    };

    let license =
        LicenseObject::new("Apache 2.0").url("https://www.apache.org/licenses/LICENSE-2.0.html");
    let contact = ContactObject::new()
        .name("Aptos Labs")
        .url("https://github.com/aptos-labs/aptos-core");
    let service = OpenApiService::new(apis, "Aptos Node API", "")
        .server("/v1")
        .description("The Aptos Node API is a RESTful API for client applications to interact with the Aptos blockchain.")
        .license(license)
        .contact(contact)
        .external_document("https://github.com/aptos-labs/aptos-core");

    let spec_json = service.spec_endpoint();
    let spec_yaml = service.spec_endpoint_yaml();
    let mut address = config.api.address;
    address.set_port(5555);

    let listener = match (&config.api.tls_cert_path, &config.api.tls_key_path) {
        (Some(tls_cert_path), Some(tls_key_path)) => {
            info!("Using TLS for API");
            let cert = std::fs::read_to_string(tls_cert_path).context(format!(
                "Failed to read TLS cert from path: {}",
                tls_cert_path
            ))?;
            let key = std::fs::read_to_string(tls_key_path).context(format!(
                "Failed to read TLS key from path: {}",
                tls_key_path
            ))?;
            let rustls_certificate = RustlsCertificate::new().cert(cert).key(key);
            let rustls_config = RustlsConfig::new().fallback(rustls_certificate);
            TcpListener::bind(address).rustls(rustls_config).boxed()
        }
        _ => {
            info!("Not using TLS for API");
            TcpListener::bind(address).boxed()
        }
    };

    let acceptor = tokio::task::block_in_place(move || {
        runtime_handle
            .block_on(async move { listener.into_acceptor().await })
            .with_context(|| format!("Failed to bind Poem to address: {}", address))
    })?;

    let actual_address = &acceptor.local_addr()[0];
    let actual_address = *actual_address
        .as_socket_addr()
        .context("Failed to get socket addr from local addr for Poem webserver")?;

    runtime_handle.spawn(async move {
        let cors = Cors::new()
            .allow_credentials(true)
            .allow_methods(vec![Method::GET, Method::POST])
            .allow_headers(vec![header::CONTENT_TYPE, header::ACCEPT]);

        let route = Route::new()
            .nest(
                "/v1",
                Route::new()
                    .nest("/", service)
                    .at("/spec.json", spec_json)
                    .at("/spec.yaml", spec_yaml)
                    .at(
                        "/set_failpoint",
                        poem::get(set_failpoints::set_failpoint_poem).data(context.clone()),
                    ),
            )
            .with(cors)
            .with(PostSizeLimit::new(size_limit))
            .catch_all_error(convert_error)
            .around(middleware_log);
        Server::new_with_acceptor(acceptor)
            .run(route)
            .await
            .map_err(anyhow::Error::msg)
    });

    info!(
        "Poem is running at {}, behind the reverse proxy at the API port",
        actual_address
    );

    Ok(actual_address)
}
