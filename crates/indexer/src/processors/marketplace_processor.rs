use std::fmt::Debug;

use aptos_api_types::{Transaction, TransactionPayload, WriteSetChange};
use aptos_types::transaction::TransactionPayload;
use async_trait::async_trait;
use field_count::FieldCount;

use crate::{
    database::{
        clean_data_for_db, execute_with_better_error, get_chunks, PgDbPool, PgPoolConnection,
    },
    indexer::{
        errors::TransactionProcessingError, processing_result::ProcessingResult,
        transaction_processor::TransactionProcessor,
    },
    models::{
        marketplace_models::{
            bids::MarketplaceBid, collections::MarketplaceCollection, offers::MarketplaceOffer,
            orders::MarketplaceOrder,
        },
        write_set_changes::WriteSetChange,
    },
    schema,
    util::parse_timestamp,
};
use diesel::{result::Error, PgConnection};

pub const NAME: &str = "marketplace_processor";

pub struct MarketplaceProcessor {
    connection_pool: PgDbPool,
}

impl MarketplaceProcessor {
    pub fn new(connection_pool: PgDbPool) -> Self {
        Self { connection_pool }
    }
}

impl Debug for MarketplaceProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "MarketplaceProcessor {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

fn insert_to_db(
    conn: &mut PgPoolConnection,
    name: &'static str,
    start_version: u64,
    end_version: u64,
    collections: Vec<MarketplaceCollection>,
    offers: Vec<MarketplaceOffer>,
    orders: Vec<MarketplaceOrder>,
    bids: Vec<MarketplaceBid>,
) -> Result<(), Error> {
    aptos_logger::trace!(
        name = name,
        start_version = start_version,
        end_version = end_version,
        "Inserting to db"
    );

    match conn
        .build_transaction()
        .read_write()
        .run::<_, Error, _>(|pg_conn| {
            insert_collections(pg_conn, &collections);
            insert_offers(pg_conn, &offers);
            insert_orders(pg_conn, &orders);
            insert_bids(pg_conn, &bids);
            Ok(())
        }) {
        Ok(_) => Ok(()),
        Err(_) => conn
            .build_transaction()
            .read_write()
            .run::<_, Error, _>(|pg_conn| {
                let collections = clean_data_for_db(collections, true);
                let offers = clean_data_for_db(offers, true);
                let orders = clean_data_for_db(orders, true);
                let bids = clean_data_for_db(bids, true);

                insert_collections(pg_conn, &collections);
                insert_offers(pg_conn, &offers);
                insert_orders(pg_conn, &orders);
                insert_bids(pg_conn, &bids);
                Ok(())
            }),
    }
}

fn insert_collections(
    conn: &mut PgConnection,
    collections: &[MarketplaceCollection],
) -> Result<(), diesel::result::Error> {
    let chunks = get_chunks(collections.len(), MarketplaceCollection::field_count());
    for (start_index, end_index) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::marketplace_collections::table)
                .values(&collections[start_index..end_index]),
            None,
        )?;
    }
    Ok(())
}

fn insert_offers(
    conn: &mut PgConnection,
    offers: &[MarketplaceOffer],
) -> Result<(), diesel::result::Error> {
    let chunks = get_chunks(offers.len(), MarketplaceOffer::field_count());
    for (start_index, end_index) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::marketplace_offers::table)
                .values(&offers[start_index..end_index]),
            None,
        )?;
    }
    Ok(())
}

fn insert_orders(
    conn: &mut PgConnection,
    orders: &[MarketplaceOrder],
) -> Result<(), diesel::result::Error> {
    let chunks = get_chunks(orders.len(), MarketplaceOffer::field_count());
    for (start_index, end_index) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::marketplace_orders::table)
                .values(&orders[start_index..end_index]),
            None,
        )?;
    }
    Ok(())
}

fn insert_bids(
    conn: &mut PgConnection,
    bids: &[MarketplaceBid],
) -> Result<(), diesel::result::Error> {
    let chunks = get_chunks(bids.len(), MarketplaceOffer::field_count());
    for (start_index, end_index) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::marketplace_bids::table)
                .values(&bids[start_index..end_index]),
            None,
        )?;
    }
    Ok(())
}

#[async_trait]
impl TransactionProcessor for MarketplaceProcessor {
    fn name(&self) -> &'static str {
        NAME
    }

    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
    ) -> Result<ProcessingResult, TransactionProcessingError> {
        let mut conn = self.get_conn();

        let mut all_collections = vec![];
        let mut all_offers = vec![];
        let mut all_orders = vec![];
        let mut all_bids = vec![];

        for txn in &transactions {
            let maybe_user_transaction_details = match txn {
                Transaction::UserTransaction(user_txn) => Some((
                    user_txn.info,
                    user_txn.request,
                    user_txn.events,
                    parse_timestamp(user_txn.timestamp.0, user_txn.info.version.0),
                )),
                _ => None,
            };

            if let Some(user_transaction_details) = maybe_user_transaction_details {
                let txn_version = user_transaction_details.0.version.0;
                let txn_timestamp = user_transaction_details.3;
                let payload = user_transaction_details.1.payload;

                for event in user_transaction_details.2 {
                    let event_type = event.typ.to_string();
                    let maybe_collection = MarketplaceCollection::from_event(
                        &event_type,
                        &event,
                        txn_version,
                        txn_timestamp,
                    );

                    if maybe_collection.is_some() {
                        all_collections.push(maybe_collection.unwrap())
                    }
                }

                let (maybe_offer, maybe_order, maybe_bid) =
                    if let TransactionPayload::EntryFunctionPayload(entry_transaction_payload) =
                        payload
                    {
                        for writeset in user_transaction_details.0.changes {
                            if let WriteSetChange::WriteTableItem(table_item) = writeset {
                                (
                                    MarketplaceOffer::from_table_item(
                                        &table_item,
                                        entry_transaction_payload,
                                        txn_version,
                                        txn_timestamp,
                                    )
                                    .unwrap(),
                                    MarketplaceOrder::from_table_item(
                                        &table_item,
                                        entry_transaction_payload,
                                        txn_version,
                                        txn_timestamp,
                                    )
                                    .unwrap(),
                                    MarketplaceBid::from_table_item(
                                        &table_item,
                                        entry_transaction_payload,
                                        txn_version,
                                        txn_timestamp,
                                    )
                                    .unwrap(),
                                )
                            }
                        }
                    } else {
                        (None, None, None)
                    };
            }
        }
    }

    fn connection_pool(&self) -> &PgDbPool {
        &self.connection_pool
    }
}
