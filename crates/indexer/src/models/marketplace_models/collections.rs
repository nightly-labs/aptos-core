#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use aptos_api_types::Event;
use diesel::{ExpressionMethods, QueryDsl, RunQueryDsl};
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

use crate::{database::PgPoolConnection, schema::marketplace_collections};

use super::utils::MarketplaceEvent;

#[derive(Debug, Deserialize, FieldCount, Identifiable, Insertable, Queryable, Serialize)]
#[diesel(primary_key(creator_address, collection_name))]
#[diesel(table_name = marketplace_collections)]
pub struct MarketplaceCollection {
    creator_address: String,
    collection_address: String,
    collection_name: String,
    creation_timestamp: chrono::NaiveDateTime,
}

impl MarketplaceCollection {
    pub fn from_event(
        event_type: &str,
        marketplace_event: &Event,
        txn_version: i64,
    ) -> Option<Self> {
        let collection_registration_event =
            match MarketplaceEvent::from_event(event_type, &marketplace_event.data, txn_version)
                .unwrap()
            {
                Some(event_type) => match event_type {
                    MarketplaceEvent::CollectionRegistrationEvent(inner) => {
                        Some(MarketplaceCollection {
                            creator_address: inner.creator,
                            collection_address: inner.collection_address,
                            collection_name: inner.collection_name,
                            creation_timestamp: inner.timestamp,
                        })
                    }
                },
                None => None,
            };

        collection_registration_event
    }

    pub fn get_pda_address(
        conn: &mut PgPoolConnection,
        creator_address: String,
        collection_name: String,
    ) -> diesel::QueryResult<Self> {
        marketplace_collections::table
            .filter(marketplace_collections::creator_address.eq(creator_address))
            .filter(marketplace_collections::collection_name.eq(collection_name))
            .first::<Self>(conn)
    }
}
