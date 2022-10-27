#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use anyhow::Result;
use aptos_api_types::{EntryFunctionPayload, WriteTableItem};
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

use crate::schema::marketplace_bids;

use super::utils::{MarketplacePayload, MarketplaceWriteSet};

#[derive(Debug, Deserialize, FieldCount, Identifiable, Insertable, Queryable, Serialize)]
#[diesel(primary_key(creator_address, collection_name))]
#[diesel(table_name = marketplace_bids)]
pub struct MarketplaceBid {
    creator_address: String,
    collection_name: String,
    token_name: String,
    property_version: i64,
    price: i64,
    maker: String,
    timestamp: chrono::NaiveDateTime,
}

impl MarketplaceBid {
    pub fn from_table_item(
        table_item: &WriteTableItem,
        payload: EntryFunctionPayload,
        txn_version: i64,
        txn_timestamp: chrono::NaiveDateTime,
    ) -> Result<Option<Self>> {
        let table_item_data = table_item.data.as_ref().unwrap();
        let maybe_bid = match MarketplaceWriteSet::from_table_item_type(
            table_item_data.key_type.as_str(),
            &table_item_data.value,
            txn_version,
        )? {
            Some(MarketplaceWriteSet::Bid(inner)) => Some(inner),
            _ => None,
        };
        let maybe_place_bid_payload = match MarketplacePayload::from_function_name(
            &payload.function.to_string(),
            payload.arguments,
            txn_version,
        )
        .unwrap()
        {
            Some(payload) => match payload {
                MarketplacePayload::PlaceBidPayload(inner) => Some(inner),
                _ => None,
            },
            None => None,
        };

        if let (Some(bid), Some(place_bid_payload)) = (maybe_bid, maybe_place_bid_payload) {
            Ok(Some(Self {
                creator_address: place_bid_payload.creator,
                collection_name: place_bid_payload.collection_name,
                token_name: place_bid_payload.token_name,
                property_version: place_bid_payload.property_version,
                price: bid.price,
                maker: bid.maker,
                timestamp: txn_timestamp,
            }))
        } else {
            Ok(None)
        }
    }
}
