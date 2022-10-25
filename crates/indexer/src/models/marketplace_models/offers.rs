#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use anyhow::Result;
use aptos_api_types::{EntryFunctionPayload, TransactionPayload, WriteTableItem};
use aptos_types::transaction::TransactionPayload;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

use crate::schema::marketplace_offers;

use super::utils::{MarketplacePayload, MarketplaceWriteSet};

#[derive(Debug, Deserialize, FieldCount, Identifiable, Insertable, Queryable, Serialize)]
#[diesel(primary_key(creator_address, collection_name))]
#[diesel(table_name = marketplace_offers)]
pub struct MarketplaceOffer {
    creator_address: String,
    collection_name: String,
    token_name: String,
    property_version: i64,
    price: i64,
    seller: String,
    timestamp: chrono::NaiveDateTime,
}

impl MarketplaceOffer {
    pub fn from_table_item(
        table_item: &WriteTableItem,
        payload: EntryFunctionPayload,
        txn_version: i64,
        txn_timestamp: chrono::NaiveDateTime,
    ) -> Result<Option<Self>> {
        let table_item_data = &table_item.data.unwrap();
        let maybe_offer = match MarketplaceWriteSet::from_table_item_type(
            table_item_data.key_type.as_str(),
            &table_item_data.value,
            txn_version,
        )? {
            Some(MarketplaceWriteSet::Offer(inner)) => Some(inner),
            _ => None,
        };
        let maybe_list_item_payload = match MarketplacePayload::from_function_name(
            &payload.function.to_string(),
            &payload.arguments,
            txn_version,
        )
        .unwrap()
        {
            Some(payload_type) => match payload_type {
                MarketplacePayload::ListItemPayload(inner) => Some(inner),
                _ => None,
            },
            None => None,
        };

        if let (Some(offer), Some(list_item_payload)) = (maybe_offer, maybe_list_item_payload) {
            Ok(Some(Self {
                creator_address: list_item_payload.creator,
                collection_name: list_item_payload.collection_name,
                token_name: list_item_payload.token_name,
                property_version: list_item_payload.property_version,
                price: offer.price,
                seller: offer.seller,
                timestamp: txn_timestamp,
            }))
        } else {
            Ok(None)
        }
    }
}
