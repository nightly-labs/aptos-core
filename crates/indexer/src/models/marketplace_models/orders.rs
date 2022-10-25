#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use anyhow::Result;
use aptos_api_types::{EntryFunctionPayload, WriteTableItem};
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

use crate::schema::marketplace_orders;

use super::utils::{MarketplacePayload, MarketplaceWriteSet};

#[derive(Debug, Deserialize, FieldCount, Identifiable, Insertable, Queryable, Serialize)]
#[diesel(primary_key(creator_address, collection_name))]
#[diesel(table_name = marketplace_orders)]
pub struct MarketplaceOrder {
    creator_address: String,
    collection_name: String,
    price: i64,
    quantity: i64,
    maker: String,
    timestamp: chrono::NaiveDateTime,
}

impl MarketplaceOrder {
    pub fn from_table_item(
        table_item: &WriteTableItem,
        payload: EntryFunctionPayload,
        txn_version: i64,
        txn_timestamp: chrono::NaiveDateTime,
    ) -> Result<Option<Self>> {
        let table_item_data = &table_item.data.unwrap();
        let maybe_order = match MarketplaceWriteSet::from_table_item_type(
            table_item_data.key_type.as_str(),
            &table_item_data.value,
            txn_version,
        )? {
            Some(MarketplaceWriteSet::Order(inner)) => Some(inner),
            _ => None,
        };
        let maybe_place_order_payload = match MarketplacePayload::from_function_name(
            &payload.function.to_string(),
            &payload.arguments,
            txn_version,
        )
        .unwrap()
        {
            Some(payload_type) => match payload_type {
                MarketplacePayload::PlaceOrderPayload(inner) => Some(inner),
                _ => None,
            },
            None => None,
        };

        if let (Some(order), Some(place_order_payload)) = (maybe_order, maybe_place_order_payload) {
            Ok(Some(Self {
                creator_address: place_order_payload.creator,
                collection_name: place_order_payload.collection_name,
                price: order.price,
                quantity: order.quantity,
                maker: serde_json::from_value(table_item_data.key.clone())?,
                timestamp: txn_timestamp,
            }))
        } else {
            Ok(None)
        }
    }
}
