// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result};
use aptos_api_types::deserialize_from_string;
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

const MARKETPLACE_ADDRESS: &str =
    "0x975c0bad4ee36fcb48fe447647834b9c09ef44349ff593e90dd816dc5a3eccdc";

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OfferType {
    pub price: i64,
    pub seller: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OrderType {
    pub price: i64,
    pub quantity: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BidType {
    pub price: i64,
    pub maker: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum MarketplaceWriteSet {
    Offer(OfferType),
    Order(OrderType),
    Bid(BidType),
}

impl MarketplaceWriteSet {
    pub fn from_table_item_type(
        data_type: &str,
        data: &serde_json::Value,
        txn_version: i64,
    ) -> Result<Option<MarketplaceWriteSet>> {
        match data_type {
            format!("{}::collection::Offer", MARKETPLACE_ADDRESS) => {
                serde_json::from_value(data.clone())
                    .map(|inner| Some(MarketplaceWriteSet::Offer(inner)))
            }
            format!("{}::collection::Order", MARKETPLACE_ADDRESS) => {
                serde_json::from_value(data.clone())
                    .map(|inner| Some(MarketplaceWriteSet::Order(inner)))
            }
            format!("{}::collection::Bid", MARKETPLACE_ADDRESS) => {
                serde_json::from_value(data.clone())
                    .map(|inner| Some(MarketplaceWriteSet::Bid(inner)))
            }
            _ => Ok(None),
        }
        .context(format!(
            "Version {} failed! Failed to parse type {}, data {:?}",
            txn_version, data_type, data,
        ))
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CollectionRegistrationEvent {
    pub creator: String,
    pub collection_address: String,
    pub collection_name: String,
    pub timestamp: chrono::NaiveDateTime,
    #[serde(deserialize_with = "deserialize_from_string")]
    event_counter: BigDecimal,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum MarketplaceEvent {
    CollectionRegistrationEvent(CollectionRegistrationEvent),
}

impl MarketplaceEvent {
    pub fn from_event(
        data_type: &str,
        data: &serde_json::Value,
        txn_version: i64,
    ) -> Result<Option<Self>> {
        match data_type {
            "marketplace-address::events::CollectionRegistrationEvent" => {
                serde_json::from_value(data.clone())
                    .map(|inner| Some(MarketplaceEvent::CollectionRegistrationEvent(inner)))
            }
            _ => Ok(None),
        }
        .context(format!(
            "Version {} failed! Failed to parse type {}. data {:?}",
            txn_version, data_type, data
        ))
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ListItemPayload {
    pub creator: String,
    pub collection_name: String,
    pub token_name: String,
    pub property_version: i64,
    price: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PlaceOrderPayload {
    pub creator: String,
    pub collection_name: String,
    price: i64,
    quantity: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PlaceBidPayload {
    pub creator: String,
    pub collection_name: String,
    pub token_name: String,
    pub property_version: i64,
    price: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum MarketplacePayload {
    ListItemPayload(ListItemPayload),
    PlaceOrderPayload(PlaceOrderPayload),
    PlaceBidPayload(PlaceBidPayload),
}

impl MarketplacePayload {
    pub fn from_function_name(
        function_name: &str,
        data: &Vec<serde_json::Value>,
        txn_version: i64,
    ) -> Result<Option<MarketplacePayload>> {
        let merged_data = Self::merge_values(data);
        match function_name {
            "marketplace-address::core::list_item" => serde_json::from_value(merged_data.clone())
                .map(|inner| Some(MarketplacePayload::ListItemPayload(inner))),
            "marketplace-address::core::place_blind_order" => {
                serde_json::from_value(merged_data.clone())
                    .map(|inner| Some(MarketplacePayload::PlaceOrderPayload(inner)))
            }
            "marketplace-address::core::place_bidding" => {
                serde_json::from_value(merged_data.clone())
                    .map(|inner| Some(MarketplacePayload::PlaceBidPayload(inner)))
            }
            _ => Ok(None),
        }
        .context(format!(
            "Version {} failed! Failed to parse function {}, data {:?}",
            txn_version, function_name, data
        ))
    }

    fn merge_values(values: &Vec<serde_json::Value>) -> serde_json::Value {
        *values
            .iter()
            .reduce(|acc, item| acc.as_object_mut().unwrap().extend(item.as_object().iter()))
            .unwrap()
    }
}
