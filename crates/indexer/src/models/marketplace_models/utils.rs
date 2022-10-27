use anyhow::{Context, Result};
use aptos_api_types::deserialize_from_string;
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;

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
        data: &Value,
        txn_version: i64,
    ) -> Result<Option<MarketplaceWriteSet>> {
        match data_type {
            "0x4bed2725cbd33afc34c556a86910456e28537ffb84df6537401c966dbaccf63b::collection::Offer" => serde_json::from_value(data.clone())
                .map(|inner| Some(MarketplaceWriteSet::Offer(inner))),
            "0x4bed2725cbd33afc34c556a86910456e28537ffb84df6537401c966dbaccf63b::collection::Order" => serde_json::from_value(data.clone())
                .map(|inner| Some(MarketplaceWriteSet::Order(inner))),
            "0x4bed2725cbd33afc34c556a86910456e28537ffb84df6537401c966dbaccf63b::collection:Bid" => serde_json::from_value(data.clone())
                .map(|inner| Some(MarketplaceWriteSet::Bid(inner))),
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
    pub fn from_event(data_type: &str, data: &Value, txn_version: i64) -> Result<Option<Self>> {
        match data_type {
            "0x4bed2725cbd33afc34c556a86910456e28537ffb84df6537401c966dbaccf63b::events::CollectionRegistrationEvent" => serde_json::from_value(data.clone())
                .map(|inner| Some(MarketplaceEvent::CollectionRegistrationEvent(inner))),
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
        data: Vec<Value>,
        txn_version: i64,
    ) -> Result<Option<MarketplacePayload>> {
        println!("{}", format!("Function name: {}", function_name));

        match function_name {
            "0x4bed2725cbd33afc34c556a86910456e28537ffb84df6537401c966dbaccf63b::core::list_item" => serde_json::from_value(merge_values_vector(data).clone())
                .map(|inner| Some(MarketplacePayload::ListItemPayload(inner))),
            "0x4bed2725cbd33afc34c556a86910456e28537ffb84df6537401c966dbaccf63b::core::place_blind_order" => serde_json::from_value(merge_values_vector(data).clone())
                .map(|inner| Some(MarketplacePayload::PlaceOrderPayload(inner))),
            "0x4bed2725cbd33afc34c556a86910456e28537ffb84df6537401c966dbaccf63b::core::place_bidding" => serde_json::from_value(merge_values_vector(data).clone())
                .map(|inner| Some(MarketplacePayload::PlaceBidPayload(inner))),
            _ => Ok(None),
        }
        .context(format!(
            "Version {} failed! Failed to parse function {}",
            txn_version, function_name
        ))
    }
}

fn merge_values_vector(values: Vec<Value>) -> Value {
    let mut values_clone = values.clone();
    let first_value = values_clone.get_mut(0).unwrap();
    for i in 1..values.clone().len() {
        merge_two_values(first_value, values[i].clone());
    }

    first_value.clone()
}

fn merge_two_values(destination: &mut Value, other: Value) {
    match (destination, other) {
        (destination @ Value::Object(_), Value::Object(other_map)) => {
            let destination_map = destination.as_object_mut().unwrap();
            for (k, v) in other_map {
                merge_two_values(destination_map.entry(k).or_insert(Value::Null), v);
            }
        }
        (destination, other) => *destination = other,
    }
}
