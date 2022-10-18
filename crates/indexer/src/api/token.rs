// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

use crate::ApiTags;
use std::sync::Arc;

use aptos_api::accept_type::AcceptType;
use aptos_api::failpoint::fail_point_poem;
use aptos_api::response::{BasicResponse, BasicResponseStatus, BasicResultWith404};
use aptos_api::Context;
use aptos_api_types::{Address, U64};
use bigdecimal::BigDecimal;
use diesel::{ExpressionMethods, QueryDsl, RunQueryDsl};
use poem_openapi::param::Path;
use poem_openapi::param::Query;
use poem_openapi::{Object, OpenApi};
use serde::{Deserialize, Serialize};

use crate::schema::current_token_ownerships;
use crate::{
    database::PgPoolConnection, models::token_models::token_ownerships::CurrentTokenOwnership,
};

pub struct TokenAPI {
    pub context: Arc<Context>,
    pub conn: PgPoolConnection,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Object)]
pub struct TokenData {
    creator_name: String,
    collection_name: String,
    token_name: String,
    property_version: BigDecimal,
    amount: BigDecimal,
}

#[OpenApi]
impl TokenAPI {
    #[oai(
        path = "/accounts/:address/tokens",
        method = "get",
        operation_id = "get_account_tokens",
        tag = "ApiTags::Tokens"
    )]
    async fn get_user_tokens(
        &self,
        accept_type: AcceptType,
        user_address: Path<Address>,
        ledger_version: Query<Option<U64>>,
    ) -> BasicResultWith404<Vec<TokenData>> {
        fail_point_poem("endpoint_get_account_resources")?;
        self.context
            .check_api_output_enabled("Get user tokens", &accept_type);
        let ownerships = current_token_ownerships::table
            .filter(current_token_ownerships::owner_address.eq(user_address.0.inner().to_hex()))
            .load::<CurrentTokenOwnership>(&mut self.conn)
            .unwrap();
        let token_datas: Vec<TokenData> = ownerships
            .iter()
            .map(|e| TokenData {
                creator_name: e.creator_address,
                collection_name: e.collection_name,
                token_name: e.name,
                property_version: e.property_version,
                amount: e.amount,
            })
            .collect();
        let (latest_ledger_version, ledger_version) = self
            .context
            .get_latest_ledger_info_and_verify_lookup_version(
                ledger_version.map(|inner| inner.0),
            )?;

        match accept_type {
            AcceptType::Json => BasicResponse::try_from_json((
                token_datas,
                &latest_ledger_version,
                BasicResponseStatus::Ok,
            )),
            AcceptType::Bcs => BasicResponse::try_from_bcs((
                token_datas,
                &latest_ledger_version,
                BasicResponseStatus::Ok,
            )),
        }
    }
}
